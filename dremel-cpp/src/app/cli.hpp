#pragma once

#include "../tests/self_test.hpp"

namespace dremel {

static std::string arg(int argc, char **argv, const std::string &name,
                       const std::string &fallback) {
  for (int i = 1; i + 1 < argc; ++i)
    if (argv[i] == name)
      return argv[i + 1];
  return fallback;
}
static bool is_relational(const Query &query) {
  return query.from.name != "events" || !query.joins.empty() || query.having ||
         query.union_query || !query.ctes.empty() ||
         std::any_of(query.select.begin(), query.select.end(),
                     [](auto &item) {
                       return contains_window(item.expr) ||
                              contains_subquery(item.expr);
                     }) ||
         contains_subquery(query.filter);
}
inline int run_cli(int argc, char **argv) {
  try {
    const std::string command = argc > 1 ? argv[1] : "help";
    if (command == "self-test")
      return self_test(argc > 2 ? argv[2] : "benchmark/queries");
    const auto path = arg(argc, argv, "--data", "data/events.csv");
    const bool direct_parquet = std::find_if(
        argv, argv + argc, [](const char *value) {
          return std::string(value) == "--direct-parquet";
        }) != argv + argc;
    const auto threads = std::stoull(arg(argc, argv, "--threads", "4"));
    const auto batch = std::stoull(arg(argc, argv, "--batch-size", "4096"));
    const auto memory_limit_mb =
        std::stoull(arg(argc, argv, "--memory-limit-mb", "0"));
    const auto query_memory_limit_mb =
        std::stoull(arg(argc, argv, "--query-memory-limit-mb", "0"));
    const auto max_result_rows =
        std::stoull(arg(argc, argv, "--max-result-rows", "0"));
    const auto max_active_queries =
        std::stoull(arg(argc, argv, "--max-active-queries", "4"));
    const auto queue_capacity =
        std::stoull(arg(argc, argv, "--queue-capacity", "128"));
    const auto scheduler_memory_mb =
        std::stoull(arg(argc, argv, "--scheduler-memory-mb", "1024"));
    auto load_start = Clock::now();
    if (direct_parquet && !path.ends_with(".parquet"))
      throw std::runtime_error("--direct-parquet requires a .parquet data file");
    auto table = direct_parquet ? parquet_metadata_table(path) : Table::load(path);
    if (memory_limit_mb &&
        table->approximate_bytes() > memory_limit_mb * 1024 * 1024)
      throw std::runtime_error(
          "RESOURCE_EXHAUSTED table requires approximately " +
          std::to_string(table->approximate_bytes()) +
          " bytes; memory limit is " + std::to_string(memory_limit_mb) +
          " MiB");
    auto load_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                       Clock::now() - load_start)
                       .count();
    ThreadPool pool(threads);
    if (command == "query") {
      auto sql = arg(argc, argv, "--sql", "SELECT COUNT(*) FROM events");
      auto q = prepare(Parser(sql).parse(), *table);
      if (q.ctes.empty())
        (void)bind_query(q);
      enforce_result_limit(max_result_rows, q, *table);
      if (direct_parquet) {
        const auto scan = parquet_scan_plan(path, q);
        q.physical.insert(
            q.physical.begin() + 1,
            "ParquetScanExec(columns=" + std::to_string(scan.columns_read) +
                "/" + std::to_string(scan.total_columns) + ";row_groups=" +
                std::to_string(scan.row_groups_read) + "/" +
                std::to_string(scan.total_row_groups) + ";rows=" +
                std::to_string(scan.rows_read) + "/" +
                std::to_string(scan.total_rows) + ";compressed_bytes=" +
                std::to_string(scan.compressed_bytes_read) + ")");
      }
      if (std::find_if(argv, argv + argc, [](const char *x) {
            return std::string(x) == "--explain";
          }) != argv + argc) {
        for (auto it = q.physical.rbegin(); it != q.physical.rend(); ++it)
          std::cout << std::string(std::distance(q.physical.rbegin(), it) * 2,
                                   ' ')
                    << *it << '\n';
      } else {
        const auto started = Clock::now();
        auto execution_table = table;
        ParquetScanMetrics scan;
        if (direct_parquet)
          std::tie(execution_table, scan) = load_parquet_direct(path, q);
        if (memory_limit_mb && execution_table->approximate_bytes() >
                                   memory_limit_mb * 1024 * 1024)
          throw std::runtime_error(
              "RESOURCE_EXHAUSTED selected Parquet columns require approximately " +
              std::to_string(execution_table->approximate_bytes()) + " bytes");
        auto memory = std::make_shared<QueryMemory>(query_memory_limit_mb);
        QueryMemoryScope memory_scope(memory);
        auto rows = is_relational(q)
                        ? execute_rel(q, Catalog::load(path, execution_table))
                        : execute(q, execution_table, pool, threads, batch);
        const auto elapsed_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() -
                                                                 started)
                .count();
        for (auto &r : rows) {
          for (auto &v : r)
            std::cout << scalar_json(v) << '\t';
          std::cout << '\n';
        }
        if (std::find_if(argv, argv + argc, [](const char *x) {
              return std::string(x) == "--stats";
            }) != argv + argc)
          std::cerr << "{\"rows_scanned\":" << execution_table->size()
                    << ",\"rows_returned\":" << rows.size()
                    << ",\"batches_scanned\":"
                    << (execution_table->size() + batch - 1) / batch
                    << ",\"columns_scanned\":" << q.columns.size()
                    << ",\"worker_threads\":" << threads
                    << ",\"logical_partitions\":" << threads * 4
                    << ",\"elapsed_ns\":" << elapsed_ns
                    << ",\"query_memory_limit_bytes\":"
                    << memory->limit_bytes()
                    << ",\"query_memory_accounted_bytes\":"
                    << memory->accounted_bytes()
                    << ",\"parquet_total_rows\":" << scan.total_rows
                    << ",\"parquet_rows_read\":" << scan.rows_read
                    << ",\"parquet_total_row_groups\":"
                    << scan.total_row_groups
                    << ",\"parquet_row_groups_read\":"
                    << scan.row_groups_read
                    << ",\"parquet_total_columns\":" << scan.total_columns
                    << ",\"parquet_columns_read\":" << scan.columns_read
                    << ",\"parquet_compressed_bytes_read\":"
                    << scan.compressed_bytes_read << "}\n";
      }
      return 0;
    }
    if (command != "bench-server") {
      std::cout << "dremel-cpp query|bench-server --data PATH --threads N "
                   "--batch-size N [--direct-parquet] [--query-memory-limit-mb N] [--sql SQL] "
                   "[--explain]\n";
      return 0;
    }
    std::unordered_map<std::string, Query> prepared;
    std::shared_ptr<Catalog> catalog;
    std::unique_ptr<AsyncScheduler> scheduler;
    std::cout << "READY\t" << load_ns << std::endl;
    std::string line;
    while (std::getline(std::cin, line)) {
      auto p = split_tabs(line);
      if (p[0] == "CONFIG")
        std::cout << "CONFIG\t" << threads << '\t' << batch << '\t'
                  << threads * 4 << '\t' << table->size();
      else if (p[0] == "PREPARE") {
        auto query = prepare(Parser(p[2]).parse(), *table);
        if (query.ctes.empty())
          (void)bind_query(query);
        enforce_result_limit(max_result_rows, query, *table);
        if (direct_parquet) {
          const auto scan = parquet_scan_plan(path, query);
          query.physical.insert(
              query.physical.begin() + 1,
              "ParquetScanExec(columns=" +
                  std::to_string(scan.columns_read) + "/" +
                  std::to_string(scan.total_columns) + ";row_groups=" +
                  std::to_string(scan.row_groups_read) + "/" +
                  std::to_string(scan.total_row_groups) + ";rows=" +
                  std::to_string(scan.rows_read) + "/" +
                  std::to_string(scan.total_rows) + ";compressed_bytes=" +
                  std::to_string(scan.compressed_bytes_read) + ")");
        }
        prepared.insert_or_assign(p[1], std::move(query));
        std::cout << "OK\t" << p[1];
      } else if (p[0] == "EXEC") {
        try {
          auto start = Clock::now();
          auto memory = std::make_shared<QueryMemory>(query_memory_limit_mb);
          QueryMemoryScope memory_scope(memory);
          const auto &query = prepared.at(p[1]);
          auto execution_table = table;
          ParquetScanMetrics scan;
          if (direct_parquet)
            std::tie(execution_table, scan) =
                load_parquet_direct(path, query);
          if (memory_limit_mb && execution_table->approximate_bytes() >
                                     memory_limit_mb * 1024 * 1024)
            throw std::runtime_error(
                "RESOURCE_EXHAUSTED selected Parquet columns require approximately " +
                std::to_string(execution_table->approximate_bytes()) +
                " bytes");
          if (is_relational(query) && !catalog && !direct_parquet)
            catalog = std::make_shared<Catalog>(Catalog::load(path, table));
          Rows rows;
          if (is_relational(query))
            rows = direct_parquet
                       ? execute_rel(query,
                                     Catalog::load(path, execution_table))
                       : execute_rel(query, *catalog);
          else
            rows = execute(query, execution_table, pool, threads, batch);
          auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        Clock::now() - start)
                        .count();
          std::cout << "RESULT\t" << ns << '\t' << rows.size() << '\t'
                    << (p[2] == "1" ? rows_json(rows) : "[]") << '\t'
                    << memory->limit_bytes() << '\t'
                    << memory->accounted_bytes() << '\t' << scan.total_rows
                    << '\t' << scan.rows_read << '\t' << scan.total_row_groups
                    << '\t' << scan.row_groups_read << '\t'
                    << scan.total_columns << '\t' << scan.columns_read << '\t'
                    << scan.compressed_bytes_read;
        } catch (const std::exception &error) {
          std::cout << "ERROR\t" << error.what();
        }
      } else if (p[0] == "E2E") {
        try {
          auto start = Clock::now();
          auto memory = std::make_shared<QueryMemory>(query_memory_limit_mb);
          QueryMemoryScope memory_scope(memory);
          auto q = prepare(Parser(p[2]).parse(), *table);
          if (q.ctes.empty())
            (void)bind_query(q);
          auto execution_table = table;
          ParquetScanMetrics scan;
          if (direct_parquet)
            std::tie(execution_table, scan) = load_parquet_direct(path, q);
          if (memory_limit_mb && execution_table->approximate_bytes() >
                                     memory_limit_mb * 1024 * 1024)
            throw std::runtime_error(
                "RESOURCE_EXHAUSTED selected Parquet columns require approximately " +
                std::to_string(execution_table->approximate_bytes()) +
                " bytes");
          if (is_relational(q) && !catalog && !direct_parquet)
            catalog = std::make_shared<Catalog>(Catalog::load(path, table));
          Rows rows;
          if (is_relational(q))
            rows = direct_parquet
                       ? execute_rel(q, Catalog::load(path, execution_table))
                       : execute_rel(q, *catalog);
          else
            rows = execute(q, execution_table, pool, threads, batch);
          auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        Clock::now() - start)
                        .count();
          std::cout << "RESULT\t" << ns << '\t' << rows.size() << '\t'
                    << (p[3] == "1" ? rows_json(rows) : "[]") << '\t'
                    << memory->limit_bytes() << '\t'
                    << memory->accounted_bytes() << '\t' << scan.total_rows
                    << '\t' << scan.rows_read << '\t' << scan.total_row_groups
                    << '\t' << scan.row_groups_read << '\t'
                    << scan.total_columns << '\t' << scan.columns_read << '\t'
                    << scan.compressed_bytes_read;
        } catch (const std::exception &error) {
          std::cout << "ERROR\t" << error.what();
        }
      } else if (p[0] == "EXPLAIN") {
        std::cout << "EXPLAIN\t" << strings_json(prepared.at(p[1]).physical);
      } else if (p[0] == "CONFIG_ASYNC") {
        std::cout << "ASYNC_CONFIG\t"
                  << std::max<std::size_t>(1, max_active_queries) << '\t'
                  << std::max<std::size_t>(1, queue_capacity) << '\t'
                  << std::max<std::size_t>(1, scheduler_memory_mb) << '\t'
                  << std::max<std::size_t>(1, (max_active_queries + 1) / 2)
                  << '\t'
                  << std::max<std::size_t>(1, (scheduler_memory_mb + 1) / 2);
      } else if (p[0] == "SUBMIT") {
        try {
          if (direct_parquet)
            throw std::runtime_error(
                "direct Parquet execution is not available for async submissions");
          if (p.size() < 7)
            throw std::runtime_error("missing SUBMIT fields");
          auto found = prepared.find(p[2]);
          if (found == prepared.end())
            throw std::runtime_error("unknown query");
          if (!catalog)
            catalog = std::make_shared<Catalog>(Catalog::load(path, table));
          if (!scheduler)
            scheduler = std::make_unique<AsyncScheduler>(
                catalog, max_active_queries, queue_capacity,
                scheduler_memory_mb);
          auto error = scheduler->submit(
              p[1], found->second, std::stoull(p[3]), p[4], std::stoull(p[5]),
              std::stoull(p[6]), p.size() > 7 && p[7] == "1");
          if (error)
            std::cout << "REJECTED\t" << *error;
          else
            std::cout << "ACCEPTED\t" << p[1];
        } catch (const std::exception &error) {
          std::cout << "REJECTED\t" << error.what();
        }
      } else if (p[0] == "POLL" || p[0] == "WAIT") {
        if (!scheduler || p.size() < 2)
          std::cout << "ERROR\tscheduler not initialized";
        else {
          std::string output;
          if (auto error = scheduler->status(p[1], p[0] == "WAIT", output))
            std::cout << "ERROR\t" << *error;
          else
            std::cout << output;
        }
      } else if (p[0] == "CANCEL") {
        if (!scheduler || p.size() < 2)
          std::cout << "ERROR\tscheduler not initialized";
        else if (auto error = scheduler->cancel(p[1]))
          std::cout << "ERROR\t" << *error;
        else
          std::cout << "CANCELLED\t" << p[1];
      } else if (p[0] == "SHUTDOWN") {
        std::cout << "BYE" << std::endl;
        break;
      } else
        std::cout << "ERROR\tunknown command";
      std::cout << std::endl;
    }
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "error: " << e.what() << '\n';
    return 1;
  }
}

} // namespace dremel
