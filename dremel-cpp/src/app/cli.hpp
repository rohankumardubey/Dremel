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
static void add_parquet_plan(Query &query, const std::string &path,
                             bool streaming, std::size_t batch) {
  const auto scan = parquet_scan_plan(path, query);
  query.physical.insert(
      query.physical.begin() + 1,
      "ParquetScanExec(columns=" + std::to_string(scan.columns_read) + "/" +
          std::to_string(scan.total_columns) + ";row_groups=" +
          std::to_string(scan.row_groups_read) + "/" +
          std::to_string(scan.total_row_groups) + ";rows=" +
          std::to_string(scan.rows_read) + "/" +
          std::to_string(scan.total_rows) + ";compressed_bytes=" +
          std::to_string(scan.compressed_bytes_read) + ")");
  if (streaming)
    query.physical.insert(
        query.physical.begin() + 2,
        "ParquetStreamExec(batch_size=" + std::to_string(batch) +
            ";fallback=" +
            (parquet_streaming_fallback(query) ? "true" : "false") + ")");
}
static void add_spill_plan(Query &query, const std::string &spill_dir,
                           bool streaming_parquet) {
  if (!spill_dir.empty() && !streaming_parquet && spillable_aggregate(query))
    query.physical.insert(query.physical.begin() + 1,
                          "SpillAggregateExec(partitions=auto)");
}
static void add_result_stream_plan(Query &query, bool enabled,
                                   bool parquet_query, bool ordered_parquet,
                                   std::size_t batch) {
  if (!enabled)
    return;
  const auto position = ordered_parquet ? 4 : parquet_query ? 2 : 1;
  query.physical.insert(query.physical.begin() + position,
                        "ResultStreamExec(batch_size=" +
                            std::to_string(batch) + ")");
}
static void add_external_sort_plan(Query &query, bool enabled,
                                   bool streaming_parquet, std::size_t batch) {
  if (!enabled)
    return;
  std::erase_if(query.physical, [](const auto &operator_name) {
    return operator_name.starts_with("SortExec") ||
           operator_name.starts_with("TopKExec");
  });
  query.physical.insert(
      query.physical.begin() + (streaming_parquet ? 3 : 1),
      "ExternalMergeSortExec(runs=auto;batch_size=" +
          std::to_string(batch) + ")");
}
static std::tuple<Rows, ParquetScanMetrics, SpillMetrics> execute_prepared(
    const Query &query, const std::string &path,
    const std::shared_ptr<Table> &table, ThreadPool &pool, std::size_t threads,
    std::size_t batch, std::size_t memory_limit_mb, bool direct_parquet,
    bool streaming_parquet, const std::string &spill_dir,
    std::shared_ptr<Catalog> &catalog) {
  if (streaming_parquet) {
    auto [rows, scan] = execute_parquet_stream(query, path, pool, threads,
                                               batch, memory_limit_mb);
    return {std::move(rows), std::move(scan), SpillMetrics{}};
  }
  auto execution_table = table;
  ParquetScanMetrics scan;
  if (direct_parquet)
    std::tie(execution_table, scan) = load_parquet_direct(path, query, batch);
  if (memory_limit_mb && execution_table->approximate_bytes() >
                             memory_limit_mb * 1024 * 1024)
    throw std::runtime_error(
        "RESOURCE_EXHAUSTED selected Parquet columns require approximately " +
        std::to_string(execution_table->approximate_bytes()) + " bytes");
  if (is_relational(query)) {
    if (direct_parquet)
      return {execute_rel(query, Catalog::load(path, execution_table)), scan,
              SpillMetrics{}};
    if (!catalog)
      catalog = std::make_shared<Catalog>(Catalog::load(path, table));
    return {execute_rel(query, *catalog), scan, SpillMetrics{}};
  }
  auto [rows, spill] = execute_with_spill(
      query, execution_table, pool, threads, batch, spill_dir);
  return {std::move(rows), scan, spill};
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
    const bool streaming_parquet = std::find_if(
        argv, argv + argc, [](const char *value) {
          return std::string(value) == "--streaming-parquet";
        }) != argv + argc;
    const bool stream_results = std::find_if(
        argv, argv + argc, [](const char *value) {
          return std::string(value) == "--stream-results";
        }) != argv + argc;
    if (direct_parquet && streaming_parquet)
      throw std::runtime_error(
          "choose either --direct-parquet or --streaming-parquet");
    const bool parquet_query = direct_parquet || streaming_parquet;
    const auto threads = std::stoull(arg(argc, argv, "--threads", "4"));
    const auto batch = std::stoull(arg(argc, argv, "--batch-size", "4096"));
    const auto memory_limit_mb =
        std::stoull(arg(argc, argv, "--memory-limit-mb", "0"));
    const auto query_memory_limit_mb =
        std::stoull(arg(argc, argv, "--query-memory-limit-mb", "0"));
    const auto spill_dir = arg(argc, argv, "--spill-dir", "");
    const auto max_result_rows =
        std::stoull(arg(argc, argv, "--max-result-rows", "0"));
    const auto max_active_queries =
        std::stoull(arg(argc, argv, "--max-active-queries", "4"));
    const auto queue_capacity =
        std::stoull(arg(argc, argv, "--queue-capacity", "128"));
    const auto scheduler_memory_mb =
        std::stoull(arg(argc, argv, "--scheduler-memory-mb", "1024"));
    auto load_start = Clock::now();
    if (parquet_query && !path.ends_with(".parquet"))
      throw std::runtime_error(
          "direct and streaming Parquet execution require a .parquet data file");
    auto table = parquet_query ? parquet_metadata_table(path) : Table::load(path);
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
      if (parquet_query)
        add_parquet_plan(q, path, streaming_parquet, batch);
      add_spill_plan(q, spill_dir, streaming_parquet);
      const bool external_sort =
          stream_results && !direct_parquet && spillable_sort(q) &&
          !spill_dir.empty() && query_memory_limit_mb > 0;
      if (stream_results && spillable_sort(q) && !external_sort)
        throw std::runtime_error(
            "EXTERNAL_SORT_REQUIRES ordered result streaming requires native "
            "data or --streaming-parquet, --spill-dir, and a nonzero --query-memory-limit-mb");
      if (stream_results && !streamable_result(q) && !external_sort)
        throw std::runtime_error(
            "STREAMING_UNSUPPORTED result streaming requires a single events "
            "scan with projection/filter and no DISTINCT, aggregation, ORDER "
            "BY, joins, windows, CTEs, unions, or subqueries");
      add_external_sort_plan(q, external_sort, streaming_parquet, batch);
      add_result_stream_plan(q, stream_results, parquet_query,
                             streaming_parquet && external_sort, batch);
      if (std::find_if(argv, argv + argc, [](const char *x) {
            return std::string(x) == "--explain";
          }) != argv + argc) {
        auto physical = q.physical;
        physical.front() += " batch_size=" + std::to_string(batch);
        for (auto it = physical.rbegin(); it != physical.rend(); ++it)
          std::cout << std::string(std::distance(physical.rbegin(), it) * 2,
                                   ' ')
                    << *it << '\n';
      } else {
        const auto started = Clock::now();
        auto memory = std::make_shared<QueryMemory>(query_memory_limit_mb);
        QueryMemoryScope memory_scope(memory);
        std::shared_ptr<Catalog> query_catalog;
        Rows rows;
        ParquetScanMetrics scan;
        SpillMetrics spill;
        ResultStreamMetrics stream;
        auto execution_table = table;
        if (stream_results) {
          StreamMemoryReservation output_buffer(8192,
                                                "streaming output buffer");
          auto sink = [&](const std::vector<Scalar> &row) {
            auto line = row_json(row) + '\n';
            StreamMemoryReservation output_memory(
                line.size(), "streaming output encoding");
            std::cout.write(line.data(), static_cast<std::streamsize>(line.size()));
            if (!std::cout)
              throw std::runtime_error("cannot write streaming result");
            return line.size();
          };
          if (external_sort) {
            if (streaming_parquet)
              std::tie(stream, scan, spill) = stream_external_sort_parquet(
                  q, path, batch, memory_limit_mb, spill_dir, sink);
            else
              std::tie(stream, spill) = stream_external_sort(
                  q, *table, batch, spill_dir, sink);
          } else if (streaming_parquet) {
            std::tie(stream, scan) = stream_parquet_results(
                q, path, batch, memory_limit_mb, sink);
          } else {
            if (direct_parquet)
              std::tie(execution_table, scan) =
                  load_parquet_direct(path, q, batch);
            if (memory_limit_mb &&
                execution_table->approximate_bytes() >
                    memory_limit_mb * 1024 * 1024)
              throw std::runtime_error(
                  "RESOURCE_EXHAUSTED selected columns require approximately " +
                  std::to_string(execution_table->approximate_bytes()) +
                  " bytes");
            stream = stream_table_results(q, *execution_table, batch, sink);
          }
          std::cout.flush();
          if (!std::cout)
            throw std::runtime_error("cannot flush streaming result");
        } else {
          std::tie(rows, scan, spill) = execute_prepared(
              q, path, table, pool, threads, batch, memory_limit_mb,
              direct_parquet, streaming_parquet, spill_dir, query_catalog);
        }
        const auto elapsed_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() -
                                                                 started)
                .count();
        if (!stream_results)
          for (const auto &row : rows)
            std::cout << row_json(row) << '\n';
        if (std::find_if(argv, argv + argc, [](const char *x) {
              return std::string(x) == "--stats";
            }) != argv + argc)
          std::cerr << "{\"rows_scanned\":"
                    << (parquet_query ? scan.rows_read : table->size())
                    << ",\"rows_returned\":"
                    << (stream_results ? stream.rows_returned : rows.size())
                    << ",\"batches_scanned\":"
                    << (parquet_query ? scan.batches_read
                                      : stream_results
                                            ? stream.batches_scanned
                                            : (table->size() + batch - 1) / batch)
                    << ",\"columns_scanned\":" << q.columns.size()
                    << ",\"worker_threads\":" << threads
                    << ",\"logical_partitions\":" << threads * 4
                    << ",\"elapsed_ns\":" << elapsed_ns
                    << ",\"query_memory_limit_bytes\":"
                    << memory->limit_bytes()
                    << ",\"query_memory_accounted_bytes\":"
                    << memory->accounted_bytes()
                    << ",\"query_memory_peak_bytes\":"
                    << memory->peak_accounted_bytes()
                    << ",\"parquet_total_rows\":" << scan.total_rows
                    << ",\"parquet_rows_read\":" << scan.rows_read
                    << ",\"parquet_total_row_groups\":"
                    << scan.total_row_groups
                    << ",\"parquet_row_groups_read\":"
                    << scan.row_groups_read
                    << ",\"parquet_total_columns\":" << scan.total_columns
                    << ",\"parquet_columns_read\":" << scan.columns_read
                    << ",\"parquet_compressed_bytes_read\":"
                    << scan.compressed_bytes_read
                    << ",\"parquet_batches_read\":" << scan.batches_read
                    << ",\"parquet_peak_decoded_batch_bytes\":"
                    << scan.peak_decoded_batch_bytes
                    << ",\"parquet_streaming_fallback\":"
                    << (scan.streaming_fallback ? "true" : "false")
                    << ",\"spill_files_created\":" << spill.files_created
                    << ",\"spill_partitions\":" << spill.partitions
                    << ",\"spill_bytes_written\":" << spill.bytes_written
                    << ",\"spill_bytes_read\":" << spill.bytes_read
                    << ",\"spill_passes\":" << spill.passes
                    << ",\"spilled\":" << (spill.spilled() ? "true" : "false")
                    << ",\"result_streamed\":"
                    << (stream_results ? "true" : "false")
                    << ",\"result_output_bytes\":" << stream.output_bytes
                    << ",\"result_batches\":" << stream.batches_scanned
                    << "}\n";
      }
      return 0;
    }
    if (command != "bench-server") {
      std::cout << "dremel-cpp query|bench-server --data PATH --threads N "
                   "--batch-size N [--direct-parquet|--streaming-parquet] [--query-memory-limit-mb N] [--spill-dir PATH] [--stream-results] [--sql SQL] "
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
        if (parquet_query)
          add_parquet_plan(query, path, streaming_parquet, batch);
        add_spill_plan(query, spill_dir, streaming_parquet);
        prepared.insert_or_assign(p[1], std::move(query));
        std::cout << "OK\t" << p[1];
      } else if (p[0] == "EXEC") {
        try {
          auto start = Clock::now();
          auto memory = std::make_shared<QueryMemory>(query_memory_limit_mb);
          QueryMemoryScope memory_scope(memory);
          const auto &query = prepared.at(p[1]);
          auto [rows, scan, spill] = execute_prepared(
              query, path, table, pool, threads, batch, memory_limit_mb,
              direct_parquet, streaming_parquet, spill_dir, catalog);
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
                    << scan.compressed_bytes_read << '\t' << scan.batches_read
                    << '\t' << scan.peak_decoded_batch_bytes << '\t'
                    << scan.streaming_fallback << '\t'
                    << memory->peak_accounted_bytes() << '\t'
                    << spill.files_created << '\t' << spill.partitions << '\t'
                    << spill.bytes_written << '\t' << spill.bytes_read << '\t'
                    << spill.passes << '\t' << spill.spilled();
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
          if (parquet_query)
            add_parquet_plan(q, path, streaming_parquet, batch);
          add_spill_plan(q, spill_dir, streaming_parquet);
          auto [rows, scan, spill] = execute_prepared(
              q, path, table, pool, threads, batch, memory_limit_mb,
              direct_parquet, streaming_parquet, spill_dir, catalog);
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
                    << scan.compressed_bytes_read << '\t' << scan.batches_read
                    << '\t' << scan.peak_decoded_batch_bytes << '\t'
                    << scan.streaming_fallback << '\t'
                    << memory->peak_accounted_bytes() << '\t'
                    << spill.files_created << '\t' << spill.partitions << '\t'
                    << spill.bytes_written << '\t' << spill.bytes_read << '\t'
                    << spill.passes << '\t' << spill.spilled();
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
          if (parquet_query)
            throw std::runtime_error(
                "Parquet query execution is not available for async submissions");
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
