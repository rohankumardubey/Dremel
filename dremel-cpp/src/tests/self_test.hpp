#pragma once

#include "../nested/levels.hpp"
#include "../server/scheduler.hpp"

namespace dremel {

static int self_test(const std::string &dir) {
  auto q = Parser("SELECT event_id, duration_ms*2 AS x FROM events WHERE "
                  "campaign_id IS NOT NULL LIMIT 3")
               .parse();
  plan(q);
  assert(q.select.size() == 2 && q.limit == 3);
  const auto bind_fails = [](const std::string &sql,
                             const std::string &message) {
    try {
      const auto query = Parser(sql).parse();
      (void)bind_query(query);
      return false;
    } catch (const std::exception &error) {
      return std::string(error.what()).find(message) != std::string::npos;
    }
  };
  assert(bind_fails("SELECT event_id FROM events WHERE event_id",
                    "WHERE requires BOOLEAN"));
  assert(bind_fails("SELECT 'x' + 1 FROM events", "incompatible"));
  assert(bind_fails("SELECT user_id FROM events e JOIN users u ON e.user_id = "
                    "u.user_id",
                    "ambiguous column user_id"));
  assert(bind_fails("SELECT CAST(event_id AS uuid) FROM events",
                    "unsupported CAST type"));
  auto star_join =
      Parser("SELECT COUNT(*) FROM events e JOIN users u ON e.user_id = "
             "u.user_id JOIN campaigns c ON e.campaign_id = c.campaign_id")
          .parse();
  star_join.optimizer_enabled = true;
  assert(optimize_query(star_join, 1'000'000) == 1);
  assert(star_join.joins[0].table.name == "campaigns" &&
         star_join.joins[1].table.name == "users");
  auto dependent_join =
      Parser("SELECT COUNT(*) FROM events e JOIN users u ON e.user_id = "
             "u.user_id JOIN campaigns c ON c.campaign_id = u.user_id")
          .parse();
  dependent_join.optimizer_enabled = true;
  assert(optimize_query(dependent_join, 1'000'000) == 0);
  assert(dependent_join.joins[0].table.name == "users" &&
         dependent_join.joins[1].table.name == "campaigns");
  GroupTable g;
  std::vector<Agg> init{{Agg::Kind::count}};
  for (std::uint64_t i = 0; i < 1000; ++i) {
    Key k;
    k.n = 1;
    k.v[0] = i;
    g.get(k, init);
  }
  assert(g.size() == 1000);
  Document d{3, {{"u", {{"en", {}}, {"fr", "FR"}}}, {{}, {{"de", "DE"}}}}};
  assert(assemble(shred(d)) == d);
  auto table = std::make_shared<Table>();
  table->event_id = {1, 2, 3, 4};
  table->user_id = {10, 10, 11, 12};
  table->timestamp = {100, 101, 102, 103};
  const auto in = table->country_dict.insert("IN");
  const auto us = table->country_dict.insert("US");
  table->country = {in, us, in, us};
  const auto mobile = table->device_dict.insert("mobile");
  const auto desktop = table->device_dict.insert("desktop");
  table->device = {mobile, desktop, mobile, mobile};
  const auto view = table->event_dict.insert("view");
  const auto click = table->event_dict.insert("click");
  table->event_type = {view, click, click, view};
  table->duration = {10, 20, 30, 40};
  table->bytes = {100, 200, 300, 400};
  table->score = {1.5, 2.5, 3.5, 4.5};
  table->success = {1, 0, 1, 0};
  table->campaign = {7, 0, 9, 11};
  table->campaign_def = {1, 0, 1, 1};
  auto run = [&](const std::string &sql, std::size_t threads) {
    ThreadPool pool(threads);
    auto query = prepare(Parser(sql).parse(), *table);
    return execute(query, table, pool, threads, 2);
  };
  assert(std::get<std::int64_t>(run("SELECT COUNT(*) FROM events", 1)[0][0]) ==
         4);
  assert(std::get<std::int64_t>(
             run("SELECT COUNT(campaign_id) FROM events", 2)[0][0]) == 3);
  const auto aggregates = run(
      "SELECT SUM(bytes), AVG(duration_ms), MIN(score), MAX(score) FROM events",
      2);
  assert(std::get<std::int64_t>(aggregates[0][0]) == 1000);
  assert(std::get<double>(aggregates[0][1]) == 25.0);
  const auto projection =
      run("SELECT event_id, bytes + duration_ms AS metric FROM events WHERE "
          "country = 'IN' ORDER BY event_id DESC LIMIT 1",
          1);
  assert(std::get<std::int64_t>(projection[0][0]) == 3);
  const auto nullable_predicate =
      run("SELECT campaign_id > 1 AS present FROM events LIMIT 2", 1);
  assert(std::get<bool>(nullable_predicate[0][0]));
  assert(std::holds_alternative<std::monostate>(nullable_predicate[1][0]));
  assert(std::get<std::int64_t>(
             run("SELECT COUNT(*) FROM events WHERE NOT (country = 'IN')",
                 1)[0][0]) == 2);
  const auto scalar_case =
      run("SELECT events.event_id, CASE WHEN score BETWEEN 2.0 AND 4.0 THEN "
          "upper(country) ELSE 'other' END AS bucket FROM events ORDER BY "
          "event_id ASC",
          1);
  assert(rows_json(scalar_case) ==
         "[[{\"t\":\"i\",\"v\":1},{\"t\":\"s\",\"v\":\"other\"}],"
         "[{\"t\":\"i\",\"v\":2},{\"t\":\"s\",\"v\":\"US\"}],"
         "[{\"t\":\"i\",\"v\":3},{\"t\":\"s\",\"v\":\"IN\"}],"
         "[{\"t\":\"i\",\"v\":4},{\"t\":\"s\",\"v\":\"other\"}]]");
  const auto scalar_in =
      run("SELECT event_id FROM events WHERE country IN ('IN', 'GB') AND "
          "event_type LIKE 'cl_ck' ORDER BY event_id ASC",
          1);
  assert(rows_json(scalar_in) == "[[{\"t\":\"i\",\"v\":3}]]");
  const auto scalar_not_in = run(
      "SELECT event_id, campaign_id NOT IN (7, NULL) AS allowed FROM events "
      "ORDER BY event_id ASC",
      1);
  assert(std::get<bool>(scalar_not_in[0][1]) == false);
  assert(std::holds_alternative<std::monostate>(scalar_not_in[1][1]));
  assert(std::holds_alternative<std::monostate>(scalar_not_in[2][1]));
  const auto scalar_call =
      run("SELECT concat(lower(country), '-', cast(event_id AS varchar)) AS "
          "label FROM events WHERE event_id = 1",
          1);
  assert(std::get<std::string>(scalar_call[0][0]) == "in-1");
  Catalog catalog;
  catalog.events = table;
  catalog.users.user_id = {10, 11, 12};
  catalog.users.segment = {"pro", "free", "free"};
  catalog.users.signup_date = {"2024-01-01", "2024-01-01", "2024-01-01"};
  catalog.users.lifetime_value = {1000, 2000, 3000};
  catalog.users.region = {"apac", "apac", "apac"};
  catalog.users.active = {true, false, true};
  auto joined = prepare(
      Parser("SELECT e.event_id, u.segment FROM events e INNER JOIN users u "
             "ON e.user_id = u.user_id WHERE u.active = true ORDER BY "
             "event_id ASC")
          .parse(),
      *table);
  assert(rows_json(execute_rel(joined, catalog)) ==
         "[[{\"t\":\"i\",\"v\":1},{\"t\":\"s\",\"v\":\"pro\"}],"
         "[{\"t\":\"i\",\"v\":2},{\"t\":\"s\",\"v\":\"pro\"}],"
         "[{\"t\":\"i\",\"v\":4},{\"t\":\"s\",\"v\":\"free\"}]]");
  auto having = prepare(
      Parser("SELECT u.segment, COUNT(*) AS cnt FROM events e JOIN users u "
             "ON e.user_id = u.user_id GROUP BY u.segment HAVING COUNT(*) "
             ">= 1 ORDER BY u.segment ASC")
          .parse(),
      *table);
  assert(rows_json(execute_rel(having, catalog)) ==
         "[[{\"t\":\"s\",\"v\":\"free\"},{\"t\":\"i\",\"v\":2}],"
         "[{\"t\":\"s\",\"v\":\"pro\"},{\"t\":\"i\",\"v\":2}]]");
  auto windowed = prepare(
      Parser("SELECT event_id, ROW_NUMBER() OVER (PARTITION BY country ORDER "
             "BY score DESC) AS rn, SUM(bytes) OVER (PARTITION BY country "
             "ORDER BY event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT "
             "ROW) AS running_bytes FROM events ORDER BY event_id ASC")
          .parse(),
      *table);
  assert(
      rows_json(execute_rel(windowed, catalog)) ==
      "[[{\"t\":\"i\",\"v\":1},{\"t\":\"i\",\"v\":2},{\"t\":\"i\",\"v\":100}],"
      "[{\"t\":\"i\",\"v\":2},{\"t\":\"i\",\"v\":2},{\"t\":\"i\",\"v\":200}],"
      "[{\"t\":\"i\",\"v\":3},{\"t\":\"i\",\"v\":1},{\"t\":\"i\",\"v\":400}],"
      "[{\"t\":\"i\",\"v\":4},{\"t\":\"i\",\"v\":1},{\"t\":\"i\",\"v\":600}]]");
  auto lagged = prepare(
      Parser("SELECT event_id, LAG(bytes, 1, 0) OVER (PARTITION BY user_id "
             "ORDER BY timestamp ASC) AS previous_bytes FROM events ORDER BY "
             "event_id ASC")
          .parse(),
      *table);
  assert(rows_json(execute_rel(lagged, catalog)) ==
         "[[{\"t\":\"i\",\"v\":1},{\"t\":\"i\",\"v\":0}],"
         "[{\"t\":\"i\",\"v\":2},{\"t\":\"i\",\"v\":100}],"
         "[{\"t\":\"i\",\"v\":3},{\"t\":\"i\",\"v\":0}],"
         "[{\"t\":\"i\",\"v\":4},{\"t\":\"i\",\"v\":0}]]");
  auto cte = prepare(
      Parser("WITH totals AS (SELECT country, SUM(bytes) AS total FROM events "
             "GROUP BY country) SELECT country, total FROM totals WHERE total "
             "> 200 ORDER BY country ASC")
          .parse(),
      *table);
  assert(rows_json(execute_rel(cte, catalog)) ==
         "[[{\"t\":\"s\",\"v\":\"IN\"},{\"t\":\"i\",\"v\":400}],"
         "[{\"t\":\"s\",\"v\":\"US\"},{\"t\":\"i\",\"v\":600}]]");
  catalog.campaigns.campaign_id = {7, 9};
  catalog.campaigns.campaign_name = {"seven", "nine"};
  catalog.campaigns.budget = {7000, 9000};
  catalog.campaigns.start_date = {"2024-01-01", "2024-01-01"};
  catalog.campaigns.end_date = {"2024-02-01", "2024-02-01"};
  catalog.campaigns.channel = {"search", "search"};
  auto scalar_subquery = prepare(
      Parser("SELECT event_id, (SELECT MAX(budget) FROM campaigns) AS "
             "max_budget FROM events WHERE event_id <= 2 ORDER BY event_id ASC")
          .parse(),
      *table);
  assert(rows_json(execute_rel(scalar_subquery, catalog)) ==
         "[[{\"t\":\"i\",\"v\":1},{\"t\":\"d\",\"v\":\"90.00\"}],"
         "[{\"t\":\"i\",\"v\":2},{\"t\":\"d\",\"v\":\"90.00\"}]]");
  auto exists_subquery = prepare(
      Parser("SELECT event_id FROM events e WHERE event_id <= 4 AND EXISTS "
             "(SELECT campaign_id FROM campaigns c WHERE c.campaign_id = "
             "e.campaign_id) ORDER BY event_id ASC")
          .parse(),
      *table);
  assert(rows_json(execute_rel(exists_subquery, catalog)) ==
         "[[{\"t\":\"i\",\"v\":1}],[{\"t\":\"i\",\"v\":3}]]");
  auto in_subquery =
      prepare(Parser("SELECT event_id FROM events WHERE campaign_id IN (SELECT "
                     "campaign_id FROM campaigns) ORDER BY event_id ASC")
                  .parse(),
              *table);
  assert(rows_json(execute_rel(in_subquery, catalog)) ==
         "[[{\"t\":\"i\",\"v\":1}],[{\"t\":\"i\",\"v\":3}]]");
  const std::string grouped =
      "SELECT country, device, COUNT(*) AS cnt FROM events GROUP BY country, "
      "device ORDER BY country ASC";
  assert(rows_json(run(grouped, 1)) == rows_json(run(grouped, 3)));
  for (int i = 1; i <= 64; ++i) {
    std::ostringstream p;
    p << dir << "/Q" << std::setw(3) << std::setfill('0') << i << ".sql";
    std::ifstream f(p.str());
    std::stringstream s;
    s << f.rdbuf();
    auto x = Parser(s.str()).parse();
    plan(x);
  }
  std::cout << "all C++ tests passed\n";
  return 0;
}

} // namespace dremel
