#pragma once

#include "aggregate.hpp"

namespace dremel {

static std::string expression_key(const ExprPtr &expression) {
  if (!expression)
    return "null-ptr";
  std::ostringstream out;
  out << static_cast<int>(expression->kind) << ':' << expression->text << ':'
      << expression->integer << ':' << std::setprecision(17)
      << expression->floating << ':' << expression->boolean << '('
      << expression_key(expression->left) << ','
      << expression_key(expression->right);
  for (auto &argument : expression->args)
    out << ',' << expression_key(argument);
  for (auto &[condition, value] : expression->branches)
    out << ",when:" << expression_key(condition)
        << ",then:" << expression_key(value);
  for (auto &column : expression->partition_by)
    out << ",partition:" << column;
  for (auto &order : expression->window_order_by)
    out << ",window-order:" << order.key << ':' << order.ascending << ':'
        << (order.nulls_first ? (*order.nulls_first ? 1 : 0) : -1);
  return out.str() + ')';
}
static void collect_aggregates(const ExprPtr &expression,
                               std::vector<ExprPtr> &output) {
  if (!expression)
    return;
  if (is_agg(expression)) {
    const auto key = expression_key(expression);
    if (std::none_of(output.begin(), output.end(), [&](auto &candidate) {
          return expression_key(candidate) == key;
        }))
      output.push_back(expression);
    return;
  }
  collect_aggregates(expression->left, output);
  collect_aggregates(expression->right, output);
  for (auto &argument : expression->args)
    collect_aggregates(argument, output);
  for (auto &[condition, value] : expression->branches) {
    collect_aggregates(condition, output);
    collect_aggregates(value, output);
  }
}
static Agg aggregate_state(const ExprPtr &expression) {
  Agg state;
  state.kind = expression->text == "count" ? Agg::Kind::count
               : expression->text == "sum" ? Agg::Kind::sum
               : expression->text == "avg" ? Agg::Kind::avg
               : expression->text == "min" ? Agg::Kind::min
                                           : Agg::Kind::max;
  return state;
}
static void update_rel_aggregate(Agg &state, const ExprPtr &expression,
                                 const Catalog &catalog, const RelRow &row,
                                 const Bindings &bindings) {
  auto value = eval_rel(expression->left, catalog, row, bindings);
  if (state.kind == Agg::Kind::count) {
    if (expression->left->kind == ExprKind::star ||
        !std::holds_alternative<std::monostate>(value))
      ++state.count;
  } else if (state.kind == Agg::Kind::sum) {
    add_sum(state, value);
  } else if (state.kind == Agg::Kind::avg) {
    if (auto numeric = number(value)) {
      state.sum += *numeric;
      ++state.count;
    }
  } else if (!std::holds_alternative<std::monostate>(value) &&
             (!state.has || (state.kind == Agg::Kind::min
                                 ? compare(value, state.extreme) < 0
                                 : compare(value, state.extreme) > 0))) {
    state.extreme = std::move(value);
    state.has = true;
  }
}
static Scalar eval_group_expr(const ExprPtr &expression, const Catalog &catalog,
                              const RelRow &row, const Bindings &bindings,
                              const std::vector<ExprPtr> &aggregates,
                              const std::vector<Scalar> &values) {
  if (is_agg(expression)) {
    const auto key = expression_key(expression);
    for (std::size_t index = 0; index < aggregates.size(); ++index)
      if (expression_key(aggregates[index]) == key)
        return values[index];
    return std::monostate{};
  }
  if (!contains_agg(expression))
    return eval_rel(expression, catalog, row, bindings);
  auto evaluate = [&](const ExprPtr &value) {
    return eval_group_expr(value, catalog, row, bindings, aggregates, values);
  };
  if (expression->kind == ExprKind::binary)
    return apply_binary_value(expression->text, evaluate(expression->left),
                              evaluate(expression->right));
  if (expression->kind == ExprKind::unary) {
    auto value = evaluate(expression->left);
    if (expression->text == "not")
      return sql_bool(value) ? Scalar{!*sql_bool(value)}
                             : Scalar{std::monostate{}};
    if (auto *integer = std::get_if<std::int64_t>(&value))
      return *integer == std::numeric_limits<std::int64_t>::min()
                 ? Scalar{std::monostate{}}
                 : Scalar{-*integer};
    if (auto *decimal = std::get_if<Decimal>(&value))
      return decimal->units == std::numeric_limits<std::int64_t>::min()
                 ? Scalar{std::monostate{}}
                 : Scalar{Decimal{-decimal->units}};
    if (auto *floating = std::get_if<double>(&value))
      return -*floating;
    return std::monostate{};
  }
  if (expression->kind == ExprKind::function)
    return eval_values(expression->text, {evaluate(expression->left)});
  if (expression->kind == ExprKind::call) {
    std::vector<Scalar> arguments;
    for (auto &argument : expression->args)
      arguments.push_back(evaluate(argument));
    return eval_values(expression->text, std::move(arguments));
  }
  if (expression->kind == ExprKind::case_when) {
    for (auto &[condition, value] : expression->branches)
      if (sql_bool(evaluate(condition)) == true)
        return evaluate(value);
    return evaluate(expression->left);
  }
  if (expression->kind == ExprKind::cast)
    return cast_value(expression->text, evaluate(expression->left));
  if (expression->kind == ExprKind::is_null)
    return static_cast<bool>(
        std::holds_alternative<std::monostate>(evaluate(expression->left)) ^
        expression->boolean);
  if (expression->kind == ExprKind::between) {
    auto value = evaluate(expression->left);
    auto low = evaluate(expression->args[0]);
    auto high = evaluate(expression->args[1]);
    if (std::holds_alternative<std::monostate>(value) ||
        std::holds_alternative<std::monostate>(low) ||
        std::holds_alternative<std::monostate>(high))
      return std::monostate{};
    return static_cast<bool>(
        ((compare(value, low) >= 0 && compare(value, high) <= 0) ^
         expression->boolean));
  }
  return eval_rel(expression, catalog, row, bindings);
}
struct RelGroup {
  RelRow row;
  std::vector<Agg> states;
};
static void collect_windows(const ExprPtr &expression,
                            std::vector<ExprPtr> &output) {
  if (!expression)
    return;
  if (expression->kind == ExprKind::window) {
    const auto key = expression_key(expression);
    if (std::none_of(output.begin(), output.end(), [&](auto &candidate) {
          return expression_key(candidate) == key;
        }))
      output.push_back(expression);
    return;
  }
  collect_windows(expression->left, output);
  collect_windows(expression->right, output);
  for (auto &argument : expression->args)
    collect_windows(argument, output);
  for (auto &[condition, value] : expression->branches) {
    collect_windows(condition, output);
    collect_windows(value, output);
  }
}
struct ResolvedWindowOrder {
  std::string table;
  std::string column;
  bool ascending;
  std::optional<bool> nulls_first;
};
static int compare_rel_order(const RelRow &left, const RelRow &right,
                             const std::vector<ResolvedWindowOrder> &order,
                             const Catalog &catalog) {
  for (auto &spec : order) {
    auto left_value = relation_scalar(catalog, left, spec.table, spec.column);
    auto right_value = relation_scalar(catalog, right, spec.table, spec.column);
    const bool left_null = std::holds_alternative<std::monostate>(left_value);
    const bool right_null = std::holds_alternative<std::monostate>(right_value);
    const bool nulls_first = spec.nulls_first.value_or(!spec.ascending);
    int ordering = 0;
    if (left_null != right_null)
      ordering = left_null == nulls_first ? -1 : 1;
    else if (!left_null) {
      ordering = compare(left_value, right_value);
      if (!spec.ascending)
        ordering = -ordering;
    }
    if (ordering)
      return ordering;
  }
  return 0;
}
static std::vector<Scalar> compute_window(const ExprPtr &expression,
                                          const std::vector<RelRow> &relation,
                                          const Catalog &catalog,
                                          const Bindings &bindings) {
  account_query_memory(
      relation.size() *
          (sizeof(Scalar) + sizeof(std::size_t) + 64),
      "window partitions and values");
  std::vector<std::pair<std::string, std::string>> partition_columns;
  partition_columns.reserve(expression->partition_by.size());
  for (auto &name : expression->partition_by)
    partition_columns.push_back(resolve_column(name, bindings));
  std::vector<ResolvedWindowOrder> resolved_order;
  resolved_order.reserve(expression->window_order_by.size());
  for (auto &spec : expression->window_order_by) {
    auto [table, column] = resolve_column(spec.key, bindings);
    resolved_order.push_back({std::move(table), std::move(column),
                              spec.ascending, spec.nulls_first});
  }
  std::unordered_map<std::string, std::vector<std::size_t>> partitions;
  for (std::size_t index = 0; index < relation.size(); ++index) {
    std::string key;
    for (auto &[table, column] : partition_columns) {
      if (!key.empty())
        key.push_back('|');
      key += scalar_hash_key(
                 relation_scalar(catalog, relation[index], table, column))
                 .value_or("n:");
    }
    partitions[key].push_back(index);
  }
  std::vector<Scalar> result(relation.size(), std::monostate{});
  for (auto &[_, indices] : partitions) {
    std::sort(indices.begin(), indices.end(), [&](auto left, auto right) {
      const auto ordering = compare_rel_order(relation[left], relation[right],
                                              resolved_order, catalog);
      return ordering ? ordering < 0 : left < right;
    });
    if (expression->text == "row_number") {
      for (std::size_t position = 0; position < indices.size(); ++position)
        result[indices[position]] = static_cast<std::int64_t>(position + 1);
    } else if (expression->text == "rank" || expression->text == "dense_rank") {
      std::size_t rank = 1, dense = 1;
      for (std::size_t position = 0; position < indices.size(); ++position) {
        if (position && compare_rel_order(relation[indices[position - 1]],
                                          relation[indices[position]],
                                          resolved_order, catalog) != 0) {
          rank = position + 1;
          ++dense;
        }
        result[indices[position]] = static_cast<std::int64_t>(
            expression->text == "rank" ? rank : dense);
      }
    } else if (expression->text == "lag" || expression->text == "lead") {
      for (std::size_t position = 0; position < indices.size(); ++position) {
        const auto index = indices[position];
        std::size_t offset = 1;
        if (expression->args.size() > 1)
          if (auto value = eval_rel(expression->args[1], catalog,
                                    relation[index], bindings);
              std::holds_alternative<std::int64_t>(value) &&
              std::get<std::int64_t>(value) >= 0)
            offset = static_cast<std::size_t>(std::get<std::int64_t>(value));
        std::optional<std::size_t> target;
        if (expression->text == "lag") {
          if (position >= offset)
            target = position - offset;
        } else if (position + offset < indices.size())
          target = position + offset;
        result[index] = target ? eval_rel(expression->args[0], catalog,
                                          relation[indices[*target]], bindings)
                        : expression->args.size() > 2
                            ? eval_rel(expression->args[2], catalog,
                                       relation[index], bindings)
                            : Scalar{std::monostate{}};
      }
    } else if (expression->text == "count" || expression->text == "sum" ||
               expression->text == "avg" || expression->text == "min" ||
               expression->text == "max") {
      auto aggregate = node(ExprKind::function);
      aggregate->text = expression->text;
      aggregate->left = expression->args[0];
      auto state = aggregate_state(aggregate);
      if (expression->window_order_by.empty()) {
        for (auto index : indices)
          update_rel_aggregate(state, aggregate, catalog, relation[index],
                               bindings);
        const auto value = finish(state);
        for (auto index : indices)
          result[index] = value;
      } else {
        for (auto index : indices) {
          update_rel_aggregate(state, aggregate, catalog, relation[index],
                               bindings);
          result[index] = finish(state);
        }
      }
    }
  }
  return result;
}
static Scalar eval_window_expr(const ExprPtr &expression, std::size_t row_index,
                               const std::vector<RelRow> &relation,
                               const Catalog &catalog, const Bindings &bindings,
                               const std::vector<ExprPtr> &windows,
                               const std::vector<std::vector<Scalar>> &values) {
  if (expression->kind == ExprKind::window) {
    const auto key = expression_key(expression);
    for (std::size_t index = 0; index < windows.size(); ++index)
      if (expression_key(windows[index]) == key)
        return values[index][row_index];
    return std::monostate{};
  }
  if (!contains_window(expression))
    return eval_rel(expression, catalog, relation[row_index], bindings);
  auto evaluate = [&](const ExprPtr &value) {
    return eval_window_expr(value, row_index, relation, catalog, bindings,
                            windows, values);
  };
  if (expression->kind == ExprKind::binary)
    return apply_binary_value(expression->text, evaluate(expression->left),
                              evaluate(expression->right));
  if (expression->kind == ExprKind::unary) {
    auto value = evaluate(expression->left);
    if (expression->text == "not")
      return sql_bool(value) ? Scalar{!*sql_bool(value)}
                             : Scalar{std::monostate{}};
    if (auto *integer = std::get_if<std::int64_t>(&value))
      return *integer == std::numeric_limits<std::int64_t>::min()
                 ? Scalar{std::monostate{}}
                 : Scalar{-*integer};
    if (auto *floating = std::get_if<double>(&value))
      return -*floating;
    return std::monostate{};
  }
  if (expression->kind == ExprKind::function)
    return eval_values(expression->text, {evaluate(expression->left)});
  if (expression->kind == ExprKind::call) {
    std::vector<Scalar> arguments;
    for (auto &argument : expression->args)
      arguments.push_back(evaluate(argument));
    return eval_values(expression->text, std::move(arguments));
  }
  if (expression->kind == ExprKind::case_when) {
    for (auto &[condition, value] : expression->branches)
      if (sql_bool(evaluate(condition)) == true)
        return evaluate(value);
    return evaluate(expression->left);
  }
  if (expression->kind == ExprKind::cast)
    return cast_value(expression->text, evaluate(expression->left));
  if (expression->kind == ExprKind::is_null)
    return static_cast<bool>(
        std::holds_alternative<std::monostate>(evaluate(expression->left)) ^
        expression->boolean);
  return std::monostate{};
}
using OutputOrder =
    std::vector<std::pair<std::size_t, const OrderSpec *>>;
static OutputOrder output_order(const Query &query) {
  OutputOrder order;
  for (auto &spec : query.order_by) {
    std::size_t index = 0;
    for (std::size_t i = 0; i < query.select.size(); ++i)
      if (query.select[i].alias == std::optional<std::string>{spec.key} ||
          (query.select[i].expr->kind == ExprKind::column &&
           (query.select[i].expr->text == spec.key ||
            base_name(query.select[i].expr->text) == spec.key))) {
        index = i;
        break;
      }
    order.emplace_back(index, &spec);
  }
  return order;
}
static bool query_row_less(const std::vector<Scalar> &left,
                           const std::vector<Scalar> &right,
                           const OutputOrder &order) {
  for (auto [index, spec] : order) {
    const bool left_null =
        std::holds_alternative<std::monostate>(left[index]);
    const bool right_null =
        std::holds_alternative<std::monostate>(right[index]);
    const bool nulls_first = spec->nulls_first.value_or(!spec->ascending);
    int comparison = 0;
    if (left_null != right_null)
      comparison = left_null == nulls_first ? -1 : 1;
    else if (!left_null) {
      comparison = compare(left[index], right[index]);
      if (!spec->ascending)
        comparison = -comparison;
    }
    if (comparison != 0)
      return comparison < 0;
  }
  for (std::size_t i = 0; i < std::min(left.size(), right.size()); ++i) {
    if (left[i].index() != right[i].index())
      return left[i].index() < right[i].index();
    if (const auto secondary = compare(left[i], right[i]); secondary != 0)
      return secondary < 0;
  }
  return left.size() < right.size();
}
static void finalize_rows(const Query &query, Rows &rows) {
  if (query.distinct) {
    std::size_t distinct_bytes{};
    for (const auto &row : rows)
      distinct_bytes += row_bytes(row);
    account_query_memory(distinct_bytes, "distinct set");
    std::unordered_set<std::string> seen;
    std::erase_if(rows, [&](const auto &row) {
      std::string key;
      for (auto &value : row) {
        if (!key.empty())
          key.push_back('|');
        key += scalar_hash_key(value).value_or("n:");
      }
      return !seen.insert(std::move(key)).second;
    });
  }
  if (!query.order_by.empty()) {
    const auto order = output_order(query);
    const auto compare_rows = [&](const auto &left, const auto &right) {
      return query_row_less(left, right, order);
    };
    const auto top_k =
        std::min(rows.size(), query.optimizer_enabled && query.limit
                                  ? *query.limit + query.offset
                                  : rows.size());
    if (top_k == 0) {
      rows.clear();
    } else if (top_k < rows.size()) {
      std::nth_element(rows.begin(), rows.begin() + top_k, rows.end(),
                       compare_rows);
      rows.resize(top_k);
    }
    std::sort(rows.begin(), rows.end(), compare_rows);
  }
  if (query.offset)
    rows.erase(rows.begin(),
               rows.begin() + std::min(query.offset, rows.size()));
  if (query.limit && rows.size() > *query.limit)
    rows.resize(*query.limit);
}
struct MaterializedRelation {
  std::string name;
  std::vector<std::string> columns;
  Rows rows;
};
static std::vector<std::string> output_columns(const Query &query) {
  std::vector<std::string> columns;
  for (std::size_t index = 0; index < query.select.size(); ++index) {
    auto &item = query.select[index];
    columns.push_back(item.alias ? *item.alias
                      : item.expr->kind == ExprKind::column
                          ? base_name(item.expr->text)
                          : "column" + std::to_string(index + 1));
  }
  return columns;
}
static Scalar eval_materialized_values(const ExprPtr &expression,
                                       const std::vector<std::string> &columns,
                                       const std::vector<Scalar> &row) {
  auto evaluate = [&](const ExprPtr &value) {
    return eval_materialized_values(value, columns, row);
  };
  switch (expression->kind) {
  case ExprKind::null:
    return std::monostate{};
  case ExprKind::column: {
    const auto name = base_name(expression->text);
    auto exact = std::find(columns.begin(), columns.end(), expression->text);
    if (exact != columns.end())
      return row[std::distance(columns.begin(), exact)];
    std::optional<std::size_t> match;
    for (std::size_t index = 0; index < columns.size(); ++index)
      if (base_name(columns[index]) == name) {
        if (match)
          return std::monostate{};
        match = index;
      }
    return match ? row[*match] : Scalar{std::monostate{}};
  }
  case ExprKind::integer:
    return expression->integer;
  case ExprKind::floating:
    return expression->floating;
  case ExprKind::boolean:
    return expression->boolean;
  case ExprKind::string:
    return expression->text;
  case ExprKind::star:
    return std::int64_t{1};
  case ExprKind::unary: {
    auto value = evaluate(expression->left);
    if (expression->text == "not")
      return sql_bool(value) ? Scalar{!*sql_bool(value)}
                             : Scalar{std::monostate{}};
    if (auto *integer = std::get_if<std::int64_t>(&value))
      return *integer == std::numeric_limits<std::int64_t>::min()
                 ? Scalar{std::monostate{}}
                 : Scalar{-*integer};
    if (auto *floating = std::get_if<double>(&value))
      return -*floating;
    return std::monostate{};
  }
  case ExprKind::binary:
    return apply_binary_value(expression->text, evaluate(expression->left),
                              evaluate(expression->right));
  case ExprKind::function:
    return eval_values(expression->text, {evaluate(expression->left)});
  case ExprKind::call: {
    std::vector<Scalar> arguments;
    for (auto &argument : expression->args)
      arguments.push_back(evaluate(argument));
    return eval_values(expression->text, std::move(arguments));
  }
  case ExprKind::case_when:
    for (auto &[condition, value] : expression->branches)
      if (sql_bool(evaluate(condition)) == true)
        return evaluate(value);
    return evaluate(expression->left);
  case ExprKind::cast:
    return cast_value(expression->text, evaluate(expression->left));
  case ExprKind::is_null:
    return static_cast<bool>(
        std::holds_alternative<std::monostate>(evaluate(expression->left)) ^
        expression->boolean);
  case ExprKind::in_list: {
    auto value = evaluate(expression->left);
    if (std::holds_alternative<std::monostate>(value))
      return std::monostate{};
    bool saw_null = false;
    for (auto &candidate_expression : expression->args) {
      auto candidate = evaluate(candidate_expression);
      if (std::holds_alternative<std::monostate>(candidate))
        saw_null = true;
      else if (compare(value, candidate) == 0)
        return !expression->boolean;
    }
    return saw_null ? Scalar{std::monostate{}} : Scalar{expression->boolean};
  }
  case ExprKind::between: {
    auto value = evaluate(expression->left);
    auto low = evaluate(expression->args[0]);
    auto high = evaluate(expression->args[1]);
    if (std::holds_alternative<std::monostate>(value) ||
        std::holds_alternative<std::monostate>(low) ||
        std::holds_alternative<std::monostate>(high))
      return std::monostate{};
    return static_cast<bool>(
        ((compare(value, low) >= 0 && compare(value, high) <= 0) ^
         expression->boolean));
  }
  case ExprKind::like: {
    auto value = evaluate(expression->left);
    auto pattern = evaluate(expression->right);
    auto *text = std::get_if<std::string>(&value);
    auto *pattern_text = std::get_if<std::string>(&pattern);
    return text && pattern_text
               ? Scalar{static_cast<bool>(like_matches(*text, *pattern_text) ^
                                          expression->boolean)}
               : Scalar{std::monostate{}};
  }
  case ExprKind::dict_eq:
  case ExprKind::window:
  case ExprKind::scalar_subquery:
  case ExprKind::exists:
  case ExprKind::in_subquery:
    return std::monostate{};
  }
  return std::monostate{};
}
static Scalar eval_materialized(const ExprPtr &expression,
                                const MaterializedRelation &relation,
                                std::size_t row) {
  return eval_materialized_values(expression, relation.columns,
                                  relation.rows[row]);
}
static void update_materialized_aggregate(Agg &state, const ExprPtr &expression,
                                          const MaterializedRelation &relation,
                                          std::size_t row) {
  auto value = eval_materialized(expression->left, relation, row);
  if (state.kind == Agg::Kind::count) {
    if (expression->left->kind == ExprKind::star ||
        !std::holds_alternative<std::monostate>(value))
      ++state.count;
  } else if (state.kind == Agg::Kind::sum) {
    add_sum(state, value);
  } else if (state.kind == Agg::Kind::avg) {
    if (auto numeric = number(value)) {
      state.sum += *numeric;
      ++state.count;
    }
  } else if (!std::holds_alternative<std::monostate>(value) &&
             (!state.has || (state.kind == Agg::Kind::min
                                 ? compare(value, state.extreme) < 0
                                 : compare(value, state.extreme) > 0))) {
    state.extreme = std::move(value);
    state.has = true;
  }
}
static Scalar eval_materialized_group(const ExprPtr &expression,
                                      const MaterializedRelation &relation,
                                      std::size_t row,
                                      const std::vector<ExprPtr> &aggregates,
                                      const std::vector<Scalar> &values) {
  if (is_agg(expression)) {
    const auto key = expression_key(expression);
    for (std::size_t index = 0; index < aggregates.size(); ++index)
      if (expression_key(aggregates[index]) == key)
        return values[index];
    return std::monostate{};
  }
  if (!contains_agg(expression))
    return eval_materialized(expression, relation, row);
  auto evaluate = [&](const ExprPtr &value) {
    return eval_materialized_group(value, relation, row, aggregates, values);
  };
  if (expression->kind == ExprKind::binary)
    return apply_binary_value(expression->text, evaluate(expression->left),
                              evaluate(expression->right));
  if (expression->kind == ExprKind::call) {
    std::vector<Scalar> arguments;
    for (auto &argument : expression->args)
      arguments.push_back(evaluate(argument));
    return eval_values(expression->text, std::move(arguments));
  }
  if (expression->kind == ExprKind::case_when) {
    for (auto &[condition, value] : expression->branches)
      if (sql_bool(evaluate(condition)) == true)
        return evaluate(value);
    return evaluate(expression->left);
  }
  if (expression->kind == ExprKind::cast)
    return cast_value(expression->text, evaluate(expression->left));
  if (expression->kind == ExprKind::is_null)
    return static_cast<bool>(
        std::holds_alternative<std::monostate>(evaluate(expression->left)) ^
        expression->boolean);
  return std::monostate{};
}
static Rows execute_materialized(const Query &query,
                                 const MaterializedRelation &relation) {
  if (query.from.name != relation.name)
    throw std::runtime_error("unknown CTE " + query.from.name);
  std::vector<std::size_t> selected;
  account_query_memory(relation.rows.size() * sizeof(std::size_t),
                       "filter selection");
  for (std::size_t row = 0; row < relation.rows.size(); ++row)
    if (!query.filter || truthy(eval_materialized(query.filter, relation, row)))
      selected.push_back(row);
  const bool aggregate =
      !query.group_by.empty() || contains_agg(query.having) ||
      std::any_of(query.select.begin(), query.select.end(),
                  [](auto &item) { return contains_agg(item.expr); });
  Rows rows;
  if (aggregate) {
    std::vector<ExprPtr> aggregate_expressions;
    for (auto &item : query.select)
      collect_aggregates(item.expr, aggregate_expressions);
    collect_aggregates(query.having, aggregate_expressions);
    std::vector<Agg> templates;
    for (auto &expression : aggregate_expressions)
      templates.push_back(aggregate_state(expression));
    std::unordered_map<std::string, std::pair<std::size_t, std::vector<Agg>>>
        groups;
    if (query.group_by.empty()) {
      account_query_memory(templates.size() * sizeof(Agg) + 64,
                           "materialized hash aggregation");
      groups.emplace("", std::pair{std::size_t{0}, templates});
    }
    for (auto row : selected) {
      std::string key;
      for (auto &column_name : query.group_by) {
        auto column = node(ExprKind::column);
        column->text = column_name;
        if (!key.empty())
          key.push_back('|');
        key += scalar_hash_key(eval_materialized(column, relation, row))
                   .value_or("n:");
      }
      if (!groups.contains(key))
        account_query_memory(sizeof(std::size_t) + sizeof(std::vector<Agg>) +
                                 key.size() + templates.size() * sizeof(Agg) +
                                 64,
                             "materialized hash aggregation");
      auto [iterator, inserted] = groups.try_emplace(key, std::pair{row, templates});
      iterator->second.first = row;
      for (std::size_t index = 0; index < iterator->second.second.size();
           ++index)
        update_materialized_aggregate(iterator->second.second[index],
                                      aggregate_expressions[index], relation,
                                      row);
    }
    for (auto &[_, group] : groups) {
      std::vector<Scalar> values;
      for (auto &state : group.second)
        values.push_back(finish(state));
      if (query.having &&
          !truthy(eval_materialized_group(query.having, relation, group.first,
                                          aggregate_expressions, values)))
        continue;
      std::vector<Scalar> output;
      for (auto &item : query.select)
        output.push_back(eval_materialized_group(
            item.expr, relation, group.first, aggregate_expressions, values));
      account_query_memory(row_bytes(output), "result materialization");
      rows.push_back(std::move(output));
    }
  } else {
    for (auto row : selected) {
      std::vector<Scalar> output;
      for (auto &item : query.select)
        output.push_back(eval_materialized(item.expr, relation, row));
      account_query_memory(row_bytes(output), "result materialization");
      rows.push_back(std::move(output));
    }
  }
  finalize_rows(query, rows);
  return rows;
}
static Rows execute_rel(const Query &query, const Catalog &catalog);
static Rows execute_rel_inner(const Query &query, const Catalog &catalog,
                              bool skip_union);
static std::optional<Rows> execute_materialized_joins(const Query &query,
                                                      const Catalog &catalog) {
  const auto find_cte = [&](const TableRef &table) {
    return std::find_if(
        query.ctes.begin(), query.ctes.end(),
        [&](const auto &entry) { return entry.first == table.name; });
  };
  if (find_cte(query.from) == query.ctes.end() &&
      std::all_of(query.joins.begin(), query.joins.end(),
                  [&](const auto &join) {
                    return find_cte(join.table) == query.ctes.end();
                  }))
    return {};
  const auto materialize = [&](const TableRef &table) {
    const auto cte = find_cte(table);
    if (cte != query.ctes.end())
      return std::pair{output_columns(*cte->second),
                       execute_rel(*cte->second, catalog)};
    auto columns = table_columns(table.name);
    if (columns.empty())
      throw std::runtime_error("unknown table " + table.name);
    Rows rows;
    for (auto &row : base_relation_rows(table.name, catalog)) {
      std::vector<Scalar> values;
      for (auto &column : columns)
        values.push_back(relation_scalar(catalog, row, table.name, column));
      account_query_memory(row_bytes(values), "materialized relation");
      rows.push_back(std::move(values));
    }
    return std::pair{columns, std::move(rows)};
  };
  auto [from_columns, rows] = materialize(query.from);
  std::vector<std::string> columns;
  for (auto &column : from_columns)
    columns.push_back(query.from.alias + "." + column);
  for (auto &join : query.joins) {
    auto [raw_right_columns, right_rows] = materialize(join.table);
    std::vector<std::string> right_columns;
    for (auto &column : raw_right_columns)
      right_columns.push_back(join.table.alias + "." + column);
    auto combined_columns = columns;
    combined_columns.insert(combined_columns.end(), right_columns.begin(),
                            right_columns.end());
    const auto left_width = columns.size();
    const auto right_width = right_columns.size();
    Rows joined;
    account_query_memory(right_rows.size(), "join match bitmap");
    std::vector<bool> matched_right(right_rows.size());
    for (auto &left : rows) {
      bool matched = false;
      for (std::size_t right_index = 0; right_index < right_rows.size();
           ++right_index) {
        auto combined = left;
        combined.insert(combined.end(), right_rows[right_index].begin(),
                        right_rows[right_index].end());
        const bool matches =
            join.kind == JoinKind::cross ||
            (join.on && truthy(eval_materialized_values(
                            join.on, combined_columns, combined)));
        if (matches) {
          account_query_memory(row_bytes(combined),
                               "materialized join output");
          joined.push_back(std::move(combined));
          matched = true;
          matched_right[right_index] = true;
        }
      }
      if (!matched &&
          (join.kind == JoinKind::left || join.kind == JoinKind::full)) {
        auto combined = left;
        combined.resize(combined.size() + right_width, std::monostate{});
        account_query_memory(row_bytes(combined), "materialized join output");
        joined.push_back(std::move(combined));
      }
    }
    if (join.kind == JoinKind::right || join.kind == JoinKind::full)
      for (std::size_t right_index = 0; right_index < right_rows.size();
           ++right_index)
        if (!matched_right[right_index]) {
          std::vector<Scalar> combined(left_width, std::monostate{});
          combined.insert(combined.end(), right_rows[right_index].begin(),
                          right_rows[right_index].end());
          account_query_memory(row_bytes(combined),
                               "materialized join output");
          joined.push_back(std::move(combined));
        }
    rows = std::move(joined);
    columns = std::move(combined_columns);
  }
  MaterializedRelation relation{query.from.name, std::move(columns),
                                std::move(rows)};
  return execute_materialized(query, relation);
}
static Rows execute_rel(const Query &query, const Catalog &catalog) {
  return execute_rel_inner(query, catalog, false);
}
static Rows execute_rel_inner(const Query &query, const Catalog &catalog,
                              bool skip_union) {
  if (!query.joins.empty() && !query.ctes.empty())
    if (auto rows = execute_materialized_joins(query, catalog))
      return *std::move(rows);
  if (!query.ctes.empty()) {
    auto cte =
        std::find_if(query.ctes.begin(), query.ctes.end(), [&](auto &entry) {
          return entry.first == query.from.name;
        });
    if (cte != query.ctes.end()) {
      MaterializedRelation relation{cte->first, output_columns(*cte->second),
                                    execute_rel(*cte->second, catalog)};
      return execute_materialized(query, relation);
    }
  }
  if (!skip_union && query.union_query) {
    auto rows = execute_rel_inner(query, catalog, true);
    auto right_rows = execute_rel(*query.union_query, catalog);
    if (!rows.empty() && !right_rows.empty() &&
        rows.front().size() != right_rows.front().size())
      throw std::runtime_error("UNION inputs must have the same column count");
    rows.insert(rows.end(), std::make_move_iterator(right_rows.begin()),
                std::make_move_iterator(right_rows.end()));
    if (!query.union_all) {
      std::size_t distinct_bytes{};
      for (const auto &row : rows)
        distinct_bytes += row_bytes(row);
      account_query_memory(distinct_bytes, "distinct set");
      std::unordered_set<std::string> seen;
      std::erase_if(rows, [&](const auto &row) {
        std::string key;
        for (auto &value : row) {
          if (!key.empty())
            key.push_back('|');
          key += scalar_hash_key(value).value_or("n:");
        }
        return !seen.insert(std::move(key)).second;
      });
    }
    return rows;
  }
  const auto bindings = bind_query(query);
  auto relation = base_relation_rows(query.from.name, catalog);
  for (auto &join : query.joins) {
    relation = apply_join(std::move(relation), join, catalog, bindings,
                          query.optimizer_enabled);
    if (execution_cancelled())
      return {};
  }
  if (query.filter)
    std::erase_if(relation, [&](const RelRow &row) {
      return execution_cancelled() ||
             !truthy(eval_rel(query.filter, catalog, row, bindings));
    });
  const bool aggregate =
      !query.group_by.empty() || contains_agg(query.having) ||
      std::any_of(query.select.begin(), query.select.end(),
                  [](auto &item) { return contains_agg(item.expr); });
  const bool has_windows =
      std::any_of(query.select.begin(), query.select.end(),
                  [](auto &item) { return contains_window(item.expr); });
  Rows rows;
  if (aggregate) {
    std::vector<ExprPtr> aggregate_expressions;
    for (auto &item : query.select)
      collect_aggregates(item.expr, aggregate_expressions);
    collect_aggregates(query.having, aggregate_expressions);
    std::vector<Agg> templates;
    for (auto &expression : aggregate_expressions)
      templates.push_back(aggregate_state(expression));
    std::unordered_map<std::string, RelGroup> groups;
    if (query.group_by.empty()) {
      account_query_memory(templates.size() * sizeof(Agg) + 64,
                           "relational hash aggregation");
      groups.emplace("", RelGroup{{}, templates});
    }
    std::vector<std::pair<std::string, std::string>> group_columns;
    group_columns.reserve(query.group_by.size());
    for (auto &column : query.group_by)
      group_columns.push_back(resolve_column(column, bindings));
    for (auto &row : relation) {
      if (execution_cancelled())
        break;
      std::string key;
      for (auto &[table, column] : group_columns) {
        auto value = relation_scalar(catalog, row, table, column);
        if (!key.empty())
          key.push_back('|');
        key += scalar_hash_key(value).value_or("n:");
      }
      if (!groups.contains(key))
        account_query_memory(sizeof(RelGroup) + key.size() +
                                 templates.size() * sizeof(Agg) + 64,
                             "relational hash aggregation");
      auto [iterator, inserted] =
          groups.try_emplace(key, RelGroup{row, templates});
      iterator->second.row = row;
      for (std::size_t index = 0; index < iterator->second.states.size();
           ++index)
        update_rel_aggregate(iterator->second.states[index],
                             aggregate_expressions[index], catalog, row,
                             bindings);
    }
    for (auto &[_, group] : groups) {
      std::vector<Scalar> aggregate_values;
      for (auto &state : group.states)
        aggregate_values.push_back(finish(state));
      if (query.having &&
          !truthy(eval_group_expr(query.having, catalog, group.row, bindings,
                                  aggregate_expressions, aggregate_values)))
        continue;
      std::vector<Scalar> output;
      for (auto &item : query.select)
        output.push_back(eval_group_expr(item.expr, catalog, group.row,
                                         bindings, aggregate_expressions,
                                         aggregate_values));
      account_query_memory(row_bytes(output), "result materialization");
      rows.push_back(std::move(output));
    }
  } else if (has_windows) {
    std::vector<ExprPtr> window_expressions;
    for (auto &item : query.select)
      collect_windows(item.expr, window_expressions);
    std::vector<std::vector<Scalar>> window_values;
    for (auto &window : window_expressions)
      window_values.push_back(
          compute_window(window, relation, catalog, bindings));
    rows.reserve(relation.size());
    for (std::size_t row_index = 0; row_index < relation.size(); ++row_index) {
      std::vector<Scalar> output;
      for (auto &item : query.select)
        output.push_back(eval_window_expr(item.expr, row_index, relation,
                                          catalog, bindings, window_expressions,
                                          window_values));
      account_query_memory(row_bytes(output), "window result materialization");
      rows.push_back(std::move(output));
    }
  } else {
    const auto top_k =
        query.optimizer_enabled && query.limit && !query.order_by.empty()
            ? std::optional{*query.limit + query.offset}
            : std::optional<std::size_t>{};
    const auto order = output_order(query);
    if (top_k)
      account_query_memory(
          (*top_k + 1) *
              (sizeof(std::vector<Scalar>) +
               query.select.size() * (sizeof(Scalar) + 64)),
          "top-k buffer");
    else
      rows.reserve(relation.size());
    std::optional<std::size_t> worst_top_k;
    for (auto &row : relation) {
      std::vector<Scalar> output;
      for (auto &item : query.select)
        output.push_back(eval_rel(item.expr, catalog, row, bindings));
      if (top_k) {
        if (!*top_k)
          continue;
        if (rows.size() < *top_k) {
          rows.push_back(std::move(output));
          if (rows.size() == *top_k)
            worst_top_k = static_cast<std::size_t>(std::distance(
                rows.begin(), std::max_element(
                                  rows.begin(), rows.end(),
                                  [&](const auto &left, const auto &right) {
                                    return query_row_less(left, right, order);
                                  })));
        } else if (query_row_less(output, rows[*worst_top_k], order)) {
          rows[*worst_top_k] = std::move(output);
          worst_top_k = static_cast<std::size_t>(std::distance(
              rows.begin(), std::max_element(
                                rows.begin(), rows.end(),
                                [&](const auto &left, const auto &right) {
                                  return query_row_less(left, right, order);
                                })));
        }
      } else {
        account_query_memory(row_bytes(output), "result materialization");
        rows.push_back(std::move(output));
      }
    }
  }
  finalize_rows(query, rows);
  return rows;
}

static Query clone_query_expressions(const Query &query);
static ExprPtr clone_expression(const ExprPtr &expression) {
  if (!expression)
    return {};
  auto copy = std::make_shared<Expr>(*expression);
  copy->left = clone_expression(expression->left);
  copy->right = clone_expression(expression->right);
  copy->args.clear();
  for (auto &argument : expression->args)
    copy->args.push_back(clone_expression(argument));
  copy->branches.clear();
  for (auto &[condition, value] : expression->branches)
    copy->branches.emplace_back(clone_expression(condition),
                                clone_expression(value));
  if (expression->subquery)
    copy->subquery =
        std::make_shared<Query>(clone_query_expressions(*expression->subquery));
  return copy;
}
static Query clone_query_expressions(const Query &query) {
  auto copy = query;
  for (auto &item : copy.select)
    item.expr = clone_expression(item.expr);
  copy.filter = clone_expression(query.filter);
  for (std::size_t index = 0; index < copy.joins.size(); ++index)
    copy.joins[index].on = clone_expression(query.joins[index].on);
  copy.having = clone_expression(query.having);
  if (query.union_query)
    copy.union_query =
        std::make_shared<Query>(clone_query_expressions(*query.union_query));
  copy.ctes.clear();
  for (const auto &[name, cte] : query.ctes)
    copy.ctes.emplace_back(
        name, std::make_shared<Query>(clone_query_expressions(*cte)));
  return copy;
}
static ExprPtr scalar_expression(const Scalar &value) {
  if (std::holds_alternative<std::monostate>(value))
    return node(ExprKind::null);
  if (auto *integer = std::get_if<std::int64_t>(&value)) {
    auto expression = node(ExprKind::integer);
    expression->integer = *integer;
    return expression;
  }
  if (auto *floating = std::get_if<double>(&value)) {
    auto expression = node(ExprKind::floating);
    expression->floating = *floating;
    return expression;
  }
  if (auto *boolean = std::get_if<bool>(&value)) {
    auto expression = node(ExprKind::boolean);
    expression->boolean = *boolean;
    return expression;
  }
  if (auto *decimal = std::get_if<Decimal>(&value)) {
    auto expression = node(ExprKind::cast);
    expression->text = "decimal(18,2)";
    expression->left = node(ExprKind::string);
    expression->left->text = decimal_text(decimal->units);
    return expression;
  }
  auto expression = node(ExprKind::string);
  expression->text = std::get<std::string>(value);
  return expression;
}
static void substitute_outer_expr(ExprPtr &expression,
                                  const std::unordered_set<std::string> &local,
                                  const Catalog &catalog, const RelRow &row,
                                  const Bindings &outer_bindings) {
  if (!expression)
    return;
  if (expression->kind == ExprKind::column) {
    const auto dot = expression->text.find('.');
    if (dot != std::string::npos) {
      const auto qualifier = expression->text.substr(0, dot);
      if (!local.contains(qualifier)) {
        try {
          const auto [table, column] =
              resolve_column(expression->text, outer_bindings);
          expression =
              scalar_expression(relation_scalar(catalog, row, table, column));
          return;
        } catch (const std::exception &) {
        }
      }
    }
  }
  substitute_outer_expr(expression->left, local, catalog, row, outer_bindings);
  substitute_outer_expr(expression->right, local, catalog, row, outer_bindings);
  for (auto &argument : expression->args)
    substitute_outer_expr(argument, local, catalog, row, outer_bindings);
  for (auto &[condition, value] : expression->branches) {
    substitute_outer_expr(condition, local, catalog, row, outer_bindings);
    substitute_outer_expr(value, local, catalog, row, outer_bindings);
  }
}
static Rows execute_subquery(const Query &query, const Catalog &catalog,
                             const RelRow &row,
                             const Bindings &outer_bindings) {
  auto copy = clone_query_expressions(query);
  std::unordered_set<std::string> local{copy.from.name, copy.from.alias};
  for (auto &join : copy.joins) {
    local.insert(join.table.name);
    local.insert(join.table.alias);
  }
  for (auto &item : copy.select)
    substitute_outer_expr(item.expr, local, catalog, row, outer_bindings);
  substitute_outer_expr(copy.filter, local, catalog, row, outer_bindings);
  for (auto &join : copy.joins)
    substitute_outer_expr(join.on, local, catalog, row, outer_bindings);
  substitute_outer_expr(copy.having, local, catalog, row, outer_bindings);
  return execute_rel(copy, catalog);
}

static Rows execute(const Query &q, const std::shared_ptr<Table> &t,
                    ThreadPool &pool, std::size_t threads, std::size_t batch) {
  Rows rows;
  const bool aggregate = !q.group_by.empty() ||
                         std::any_of(q.select.begin(), q.select.end(),
                                     [](auto &s) { return is_agg(s.expr); });
  if (aggregate) {
    const auto parts = std::max<std::size_t>(1, threads) * 4;
    std::vector<std::future<GroupTable>> f;
    const auto memory = query_memory;
    for (std::size_t p = 0; p < parts; ++p) {
      auto start = p * t->size() / parts, end = (p + 1) * t->size() / parts;
      f.push_back(pool.submit([&, start, end, memory] {
        QueryMemoryScope scope(memory);
        return partition(q, *t, start, end, batch);
      }));
    }
    auto init = states(q);
    GroupTable final;
    for (auto &future : f)
      for (auto &e : future.get().entries())
        merge(final.get(e.key, init), e.states);
    for (auto &e : final.entries()) {
      std::vector<Scalar> row;
      std::size_t ai = 0;
      for (auto &s : q.select) {
        if (is_agg(s.expr))
          row.push_back(finish(e.states[ai++]));
        else {
          auto it =
              std::find(q.group_by.begin(), q.group_by.end(), s.expr->text);
          row.push_back(t->key_scalar(
              s.expr->text, e.key.v[std::distance(q.group_by.begin(), it)]));
        }
      }
      account_query_memory(row_bytes(row), "result materialization");
      rows.push_back(std::move(row));
    }
  } else {
    bool done = false;
    std::vector<std::size_t> selection;
    account_query_memory(std::max<std::size_t>(1, batch) * sizeof(std::size_t),
                         "scan selection");
    selection.reserve(std::max<std::size_t>(1, batch));
    const auto top_k = q.optimizer_enabled && q.limit && !q.order_by.empty()
                           ? std::optional{*q.limit + q.offset}
                           : std::optional<std::size_t>{};
    const auto order = output_order(q);
    if (top_k)
      account_query_memory(
          (*top_k + 1) *
              (sizeof(std::vector<Scalar>) +
               q.select.size() * (sizeof(Scalar) + 64)),
          "top-k buffer");
    std::optional<std::size_t> worst_top_k;
    for (std::size_t bs = 0; bs < t->size() && !done;
         bs += std::max<std::size_t>(1, batch)) {
      selection.clear();
      for (auto i = bs; i < std::min(t->size(), bs + batch); ++i) {
        if (!q.filter || truthy(eval(q.filter, *t, i)))
          selection.push_back(i);
      }
      for (auto i : selection) {
        std::vector<Scalar> row;
        for (auto &s : q.select)
          row.push_back(eval(s.expr, *t, i));
        if (top_k) {
          if (!*top_k)
            continue;
          if (rows.size() < *top_k) {
            rows.push_back(std::move(row));
            if (rows.size() == *top_k)
              worst_top_k = static_cast<std::size_t>(std::distance(
                  rows.begin(), std::max_element(
                                    rows.begin(), rows.end(),
                                    [&](const auto &left, const auto &right) {
                                      return query_row_less(left, right, order);
                                    })));
          } else {
            if (query_row_less(row, rows[*worst_top_k], order)) {
              rows[*worst_top_k] = std::move(row);
              worst_top_k = static_cast<std::size_t>(std::distance(
                  rows.begin(), std::max_element(
                                    rows.begin(), rows.end(),
                                    [&](const auto &left, const auto &right) {
                                      return query_row_less(left, right, order);
                                    })));
            }
          }
        } else {
          account_query_memory(row_bytes(row), "result materialization");
          rows.push_back(std::move(row));
        }
        if (q.order_by.empty() && !q.distinct && q.limit &&
            rows.size() >= *q.limit + q.offset) {
          done = true;
          break;
        }
      }
    }
  }
  finalize_rows(q, rows);
  return rows;
}

} // namespace dremel
