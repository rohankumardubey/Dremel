#pragma once

#include "../execution/scalar.hpp"

namespace dremel {

static std::optional<Scalar> literal_value(const ExprPtr &expression) {
  if (!expression)
    return {};
  switch (expression->kind) {
  case ExprKind::null:
    return Scalar{std::monostate{}};
  case ExprKind::integer:
    return Scalar{expression->integer};
  case ExprKind::floating:
    return Scalar{expression->floating};
  case ExprKind::boolean:
    return Scalar{expression->boolean};
  case ExprKind::string:
    return Scalar{expression->text};
  default:
    return {};
  }
}
static ExprPtr literal_expression(const Scalar &value) {
  if (std::holds_alternative<std::monostate>(value))
    return node(ExprKind::null);
  if (const auto *integer = std::get_if<std::int64_t>(&value)) {
    auto expression = node(ExprKind::integer);
    expression->integer = *integer;
    return expression;
  }
  if (const auto *decimal = std::get_if<Decimal>(&value)) {
    auto expression = node(ExprKind::cast);
    expression->text = "decimal(18,2)";
    expression->left = node(ExprKind::string);
    expression->left->text = decimal_text(decimal->units);
    return expression;
  }
  if (const auto *floating = std::get_if<double>(&value)) {
    auto expression = node(ExprKind::floating);
    expression->floating = *floating;
    return expression;
  }
  if (const auto *boolean = std::get_if<bool>(&value)) {
    auto expression = node(ExprKind::boolean);
    expression->boolean = *boolean;
    return expression;
  }
  auto expression = node(ExprKind::string);
  expression->text = std::get<std::string>(value);
  return expression;
}

struct PushedFilter {
  std::string table;
  ExprPtr expression;
  bool derived{};
};

static void split_conjuncts(const ExprPtr &expression,
                            std::vector<ExprPtr> &output) {
  if (expression && expression->kind == ExprKind::binary &&
      expression->text == "and") {
    split_conjuncts(expression->left, output);
    split_conjuncts(expression->right, output);
  } else if (expression)
    output.push_back(expression);
}

static bool expression_relations(const ExprPtr &expression,
                                 const Bindings &bindings,
                                 std::set<std::string> &output) {
  if (!expression)
    return true;
  if (expression->kind == ExprKind::column ||
      expression->kind == ExprKind::dict_eq) {
    try {
      output.insert(resolve_column(expression->text, bindings).first);
      return true;
    } catch (const std::exception &) {
      return false;
    }
  }
  if (expression->kind == ExprKind::window || expression->subquery)
    return false;
  if (!expression_relations(expression->left, bindings, output) ||
      !expression_relations(expression->right, bindings, output))
    return false;
  for (auto &argument : expression->args)
    if (!expression_relations(argument, bindings, output))
      return false;
  for (auto &[condition, value] : expression->branches)
    if (!expression_relations(condition, bindings, output) ||
        !expression_relations(value, bindings, output))
      return false;
  return true;
}

struct ColumnLiteralFilter {
  std::string column;
  std::string operation;
  ExprPtr literal;
  bool column_on_left{};
};

static std::optional<ColumnLiteralFilter>
column_literal_filter(const ExprPtr &expression) {
  if (!expression || expression->kind != ExprKind::binary)
    return {};
  if (expression->left && expression->left->kind == ExprKind::column &&
      literal_value(expression->right))
    return ColumnLiteralFilter{expression->left->text, expression->text,
                               expression->right, true};
  if (expression->right && expression->right->kind == ExprKind::column &&
      literal_value(expression->left))
    return ColumnLiteralFilter{expression->right->text, expression->text,
                               expression->left, false};
  return {};
}

static ExprPtr equivalent_filter(const ExprPtr &expression,
                                 const std::string &from,
                                 const std::string &to) {
  const auto filter = column_literal_filter(expression);
  if (!filter || filter->column != from)
    return {};
  auto replacement = node(ExprKind::binary);
  replacement->text = filter->operation;
  auto column = node(ExprKind::column);
  column->text = to;
  if (filter->column_on_left) {
    replacement->left = std::move(column);
    replacement->right = filter->literal;
  } else {
    replacement->left = filter->literal;
    replacement->right = std::move(column);
  }
  return replacement;
}

static std::string filter_signature(const ExprPtr &expression) {
  if (!expression)
    return "null";
  std::ostringstream output;
  output << static_cast<int>(expression->kind) << ':' << expression->text
         << ':' << expression->integer << ':' << std::setprecision(17)
         << expression->floating << ':' << expression->boolean << '('
         << filter_signature(expression->left) << ','
         << filter_signature(expression->right);
  for (auto &argument : expression->args)
    output << ',' << filter_signature(argument);
  return output.str() + ')';
}

static std::vector<PushedFilter> pushed_filters(const Query &query) {
  if (!query.optimizer_enabled)
    return {};
  if (std::any_of(query.joins.begin(), query.joins.end(), [](const auto &join) {
        return join.kind != JoinKind::inner && join.kind != JoinKind::cross;
      }))
    return {};
  Bindings bindings;
  try {
    bindings = query_bindings(query);
  } catch (const std::exception &) {
    return {};
  }
  std::vector<PushedFilter> output;
  std::vector<ExprPtr> conjuncts;
  split_conjuncts(query.filter, conjuncts);
  for (auto &expression : conjuncts) {
    std::set<std::string> relations;
    if (expression_relations(expression, bindings, relations) &&
        relations.size() == 1)
      output.push_back({*relations.begin(), expression, false});
  }
  const auto seeds = output;
  for (auto &join : query.joins) {
    if (join.kind != JoinKind::inner || !join.on ||
        join.on->kind != ExprKind::binary || join.on->text != "=" ||
        !join.on->left || !join.on->right ||
        join.on->left->kind != ExprKind::column ||
        join.on->right->kind != ExprKind::column)
      continue;
    for (auto &seed : seeds)
      for (const auto &[from, to] :
           std::array<std::pair<std::string, std::string>, 2>{
               std::pair{join.on->left->text, join.on->right->text},
               std::pair{join.on->right->text, join.on->left->text}}) {
        auto expression = equivalent_filter(seed.expression, from, to);
        if (!expression)
          continue;
        std::string table;
        try {
          table = resolve_column(to, bindings).first;
        } catch (const std::exception &) {
          continue;
        }
        const auto signature = table + ':' + filter_signature(expression);
        if (std::any_of(output.begin(), output.end(), [&](const auto &filter) {
              return filter.table + ':' + filter_signature(filter.expression) ==
                     signature;
            }))
          continue;
        output.push_back({std::move(table), std::move(expression), true});
      }
  }
  return output;
}

static ExprPtr residual_filter(const Query &query) {
  std::set<std::string> pushed;
  for (auto &filter : pushed_filters(query))
    if (!filter.derived)
      pushed.insert(filter_signature(filter.expression));
  if (pushed.empty())
    return query.filter;
  std::vector<ExprPtr> conjuncts;
  split_conjuncts(query.filter, conjuncts);
  std::erase_if(conjuncts, [&](const auto &expression) {
    return pushed.contains(filter_signature(expression));
  });
  if (conjuncts.empty())
    return {};
  auto residual = conjuncts.front();
  for (std::size_t index = 1; index < conjuncts.size(); ++index) {
    auto conjunction = node(ExprKind::binary);
    conjunction->text = "and";
    conjunction->left = std::move(residual);
    conjunction->right = conjuncts[index];
    residual = std::move(conjunction);
  }
  return residual;
}

static bool filter_always_false(const ExprPtr &filter) {
  return filter &&
         (filter->kind == ExprKind::null ||
          (filter->kind == ExprKind::boolean && !filter->boolean));
}

struct ComparisonConstraint {
  std::string column;
  std::string operation;
  std::int64_t value{};
};

static std::optional<ComparisonConstraint>
comparison_constraint(const ExprPtr &expression) {
  if (!expression || expression->kind != ExprKind::binary)
    return {};
  if (expression->left && expression->right &&
      expression->left->kind == ExprKind::column &&
      expression->right->kind == ExprKind::integer)
    return ComparisonConstraint{expression->left->text, expression->text,
                                expression->right->integer};
  if (expression->left && expression->right &&
      expression->left->kind == ExprKind::integer &&
      expression->right->kind == ExprKind::column) {
    auto operation = expression->text;
    if (operation == "<")
      operation = ">";
    else if (operation == "<=")
      operation = ">=";
    else if (operation == ">")
      operation = "<";
    else if (operation == ">=")
      operation = "<=";
    return ComparisonConstraint{expression->right->text, operation,
                                expression->left->integer};
  }
  return {};
}

static bool contradictory_filter(const ExprPtr &expression) {
  struct Bounds {
    std::optional<std::int64_t> equal;
    std::optional<std::pair<std::int64_t, bool>> lower, upper;
  };
  std::vector<ExprPtr> conjuncts;
  split_conjuncts(expression, conjuncts);
  if (std::any_of(conjuncts.begin(), conjuncts.end(),
                  [](const auto &value) { return filter_always_false(value); }))
    return true;
  std::unordered_map<std::string, Bounds> columns;
  for (auto &conjunct : conjuncts) {
    const auto constraint = comparison_constraint(conjunct);
    if (!constraint)
      continue;
    auto &bounds = columns[constraint->column];
    if (constraint->operation == "=") {
      if (bounds.equal && *bounds.equal != constraint->value)
        return true;
      bounds.equal = constraint->value;
    } else if (constraint->operation == ">" ||
               constraint->operation == ">=") {
      const auto candidate =
          std::pair{constraint->value, constraint->operation == ">="};
      if (!bounds.lower || candidate.first > bounds.lower->first ||
          (candidate.first == bounds.lower->first && !candidate.second &&
           bounds.lower->second))
        bounds.lower = candidate;
    } else if (constraint->operation == "<" ||
               constraint->operation == "<=") {
      const auto candidate =
          std::pair{constraint->value, constraint->operation == "<="};
      if (!bounds.upper || candidate.first < bounds.upper->first ||
          (candidate.first == bounds.upper->first && !candidate.second &&
           bounds.upper->second))
        bounds.upper = candidate;
    }
  }
  for (auto &[_, bounds] : columns) {
    if (bounds.equal &&
        ((bounds.lower &&
          (*bounds.equal < bounds.lower->first ||
           (*bounds.equal == bounds.lower->first && !bounds.lower->second))) ||
         (bounds.upper &&
          (*bounds.equal > bounds.upper->first ||
           (*bounds.equal == bounds.upper->first && !bounds.upper->second)))))
      return true;
    if (bounds.lower && bounds.upper &&
        (bounds.lower->first > bounds.upper->first ||
         (bounds.lower->first == bounds.upper->first &&
          !(bounds.lower->second && bounds.upper->second))))
      return true;
  }
  return false;
}

static std::size_t predicate_rank(const ExprPtr &expression) {
  if (!expression)
    return 3;
  if (expression->kind == ExprKind::dict_eq)
    return 0;
  if (expression->kind == ExprKind::binary &&
      (literal_value(expression->left) || literal_value(expression->right)) &&
      (expression->text == "=" || expression->text == "!=" ||
       expression->text == "<" || expression->text == "<=" ||
       expression->text == ">" || expression->text == ">=")) {
    const auto non_literal = literal_value(expression->left)
                                 ? expression->right
                                 : expression->left;
    return non_literal && non_literal->kind == ExprKind::column ? 1 : 4;
  }
  if (expression->kind == ExprKind::is_null ||
      expression->kind == ExprKind::between ||
      expression->kind == ExprKind::in_list)
    return 2;
  if (expression->kind == ExprKind::like ||
      expression->kind == ExprKind::function ||
      expression->kind == ExprKind::call)
    return 4;
  return 3;
}

static bool reorder_conjuncts(ExprPtr &filter) {
  std::vector<ExprPtr> conjuncts;
  split_conjuncts(filter, conjuncts);
  if (conjuncts.size() < 2)
    return false;
  const auto original = filter_signature(filter);
  std::stable_sort(conjuncts.begin(), conjuncts.end(), [](const auto &left,
                                                          const auto &right) {
    return predicate_rank(left) < predicate_rank(right);
  });
  auto rebuilt = conjuncts.front();
  for (std::size_t index = 1; index < conjuncts.size(); ++index) {
    auto conjunction = node(ExprKind::binary);
    conjunction->text = "and";
    conjunction->left = std::move(rebuilt);
    conjunction->right = conjuncts[index];
    rebuilt = std::move(conjunction);
  }
  const bool changed = original != filter_signature(rebuilt);
  filter = std::move(rebuilt);
  return changed;
}

static std::size_t estimated_filtered_rows(
    const std::string &table, const std::vector<PushedFilter> &filters,
    std::size_t event_rows) {
  std::size_t estimate = table == "events"      ? event_rows
                         : table == "users"     ? 250'000
                         : table == "campaigns" ? 5'000
                                                : 1'000;
  for (auto &pushed : filters) {
    if (pushed.table != table)
      continue;
    const auto filter = column_literal_filter(pushed.expression);
    if (filter && filter->literal->kind == ExprKind::integer &&
        base_name(filter->column).ends_with("_id")) {
      auto operation = filter->operation;
      if (!filter->column_on_left) {
        if (operation == "<")
          operation = ">";
        else if (operation == "<=")
          operation = ">=";
        else if (operation == ">")
          operation = "<";
        else if (operation == ">=")
          operation = "<=";
      }
      const auto value = filter->literal->integer;
      if (operation == "=")
        estimate = 1;
      else if (operation == "<")
        estimate = std::min(
            estimate,
            static_cast<std::size_t>(std::max<std::int64_t>(0, value)));
      else if (operation == "<=") {
        const auto inclusive =
            value == std::numeric_limits<std::int64_t>::max() ? value
                                                               : value + 1;
        estimate = std::min(
            estimate,
            static_cast<std::size_t>(std::max<std::int64_t>(0, inclusive)));
      } else if (operation == ">" || operation == ">=")
        estimate = (estimate + 1) / 2;
      else
        estimate = (estimate + 9) / 10;
      continue;
    }
    estimate = (estimate + 9) / 10;
  }
  return std::max<std::size_t>(1, estimate);
}
static std::size_t optimize_expression(ExprPtr &expression) {
  if (!expression)
    return 0;
  std::size_t rewrites{};
  rewrites += optimize_expression(expression->left);
  rewrites += optimize_expression(expression->right);
  for (auto &argument : expression->args)
    rewrites += optimize_expression(argument);
  for (auto &[condition, value] : expression->branches) {
    rewrites += optimize_expression(condition);
    rewrites += optimize_expression(value);
  }
  ExprPtr replacement;
  if (expression->kind == ExprKind::unary) {
    if (auto value = literal_value(expression->left)) {
      if (expression->text == "not") {
        if (const auto *boolean = std::get_if<bool>(&*value))
          replacement = literal_expression(Scalar{!*boolean});
        else
          replacement = literal_expression(Scalar{std::monostate{}});
      } else if (const auto *integer = std::get_if<std::int64_t>(&*value)) {
        replacement = literal_expression(
            *integer == std::numeric_limits<std::int64_t>::min()
                ? Scalar{std::monostate{}}
                : Scalar{-*integer});
      } else if (const auto *floating = std::get_if<double>(&*value))
        replacement = literal_expression(Scalar{-*floating});
      else
        replacement = literal_expression(Scalar{std::monostate{}});
    }
  } else if (expression->kind == ExprKind::binary) {
    auto left = literal_value(expression->left);
    auto right = literal_value(expression->right);
    if (left && right)
      replacement = literal_expression(
          apply_binary_value(expression->text, *left, *right));
    else if (expression->text == "and") {
      if (left && std::get_if<bool>(&*left) && std::get<bool>(*left))
        replacement = expression->right;
      else if (right && std::get_if<bool>(&*right) && std::get<bool>(*right))
        replacement = expression->left;
      else if ((left && std::get_if<bool>(&*left) && !std::get<bool>(*left)) ||
               (right && std::get_if<bool>(&*right) && !std::get<bool>(*right)))
        replacement = literal_expression(Scalar{false});
    } else if (expression->text == "or") {
      if (left && std::get_if<bool>(&*left) && !std::get<bool>(*left))
        replacement = expression->right;
      else if (right && std::get_if<bool>(&*right) && !std::get<bool>(*right))
        replacement = expression->left;
      else if ((left && std::get_if<bool>(&*left) && std::get<bool>(*left)) ||
               (right && std::get_if<bool>(&*right) && std::get<bool>(*right)))
        replacement = literal_expression(Scalar{true});
    }
  } else if (expression->kind == ExprKind::function &&
             expression->text != "date" && expression->text != "timestamp" &&
             expression->text != "interval") {
    if (auto value = literal_value(expression->left))
      replacement = literal_expression(
          eval_values(expression->text, std::vector<Scalar>{*value}));
  } else if (expression->kind == ExprKind::call && expression->text != "date" &&
             expression->text != "timestamp" &&
             expression->text != "interval" &&
             expression->text != "date_trunc" &&
             expression->text != "extract") {
    std::vector<Scalar> arguments;
    bool all_literals = true;
    for (auto &argument : expression->args) {
      auto value = literal_value(argument);
      if (!value) {
        all_literals = false;
        break;
      }
      arguments.push_back(*value);
    }
    if (all_literals)
      replacement = literal_expression(
          eval_values(expression->text, std::move(arguments)));
  } else if (expression->kind == ExprKind::cast) {
    if (auto value = literal_value(expression->left))
      replacement = literal_expression(cast_value(expression->text, *value));
  } else if (expression->kind == ExprKind::is_null) {
    if (auto value = literal_value(expression->left))
      replacement = literal_expression(
          Scalar{std::holds_alternative<std::monostate>(*value) ^
                 expression->boolean});
  } else if (expression->kind == ExprKind::case_when) {
    bool all_constant_false_or_null = true;
    for (auto &[condition, value] : expression->branches) {
      auto scalar = literal_value(condition);
      if (!scalar) {
        all_constant_false_or_null = false;
        break;
      }
      if (const auto *boolean = std::get_if<bool>(&*scalar);
          boolean && *boolean) {
        replacement = value;
        break;
      }
      if (!std::holds_alternative<std::monostate>(*scalar) &&
          !std::holds_alternative<bool>(*scalar)) {
        all_constant_false_or_null = false;
        break;
      }
    }
    if (!replacement && all_constant_false_or_null)
      replacement = expression->left;
  }
  if (replacement) {
    expression = std::move(replacement);
    ++rewrites;
  }
  return rewrites;
}
static std::size_t optimize_query(Query &query, std::size_t event_rows) {
  std::size_t rewrites{};
  for (auto &item : query.select)
    rewrites += optimize_expression(item.expr);
  rewrites += optimize_expression(query.filter);
  if (query.filter && contradictory_filter(query.filter)) {
    query.filter = literal_expression(Scalar{false});
    ++rewrites;
  } else if (query.filter && reorder_conjuncts(query.filter))
    ++rewrites;
  for (auto &join : query.joins)
    rewrites += optimize_expression(join.on);
  rewrites += optimize_expression(query.having);
  bool safe_star_reorder = false;
  try {
    const auto bindings = query_bindings(query);
    safe_star_reorder = std::all_of(
        query.joins.begin(), query.joins.end(), [&](const auto &join) {
          if (join.kind != JoinKind::inner || !join.on ||
              join.on->kind != ExprKind::binary || join.on->text != "=" ||
              !join.on->left || !join.on->right ||
              join.on->left->kind != ExprKind::column ||
              join.on->right->kind != ExprKind::column)
            return false;
          const auto left = resolve_column(join.on->left->text, bindings).first;
          const auto right =
              resolve_column(join.on->right->text, bindings).first;
          return (left == query.from.name && right == join.table.name) ||
                 (right == query.from.name && left == join.table.name);
        });
  } catch (const std::exception &) {
    safe_star_reorder = false;
  }
  if (query.optimizer_enabled && query.joins.size() > 1 && safe_star_reorder) {
    const auto filters = pushed_filters(query);
    std::vector<std::string> original;
    for (auto &join : query.joins)
      original.push_back(join.table.name);
    std::stable_sort(query.joins.begin(), query.joins.end(),
                     [&](const auto &left, const auto &right) {
                       return estimated_filtered_rows(left.table.name, filters,
                                                      event_rows) <
                              estimated_filtered_rows(right.table.name, filters,
                                                      event_rows);
                     });
    std::vector<std::string> optimized;
    for (auto &join : query.joins)
      optimized.push_back(join.table.name);
    if (original != optimized)
      ++rewrites;
  }
  return rewrites;
}
static Query prepare(Query q, const Table &t) {
  q.optimizer_enabled = std::getenv("DREMEL_DISABLE_OPTIMIZER") == nullptr;
  auto rewrites = q.optimizer_enabled ? optimize_query(q, t.size()) : 0;
  plan(q);
  prepare_expr(q.filter, t);
  for (auto &join : q.joins)
    prepare_expr(join.on, t);
  prepare_expr(q.having, t);
  std::vector<PushedFilter> scan_filters;
  if (q.optimizer_enabled) {
    std::vector<std::string> scan_operators;
    if (filter_always_false(q.filter))
      scan_operators.push_back("EmptyScanExec(reason=contradiction)");
    std::map<std::string, std::pair<std::size_t, std::size_t>> by_table;
    scan_filters = pushed_filters(q);
    rewrites += scan_filters.size();
    for (auto &filter : scan_filters) {
      auto &counts = by_table[filter.table];
      ++counts.first;
      counts.second += filter.derived ? 1 : 0;
    }
    for (auto &[table, counts] : by_table)
      scan_operators.push_back(
          "ScanFilterExec(table=" + table +
          ";predicates=" + std::to_string(counts.first) +
          ";derived=" + std::to_string(counts.second) + ")");
    q.physical.insert(q.physical.begin() + 1, scan_operators.begin(),
                      scan_operators.end());
  }
  auto estimate = q.from.name == "users"       ? std::size_t{250'000}
                  : q.from.name == "campaigns" ? std::size_t{5'000}
                                               : t.size();
  if (filter_always_false(q.filter))
    estimate = 0;
  else if (q.optimizer_enabled && !scan_filters.empty())
    estimate = estimated_filtered_rows(q.from.name, scan_filters, t.size());
  else if (q.filter)
    estimate = (estimate + 9) / 10;
  if (q.limit)
    estimate = std::min(estimate, *q.limit);
  q.physical.push_back("EstimateExec(rows=" + std::to_string(estimate) + ")");
  if (q.from.name == "events") {
    const auto campaign_nulls = static_cast<std::size_t>(
        std::count(t.campaign_def.begin(), t.campaign_def.end(), 0));
    q.physical.push_back(
        "StatsExec(table=events;rows=" + std::to_string(t.size()) +
        ";campaign_nulls=" + std::to_string(campaign_nulls) +
        ";country_distinct=" + std::to_string(t.country_dict.values.size()) +
        ";device_distinct=" + std::to_string(t.device_dict.values.size()) +
        ";event_type_distinct=" + std::to_string(t.event_dict.values.size()) +
        ";event_id_min=" +
        std::to_string(t.event_id.empty() ? 0 : t.event_id.front()) +
        ";event_id_max=" +
        std::to_string(t.event_id.empty() ? 0 : t.event_id.back()) + ")");
  } else if (q.from.name == "users")
    q.physical.push_back("StatsExec(table=users;rows=250000;nulls=0;distinct="
                         "user_id:250000;min=user_id:1;max=user_id:250000)");
  else if (q.from.name == "campaigns")
    q.physical.push_back(
        "StatsExec(table=campaigns;rows=5000;nulls=0;distinct=campaign_id:5000;"
        "min=campaign_id:1;max=campaign_id:5000)");
  else
    q.physical.push_back("StatsExec(table=" + q.from.name +
                         ";rows=" + std::to_string(estimate) + ")");
  q.physical.push_back(
      q.optimizer_enabled
          ? "OptimizerExec(rewrites=" + std::to_string(rewrites) +
                ";rules=constant_folding+3vl+projection_pruning+predicate_"
                "pushdown+transitive_predicates+range_contradiction+filter_"
                "ordering+selectivity_join_order+join_selection+runtime_"
                "filter+topk)"
          : "OptimizerExec(disabled=true)");
  return q;
}

} // namespace dremel
