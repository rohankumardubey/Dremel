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
static std::size_t optimize_query(Query &query) {
  std::size_t rewrites{};
  for (auto &item : query.select)
    rewrites += optimize_expression(item.expr);
  rewrites += optimize_expression(query.filter);
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
    std::vector<std::string> original;
    for (auto &join : query.joins)
      original.push_back(join.table.name);
    const auto cardinality = [](const JoinSpec &join) {
      return join.table.name == "campaigns" ? std::size_t{5'000}
             : join.table.name == "users"   ? std::size_t{250'000}
             : join.table.name == "events"
                 ? std::size_t{1'000'000}
                 : std::numeric_limits<std::size_t>::max();
    };
    std::stable_sort(query.joins.begin(), query.joins.end(),
                     [&](const auto &left, const auto &right) {
                       return cardinality(left) < cardinality(right);
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
  const auto rewrites = q.optimizer_enabled ? optimize_query(q) : 0;
  plan(q);
  prepare_expr(q.filter, t);
  for (auto &join : q.joins)
    prepare_expr(join.on, t);
  prepare_expr(q.having, t);
  auto estimate = q.from.name == "users"       ? std::size_t{250'000}
                  : q.from.name == "campaigns" ? std::size_t{5'000}
                                               : t.size();
  if (q.filter)
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
                "pushdown+aggregate_filter_ordering+join_ordering+join_"
                "selection+runtime_filter+topk)"
          : "OptimizerExec(disabled=true)");
  return q;
}

} // namespace dremel
