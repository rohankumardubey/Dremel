#pragma once

#include "../optimizer/optimizer.hpp"

namespace dremel {

static Scalar eval(const ExprPtr &e, const Table &t, std::size_t i) {
  switch (e->kind) {
  case ExprKind::null:
    return std::monostate{};
  case ExprKind::column:
    return t.scalar(e->text, i);
  case ExprKind::integer:
    return e->integer;
  case ExprKind::floating:
    return e->floating;
  case ExprKind::boolean:
    return e->boolean;
  case ExprKind::string:
    return e->text;
  case ExprKind::star:
    return std::int64_t{1};
  case ExprKind::dict_eq: {
    std::uint32_t v = UINT32_MAX;
    const auto column = base_name(e->text);
    if (column == "country")
      v = t.country[i];
    else if (column == "device")
      v = t.device[i];
    else if (column == "event_type")
      v = t.event_type[i];
    return Scalar{static_cast<bool>((v == e->dict_id) ^ e->boolean)};
  }
  case ExprKind::is_null:
    return Scalar{static_cast<bool>(
        std::holds_alternative<std::monostate>(eval(e->left, t, i)) ^
        e->boolean)};
  case ExprKind::unary: {
    auto v = eval(e->left, t, i);
    if (e->text == "not")
      return sql_bool(v) ? Scalar{!*sql_bool(v)} : Scalar{std::monostate{}};
    if (auto *x = std::get_if<std::int64_t>(&v))
      return -*x;
    if (auto *x = std::get_if<Decimal>(&v))
      return x->units == std::numeric_limits<std::int64_t>::min()
                 ? Scalar{std::monostate{}}
                 : Scalar{Decimal{-x->units}};
    if (auto *x = std::get_if<double>(&v))
      return -*x;
    return std::monostate{};
  }
  case ExprKind::function:
    return eval_call(e->text, std::vector<ExprPtr>{e->left}, t, i);
  case ExprKind::call:
    return eval_call(e->text, e->args, t, i);
  case ExprKind::case_when:
    for (auto &[condition, value] : e->branches)
      if (sql_bool(eval(condition, t, i)) == true)
        return eval(value, t, i);
    return eval(e->left, t, i);
  case ExprKind::cast: {
    auto value = eval(e->left, t, i);
    if (std::holds_alternative<std::monostate>(value))
      return std::monostate{};
    try {
      if (e->text == "bigint" || e->text == "int64" || e->text == "integer") {
        if (auto *integer = std::get_if<std::int64_t>(&value))
          return *integer;
        if (auto *floating = std::get_if<double>(&value))
          return std::isfinite(*floating) &&
                         *floating >=
                             static_cast<double>(
                                 std::numeric_limits<std::int64_t>::min()) &&
                         *floating <=
                             static_cast<double>(
                                 std::numeric_limits<std::int64_t>::max())
                     ? Scalar{static_cast<std::int64_t>(*floating)}
                     : Scalar{std::monostate{}};
        return std::stoll(std::get<std::string>(value));
      }
      if (e->text == "double" || e->text == "float" || e->text == "real") {
        if (auto *integer = std::get_if<std::int64_t>(&value))
          return static_cast<double>(*integer);
        if (auto *floating = std::get_if<double>(&value))
          return *floating;
        return std::stod(std::get<std::string>(value));
      }
      if (e->text == "varchar" || e->text == "string" || e->text == "text") {
        if (auto *text = std::get_if<std::string>(&value))
          return *text;
        if (auto *integer = std::get_if<std::int64_t>(&value))
          return std::to_string(*integer);
        if (auto *floating = std::get_if<double>(&value)) {
          std::ostringstream out;
          out << *floating;
          return out.str();
        }
        return std::get<bool>(value) ? std::string{"true"}
                                     : std::string{"false"};
      }
      if (e->text == "boolean" || e->text == "bool") {
        if (auto *boolean = std::get_if<bool>(&value))
          return *boolean;
        if (auto *text = std::get_if<std::string>(&value)) {
          if (*text == "true")
            return true;
          if (*text == "false")
            return false;
        }
      }
    } catch (const std::exception &) {
    }
    return std::monostate{};
  }
  case ExprKind::in_list: {
    auto value = eval(e->left, t, i);
    if (std::holds_alternative<std::monostate>(value))
      return std::monostate{};
    bool saw_null = false;
    for (auto &candidate_expression : e->args) {
      auto candidate = eval(candidate_expression, t, i);
      if (std::holds_alternative<std::monostate>(candidate))
        saw_null = true;
      else if (compare(value, candidate) == 0)
        return !e->boolean;
    }
    return saw_null ? Scalar{std::monostate{}} : Scalar{e->boolean};
  }
  case ExprKind::between: {
    auto value = eval(e->left, t, i);
    auto low = eval(e->args[0], t, i);
    auto high = eval(e->args[1], t, i);
    if (std::holds_alternative<std::monostate>(value) ||
        std::holds_alternative<std::monostate>(low) ||
        std::holds_alternative<std::monostate>(high))
      return std::monostate{};
    return static_cast<bool>(
        ((compare(value, low) >= 0 && compare(value, high) <= 0) ^ e->boolean));
  }
  case ExprKind::like: {
    auto value = eval(e->left, t, i);
    auto pattern = eval(e->right, t, i);
    auto *text = std::get_if<std::string>(&value);
    auto *pattern_text = std::get_if<std::string>(&pattern);
    return text && pattern_text
               ? Scalar{static_cast<bool>(like_matches(*text, *pattern_text) ^
                                          e->boolean)}
               : Scalar{std::monostate{}};
  }
  case ExprKind::window:
  case ExprKind::scalar_subquery:
  case ExprKind::exists:
  case ExprKind::in_subquery:
    return std::monostate{};
  case ExprKind::binary:
    break;
  }
  if (e->text == "and") {
    const auto left_value = eval(e->left, t, i);
    auto left = sql_bool(left_value);
    if (left == false)
      return false;
    auto right = sql_bool(eval(e->right, t, i));
    if (right == false)
      return false;
    if (left == true && right == true)
      return true;
    return std::monostate{};
  }
  if (e->text == "or") {
    const auto left_value = eval(e->left, t, i);
    auto left = sql_bool(left_value);
    if (left == true)
      return true;
    auto right = sql_bool(eval(e->right, t, i));
    if (right == true)
      return true;
    if (left == false && right == false)
      return false;
    return std::monostate{};
  }
  return apply_binary_value(e->text, eval(e->left, t, i), eval(e->right, t, i));
}

static Rows execute_subquery(const Query &query, const Catalog &catalog,
                             const RelRow &row, const Bindings &outer_bindings);
static std::optional<std::optional<std::size_t>>
simple_campaign_lookup(const Query &query, const Catalog &catalog,
                       const RelRow &row, const Bindings &outer_bindings) {
  if (query.from.name != "campaigns" || !query.joins.empty() ||
      !query.group_by.empty() || query.having || query.union_query ||
      !query.ctes.empty() || !query.filter ||
      query.filter->kind != ExprKind::binary || query.filter->text != "=")
    return {};
  const auto is_local_id = [&](const ExprPtr &expression) {
    if (!expression || expression->kind != ExprKind::column ||
        base_name(expression->text) != "campaign_id")
      return false;
    const auto dot = expression->text.find('.');
    if (dot == std::string::npos)
      return false;
    const auto qualifier = expression->text.substr(0, dot);
    return qualifier == query.from.name || qualifier == query.from.alias;
  };
  ExprPtr outer;
  if (is_local_id(query.filter->left))
    outer = query.filter->right;
  else if (is_local_id(query.filter->right))
    outer = query.filter->left;
  else
    return {};
  if (!outer || outer->kind != ExprKind::column)
    return {};
  const auto [table, column] = resolve_column(outer->text, outer_bindings);
  const auto value = relation_scalar(catalog, row, table, column);
  if (std::holds_alternative<std::monostate>(value))
    return std::optional<std::size_t>{};
  const auto *id = std::get_if<std::int64_t>(&value);
  if (!id)
    return {};
  const auto found = catalog.campaigns.index.find(*id);
  if (found != catalog.campaigns.index.end() && !found->second.empty())
    return std::optional<std::size_t>{found->second.front()};
  const auto fallback = std::find(catalog.campaigns.campaign_id.begin(),
                                  catalog.campaigns.campaign_id.end(), *id);
  return fallback == catalog.campaigns.campaign_id.end()
             ? std::optional<std::size_t>{}
             : std::optional<std::size_t>{
                   static_cast<std::size_t>(std::distance(
                       catalog.campaigns.campaign_id.begin(), fallback))};
}
static std::optional<Scalar>
simple_campaign_max_budget(const Query &query, const Catalog &catalog,
                           const RelRow &row, const Bindings &outer_bindings) {
  if (query.from.name != "campaigns" || query.select.size() != 1 ||
      !query.joins.empty())
    return {};
  const auto &expression = query.select.front().expr;
  if (!expression || expression->kind != ExprKind::function ||
      expression->text != "max" || !expression->left ||
      expression->left->kind != ExprKind::column ||
      base_name(expression->left->text) != "budget")
    return {};
  if (!query.filter) {
    const auto maximum = std::max_element(catalog.campaigns.budget.begin(),
                                          catalog.campaigns.budget.end());
    return maximum == catalog.campaigns.budget.end()
               ? Scalar{std::monostate{}}
               : Scalar{Decimal{*maximum}};
  }
  if (auto index = simple_campaign_lookup(query, catalog, row, outer_bindings))
    return *index ? Scalar{Decimal{catalog.campaigns.budget[**index]}}
                  : Scalar{std::monostate{}};
  return {};
}
static std::optional<bool>
simple_campaign_id_membership(const Query &query, const Scalar &value,
                              const Catalog &catalog) {
  if (query.from.name != "campaigns" || query.select.size() != 1 ||
      query.filter || !query.joins.empty() || !query.select.front().expr ||
      query.select.front().expr->kind != ExprKind::column ||
      base_name(query.select.front().expr->text) != "campaign_id")
    return {};
  if (const auto *id = std::get_if<std::int64_t>(&value))
    return catalog.campaigns.index.contains(*id) ||
           std::find(catalog.campaigns.campaign_id.begin(),
                     catalog.campaigns.campaign_id.end(),
                     *id) != catalog.campaigns.campaign_id.end();
  return {};
}
static Scalar eval_rel(const ExprPtr &expression, const Catalog &catalog,
                       const RelRow &row, const Bindings &bindings) {
  switch (expression->kind) {
  case ExprKind::null:
    return std::monostate{};
  case ExprKind::column: {
    const auto [table, column] = resolve_column(expression->text, bindings);
    return relation_scalar(catalog, row, table, column);
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
  case ExprKind::dict_eq: {
    if (!row.event)
      return std::monostate{};
    std::uint32_t value = UINT32_MAX;
    const auto column = base_name(expression->text);
    if (column == "country")
      value = catalog.events->country[*row.event];
    else if (column == "device")
      value = catalog.events->device[*row.event];
    else if (column == "event_type")
      value = catalog.events->event_type[*row.event];
    return static_cast<bool>((value == expression->dict_id) ^
                             expression->boolean);
  }
  case ExprKind::is_null:
    return static_cast<bool>(std::holds_alternative<std::monostate>(eval_rel(
                                 expression->left, catalog, row, bindings)) ^
                             expression->boolean);
  case ExprKind::unary: {
    auto value = eval_rel(expression->left, catalog, row, bindings);
    if (expression->text == "not")
      return sql_bool(value) ? Scalar{!*sql_bool(value)}
                             : Scalar{std::monostate{}};
    if (auto *integer = std::get_if<std::int64_t>(&value)) {
      if (*integer == std::numeric_limits<std::int64_t>::min())
        return std::monostate{};
      return -*integer;
    }
    if (auto *decimal = std::get_if<Decimal>(&value))
      return decimal->units == std::numeric_limits<std::int64_t>::min()
                 ? Scalar{std::monostate{}}
                 : Scalar{Decimal{-decimal->units}};
    if (auto *floating = std::get_if<double>(&value))
      return -*floating;
    return std::monostate{};
  }
  case ExprKind::binary: {
    auto left = eval_rel(expression->left, catalog, row, bindings);
    if ((expression->text == "and" && sql_bool(left) == false) ||
        (expression->text == "or" && sql_bool(left) == true))
      return left;
    return apply_binary_value(
        expression->text, left,
        eval_rel(expression->right, catalog, row, bindings));
  }
  case ExprKind::function:
    return eval_values(expression->text,
                       {eval_rel(expression->left, catalog, row, bindings)});
  case ExprKind::call: {
    std::vector<Scalar> values;
    for (auto &argument : expression->args)
      values.push_back(eval_rel(argument, catalog, row, bindings));
    return eval_values(expression->text, std::move(values));
  }
  case ExprKind::case_when:
    for (auto &[condition, value] : expression->branches)
      if (sql_bool(eval_rel(condition, catalog, row, bindings)) == true)
        return eval_rel(value, catalog, row, bindings);
    return eval_rel(expression->left, catalog, row, bindings);
  case ExprKind::cast:
    return cast_value(expression->text,
                      eval_rel(expression->left, catalog, row, bindings));
  case ExprKind::in_list: {
    auto value = eval_rel(expression->left, catalog, row, bindings);
    if (std::holds_alternative<std::monostate>(value))
      return std::monostate{};
    bool saw_null = false;
    for (auto &candidate_expression : expression->args) {
      auto candidate = eval_rel(candidate_expression, catalog, row, bindings);
      if (std::holds_alternative<std::monostate>(candidate))
        saw_null = true;
      else if (compare(value, candidate) == 0)
        return !expression->boolean;
    }
    return saw_null ? Scalar{std::monostate{}} : Scalar{expression->boolean};
  }
  case ExprKind::between: {
    auto value = eval_rel(expression->left, catalog, row, bindings);
    auto low = eval_rel(expression->args[0], catalog, row, bindings);
    auto high = eval_rel(expression->args[1], catalog, row, bindings);
    if (std::holds_alternative<std::monostate>(value) ||
        std::holds_alternative<std::monostate>(low) ||
        std::holds_alternative<std::monostate>(high))
      return std::monostate{};
    return static_cast<bool>(
        ((compare(value, low) >= 0 && compare(value, high) <= 0) ^
         expression->boolean));
  }
  case ExprKind::like: {
    auto value = eval_rel(expression->left, catalog, row, bindings);
    auto pattern = eval_rel(expression->right, catalog, row, bindings);
    auto *text = std::get_if<std::string>(&value);
    auto *pattern_text = std::get_if<std::string>(&pattern);
    return text && pattern_text
               ? Scalar{static_cast<bool>(like_matches(*text, *pattern_text) ^
                                          expression->boolean)}
               : Scalar{std::monostate{}};
  }
  case ExprKind::window:
    return std::monostate{};
  case ExprKind::scalar_subquery: {
    if (auto value = simple_campaign_max_budget(*expression->subquery, catalog,
                                                row, bindings))
      return *value;
    auto rows = execute_subquery(*expression->subquery, catalog, row, bindings);
    return rows.empty() || rows.front().empty() ? Scalar{std::monostate{}}
                                                : rows.front().front();
  }
  case ExprKind::exists:
    if (auto index = simple_campaign_lookup(*expression->subquery, catalog, row,
                                            bindings))
      return index->has_value();
    else
      return !execute_subquery(*expression->subquery, catalog, row, bindings)
                  .empty();
  case ExprKind::in_subquery: {
    auto value = eval_rel(expression->left, catalog, row, bindings);
    if (std::holds_alternative<std::monostate>(value))
      return std::monostate{};
    if (auto found = simple_campaign_id_membership(*expression->subquery, value,
                                                   catalog))
      return static_cast<bool>(*found ^ expression->boolean);
    bool saw_null = false;
    for (auto &candidate_row :
         execute_subquery(*expression->subquery, catalog, row, bindings)) {
      if (candidate_row.empty())
        continue;
      auto &candidate = candidate_row.front();
      if (std::holds_alternative<std::monostate>(candidate))
        saw_null = true;
      else if (compare(value, candidate) == 0)
        return !expression->boolean;
    }
    return saw_null ? Scalar{std::monostate{}} : Scalar{expression->boolean};
  }
  }
  return std::monostate{};
}

} // namespace dremel
