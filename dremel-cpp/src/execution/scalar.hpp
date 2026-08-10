#pragma once

#include "../storage/catalog.hpp"

namespace dremel {

static void prepare_expr(ExprPtr &e, const Table &t) {
  if (!e)
    return;
  prepare_expr(e->left, t);
  prepare_expr(e->right, t);
  for (auto &arg : e->args)
    prepare_expr(arg, t);
  for (auto &[condition, value] : e->branches) {
    prepare_expr(condition, t);
    prepare_expr(value, t);
  }
  if (e->kind == ExprKind::binary && (e->text == "=" || e->text == "!=")) {
    ExprPtr column, literal;
    if (e->left->kind == ExprKind::column &&
        e->right->kind == ExprKind::string) {
      column = e->left;
      literal = e->right;
    } else if (e->right->kind == ExprKind::column &&
               e->left->kind == ExprKind::string) {
      column = e->right;
      literal = e->left;
    }
    if (column) {
      if (auto id = t.dict_id(column->text, literal->text)) {
        const bool negated = e->text == "!=";
        const std::string column_name = column->text;
        e->kind = ExprKind::dict_eq;
        e->dict_id = *id;
        e->boolean = negated;
        e->text = column_name;
        e->left.reset();
        e->right.reset();
      }
    }
  }
}
static Query prepare(Query q, const Table &t);
static std::optional<double> number(const Scalar &v) {
  if (auto *i = std::get_if<std::int64_t>(&v))
    return static_cast<double>(*i);
  if (auto *decimal = std::get_if<Decimal>(&v))
    return static_cast<double>(decimal->units) / 100.0;
  if (auto *f = std::get_if<double>(&v))
    return *f;
  return {};
}
static bool truthy(const Scalar &v) {
  if (auto *b = std::get_if<bool>(&v))
    return *b;
  if (auto *i = std::get_if<std::int64_t>(&v))
    return *i != 0;
  return false;
}
static std::optional<bool> sql_bool(const Scalar &v) {
  if (auto *boolean = std::get_if<bool>(&v))
    return *boolean;
  return std::nullopt;
}
static int compare(const Scalar &a, const Scalar &b) {
  if (auto *left = std::get_if<Decimal>(&a)) {
    if (auto *right = std::get_if<Decimal>(&b))
      return left->units < right->units ? -1 : left->units > right->units;
    if (auto *right = std::get_if<std::int64_t>(&b)) {
      std::int64_t scaled;
      if (!__builtin_mul_overflow(*right, std::int64_t{100}, &scaled))
        return left->units < scaled ? -1 : left->units > scaled;
    }
  }
  if (auto *left = std::get_if<std::int64_t>(&a))
    if (auto *right = std::get_if<Decimal>(&b)) {
      std::int64_t scaled;
      if (!__builtin_mul_overflow(*left, std::int64_t{100}, &scaled))
        return scaled < right->units ? -1 : scaled > right->units;
    }
  if (auto x = number(a); x && number(b)) {
    auto y = *number(b);
    return *x < y ? -1 : *x > y ? 1 : 0;
  }
  if (a.index() != b.index())
    return 0;
  if (auto *x = std::get_if<std::string>(&a)) {
    auto &y = std::get<std::string>(b);
    return *x < y ? -1 : *x > y ? 1 : 0;
  }
  if (auto *x = std::get_if<bool>(&a)) {
    auto y = std::get<bool>(b);
    return *x == y ? 0 : *x ? 1 : -1;
  }
  return 0;
}
static Scalar eval(const ExprPtr &e, const Table &t, std::size_t i);

static bool like_matches(const std::string &value, const std::string &pattern) {
  std::vector<bool> previous(value.size() + 1);
  previous[0] = true;
  for (const char token : pattern) {
    std::vector<bool> current(value.size() + 1);
    if (token == '%') {
      current[0] = previous[0];
      for (std::size_t index = 1; index <= value.size(); ++index)
        current[index] = previous[index] || current[index - 1];
    } else {
      for (std::size_t index = 1; index <= value.size(); ++index)
        current[index] =
            previous[index - 1] && (token == '_' || token == value[index - 1]);
    }
    previous = std::move(current);
  }
  return previous[value.size()];
}

static Scalar eval_values(const std::string &name, std::vector<Scalar> values);

static Scalar eval_call(const std::string &name,
                        const std::vector<ExprPtr> &args, const Table &t,
                        std::size_t row) {
  std::vector<Scalar> values;
  values.reserve(args.size());
  for (auto &arg : args)
    values.push_back(eval(arg, t, row));
  return eval_values(name, std::move(values));
}

static std::tuple<std::int64_t, std::int64_t, std::int64_t>
civil_from_days(std::int64_t days) {
  const auto shifted = days + 719468;
  const auto era =
      shifted >= 0 ? shifted / 146097 : (shifted - 146096) / 146097;
  const auto day_of_era = shifted - era * 146097;
  const auto year_of_era = (day_of_era - day_of_era / 1460 +
                            day_of_era / 36524 - day_of_era / 146096) /
                           365;
  auto year = year_of_era + era * 400;
  const auto day_of_year =
      day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
  const auto month_prime = (5 * day_of_year + 2) / 153;
  const auto day = day_of_year - (153 * month_prime + 2) / 5 + 1;
  const auto month = month_prime + (month_prime < 10 ? 3 : -9);
  year += month <= 2;
  return {year, month, day};
}
static std::optional<std::int64_t>
days_from_civil(std::int64_t year, std::int64_t month, std::int64_t day) {
  if (month < 1 || month > 12 || day < 1 || day > 31)
    return {};
  const auto adjusted_year = year - (month <= 2);
  const auto era =
      adjusted_year >= 0 ? adjusted_year / 400 : (adjusted_year - 399) / 400;
  const auto year_of_era = adjusted_year - era * 400;
  const auto shifted_month = month + (month > 2 ? -3 : 9);
  const auto day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
  const auto day_of_era =
      year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
  const auto days = era * 146097 + day_of_era - 719468;
  if (civil_from_days(days) != std::tuple{year, month, day})
    return {};
  return days;
}
static std::optional<std::int64_t> parse_date(const std::string &value) {
  try {
    if (value.size() != 10 || value[4] != '-' || value[7] != '-')
      return {};
    return days_from_civil(std::stoll(value.substr(0, 4)),
                           std::stoll(value.substr(5, 2)),
                           std::stoll(value.substr(8, 2)));
  } catch (const std::exception &) {
    return {};
  }
}
static std::string format_date(std::int64_t days) {
  auto [year, month, day] = civil_from_days(days);
  std::ostringstream output;
  output << std::setw(4) << std::setfill('0') << year << '-' << std::setw(2)
         << month << '-' << std::setw(2) << day;
  return output.str();
}
static std::optional<std::int64_t> parse_timestamp(const std::string &value) {
  try {
    if (value.size() < 10)
      return {};
    auto days = parse_date(value.substr(0, 10));
    if (!days)
      return {};
    if (value.size() == 10)
      return *days * 86400;
    if ((value[10] != 'T' && value[10] != ' ') || value.size() < 19 ||
        value[13] != ':' || value[16] != ':')
      return {};
    const auto hour = std::stoll(value.substr(11, 2));
    const auto minute = std::stoll(value.substr(14, 2));
    const auto second = std::stoll(value.substr(17, 2));
    if (hour > 23 || minute > 59 || second > 59)
      return {};
    const auto suffix = value.substr(19);
    if (!suffix.empty() && suffix != "Z" && suffix.front() != '.')
      return {};
    return *days * 86400 + hour * 3600 + minute * 60 + second;
  } catch (const std::exception &) {
    return {};
  }
}
static std::optional<std::int64_t> interval_seconds(const std::string &value) {
  std::istringstream input(value);
  std::int64_t amount;
  std::string unit, extra;
  if (!(input >> amount >> unit) || input >> extra)
    return {};
  if (unit.ends_with('s'))
    unit.pop_back();
  const std::int64_t multiplier = unit == "microsecond" ? 0
                                  : unit == "second"    ? 1
                                  : unit == "minute"    ? 60
                                  : unit == "hour"      ? 3600
                                  : unit == "day"       ? 86400
                                  : unit == "week"      ? 604800
                                                        : -1;
  if (multiplier < 0)
    return {};
  std::int64_t result;
  return __builtin_mul_overflow(amount, multiplier, &result)
             ? std::optional<std::int64_t>{}
             : std::optional<std::int64_t>{result};
}
static std::optional<std::array<std::int64_t, 6>>
temporal_components(const Scalar &value) {
  std::optional<std::int64_t> seconds;
  if (const auto *integer = std::get_if<std::int64_t>(&value))
    seconds = *integer;
  else if (const auto *text = std::get_if<std::string>(&value))
    seconds = parse_timestamp(*text);
  if (!seconds)
    return {};
  auto days = *seconds / 86400;
  auto seconds_of_day = *seconds % 86400;
  if (seconds_of_day < 0) {
    seconds_of_day += 86400;
    --days;
  }
  auto [year, month, day] = civil_from_days(days);
  return std::array<std::int64_t, 6>{year,
                                     month,
                                     day,
                                     seconds_of_day / 3600,
                                     seconds_of_day / 60 % 60,
                                     seconds_of_day % 60};
}

static Scalar eval_values(const std::string &name, std::vector<Scalar> values) {
  if (name == "coalesce") {
    for (auto &value : values)
      if (!std::holds_alternative<std::monostate>(value))
        return value;
    return std::monostate{};
  }
  if (name == "nullif" && values.size() == 2)
    return compare(values[0], values[1]) == 0 ? Scalar{std::monostate{}}
                                              : values[0];
  if ((name == "lower" || name == "upper") && values.size() == 1) {
    auto *text = std::get_if<std::string>(&values[0]);
    if (!text)
      return std::monostate{};
    auto result = *text;
    std::transform(result.begin(), result.end(), result.begin(), [&](char c) {
      const auto byte = static_cast<unsigned char>(c);
      return static_cast<char>(name == "lower" ? std::tolower(byte)
                                               : std::toupper(byte));
    });
    return result;
  }
  if (name == "length" && values.size() == 1) {
    if (auto *text = std::get_if<std::string>(&values[0]))
      return static_cast<std::int64_t>(text->size());
    return std::monostate{};
  }
  if (name == "abs" && values.size() == 1) {
    if (auto *integer = std::get_if<std::int64_t>(&values[0])) {
      if (*integer == std::numeric_limits<std::int64_t>::min())
        return std::monostate{};
      return *integer < 0 ? -*integer : *integer;
    }
    if (auto *decimal = std::get_if<Decimal>(&values[0])) {
      if (decimal->units == std::numeric_limits<std::int64_t>::min())
        return std::monostate{};
      return Decimal{decimal->units < 0 ? -decimal->units : decimal->units};
    }
    if (auto *floating = std::get_if<double>(&values[0]))
      return std::abs(*floating);
    return std::monostate{};
  }
  if (name == "concat") {
    std::string result;
    for (auto &value : values) {
      if (std::holds_alternative<std::monostate>(value))
        return std::monostate{};
      if (auto *text = std::get_if<std::string>(&value))
        result += *text;
      else if (auto *integer = std::get_if<std::int64_t>(&value))
        result += std::to_string(*integer);
      else if (auto *decimal = std::get_if<Decimal>(&value))
        result += decimal_text(decimal->units);
      else if (auto *floating = std::get_if<double>(&value)) {
        std::ostringstream out;
        out << *floating;
        result += out.str();
      } else
        result += std::get<bool>(value) ? "true" : "false";
    }
    return result;
  }
  if (name == "substring" && (values.size() == 2 || values.size() == 3)) {
    auto *text = std::get_if<std::string>(&values[0]);
    auto *start_value = std::get_if<std::int64_t>(&values[1]);
    if (!text || !start_value)
      return std::monostate{};
    const auto start =
        static_cast<std::size_t>(std::max<std::int64_t>(0, *start_value - 1));
    auto length = std::string::npos;
    if (values.size() == 3) {
      auto *length_value = std::get_if<std::int64_t>(&values[2]);
      if (!length_value)
        return std::monostate{};
      length =
          static_cast<std::size_t>(std::max<std::int64_t>(0, *length_value));
    }
    return start >= text->size() ? std::string{} : text->substr(start, length);
  }
  if (name == "date" && values.size() == 1) {
    if (auto *text = std::get_if<std::string>(&values[0]))
      if (auto days = parse_date(*text))
        return format_date(*days);
    if (auto *seconds = std::get_if<std::int64_t>(&values[0]))
      return format_date(*seconds / 86400);
    return std::monostate{};
  }
  if (name == "timestamp" && values.size() == 1) {
    if (auto *text = std::get_if<std::string>(&values[0]))
      if (auto seconds = parse_timestamp(*text))
        return *seconds;
    if (auto *seconds = std::get_if<std::int64_t>(&values[0]))
      return *seconds;
    return std::monostate{};
  }
  if (name == "interval" && values.size() == 1) {
    if (auto *text = std::get_if<std::string>(&values[0]))
      if (auto seconds = interval_seconds(*text))
        return *seconds;
    return std::monostate{};
  }
  if (name == "extract" && values.size() == 2) {
    auto *field = std::get_if<std::string>(&values[0]);
    auto components = temporal_components(values[1]);
    if (!field || !components)
      return std::monostate{};
    const auto index = *field == "year"     ? 0
                       : *field == "month"  ? 1
                       : *field == "day"    ? 2
                       : *field == "hour"   ? 3
                       : *field == "minute" ? 4
                       : *field == "second" ? 5
                                            : 6;
    return index < 6 ? Scalar{(*components)[index]} : Scalar{std::monostate{}};
  }
  if (name == "date_trunc" && values.size() == 2) {
    auto *unit = std::get_if<std::string>(&values[0]);
    auto components = temporal_components(values[1]);
    if (!unit || !components)
      return std::monostate{};
    auto [year, month, day, hour, minute, second] = *components;
    if (*unit == "year")
      month = 1, day = 1, hour = minute = second = 0;
    else if (*unit == "month")
      day = 1, hour = minute = second = 0;
    else if (*unit == "day")
      hour = minute = second = 0;
    else if (*unit == "hour")
      minute = second = 0;
    else if (*unit == "minute")
      second = 0;
    else if (*unit != "second")
      return std::monostate{};
    auto days = days_from_civil(year, month, day);
    if (!days)
      return std::monostate{};
    const auto truncated = *days * 86400 + hour * 3600 + minute * 60 + second;
    if (std::holds_alternative<std::string>(values[1]) &&
        (*unit == "year" || *unit == "month" || *unit == "day"))
      return format_date(*days);
    return truncated;
  }
  return std::monostate{};
}

static Scalar cast_value(const std::string &type, const Scalar &value) {
  if (std::holds_alternative<std::monostate>(value))
    return std::monostate{};
  try {
    if (type == "bigint" || type == "int64" || type == "integer") {
      if (auto *integer = std::get_if<std::int64_t>(&value))
        return *integer;
      if (auto *decimal = std::get_if<Decimal>(&value))
        return decimal->units / 100;
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
    if (type == "double" || type == "float" || type == "real") {
      if (auto *integer = std::get_if<std::int64_t>(&value))
        return static_cast<double>(*integer);
      if (auto *decimal = std::get_if<Decimal>(&value))
        return static_cast<double>(decimal->units) / 100.0;
      if (auto *floating = std::get_if<double>(&value))
        return *floating;
      return std::stod(std::get<std::string>(value));
    }
    if (type == "varchar" || type == "string" || type == "text") {
      if (auto *text = std::get_if<std::string>(&value))
        return *text;
      if (auto *integer = std::get_if<std::int64_t>(&value))
        return std::to_string(*integer);
      if (auto *decimal = std::get_if<Decimal>(&value))
        return decimal_text(decimal->units);
      if (auto *floating = std::get_if<double>(&value)) {
        std::ostringstream out;
        out << *floating;
        return out.str();
      }
      return std::get<bool>(value) ? std::string{"true"} : std::string{"false"};
    }
    if (type == "boolean" || type == "bool") {
      if (auto *boolean = std::get_if<bool>(&value))
        return *boolean;
      if (auto *text = std::get_if<std::string>(&value)) {
        if (*text == "true")
          return true;
        if (*text == "false")
          return false;
      }
    }
    if (type == "date") {
      if (auto *text = std::get_if<std::string>(&value))
        if (auto days = parse_date(*text))
          return format_date(*days);
      if (auto *seconds = std::get_if<std::int64_t>(&value))
        return format_date(*seconds / 86400);
    }
    if (type == "timestamp") {
      if (auto *text = std::get_if<std::string>(&value))
        if (auto seconds = parse_timestamp(*text))
          return *seconds;
      if (auto *seconds = std::get_if<std::int64_t>(&value))
        return *seconds;
    }
    if (type.starts_with("decimal")) {
      if (auto *decimal = std::get_if<Decimal>(&value))
        return *decimal;
      if (auto *integer = std::get_if<std::int64_t>(&value)) {
        std::int64_t units;
        return __builtin_mul_overflow(*integer, std::int64_t{100}, &units)
                   ? Scalar{std::monostate{}}
                   : Scalar{Decimal{units}};
      }
      if (auto *floating = std::get_if<double>(&value)) {
        const auto scaled = *floating * 100.0;
        return std::isfinite(scaled) &&
                       scaled >=
                           static_cast<double>(
                               std::numeric_limits<std::int64_t>::min()) &&
                       scaled <= static_cast<double>(
                                     std::numeric_limits<std::int64_t>::max())
                   ? Scalar{Decimal{
                         static_cast<std::int64_t>(std::round(scaled))}}
                   : Scalar{std::monostate{}};
      }
      if (auto *text = std::get_if<std::string>(&value))
        try {
          return Decimal{parse_decimal_units(*text)};
        } catch (const std::exception &) {
          const auto scaled = std::stod(*text) * 100.0;
          return Decimal{static_cast<std::int64_t>(std::round(scaled))};
        }
    }
  } catch (const std::exception &) {
  }
  return std::monostate{};
}

static Scalar apply_binary_value(const std::string &op, const Scalar &left,
                                 const Scalar &right) {
  if (op == "and") {
    const auto x = sql_bool(left), y = sql_bool(right);
    if (x == false || y == false)
      return false;
    if (x == true && y == true)
      return true;
    return std::monostate{};
  }
  if (op == "or") {
    const auto x = sql_bool(left), y = sql_bool(right);
    if (x == true || y == true)
      return true;
    if (x == false && y == false)
      return false;
    return std::monostate{};
  }
  if (std::holds_alternative<std::monostate>(left) ||
      std::holds_alternative<std::monostate>(right))
    return std::monostate{};
  if ((op == "+" || op == "-") && std::holds_alternative<std::string>(left) &&
      std::holds_alternative<std::int64_t>(right)) {
    const auto days = parse_date(std::get<std::string>(left));
    const auto interval = std::get<std::int64_t>(right);
    if (days && interval % 86400 == 0) {
      const auto delta = (op == "+" ? interval : -interval) / 86400;
      return format_date(*days + delta);
    }
  }
  if (op == "=")
    return compare(left, right) == 0;
  if (op == "!=")
    return compare(left, right) != 0;
  if (op == "<")
    return compare(left, right) < 0;
  if (op == "<=")
    return compare(left, right) <= 0;
  if (op == ">")
    return compare(left, right) > 0;
  if (op == ">=")
    return compare(left, right) >= 0;
  auto x = number(left), y = number(right);
  if (!x || !y || (op == "/" && *y == 0.0))
    return std::monostate{};
  if (op != "/" &&
      (std::holds_alternative<Decimal>(left) ||
       std::holds_alternative<Decimal>(right)) &&
      !std::holds_alternative<double>(left) &&
      !std::holds_alternative<double>(right)) {
    const auto units = [](const Scalar &value, std::int64_t &output) -> bool {
      if (auto *decimal = std::get_if<Decimal>(&value)) {
        output = decimal->units;
        return true;
      }
      return !__builtin_mul_overflow(std::get<std::int64_t>(value),
                                     std::int64_t{100}, &output);
    };
    std::int64_t a, b, result;
    if (!units(left, a) || !units(right, b))
      return std::monostate{};
    if (op == "+")
      return __builtin_add_overflow(a, b, &result) ? Scalar{std::monostate{}}
                                                   : Scalar{Decimal{result}};
    if (op == "-")
      return __builtin_sub_overflow(a, b, &result) ? Scalar{std::monostate{}}
                                                   : Scalar{Decimal{result}};
    const auto product =
        static_cast<__int128>(a) * static_cast<__int128>(b) / 100;
    return product < std::numeric_limits<std::int64_t>::min() ||
                   product > std::numeric_limits<std::int64_t>::max()
               ? Scalar{std::monostate{}}
               : Scalar{Decimal{static_cast<std::int64_t>(product)}};
  }
  if (op != "/" && std::holds_alternative<std::int64_t>(left) &&
      std::holds_alternative<std::int64_t>(right)) {
    const auto a = std::get<std::int64_t>(left);
    const auto b = std::get<std::int64_t>(right);
    std::int64_t result{};
    const bool overflow = op == "+"   ? __builtin_add_overflow(a, b, &result)
                          : op == "-" ? __builtin_sub_overflow(a, b, &result)
                                      : __builtin_mul_overflow(a, b, &result);
    return overflow ? Scalar{std::monostate{}} : Scalar{result};
  }
  return op == "+"   ? Scalar{*x + *y}
         : op == "-" ? Scalar{*x - *y}
         : op == "*" ? Scalar{*x * *y}
                     : Scalar{*x / *y};
}

} // namespace dremel
