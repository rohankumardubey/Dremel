#pragma once

#include "../sql/parser.hpp"

namespace dremel {

struct Table;
static std::shared_ptr<Table> load_arrow_ipc(const std::string &path);
static std::shared_ptr<Table> load_parquet(const std::string &path);

struct Dictionary {
  std::vector<std::string> values;
  std::unordered_map<std::string, std::uint32_t> ids;
  std::uint32_t insert(const std::string &s) {
    if (auto i = ids.find(s); i != ids.end())
      return i->second;
    auto id = static_cast<std::uint32_t>(values.size());
    values.push_back(s);
    ids.emplace(s, id);
    return id;
  }
};
template <class T>
static std::vector<T> read_vector(std::istream &in, std::size_t count) {
  static_assert(std::endian::native == std::endian::little,
                "DREMCOL1 currently requires a little-endian host");
  std::vector<T> values(count);
  in.read(reinterpret_cast<char *>(values.data()),
          static_cast<std::streamsize>(count * sizeof(T)));
  if (!in)
    throw std::runtime_error("truncated column store");
  return values;
}
template <class T> static T read_scalar(std::istream &in) {
  auto values = read_vector<T>(in, 1);
  return values[0];
}
static std::pair<Dictionary, std::vector<std::uint32_t>>
read_dictionary(std::istream &in, std::size_t rows) {
  Dictionary dictionary;
  const auto count = read_scalar<std::uint32_t>(in);
  for (std::uint32_t i = 0; i < count; ++i) {
    const auto length = read_scalar<std::uint32_t>(in);
    std::string value(length, '\0');
    in.read(value.data(), length);
    if (!in)
      throw std::runtime_error("truncated dictionary");
    dictionary.insert(value);
  }
  return {std::move(dictionary), read_vector<std::uint32_t>(in, rows)};
}
struct Table {
  std::vector<std::int64_t> event_id, user_id, timestamp, duration, bytes,
      campaign;
  std::vector<double> score;
  std::vector<std::uint32_t> country, device, event_type;
  std::vector<std::uint8_t> success, campaign_def;
  Dictionary country_dict, device_dict, event_dict;
  std::size_t size() const { return event_id.size(); }
  std::size_t approximate_bytes() const {
    std::size_t strings = 0;
    for (auto *d : {&country_dict, &device_dict, &event_dict})
      for (auto &v : d->values)
        strings += v.size();
    return size() * (6 * 8 + 3 * 4 + 8 + 2) + strings;
  }
  std::size_t group_upper_bound(const std::vector<std::string> &columns) const {
    std::size_t bound = 1;
    for (auto &qualified : columns) {
      const auto c = base_name(qualified);
      std::size_t n = c == "country"      ? country_dict.values.size()
                      : c == "device"     ? device_dict.values.size()
                      : c == "event_type" ? event_dict.values.size()
                      : c == "success"    ? 2
                                          : size();
      bound = std::min(size(), bound > size() / std::max<std::size_t>(1, n)
                                   ? size()
                                   : bound * n);
    }
    return bound;
  }
  static std::shared_ptr<Table> load(const std::string &path) {
    if (path.ends_with(".dremel"))
      return load_binary(path);
    if (path.ends_with(".arrow"))
      return load_arrow_ipc(path);
    if (path.ends_with(".parquet"))
      return load_parquet(path);
    return load_csv(path);
  }
  static std::shared_ptr<Table> load_binary(const std::string &path) {
    std::ifstream in(path, std::ios::binary);
    if (!in)
      throw std::runtime_error("cannot open " + path);
    std::array<char, 8> magic{};
    in.read(magic.data(), magic.size());
    if (std::string_view(magic.data(), magic.size()) != "DREMCOL1")
      throw std::runtime_error("invalid column-store magic");
    const auto version = read_scalar<std::uint32_t>(in);
    if (version != 1)
      throw std::runtime_error("unsupported column-store version");
    const auto rows = static_cast<std::size_t>(read_scalar<std::uint64_t>(in));
    auto t = std::make_shared<Table>();
    t->event_id = read_vector<std::int64_t>(in, rows);
    t->user_id = read_vector<std::int64_t>(in, rows);
    t->timestamp = read_vector<std::int64_t>(in, rows);
    std::tie(t->country_dict, t->country) = read_dictionary(in, rows);
    std::tie(t->device_dict, t->device) = read_dictionary(in, rows);
    std::tie(t->event_dict, t->event_type) = read_dictionary(in, rows);
    t->duration = read_vector<std::int64_t>(in, rows);
    t->bytes = read_vector<std::int64_t>(in, rows);
    t->score = read_vector<double>(in, rows);
    t->success = read_vector<std::uint8_t>(in, rows);
    t->campaign = read_vector<std::int64_t>(in, rows);
    t->campaign_def = read_vector<std::uint8_t>(in, rows);
    return t;
  }
  static std::shared_ptr<Table> load_csv(const std::string &path) {
    auto t = std::make_shared<Table>();
    std::ifstream in(path);
    if (!in)
      throw std::runtime_error("cannot open " + path);
    std::string line;
    std::getline(in, line);
    while (std::getline(in, line)) {
      std::vector<std::string> p;
      std::size_t s = 0;
      for (;;) {
        auto c = line.find(',', s);
        p.push_back(line.substr(s, c == std::string::npos ? c : c - s));
        if (c == std::string::npos)
          break;
        s = c + 1;
      }
      if (p.size() != 11)
        throw std::runtime_error("bad CSV");
      t->event_id.push_back(std::stoll(p[0]));
      t->user_id.push_back(std::stoll(p[1]));
      t->timestamp.push_back(std::stoll(p[2]));
      t->country.push_back(t->country_dict.insert(p[3]));
      t->device.push_back(t->device_dict.insert(p[4]));
      t->event_type.push_back(t->event_dict.insert(p[5]));
      t->duration.push_back(std::stoll(p[6]));
      t->bytes.push_back(std::stoll(p[7]));
      t->score.push_back(std::stod(p[8]));
      t->success.push_back(p[9] == "true");
      if (p[10].empty()) {
        t->campaign.push_back(0);
        t->campaign_def.push_back(0);
      } else {
        t->campaign.push_back(std::stoll(p[10]));
        t->campaign_def.push_back(1);
      }
    }
    return t;
  }
  std::optional<std::uint32_t> dict_id(const std::string &c,
                                       const std::string &s) const {
    const auto column = base_name(c);
    const auto *d = column == "country"      ? &country_dict
                    : column == "device"     ? &device_dict
                    : column == "event_type" ? &event_dict
                                             : nullptr;
    if (!d)
      return {};
    auto i = d->ids.find(s);
    return i == d->ids.end() ? std::optional<std::uint32_t>{} : i->second;
  }
  Scalar scalar(const std::string &c, std::size_t i) const {
    const auto column = base_name(c);
    if (column == "event_id")
      return event_id[i];
    if (column == "user_id")
      return user_id[i];
    if (column == "timestamp")
      return timestamp[i];
    if (column == "country")
      return country_dict.values[country[i]];
    if (column == "device")
      return device_dict.values[device[i]];
    if (column == "event_type")
      return event_dict.values[event_type[i]];
    if (column == "duration_ms")
      return duration[i];
    if (column == "bytes")
      return bytes[i];
    if (column == "score")
      return score[i];
    if (column == "success")
      return success[i] != 0;
    if (column == "campaign_id")
      return campaign_def[i] ? Scalar{campaign[i]} : Scalar{std::monostate{}};
    return std::monostate{};
  }
  std::uint64_t raw_key(const std::string &c, std::size_t i) const {
    const auto column = base_name(c);
    if (column == "country")
      return country[i];
    if (column == "device")
      return device[i];
    if (column == "event_type")
      return event_type[i];
    if (column == "success")
      return success[i];
    if (column == "campaign_id")
      return static_cast<std::uint64_t>(campaign[i]);
    auto v = scalar(c, i);
    return std::holds_alternative<std::int64_t>(v)
               ? static_cast<std::uint64_t>(std::get<std::int64_t>(v))
               : 0;
  }
  Scalar key_scalar(const std::string &c, std::uint64_t k) const {
    const auto column = base_name(c);
    if (column == "country")
      return country_dict.values[k];
    if (column == "device")
      return device_dict.values[k];
    if (column == "event_type")
      return event_dict.values[k];
    if (column == "success")
      return k != 0;
    return static_cast<std::int64_t>(k);
  }
};

static std::int64_t parse_decimal_units(const std::string &input) {
  const bool negative = !input.empty() && input.front() == '-';
  const auto value = negative ? input.substr(1) : input;
  const auto dot = value.find('.');
  if (dot != std::string::npos && value.find('.', dot + 1) != std::string::npos)
    throw std::runtime_error("bad decimal");
  const auto whole_text =
      dot == std::string::npos ? value : value.substr(0, dot);
  auto fraction =
      dot == std::string::npos ? std::string{} : value.substr(dot + 1);
  if (fraction.size() > 2)
    throw std::runtime_error(
        "DECIMAL(18,2) requires at most two fractional digits");
  while (fraction.size() < 2)
    fraction.push_back('0');
  const auto whole = std::stoll(whole_text);
  const auto fractional = fraction.empty() ? 0 : std::stoll(fraction);
  std::int64_t units;
  if (__builtin_mul_overflow(whole, std::int64_t{100}, &units) ||
      __builtin_add_overflow(units, fractional, &units))
    throw std::runtime_error("decimal overflow");
  return negative ? -units : units;
}
static std::string decimal_text(std::int64_t units) {
  const auto magnitude = units < 0
                             ? static_cast<std::uint64_t>(-(units + 1)) + 1
                             : static_cast<std::uint64_t>(units);
  std::ostringstream output;
  if (units < 0)
    output << '-';
  output << magnitude / 100 << '.' << std::setw(2) << std::setfill('0')
         << magnitude % 100;
  return output.str();
}

struct UsersTable {
  std::vector<std::int64_t> user_id;
  std::vector<std::string> segment, signup_date, region;
  std::vector<std::int64_t> lifetime_value;
  std::vector<bool> active;
};
struct CampaignsTable {
  std::vector<std::int64_t> campaign_id;
  std::vector<std::string> campaign_name, start_date, end_date, channel;
  std::vector<std::int64_t> budget;
  std::unordered_map<std::int64_t, std::vector<std::size_t>> index;
};
static UsersTable load_users_interoperable(const std::string &path);
static CampaignsTable load_campaigns_interoperable(const std::string &path);
struct Catalog {
  std::shared_ptr<Table> events;
  UsersTable users;
  CampaignsTable campaigns;

  static Catalog load(const std::string &events_path,
                      std::shared_ptr<Table> events) {
    Catalog catalog;
    catalog.events = std::move(events);
    const auto directory = std::filesystem::path(events_path).parent_path();
    if (events_path.ends_with(".arrow") || events_path.ends_with(".parquet")) {
      const auto event_name =
          std::filesystem::path(events_path).filename().string();
      auto companion = [&](const std::string &table) {
        auto name = event_name;
        name.replace(0, std::string("events").size(), table);
        return (directory / name).string();
      };
      catalog.users = load_users_interoperable(companion("users"));
      catalog.campaigns = load_campaigns_interoperable(companion("campaigns"));
      return catalog;
    }
    auto fields = [](const std::string &line) {
      std::vector<std::string> result;
      std::size_t start = 0;
      for (;;) {
        const auto comma = line.find(',', start);
        result.push_back(line.substr(
            start, comma == std::string::npos ? comma : comma - start));
        if (comma == std::string::npos)
          return result;
        start = comma + 1;
      }
    };
    std::ifstream users_file(directory / "users.csv");
    if (!users_file)
      throw std::runtime_error("cannot open users.csv");
    std::string line;
    std::getline(users_file, line);
    while (std::getline(users_file, line)) {
      auto value = fields(line);
      if (value.size() != 6)
        throw std::runtime_error("bad users row");
      catalog.users.user_id.push_back(std::stoll(value[0]));
      catalog.users.segment.push_back(std::move(value[1]));
      catalog.users.signup_date.push_back(std::move(value[2]));
      catalog.users.lifetime_value.push_back(parse_decimal_units(value[3]));
      catalog.users.region.push_back(std::move(value[4]));
      catalog.users.active.push_back(value[5] == "true");
    }
    std::ifstream campaigns_file(directory / "campaigns.csv");
    if (!campaigns_file)
      throw std::runtime_error("cannot open campaigns.csv");
    std::getline(campaigns_file, line);
    while (std::getline(campaigns_file, line)) {
      auto value = fields(line);
      if (value.size() != 6)
        throw std::runtime_error("bad campaigns row");
      const auto campaign_id = std::stoll(value[0]);
      catalog.campaigns.index[campaign_id].push_back(
          catalog.campaigns.campaign_id.size());
      catalog.campaigns.campaign_id.push_back(campaign_id);
      catalog.campaigns.campaign_name.push_back(std::move(value[1]));
      catalog.campaigns.budget.push_back(parse_decimal_units(value[2]));
      catalog.campaigns.start_date.push_back(std::move(value[3]));
      catalog.campaigns.end_date.push_back(std::move(value[4]));
      catalog.campaigns.channel.push_back(std::move(value[5]));
    }
    return catalog;
  }
};
struct RelRow {
  std::optional<std::size_t> event, user, campaign;
};

static const std::vector<std::string> &table_columns(const std::string &table) {
  static const std::vector<std::string> events{
      "event_id", "user_id",    "timestamp",   "country",
      "device",   "event_type", "duration_ms", "bytes",
      "score",    "success",    "campaign_id"};
  static const std::vector<std::string> users{"user_id",     "segment",
                                              "signup_date", "lifetime_value",
                                              "region",      "active"};
  static const std::vector<std::string> campaigns{
      "campaign_id", "campaign_name", "budget",
      "start_date",  "end_date",      "channel"};
  static const std::vector<std::string> empty;
  return table == "events"      ? events
         : table == "users"     ? users
         : table == "campaigns" ? campaigns
                                : empty;
}
using Bindings = std::unordered_map<std::string, std::string>;
static Bindings query_bindings(const Query &query) {
  Bindings bindings;
  auto add = [&](const TableRef &table) {
    if (table_columns(table.name).empty())
      throw std::runtime_error("unknown table " + table.name);
    if (bindings.contains(table.alias))
      throw std::runtime_error("duplicate table alias " + table.alias);
    bindings.emplace(table.alias, table.name);
    bindings.try_emplace(table.name, table.name);
  };
  add(query.from);
  for (auto &join : query.joins)
    add(join.table);
  return bindings;
}
static std::pair<std::string, std::string>
resolve_column(const std::string &column, const Bindings &bindings) {
  if (const auto dot = column.find('.'); dot != std::string::npos) {
    const auto qualifier = column.substr(0, dot);
    const auto name = column.substr(dot + 1);
    auto binding = bindings.find(qualifier);
    if (binding == bindings.end())
      throw std::runtime_error("unknown table or alias " + qualifier);
    if (std::find(table_columns(binding->second).begin(),
                  table_columns(binding->second).end(),
                  name) == table_columns(binding->second).end())
      throw std::runtime_error("unknown column " + column);
    return {binding->second, name};
  }
  std::set<std::string> matches;
  for (auto &[_, table] : bindings)
    if (std::find(table_columns(table).begin(), table_columns(table).end(),
                  column) != table_columns(table).end())
      matches.insert(table);
  if (matches.empty())
    throw std::runtime_error("unknown column " + column);
  if (matches.size() != 1)
    throw std::runtime_error("ambiguous column " + column);
  return {*matches.begin(), column};
}
enum class SqlType {
  null_value,
  boolean,
  integer,
  decimal,
  floating,
  string,
  date,
  timestamp,
  interval,
  unknown
};
static SqlType column_type(const std::string &table,
                           const std::string &column) {
  if ((table == "events" && (column == "event_id" || column == "user_id" ||
                             column == "duration_ms" || column == "bytes" ||
                             column == "campaign_id")) ||
      (table == "users" && column == "user_id") ||
      (table == "campaigns" && column == "campaign_id"))
    return SqlType::integer;
  if (table == "events" && column == "timestamp")
    return SqlType::timestamp;
  if (table == "events" && column == "score")
    return SqlType::floating;
  if ((table == "events" && column == "success") ||
      (table == "users" && column == "active"))
    return SqlType::boolean;
  if ((table == "users" && column == "lifetime_value") ||
      (table == "campaigns" && column == "budget"))
    return SqlType::decimal;
  if ((table == "users" && column == "signup_date") ||
      (table == "campaigns" &&
       (column == "start_date" || column == "end_date")))
    return SqlType::date;
  if ((table == "events" &&
       (column == "country" || column == "device" || column == "event_type")) ||
      (table == "users" && (column == "segment" || column == "region")) ||
      (table == "campaigns" &&
       (column == "campaign_name" || column == "channel")))
    return SqlType::string;
  return SqlType::unknown;
}
static bool numeric_type(SqlType type) {
  return type == SqlType::integer || type == SqlType::decimal ||
         type == SqlType::floating;
}
static SqlType common_type(SqlType left, SqlType right) {
  if (left == right)
    return left;
  if (left == SqlType::null_value || left == SqlType::unknown)
    return right;
  if (right == SqlType::null_value || right == SqlType::unknown)
    return left;
  if (numeric_type(left) && numeric_type(right)) {
    if (left == SqlType::floating || right == SqlType::floating)
      return SqlType::floating;
    if (left == SqlType::decimal || right == SqlType::decimal)
      return SqlType::decimal;
    return SqlType::integer;
  }
  throw std::runtime_error("incompatible SQL types");
}
static SqlType infer_type(const ExprPtr &expression, const Bindings &bindings) {
  if (!expression)
    return SqlType::unknown;
  switch (expression->kind) {
  case ExprKind::null:
    return SqlType::null_value;
  case ExprKind::column: {
    auto [table, column] = resolve_column(expression->text, bindings);
    return column_type(table, column);
  }
  case ExprKind::integer:
  case ExprKind::star:
    return SqlType::integer;
  case ExprKind::floating:
    return SqlType::floating;
  case ExprKind::boolean:
  case ExprKind::dict_eq:
  case ExprKind::is_null:
  case ExprKind::exists:
  case ExprKind::in_subquery:
    return SqlType::boolean;
  case ExprKind::string:
    return SqlType::string;
  case ExprKind::scalar_subquery:
    return SqlType::unknown;
  case ExprKind::unary: {
    const auto value = infer_type(expression->left, bindings);
    if (expression->text == "not") {
      if (value != SqlType::boolean && value != SqlType::null_value &&
          value != SqlType::unknown)
        throw std::runtime_error("NOT requires BOOLEAN");
      return SqlType::boolean;
    }
    if (!numeric_type(value) && value != SqlType::null_value &&
        value != SqlType::unknown)
      throw std::runtime_error("unary minus requires numeric operand");
    return value;
  }
  case ExprKind::binary: {
    const auto left = infer_type(expression->left, bindings);
    const auto right = infer_type(expression->right, bindings);
    if (expression->text == "and" || expression->text == "or") {
      const auto valid = [](SqlType type) {
        return type == SqlType::boolean || type == SqlType::null_value ||
               type == SqlType::unknown;
      };
      if (!valid(left) || !valid(right))
        throw std::runtime_error(expression->text +
                                 " requires BOOLEAN operands");
      return SqlType::boolean;
    }
    if (expression->text == "=" || expression->text == "!=" ||
        expression->text == "<" || expression->text == "<=" ||
        expression->text == ">" || expression->text == ">=") {
      (void)common_type(left, right);
      return SqlType::boolean;
    }
    if ((expression->text == "+" || expression->text == "-") &&
        left == SqlType::date && right == SqlType::interval)
      return SqlType::date;
    if ((expression->text == "+" || expression->text == "-") &&
        left == SqlType::timestamp && right == SqlType::interval)
      return SqlType::timestamp;
    if (numeric_type(left) && numeric_type(right))
      return expression->text == "/" ? SqlType::floating
                                     : common_type(left, right);
    if (left == SqlType::null_value || right == SqlType::null_value ||
        left == SqlType::unknown || right == SqlType::unknown)
      return SqlType::unknown;
    throw std::runtime_error("arithmetic has incompatible operand types");
  }
  case ExprKind::function: {
    const auto argument = infer_type(expression->left, bindings);
    if (expression->text == "count" || expression->text == "length")
      return SqlType::integer;
    if (expression->text == "avg")
      return SqlType::floating;
    if (expression->text == "abs") {
      if (!numeric_type(argument) && argument != SqlType::null_value &&
          argument != SqlType::unknown)
        throw std::runtime_error("ABS requires a numeric operand");
      return argument;
    }
    if (expression->text == "sum" || expression->text == "min" ||
        expression->text == "max")
      return argument;
    if (expression->text == "lower" || expression->text == "upper" ||
        expression->text == "substring")
      return SqlType::string;
    if (expression->text == "date")
      return SqlType::date;
    if (expression->text == "timestamp")
      return SqlType::timestamp;
    if (expression->text == "interval")
      return SqlType::interval;
    return SqlType::unknown;
  }
  case ExprKind::call: {
    std::vector<SqlType> types;
    for (auto &argument : expression->args)
      types.push_back(infer_type(argument, bindings));
    if (expression->text == "count" || expression->text == "extract" ||
        expression->text == "length")
      return SqlType::integer;
    if (expression->text == "avg")
      return SqlType::floating;
    if (expression->text == "sum" || expression->text == "min" ||
        expression->text == "max")
      return types.empty() ? SqlType::unknown : types.front();
    if (expression->text == "coalesce") {
      auto result = SqlType::null_value;
      for (auto type : types)
        result = common_type(result, type);
      return result;
    }
    if (expression->text == "nullif") {
      if (types.size() == 2)
        (void)common_type(types[0], types[1]);
      return types.empty() ? SqlType::unknown : types.front();
    }
    if (expression->text == "concat" || expression->text == "substring")
      return SqlType::string;
    if (expression->text == "date")
      return SqlType::date;
    if (expression->text == "timestamp")
      return SqlType::timestamp;
    if (expression->text == "interval")
      return SqlType::interval;
    if (expression->text == "date_trunc")
      return types.size() > 1 ? types[1] : SqlType::unknown;
    return SqlType::unknown;
  }
  case ExprKind::case_when: {
    auto result = infer_type(expression->left, bindings);
    for (auto &[condition, value] : expression->branches) {
      const auto condition_type = infer_type(condition, bindings);
      if (condition_type != SqlType::boolean &&
          condition_type != SqlType::null_value &&
          condition_type != SqlType::unknown)
        throw std::runtime_error("CASE WHEN requires BOOLEAN");
      result = common_type(result, infer_type(value, bindings));
    }
    return result;
  }
  case ExprKind::cast:
    if (expression->text.starts_with("decimal"))
      return SqlType::decimal;
    if (expression->text == "bigint" || expression->text == "int64" ||
        expression->text == "integer")
      return SqlType::integer;
    if (expression->text == "double" || expression->text == "float" ||
        expression->text == "real")
      return SqlType::floating;
    if (expression->text == "boolean" || expression->text == "bool")
      return SqlType::boolean;
    if (expression->text == "date")
      return SqlType::date;
    if (expression->text == "timestamp")
      return SqlType::timestamp;
    return SqlType::string;
  case ExprKind::in_list: {
    const auto value = infer_type(expression->left, bindings);
    for (auto &candidate : expression->args)
      (void)common_type(value, infer_type(candidate, bindings));
    return SqlType::boolean;
  }
  case ExprKind::between: {
    const auto value = infer_type(expression->left, bindings);
    (void)common_type(value, infer_type(expression->args[0], bindings));
    (void)common_type(value, infer_type(expression->args[1], bindings));
    return SqlType::boolean;
  }
  case ExprKind::like:
    (void)common_type(infer_type(expression->left, bindings), SqlType::string);
    (void)common_type(infer_type(expression->right, bindings), SqlType::string);
    return SqlType::boolean;
  case ExprKind::window:
    if (expression->text == "row_number" || expression->text == "rank" ||
        expression->text == "dense_rank" || expression->text == "count")
      return SqlType::integer;
    if (expression->text == "avg")
      return SqlType::floating;
    return expression->args.empty()
               ? SqlType::unknown
               : infer_type(expression->args.front(), bindings);
  }
  return SqlType::unknown;
}
static void bind_expr(const ExprPtr &expression, const Bindings &bindings) {
  if (!expression)
    return;
  if (expression->kind == ExprKind::column ||
      expression->kind == ExprKind::dict_eq)
    (void)resolve_column(expression->text, bindings);
  if (expression->kind == ExprKind::cast) {
    const auto &type = expression->text;
    static const std::set<std::string> supported{
        "bigint", "int64",   "integer",   "double", "float",
        "real",   "varchar", "string",    "text",   "boolean",
        "bool",   "date",    "timestamp", "decimal"};
    if (!supported.contains(type) && !type.starts_with("decimal("))
      throw std::runtime_error("unsupported CAST type " + type);
  }
  bind_expr(expression->left, bindings);
  bind_expr(expression->right, bindings);
  for (auto &argument : expression->args)
    bind_expr(argument, bindings);
  for (auto &[condition, value] : expression->branches) {
    bind_expr(condition, bindings);
    bind_expr(value, bindings);
  }
  for (auto &column : expression->partition_by)
    (void)resolve_column(column, bindings);
  for (auto &order : expression->window_order_by)
    (void)resolve_column(order.key, bindings);
}
static Bindings bind_query(const Query &query) {
  auto bindings = query_bindings(query);
  for (auto &item : query.select) {
    bind_expr(item.expr, bindings);
    (void)infer_type(item.expr, bindings);
  }
  bind_expr(query.filter, bindings);
  if (query.filter) {
    const auto type = infer_type(query.filter, bindings);
    if (type != SqlType::boolean && type != SqlType::null_value &&
        type != SqlType::unknown)
      throw std::runtime_error("WHERE requires BOOLEAN");
  }
  for (auto &join : query.joins)
    bind_expr(join.on, bindings);
  for (auto &join : query.joins)
    if (join.on) {
      const auto type = infer_type(join.on, bindings);
      if (type != SqlType::boolean && type != SqlType::null_value &&
          type != SqlType::unknown)
        throw std::runtime_error("JOIN ON requires BOOLEAN");
    }
  bind_expr(query.having, bindings);
  if (query.having) {
    const auto type = infer_type(query.having, bindings);
    if (type != SqlType::boolean && type != SqlType::null_value &&
        type != SqlType::unknown)
      throw std::runtime_error("HAVING requires BOOLEAN");
  }
  for (auto &group : query.group_by)
    (void)resolve_column(group, bindings);
  return bindings;
}
static Scalar relation_scalar(const Catalog &catalog, const RelRow &row,
                              const std::string &table,
                              const std::string &column) {
  if (table == "events")
    return row.event ? catalog.events->scalar(column, *row.event)
                     : Scalar{std::monostate{}};
  if (table == "users" && row.user) {
    const auto index = *row.user;
    if (column == "user_id")
      return catalog.users.user_id[index];
    if (column == "segment")
      return catalog.users.segment[index];
    if (column == "signup_date")
      return catalog.users.signup_date[index];
    if (column == "lifetime_value")
      return Decimal{catalog.users.lifetime_value[index]};
    if (column == "region")
      return catalog.users.region[index];
    if (column == "active")
      return static_cast<bool>(catalog.users.active[index]);
  }
  if (table == "campaigns" && row.campaign) {
    const auto index = *row.campaign;
    if (column == "campaign_id")
      return catalog.campaigns.campaign_id[index];
    if (column == "campaign_name")
      return catalog.campaigns.campaign_name[index];
    if (column == "budget")
      return Decimal{catalog.campaigns.budget[index]};
    if (column == "start_date")
      return catalog.campaigns.start_date[index];
    if (column == "end_date")
      return catalog.campaigns.end_date[index];
    if (column == "channel")
      return catalog.campaigns.channel[index];
  }
  return std::monostate{};
}
static void enforce_result_limit(std::size_t limit, const Query &q,
                                 const Table &t) {
  if (!limit)
    return;
  const bool aggregate = !q.group_by.empty() ||
                         std::any_of(q.select.begin(), q.select.end(),
                                     [](auto &s) { return is_agg(s.expr); });
  const auto upper =
      aggregate ? (q.group_by.empty() ? 1 : t.group_upper_bound(q.group_by))
                : std::min(t.size(), q.limit.value_or(t.size()));
  if (upper > limit)
    throw std::runtime_error(
        "RESOURCE_EXHAUSTED result upper bound " + std::to_string(upper) +
        " exceeds max-result-rows " + std::to_string(limit));
}

} // namespace dremel
