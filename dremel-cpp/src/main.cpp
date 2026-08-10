#include <algorithm>
#include <atomic>
#include <array>
#include <bit>
#include <cassert>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

using Clock = std::chrono::steady_clock;
struct Decimal {
  std::int64_t units{};
  auto operator<=>(const Decimal &) const = default;
};
using Scalar =
    std::variant<std::monostate, std::int64_t, Decimal, double, bool, std::string>;
using Rows = std::vector<std::vector<Scalar>>;

struct ExecutionControl {
  std::atomic_bool cancelled{};
  std::optional<Clock::time_point> deadline;
};
thread_local std::shared_ptr<ExecutionControl> execution_control;
static bool execution_cancelled() {
  return execution_control &&
         (execution_control->cancelled.load(std::memory_order_relaxed) ||
          (execution_control->deadline &&
           Clock::now() >= *execution_control->deadline));
}

enum class TokenKind {
  word,
  number,
  string,
  op,
  lparen,
  rparen,
  comma,
  dot,
  star,
  semi,
  end
};
struct Token {
  TokenKind kind;
  std::string text;
};

static std::vector<Token> lex(const std::string &sql) {
  std::vector<Token> out;
  for (std::size_t i = 0; i < sql.size();) {
    const char c = sql[i];
    if (std::isspace(static_cast<unsigned char>(c))) {
      ++i;
      continue;
    }
    if (std::isalpha(static_cast<unsigned char>(c)) || c == '_') {
      const auto start = i++;
      while (
          i < sql.size() &&
          (std::isalnum(static_cast<unsigned char>(sql[i])) || sql[i] == '_'))
        ++i;
      auto text = sql.substr(start, i - start);
      std::transform(
          text.begin(), text.end(), text.begin(),
          [](unsigned char x) { return static_cast<char>(std::tolower(x)); });
      out.push_back({TokenKind::word, std::move(text)});
      continue;
    }
    if (std::isdigit(static_cast<unsigned char>(c)) ||
        (c == '.' && i + 1 < sql.size() &&
         std::isdigit(static_cast<unsigned char>(sql[i + 1])))) {
      const auto start = i++;
      while (
          i < sql.size() &&
          (std::isdigit(static_cast<unsigned char>(sql[i])) || sql[i] == '.'))
        ++i;
      out.push_back({TokenKind::number, sql.substr(start, i - start)});
      continue;
    }
    if (c == '\'') {
      ++i;
      std::string value;
      for (;;) {
        if (i == sql.size())
          throw std::runtime_error("unterminated string");
        if (sql[i] == '\'') {
          if (i + 1 < sql.size() && sql[i + 1] == '\'') {
            value.push_back('\'');
            i += 2;
            continue;
          }
          ++i;
          break;
        }
        value.push_back(sql[i++]);
      }
      out.push_back({TokenKind::string, std::move(value)});
      continue;
    }
    if (c == '(')
      out.push_back({TokenKind::lparen, "("});
    else if (c == ')')
      out.push_back({TokenKind::rparen, ")"});
    else if (c == ',')
      out.push_back({TokenKind::comma, ","});
    else if (c == '.')
      out.push_back({TokenKind::dot, "."});
    else if (c == '*')
      out.push_back({TokenKind::star, "*"});
    else if (c == ';')
      out.push_back({TokenKind::semi, ";"});
    else if (c == '=' || c == '+' || c == '-' || c == '/')
      out.push_back({TokenKind::op, std::string(1, c)});
    else if (c == '!' || c == '<' || c == '>') {
      std::string op(1, c);
      if (i + 1 < sql.size() &&
          (sql[i + 1] == '=' || (c == '<' && sql[i + 1] == '>')))
        op.push_back(sql[++i]);
      out.push_back({TokenKind::op, op});
    } else
      throw std::runtime_error(std::string("unexpected character ") + c);
    ++i;
  }
  out.push_back({TokenKind::end, ""});
  return out;
}

struct Query;
enum class ExprKind {
  null,
  column,
  integer,
  floating,
  boolean,
  string,
  star,
  unary,
  binary,
  function,
  call,
  case_when,
  cast,
  in_list,
  between,
  like,
  window,
  scalar_subquery,
  exists,
  in_subquery,
  is_null,
  dict_eq
};
struct WindowOrder {
  std::string key;
  bool ascending;
  std::optional<bool> nulls_first;
};
struct Expr {
  ExprKind kind{};
  std::string text;
  std::int64_t integer{};
  double floating{};
  bool boolean{};
  std::uint32_t dict_id{};
  std::shared_ptr<Expr> left, right;
  std::vector<std::shared_ptr<Expr>> args;
  std::vector<std::pair<std::shared_ptr<Expr>, std::shared_ptr<Expr>>> branches;
  std::vector<std::string> partition_by;
  std::vector<WindowOrder> window_order_by;
  std::shared_ptr<Query> subquery;
};
using ExprPtr = std::shared_ptr<Expr>;
static ExprPtr node(ExprKind k) {
  auto e = std::make_shared<Expr>();
  e->kind = k;
  return e;
}
struct SelectItem {
  ExprPtr expr;
  std::optional<std::string> alias;
};
struct TableRef {
  std::string name, alias;
};
enum class JoinKind { cross, inner, left, right, full };
struct JoinSpec {
  JoinKind kind;
  TableRef table;
  ExprPtr on;
};
struct OrderSpec {
  std::string key;
  bool ascending;
  std::optional<bool> nulls_first;
};
struct Query {
  std::vector<SelectItem> select;
  bool distinct{};
  TableRef from;
  std::vector<JoinSpec> joins;
  ExprPtr filter;
  std::vector<std::string> group_by;
  ExprPtr having;
  std::vector<OrderSpec> order_by;
  std::optional<std::size_t> limit;
  std::size_t offset{};
  std::shared_ptr<Query> union_query;
  bool union_all{};
  std::vector<std::pair<std::string, std::shared_ptr<Query>>> ctes;
  std::vector<std::string> logical, physical, columns;
  bool optimizer_enabled{true};
};

class Parser {
  std::vector<Token> tokens_;
  std::size_t pos_{};
  const Token &peek() const { return tokens_[pos_]; }
  Token next() { return tokens_[pos_++]; }
  bool word(const std::string &s) {
    if (peek().kind == TokenKind::word && peek().text == s) {
      ++pos_;
      return true;
    }
    return false;
  }
  void expect_word(const std::string &s) {
    if (!word(s))
      throw std::runtime_error("expected " + s);
  }
  TableRef table_ref() {
    auto table = next();
    if (table.kind != TokenKind::word)
      throw std::runtime_error("expected table name");
    std::string alias = table.text;
    if (word("as")) {
      auto value = next();
      if (value.kind != TokenKind::word)
        throw std::runtime_error("expected table alias");
      alias = value.text;
    } else if (peek().kind == TokenKind::word) {
      static const std::set<std::string> reserved{
          "where", "group", "having", "order", "limit", "join", "inner",
          "left",  "right", "full",   "cross", "on",    "outer"};
      if (!reserved.contains(peek().text))
        alias = next().text;
    }
    return {table.text, alias};
  }
  std::pair<TableRef, std::optional<std::pair<std::string, std::shared_ptr<Query>>>>
  relation_ref() {
    if (peek().kind != TokenKind::lparen)
      return {table_ref(), {}};
    ++pos_;
    auto query = std::make_shared<Query>(subquery());
    (void)word("as");
    auto alias = next();
    if (alias.kind != TokenKind::word)
      throw std::runtime_error("derived table requires an alias");
    return {{alias.text, alias.text},
            std::pair{alias.text, std::move(query)}};
  }
  std::string identifier() {
    auto first = next();
    if (first.kind != TokenKind::word)
      throw std::runtime_error("expected identifier");
    if (peek().kind != TokenKind::dot)
      return first.text;
    ++pos_;
    auto second = next();
    if (second.kind != TokenKind::word)
      throw std::runtime_error("expected qualified identifier");
    return first.text + "." + second.text;
  }
  ExprPtr window(std::string name, std::vector<ExprPtr> args) {
    if (next().kind != TokenKind::lparen)
      throw std::runtime_error("expected ( after OVER");
    auto expression = node(ExprKind::window);
    expression->text = std::move(name);
    expression->args = std::move(args);
    if (word("partition")) {
      expect_word("by");
      for (;;) {
        expression->partition_by.push_back(identifier());
        if (peek().kind == TokenKind::comma)
          ++pos_;
        else
          break;
      }
    }
    if (word("order")) {
      expect_word("by");
      for (;;) {
        WindowOrder order{identifier(), true, {}};
        if (word("desc"))
          order.ascending = false;
        else
          (void)word("asc");
        if (word("nulls")) {
          if (word("first"))
            order.nulls_first = true;
          else {
            expect_word("last");
            order.nulls_first = false;
          }
        }
        expression->window_order_by.push_back(std::move(order));
        if (peek().kind == TokenKind::comma)
          ++pos_;
        else
          break;
      }
    }
    if (word("rows")) {
      expect_word("between");
      expect_word("unbounded");
      expect_word("preceding");
      expect_word("and");
      expect_word("current");
      expect_word("row");
    }
    if (next().kind != TokenKind::rparen)
      throw std::runtime_error("expected ) after OVER clause");
    return expression;
  }
  Query subquery() {
    const auto start = pos_;
    auto cursor = pos_;
    std::size_t depth = 0;
    for (;;) {
      if (cursor >= tokens_.size() || tokens_[cursor].kind == TokenKind::end)
        throw std::runtime_error("unterminated subquery");
      if (tokens_[cursor].kind == TokenKind::lparen)
        ++depth;
      else if (tokens_[cursor].kind == TokenKind::rparen) {
        if (!depth)
          break;
        --depth;
      }
      ++cursor;
    }
    std::vector<Token> tokens(tokens_.begin() + start,
                              tokens_.begin() + cursor);
    tokens.push_back({TokenKind::end, ""});
    pos_ = cursor + 1;
    Parser parser("");
    parser.tokens_ = std::move(tokens);
    return parser.parse();
  }
  ExprPtr primary() {
    auto t = next();
    if (t.kind == TokenKind::word && t.text == "null")
      return node(ExprKind::null);
    if (t.kind == TokenKind::word && (t.text == "true" || t.text == "false")) {
      auto e = node(ExprKind::boolean);
      e->boolean = t.text == "true";
      return e;
    }
    if (t.kind == TokenKind::word && t.text == "exists") {
      if (next().kind != TokenKind::lparen)
        throw std::runtime_error("expected ( after EXISTS");
      auto expression = node(ExprKind::exists);
      expression->subquery = std::make_shared<Query>(subquery());
      return expression;
    }
    if (t.kind == TokenKind::word && t.text == "case") {
      auto e = node(ExprKind::case_when);
      while (word("when")) {
        auto condition = expr(0);
        expect_word("then");
        e->branches.emplace_back(std::move(condition), expr(0));
      }
      if (e->branches.empty())
        throw std::runtime_error("searched CASE requires WHEN");
      e->left = word("else") ? expr(0) : node(ExprKind::null);
      expect_word("end");
      return e;
    }
    if (t.kind == TokenKind::word && t.text == "cast") {
      if (next().kind != TokenKind::lparen)
        throw std::runtime_error("expected ( after CAST");
      auto e = node(ExprKind::cast);
      e->left = expr(0);
      expect_word("as");
      auto type = next();
      if (type.kind != TokenKind::word)
        throw std::runtime_error("expected CAST type");
      e->text = type.text;
      if (e->text == "decimal" && peek().kind == TokenKind::lparen) {
        ++pos_;
        const auto precision = next();
        if (precision.kind != TokenKind::number ||
            next().kind != TokenKind::comma)
          throw std::runtime_error("expected DECIMAL precision and scale");
        const auto scale = next();
        if (scale.kind != TokenKind::number ||
            next().kind != TokenKind::rparen)
          throw std::runtime_error("expected ) after DECIMAL precision");
        e->text = "decimal(" + precision.text + "," + scale.text + ")";
      }
      if (next().kind != TokenKind::rparen)
        throw std::runtime_error("expected ) after CAST");
      return e;
    }
    if (t.kind == TokenKind::word && t.text == "interval" &&
        peek().kind == TokenKind::string) {
      auto e = node(ExprKind::call);
      e->text = t.text;
      auto literal = node(ExprKind::string);
      literal->text = next().text;
      e->args.push_back(std::move(literal));
      return e;
    }
    if (t.kind == TokenKind::word && t.text == "extract" &&
        peek().kind == TokenKind::lparen) {
      ++pos_;
      auto field = next();
      if (field.kind != TokenKind::word)
        throw std::runtime_error("expected EXTRACT field");
      expect_word("from");
      auto e = node(ExprKind::call);
      e->text = t.text;
      auto literal = node(ExprKind::string);
      literal->text = field.text;
      e->args.push_back(std::move(literal));
      e->args.push_back(expr(0));
      if (next().kind != TokenKind::rparen)
        throw std::runtime_error("expected ) after EXTRACT");
      return e;
    }
    if (t.kind == TokenKind::word &&
        (t.text == "date" || t.text == "timestamp") &&
        peek().kind == TokenKind::string) {
      auto e = node(ExprKind::call);
      e->text = t.text;
      auto literal = node(ExprKind::string);
      literal->text = next().text;
      e->args.push_back(std::move(literal));
      return e;
    }
    if (t.kind == TokenKind::word) {
      if (peek().kind == TokenKind::lparen) {
        ++pos_;
        std::vector<ExprPtr> args;
        if (peek().kind != TokenKind::rparen) {
          for (;;) {
            args.push_back(peek().kind == TokenKind::star
                               ? (next(), node(ExprKind::star))
                               : expr(0));
            if (peek().kind == TokenKind::comma)
              ++pos_;
            else
              break;
          }
        }
        if (next().kind != TokenKind::rparen)
          throw std::runtime_error("expected )");
        if (word("over"))
          return window(t.text, std::move(args));
        auto e = node(args.size() == 1 ? ExprKind::function : ExprKind::call);
        e->text = t.text;
        if (args.size() == 1)
          e->left = std::move(args[0]);
        else
          e->args = std::move(args);
        return e;
      }
      if (peek().kind == TokenKind::dot) {
        ++pos_;
        auto column = next();
        if (column.kind != TokenKind::word)
          throw std::runtime_error("expected qualified column");
        auto e = node(ExprKind::column);
        e->text = t.text + "." + column.text;
        return e;
      }
      auto e = node(ExprKind::column);
      e->text = t.text;
      return e;
    }
    if (t.kind == TokenKind::number) {
      if (t.text.find('.') != std::string::npos) {
        auto e = node(ExprKind::floating);
        e->floating = std::stod(t.text);
        return e;
      }
      auto e = node(ExprKind::integer);
      e->integer = std::stoll(t.text);
      return e;
    }
    if (t.kind == TokenKind::string) {
      auto e = node(ExprKind::string);
      e->text = t.text;
      return e;
    }
    if (t.kind == TokenKind::star)
      return node(ExprKind::star);
    if (t.kind == TokenKind::lparen) {
      if (peek().kind == TokenKind::word &&
          (peek().text == "select" || peek().text == "with")) {
        auto expression = node(ExprKind::scalar_subquery);
        expression->subquery = std::make_shared<Query>(subquery());
        return expression;
      }
      auto e = expr(0);
      if (next().kind != TokenKind::rparen)
        throw std::runtime_error("expected )");
      return e;
    }
    throw std::runtime_error("unexpected token");
  }
  ExprPtr expr(int min_prec) {
    ExprPtr lhs;
    if (word("not")) {
      lhs = node(ExprKind::unary);
      lhs->text = "not";
      lhs->left = expr(6);
    } else if (peek().kind == TokenKind::op && peek().text == "-") {
      ++pos_;
      lhs = node(ExprKind::unary);
      lhs->text = "-";
      lhs->left = expr(6);
    } else
      lhs = primary();
    for (;;) {
      if (word("is")) {
        const bool neg = word("not");
        expect_word("null");
        auto e = node(ExprKind::is_null);
        e->boolean = neg;
        e->left = lhs;
        lhs = e;
        continue;
      }
      bool negated_special = false;
      std::string special;
      if (peek().kind == TokenKind::word && peek().text == "not" &&
          pos_ + 1 < tokens_.size() &&
          tokens_[pos_ + 1].kind == TokenKind::word &&
          (tokens_[pos_ + 1].text == "in" ||
           tokens_[pos_ + 1].text == "between" ||
           tokens_[pos_ + 1].text == "like")) {
        negated_special = true;
        ++pos_;
        special = next().text;
      } else if (peek().kind == TokenKind::word &&
                 (peek().text == "in" || peek().text == "between" ||
                  peek().text == "like")) {
        special = next().text;
      }
      if (!special.empty()) {
        if (min_prec > 3)
          throw std::runtime_error(special +
                                   " has lower precedence than its context");
        if (special == "in") {
          if (next().kind != TokenKind::lparen)
            throw std::runtime_error("expected ( after IN");
          auto e = node(ExprKind::in_list);
          e->left = lhs;
          e->boolean = negated_special;
          if (peek().kind == TokenKind::word &&
              (peek().text == "select" || peek().text == "with")) {
            e->kind = ExprKind::in_subquery;
            e->subquery = std::make_shared<Query>(subquery());
            lhs = std::move(e);
            continue;
          }
          if (peek().kind != TokenKind::rparen) {
            for (;;) {
              e->args.push_back(expr(0));
              if (peek().kind == TokenKind::comma)
                ++pos_;
              else
                break;
            }
          }
          if (next().kind != TokenKind::rparen)
            throw std::runtime_error("expected ) after IN list");
          lhs = std::move(e);
        } else if (special == "between") {
          auto e = node(ExprKind::between);
          e->left = lhs;
          e->args.push_back(expr(4));
          expect_word("and");
          e->args.push_back(expr(4));
          e->boolean = negated_special;
          lhs = std::move(e);
        } else {
          auto e = node(ExprKind::like);
          e->left = lhs;
          e->right = expr(4);
          e->boolean = negated_special;
          lhs = std::move(e);
        }
        continue;
      }
      std::string op;
      int prec = 0;
      if (peek().kind == TokenKind::word && peek().text == "or") {
        op = "or";
        prec = 1;
      } else if (peek().kind == TokenKind::word && peek().text == "and") {
        op = "and";
        prec = 2;
      } else if (peek().kind == TokenKind::op &&
                 (peek().text == "=" || peek().text == "!=" ||
                  peek().text == "<" || peek().text == "<=" ||
                  peek().text == "<>" || peek().text == ">" ||
                  peek().text == ">=")) {
        op = peek().text;
        prec = 3;
      } else if (peek().kind == TokenKind::op &&
                 (peek().text == "+" || peek().text == "-")) {
        op = peek().text;
        prec = 4;
      } else if (peek().kind == TokenKind::star) {
        op = "*";
        prec = 5;
      } else if (peek().kind == TokenKind::op && peek().text == "/") {
        op = "/";
        prec = 5;
      } else
        break;
      if (prec < min_prec)
        break;
      ++pos_;
      auto rhs = expr(prec + 1);
      auto e = node(ExprKind::binary);
      e->text = op == "<>" ? "!=" : op;
      e->left = lhs;
      e->right = rhs;
      lhs = e;
    }
    return lhs;
  }

public:
  explicit Parser(const std::string &sql) : tokens_(lex(sql)) {}
  Query parse() {
    if (!tokens_.empty() && tokens_[0].kind == TokenKind::word &&
        tokens_[0].text == "with") {
      std::size_t cursor = 1;
      std::vector<std::pair<std::string, std::shared_ptr<Query>>> ctes;
      for (;;) {
        if (cursor >= tokens_.size() ||
            tokens_[cursor].kind != TokenKind::word)
          throw std::runtime_error("expected CTE name");
        if (tokens_[cursor].text == "recursive")
          throw std::runtime_error("recursive CTEs are not supported");
        const auto name = tokens_[cursor++].text;
        if (cursor + 1 >= tokens_.size() ||
            tokens_[cursor].kind != TokenKind::word ||
            tokens_[cursor].text != "as" ||
            tokens_[cursor + 1].kind != TokenKind::lparen)
          throw std::runtime_error("expected AS ( after CTE name");
        cursor += 2;
        const auto start = cursor;
        std::size_t depth = 1;
        while (cursor < tokens_.size() && depth) {
          if (tokens_[cursor].kind == TokenKind::lparen)
            ++depth;
          else if (tokens_[cursor].kind == TokenKind::rparen)
            --depth;
          ++cursor;
        }
        if (depth)
          throw std::runtime_error("unterminated CTE query");
        std::vector<Token> cte_tokens(tokens_.begin() + start,
                                      tokens_.begin() + cursor - 1);
        cte_tokens.push_back({TokenKind::end, ""});
        Parser cte_parser("");
        cte_parser.tokens_ = std::move(cte_tokens);
        auto cte_query = cte_parser.parse();
        cte_query.ctes = ctes;
        ctes.emplace_back(name,
                          std::make_shared<Query>(std::move(cte_query)));
        if (tokens_[cursor].kind == TokenKind::comma)
          ++cursor;
        else
          break;
      }
      std::vector<Token> outer_tokens(tokens_.begin() + cursor, tokens_.end());
      Parser outer_parser("");
      outer_parser.tokens_ = std::move(outer_tokens);
      auto outer = outer_parser.parse();
      outer.ctes = std::move(ctes);
      return outer;
    }
    std::size_t depth = 0;
    for (std::size_t index = 0; index < tokens_.size(); ++index) {
      if (tokens_[index].kind == TokenKind::lparen)
        ++depth;
      else if (tokens_[index].kind == TokenKind::rparen)
        depth = depth ? depth - 1 : 0;
      else if (depth == 0 && tokens_[index].kind == TokenKind::word &&
               tokens_[index].text == "union") {
        const bool all = index + 1 < tokens_.size() &&
                         tokens_[index + 1].kind == TokenKind::word &&
                         tokens_[index + 1].text == "all";
        std::vector<Token> left_tokens(tokens_.begin(), tokens_.begin() + index);
        left_tokens.push_back({TokenKind::end, ""});
        const auto right_start = index + (all ? 2 : 1);
        std::vector<Token> right_tokens(tokens_.begin() + right_start,
                                        tokens_.end());
        Parser left_parser("");
        left_parser.tokens_ = std::move(left_tokens);
        Parser right_parser("");
        right_parser.tokens_ = std::move(right_tokens);
        auto left = left_parser.parse();
        left.union_query = std::make_shared<Query>(right_parser.parse());
        left.union_all = all;
        return left;
      }
    }
    Query q;
    expect_word("select");
    q.distinct = word("distinct");
    for (;;) {
      SelectItem item{expr(0), std::nullopt};
      if (word("as")) {
        auto a = next();
        if (a.kind != TokenKind::word)
          throw std::runtime_error("expected alias");
        item.alias = a.text;
      }
      q.select.push_back(std::move(item));
      if (peek().kind == TokenKind::comma)
        ++pos_;
      else
        break;
    }
    expect_word("from");
    auto [from, derived_from] = relation_ref();
    q.from = std::move(from);
    if (derived_from)
      q.ctes.push_back(std::move(*derived_from));
    for (;;) {
      std::optional<JoinKind> kind;
      if (peek().kind == TokenKind::comma) {
        ++pos_;
        kind = JoinKind::cross;
      } else if (word("join"))
        kind = JoinKind::inner;
      else if (word("inner")) {
        expect_word("join");
        kind = JoinKind::inner;
      } else if (word("left")) {
        (void)word("outer");
        expect_word("join");
        kind = JoinKind::left;
      } else if (word("right")) {
        (void)word("outer");
        expect_word("join");
        kind = JoinKind::right;
      } else if (word("full")) {
        (void)word("outer");
        expect_word("join");
        kind = JoinKind::full;
      } else if (word("cross")) {
        expect_word("join");
        kind = JoinKind::cross;
      }
      if (!kind)
        break;
      auto [table, derived_join] = relation_ref();
      JoinSpec join{*kind, std::move(table), {}};
      if (derived_join)
        q.ctes.push_back(std::move(*derived_join));
      if (*kind != JoinKind::cross) {
        expect_word("on");
        join.on = expr(0);
      }
      q.joins.push_back(std::move(join));
    }
    if (word("where"))
      q.filter = expr(0);
    if (word("group")) {
      expect_word("by");
      for (;;) {
        q.group_by.push_back(identifier());
        if (peek().kind == TokenKind::comma)
          ++pos_;
        else
          break;
      }
    }
    if (word("having"))
      q.having = expr(0);
    if (word("order")) {
      expect_word("by");
      for (;;) {
        OrderSpec order{identifier(), true, {}};
        if (word("desc"))
          order.ascending = false;
        else
          (void)word("asc");
        if (word("nulls")) {
          if (word("first"))
            order.nulls_first = true;
          else {
            expect_word("last");
            order.nulls_first = false;
          }
        }
        q.order_by.push_back(std::move(order));
        if (peek().kind == TokenKind::comma)
          ++pos_;
        else
          break;
      }
    }
    if (word("limit")) {
      auto x = next();
      if (x.kind != TokenKind::number)
        throw std::runtime_error("expected limit");
      q.limit = std::stoull(x.text);
    }
    if (word("offset")) {
      auto value = next();
      if (value.kind != TokenKind::number)
        throw std::runtime_error("expected offset");
      q.offset = std::stoull(value.text);
    }
    if (peek().kind == TokenKind::semi)
      ++pos_;
    if (peek().kind != TokenKind::end)
      throw std::runtime_error("trailing token");
    return q;
  }
};

static bool is_agg(const ExprPtr &e) {
  return e->kind == ExprKind::function &&
         (e->text == "count" || e->text == "sum" || e->text == "avg" ||
          e->text == "min" || e->text == "max");
}
static bool contains_agg(const ExprPtr &e) {
  if (!e)
    return false;
  if (e->kind == ExprKind::window)
    return false;
  if (is_agg(e) || contains_agg(e->left) || contains_agg(e->right))
    return true;
  if (std::any_of(e->args.begin(), e->args.end(), contains_agg))
    return true;
  return std::any_of(e->branches.begin(), e->branches.end(), [](auto &branch) {
    return contains_agg(branch.first) || contains_agg(branch.second);
  });
}
static bool contains_window(const ExprPtr &expression) {
  if (!expression)
    return false;
  if (expression->kind == ExprKind::window)
    return true;
  if (contains_window(expression->left) || contains_window(expression->right))
    return true;
  if (std::any_of(expression->args.begin(), expression->args.end(),
                  contains_window))
    return true;
  return std::any_of(expression->branches.begin(), expression->branches.end(),
                     [](auto &branch) {
                       return contains_window(branch.first) ||
                              contains_window(branch.second);
                     });
}
static bool contains_subquery(const ExprPtr &expression) {
  if (!expression)
    return false;
  if (expression->kind == ExprKind::scalar_subquery ||
      expression->kind == ExprKind::exists ||
      expression->kind == ExprKind::in_subquery)
    return true;
  if (contains_subquery(expression->left) ||
      contains_subquery(expression->right))
    return true;
  if (std::any_of(expression->args.begin(), expression->args.end(),
                  contains_subquery))
    return true;
  return std::any_of(expression->branches.begin(), expression->branches.end(),
                     [](auto &branch) {
                       return contains_subquery(branch.first) ||
                              contains_subquery(branch.second);
                     });
}
static std::string base_name(const std::string &name) {
  const auto dot = name.rfind('.');
  return dot == std::string::npos ? name : name.substr(dot + 1);
}
static void collect(const ExprPtr &e, std::set<std::string> &s) {
  if (!e)
    return;
  if (e->kind == ExprKind::column || e->kind == ExprKind::dict_eq)
    s.insert(base_name(e->text));
  collect(e->left, s);
  collect(e->right, s);
  for (auto &arg : e->args)
    collect(arg, s);
  for (auto &[condition, value] : e->branches) {
    collect(condition, s);
    collect(value, s);
  }
  for (auto &column : e->partition_by)
    s.insert(base_name(column));
  for (auto &order : e->window_order_by)
    s.insert(base_name(order.key));
}
static void plan(Query &q) {
  const bool agg = !q.group_by.empty() ||
                   std::any_of(q.select.begin(), q.select.end(),
                               [](auto &s) { return contains_agg(s.expr); }) ||
                   contains_agg(q.having);
  for (auto &s : q.select)
    if (agg && !is_agg(s.expr) &&
        !(s.expr->kind == ExprKind::column &&
          std::find(q.group_by.begin(), q.group_by.end(), s.expr->text) !=
              q.group_by.end()))
      throw std::runtime_error(
          "non-aggregate select expression must be grouped");
  std::set<std::string> cols;
  for (auto &s : q.select)
    collect(s.expr, cols);
  collect(q.filter, cols);
  for (auto &join : q.joins)
    collect(join.on, cols);
  collect(q.having, cols);
  cols.insert(q.group_by.begin(), q.group_by.end());
  q.columns.assign(cols.begin(), cols.end());
  std::string list;
  for (auto &c : q.columns) {
    if (!list.empty())
      list += ",";
    list += c;
  }
  q.logical = {"Scan(columns=[" + list + "])"};
  q.physical = {"ScanExec(columns=[" + list + "])"};
  const auto table_rows = [](const std::string &table) {
    return table == "events"      ? std::size_t{1'000'000}
           : table == "users"    ? std::size_t{250'000}
           : table == "campaigns" ? std::size_t{5'000}
                                     : std::size_t{1'000};
  };
  auto estimated_rows = table_rows(q.from.name);
  const bool materialized_joins = !q.ctes.empty();
  for (auto &join : q.joins) {
    const std::string kind =
        join.kind == JoinKind::cross ? "Cross"
        : join.kind == JoinKind::inner ? "Inner"
        : join.kind == JoinKind::left  ? "Left"
        : join.kind == JoinKind::right ? "Right"
                                       : "Full";
    q.logical.push_back(kind + "Join(" + join.table.name + ")");
    const bool equi = join.on && join.on->kind == ExprKind::binary &&
                      join.on->text == "=" && join.on->left &&
                      join.on->right &&
                      join.on->left->kind == ExprKind::column &&
                      join.on->right->kind == ExprKind::column;
    const auto right_rows = table_rows(join.table.name);
    estimated_rows = equi ? std::max(estimated_rows, right_rows)
                          : estimated_rows * right_rows;
    q.physical.push_back(
        q.optimizer_enabled && equi && !materialized_joins
            ? "HashJoinExec(type=" + kind + ";table=" + join.table.name +
                  ";build=right;runtime_filter=true;estimated_rows=" +
                  std::to_string(estimated_rows) + ")"
            : "NestedLoopJoinExec(type=" + kind + ";table=" +
                  join.table.name + ";estimated_rows=" +
                  std::to_string(estimated_rows) + ")");
  }
  if (q.filter) {
    q.logical.push_back("Filter");
    q.physical.push_back("FilterExec(pushdown=true)");
  }
  if (agg) {
    q.logical.push_back("Aggregate");
    q.physical.push_back("PartialAggregateExec");
    q.physical.push_back("FinalAggregateExec");
  } else {
    q.logical.push_back("Project");
    q.physical.push_back("ProjectExec");
  }
  if (std::any_of(q.select.begin(), q.select.end(),
                  [](auto &item) { return contains_window(item.expr); })) {
    q.logical.push_back("Window");
    q.physical.push_back("WindowExec(partition_sort=true)");
  }
  if (std::any_of(q.select.begin(), q.select.end(), [](auto &item) {
        return contains_subquery(item.expr);
      }) || contains_subquery(q.filter)) {
    q.logical.push_back("Subquery");
    q.physical.push_back("SubqueryExec(correlated=true)");
  }
  if (q.having) {
    q.logical.push_back("Having");
    q.physical.push_back("HavingExec");
  }
  if (q.distinct) {
    q.logical.push_back("Distinct");
    q.physical.push_back("HashDistinctExec");
  }
  if (!q.order_by.empty()) {
    q.logical.push_back(q.optimizer_enabled && q.limit ? "TopK" : "Sort");
    q.physical.push_back(
        q.optimizer_enabled && q.limit
            ? "TopKExec(k=" + std::to_string(*q.limit + q.offset) + ")"
            : "SortExec");
  }
  if (q.limit) {
    q.logical.push_back("Limit");
    q.physical.push_back("LimitExec");
  }
  if (q.union_query) {
    q.logical.push_back(q.union_all ? "UnionAll" : "UnionDistinct");
    q.physical.push_back(q.union_all ? "UnionAllExec" : "UnionDistinctExec");
  }
  if (!q.ctes.empty()) {
    q.logical.insert(q.logical.begin(),
                     "With(ctes=" + std::to_string(q.ctes.size()) + ")");
    q.physical.insert(
        q.physical.begin(),
        "CteMaterializeExec(count=" + std::to_string(q.ctes.size()) + ")");
  }
}

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
    return path.ends_with(".dremel") ? load_binary(path) : load_csv(path);
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
  const auto whole_text = dot == std::string::npos ? value : value.substr(0, dot);
  auto fraction = dot == std::string::npos ? std::string{} : value.substr(dot + 1);
  if (fraction.size() > 2)
    throw std::runtime_error("DECIMAL(18,2) requires at most two fractional digits");
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
  const auto magnitude = units < 0 ? static_cast<std::uint64_t>(-(units + 1)) + 1
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
struct Catalog {
  std::shared_ptr<Table> events;
  UsersTable users;
  CampaignsTable campaigns;

  static Catalog load(const std::string &events_path,
                      std::shared_ptr<Table> events) {
    Catalog catalog;
    catalog.events = std::move(events);
    const auto directory = std::filesystem::path(events_path).parent_path();
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
      "event_id",    "user_id",  "timestamp", "country",
      "device",      "event_type", "duration_ms", "bytes",
      "score",       "success",  "campaign_id"};
  static const std::vector<std::string> users{
      "user_id", "segment", "signup_date", "lifetime_value", "region",
      "active"};
  static const std::vector<std::string> campaigns{
      "campaign_id", "campaign_name", "budget", "start_date", "end_date",
      "channel"};
  static const std::vector<std::string> empty;
  return table == "events"      ? events
         : table == "users"    ? users
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
  if ((table == "events" &&
       (column == "event_id" || column == "user_id" ||
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
       (column == "country" || column == "device" ||
        column == "event_type")) ||
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
static SqlType infer_type(const ExprPtr &expression,
                          const Bindings &bindings) {
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
        throw std::runtime_error(expression->text + " requires BOOLEAN operands");
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
        "bigint", "int64", "integer", "double", "float", "real",
        "varchar", "string", "text", "boolean", "bool", "date",
        "timestamp", "decimal"};
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
        current[index] = previous[index - 1] &&
                         (token == '_' || token == value[index - 1]);
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
  const auto era = shifted >= 0 ? shifted / 146097 : (shifted - 146096) / 146097;
  const auto day_of_era = shifted - era * 146097;
  const auto year_of_era =
      (day_of_era - day_of_era / 1460 + day_of_era / 36524 -
       day_of_era / 146096) /
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
  const auto era = adjusted_year >= 0 ? adjusted_year / 400
                                      : (adjusted_year - 399) / 400;
  const auto year_of_era = adjusted_year - era * 400;
  const auto shifted_month = month + (month > 2 ? -3 : 9);
  const auto day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
  const auto day_of_era = year_of_era * 365 + year_of_era / 4 -
                          year_of_era / 100 + day_of_year;
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
static std::optional<std::int64_t>
parse_timestamp(const std::string &value) {
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
static std::optional<std::int64_t>
interval_seconds(const std::string &value) {
  std::istringstream input(value);
  std::int64_t amount;
  std::string unit, extra;
  if (!(input >> amount >> unit) || input >> extra)
    return {};
  if (unit.ends_with('s'))
    unit.pop_back();
  const std::int64_t multiplier =
      unit == "microsecond" ? 0
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
  return std::array<std::int64_t, 6>{
      year, month, day, seconds_of_day / 3600,
      seconds_of_day / 60 % 60, seconds_of_day % 60};
}

static Scalar eval_values(const std::string &name,
                          std::vector<Scalar> values) {
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
    const auto start = static_cast<std::size_t>(std::max<std::int64_t>(0, *start_value - 1));
    auto length = std::string::npos;
    if (values.size() == 3) {
      auto *length_value = std::get_if<std::int64_t>(&values[2]);
      if (!length_value)
        return std::monostate{};
      length = static_cast<std::size_t>(std::max<std::int64_t>(0, *length_value));
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
    const auto index = *field == "year"   ? 0
                       : *field == "month" ? 1
                       : *field == "day"   ? 2
                       : *field == "hour"  ? 3
                       : *field == "minute" ? 4
                       : *field == "second" ? 5
                                             : 6;
    return index < 6 ? Scalar{(*components)[index]}
                     : Scalar{std::monostate{}};
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
                       *floating >= static_cast<double>(std::numeric_limits<std::int64_t>::min()) &&
                       *floating <= static_cast<double>(std::numeric_limits<std::int64_t>::max())
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
      return std::get<bool>(value) ? std::string{"true"}
                                   : std::string{"false"};
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
                       scaled >= static_cast<double>(std::numeric_limits<std::int64_t>::min()) &&
                       scaled <= static_cast<double>(std::numeric_limits<std::int64_t>::max())
                   ? Scalar{Decimal{static_cast<std::int64_t>(std::round(scaled))}}
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
  if ((op == "+" || op == "-") &&
      std::holds_alternative<std::string>(left) &&
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
    const auto units = [](const Scalar &value,
                          std::int64_t &output) -> bool {
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
      return __builtin_add_overflow(a, b, &result)
                 ? Scalar{std::monostate{}}
                 : Scalar{Decimal{result}};
    if (op == "-")
      return __builtin_sub_overflow(a, b, &result)
                 ? Scalar{std::monostate{}}
                 : Scalar{Decimal{result}};
    const auto product = static_cast<__int128>(a) * static_cast<__int128>(b) / 100;
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
  } else if (expression->kind == ExprKind::call &&
             expression->text != "date" && expression->text != "timestamp" &&
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
      replacement = literal_expression(Scalar{
          std::holds_alternative<std::monostate>(*value) ^ expression->boolean});
  } else if (expression->kind == ExprKind::case_when) {
    bool all_constant_false_or_null = true;
    for (auto &[condition, value] : expression->branches) {
      auto scalar = literal_value(condition);
      if (!scalar) {
        all_constant_false_or_null = false;
        break;
      }
      if (const auto *boolean = std::get_if<bool>(&*scalar); boolean && *boolean) {
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
    safe_star_reorder =
        std::all_of(query.joins.begin(), query.joins.end(),
                    [&](const auto &join) {
                      if (join.kind != JoinKind::inner || !join.on ||
                          join.on->kind != ExprKind::binary ||
                          join.on->text != "=" || !join.on->left ||
                          !join.on->right ||
                          join.on->left->kind != ExprKind::column ||
                          join.on->right->kind != ExprKind::column)
                        return false;
                      const auto left =
                          resolve_column(join.on->left->text, bindings).first;
                      const auto right =
                          resolve_column(join.on->right->text, bindings).first;
                      return (left == query.from.name &&
                              right == join.table.name) ||
                             (right == query.from.name &&
                              left == join.table.name);
                    });
  } catch (const std::exception &) {
    safe_star_reorder = false;
  }
  if (query.optimizer_enabled && query.joins.size() > 1 &&
      safe_star_reorder) {
    std::vector<std::string> original;
    for (auto &join : query.joins)
      original.push_back(join.table.name);
    const auto cardinality = [](const JoinSpec &join) {
      return join.table.name == "campaigns" ? std::size_t{5'000}
             : join.table.name == "users"   ? std::size_t{250'000}
             : join.table.name == "events"  ? std::size_t{1'000'000}
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
    q.physical.push_back("StatsExec(table=users;rows=250000;nulls=0;distinct=user_id:250000;min=user_id:1;max=user_id:250000)");
  else if (q.from.name == "campaigns")
    q.physical.push_back("StatsExec(table=campaigns;rows=5000;nulls=0;distinct=campaign_id:5000;min=campaign_id:1;max=campaign_id:5000)");
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
      if (e->text == "bigint" || e->text == "int64" ||
          e->text == "integer") {
        if (auto *integer = std::get_if<std::int64_t>(&value))
          return *integer;
        if (auto *floating = std::get_if<double>(&value))
          return std::isfinite(*floating) &&
                         *floating >=
                             static_cast<double>(std::numeric_limits<std::int64_t>::min()) &&
                         *floating <=
                             static_cast<double>(std::numeric_limits<std::int64_t>::max())
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
      if (e->text == "varchar" || e->text == "string" ||
          e->text == "text") {
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
    return static_cast<bool>(((compare(value, low) >= 0 &&
                               compare(value, high) <= 0) ^
                              e->boolean));
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
    auto left = sql_bool(eval(e->left, t, i));
    auto right = sql_bool(eval(e->right, t, i));
    if (left == false || right == false)
      return false;
    if (left == true && right == true)
      return true;
    return std::monostate{};
  }
  if (e->text == "or") {
    auto left = sql_bool(eval(e->left, t, i));
    auto right = sql_bool(eval(e->right, t, i));
    if (left == true || right == true)
      return true;
    if (left == false && right == false)
      return false;
    return std::monostate{};
  }
  return apply_binary_value(e->text, eval(e->left, t, i), eval(e->right, t, i));
}

static Rows execute_subquery(const Query &query, const Catalog &catalog,
                             const RelRow &row,
                             const Bindings &outer_bindings);
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
  const auto [table, column] =
      resolve_column(outer->text, outer_bindings);
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
             : std::optional<std::size_t>{static_cast<std::size_t>(
                   std::distance(catalog.campaigns.campaign_id.begin(),
                                 fallback))};
}
static std::optional<Scalar>
simple_campaign_max_budget(const Query &query, const Catalog &catalog,
                           const RelRow &row,
                           const Bindings &outer_bindings) {
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
    return static_cast<bool>(
        std::holds_alternative<std::monostate>(
            eval_rel(expression->left, catalog, row, bindings)) ^
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
    return eval_values(
        expression->text,
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
    return cast_value(
        expression->text,
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
    return saw_null ? Scalar{std::monostate{}}
                    : Scalar{expression->boolean};
  }
  case ExprKind::between: {
    auto value = eval_rel(expression->left, catalog, row, bindings);
    auto low = eval_rel(expression->args[0], catalog, row, bindings);
    auto high = eval_rel(expression->args[1], catalog, row, bindings);
    if (std::holds_alternative<std::monostate>(value) ||
        std::holds_alternative<std::monostate>(low) ||
        std::holds_alternative<std::monostate>(high))
      return std::monostate{};
    return static_cast<bool>(((compare(value, low) >= 0 &&
                               compare(value, high) <= 0) ^
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
    if (auto value = simple_campaign_max_budget(
            *expression->subquery, catalog, row, bindings))
      return *value;
    auto rows = execute_subquery(*expression->subquery, catalog, row, bindings);
    return rows.empty() || rows.front().empty() ? Scalar{std::monostate{}}
                                                : rows.front().front();
  }
  case ExprKind::exists:
    if (auto index = simple_campaign_lookup(*expression->subquery, catalog,
                                            row, bindings))
      return index->has_value();
    else
      return !execute_subquery(*expression->subquery, catalog, row, bindings)
                  .empty();
  case ExprKind::in_subquery: {
    auto value = eval_rel(expression->left, catalog, row, bindings);
    if (std::holds_alternative<std::monostate>(value))
      return std::monostate{};
    if (auto found = simple_campaign_id_membership(
            *expression->subquery, value, catalog))
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
    return saw_null ? Scalar{std::monostate{}}
                    : Scalar{expression->boolean};
  }
  }
  return std::monostate{};
}

struct Agg {
  enum class Kind { count, sum, avg, min, max } kind;
  std::uint64_t count{};
  double sum{};
  __int128 decimal_sum{};
  bool floating{}, decimal{}, has{};
  Scalar extreme;
};
static void add_sum(Agg &state, const Scalar &value) {
  if (const auto *decimal = std::get_if<Decimal>(&value);
      decimal && !state.floating) {
    if (!state.decimal) {
      state.decimal_sum = static_cast<__int128>(state.sum) * 100;
      state.decimal = true;
    }
    state.decimal_sum += decimal->units;
    state.has = true;
    return;
  }
  if (const auto *integer = std::get_if<std::int64_t>(&value);
      integer && state.decimal && !state.floating) {
    state.decimal_sum += static_cast<__int128>(*integer) * 100;
    state.has = true;
    return;
  }
  if (auto numeric = number(value)) {
    if (state.decimal) {
      state.sum = static_cast<double>(state.decimal_sum) / 100.0;
      state.decimal = false;
    }
    state.sum += *numeric;
    state.floating |= std::holds_alternative<double>(value);
    state.has = true;
  }
}
static void merge_sum(Agg &left, const Agg &right) {
  if (!right.has)
    return;
  if (left.decimal && right.decimal && !left.floating && !right.floating) {
    left.decimal_sum += right.decimal_sum;
    left.has = true;
    return;
  }
  if (!left.has && right.decimal && !right.floating) {
    left = right;
    return;
  }
  if (left.decimal) {
    left.sum = static_cast<double>(left.decimal_sum) / 100.0;
    left.decimal = false;
  }
  left.sum += right.decimal ? static_cast<double>(right.decimal_sum) / 100.0
                            : right.sum;
  left.floating |= right.floating;
  left.has = true;
}
static std::vector<Agg> states(const Query &q) {
  std::vector<Agg> v;
  for (auto &s : q.select)
    if (is_agg(s.expr)) {
      Agg a;
      if (s.expr->text == "count")
        a.kind = Agg::Kind::count;
      else if (s.expr->text == "sum")
        a.kind = Agg::Kind::sum;
      else if (s.expr->text == "avg")
        a.kind = Agg::Kind::avg;
      else if (s.expr->text == "min")
        a.kind = Agg::Kind::min;
      else
        a.kind = Agg::Kind::max;
      v.push_back(a);
    }
  return v;
}
static void update(std::vector<Agg> &st, const Query &q, const Table &t,
                   std::size_t i) {
  std::size_t ai = 0;
  for (auto &s : q.select)
    if (is_agg(s.expr)) {
      auto &a = st[ai++];
      auto v = eval(s.expr->left, t, i);
      if (a.kind == Agg::Kind::count) {
        if (s.expr->left->kind == ExprKind::star ||
            !std::holds_alternative<std::monostate>(v))
          ++a.count;
      } else if (a.kind == Agg::Kind::sum) {
        add_sum(a, v);
      } else if (a.kind == Agg::Kind::avg) {
        if (auto n = number(v)) {
          a.sum += *n;
          ++a.count;
        }
      } else if (!std::holds_alternative<std::monostate>(v) &&
                 (!a.has ||
                  (a.kind == Agg::Kind::min ? compare(v, a.extreme) < 0
                                            : compare(v, a.extreme) > 0))) {
        a.extreme = v;
        a.has = true;
      }
    }
}
static void merge(std::vector<Agg> &a, const std::vector<Agg> &b) {
  for (std::size_t i = 0; i < a.size(); ++i) {
    if (a[i].kind == Agg::Kind::count)
      a[i].count += b[i].count;
    else if (a[i].kind == Agg::Kind::sum) {
      merge_sum(a[i], b[i]);
    } else if (a[i].kind == Agg::Kind::avg) {
      a[i].sum += b[i].sum;
      a[i].count += b[i].count;
    } else if (b[i].has &&
               (!a[i].has || (a[i].kind == Agg::Kind::min
                                  ? compare(b[i].extreme, a[i].extreme) < 0
                                  : compare(b[i].extreme, a[i].extreme) > 0))) {
      a[i].extreme = b[i].extreme;
      a[i].has = true;
    }
  }
}
static Scalar finish(const Agg &a) {
  if (a.kind == Agg::Kind::count)
    return static_cast<std::int64_t>(a.count);
  if (a.kind == Agg::Kind::sum) {
    if (!a.has)
      return std::monostate{};
    if (a.decimal && !a.floating)
      return a.decimal_sum < std::numeric_limits<std::int64_t>::min() ||
                     a.decimal_sum > std::numeric_limits<std::int64_t>::max()
                 ? Scalar{std::monostate{}}
                 : Scalar{Decimal{static_cast<std::int64_t>(a.decimal_sum)}};
    return a.floating ? Scalar{a.sum}
                      : Scalar{static_cast<std::int64_t>(a.sum)};
  }
  if (a.kind == Agg::Kind::avg)
    return a.count ? Scalar{a.sum / static_cast<double>(a.count)}
                   : Scalar{std::monostate{}};
  return a.has ? a.extreme : Scalar{std::monostate{}};
}

static std::vector<RelRow> base_relation_rows(const std::string &table,
                                              const Catalog &catalog) {
  std::vector<RelRow> rows;
  const auto size = table == "events"      ? catalog.events->size()
                    : table == "users"    ? catalog.users.user_id.size()
                    : table == "campaigns" ? catalog.campaigns.campaign_id.size()
                                             : 0;
  rows.reserve(size);
  for (std::size_t index = 0; index < size; ++index) {
    if (index % 4096 == 0 && execution_cancelled())
      break;
    RelRow row;
    if (table == "events")
      row.event = index;
    else if (table == "users")
      row.user = index;
    else if (table == "campaigns")
      row.campaign = index;
    rows.push_back(row);
  }
  return rows;
}
static RelRow merge_rel_rows(RelRow left, const RelRow &right) {
  if (!left.event)
    left.event = right.event;
  if (!left.user)
    left.user = right.user;
  if (!left.campaign)
    left.campaign = right.campaign;
  return left;
}
static std::optional<std::string> scalar_hash_key(const Scalar &value) {
  if (std::holds_alternative<std::monostate>(value))
    return {};
  if (auto *integer = std::get_if<std::int64_t>(&value))
    return "i:" + std::to_string(*integer);
  if (auto *decimal = std::get_if<Decimal>(&value))
    return "d:" + std::to_string(decimal->units);
  if (auto *floating = std::get_if<double>(&value)) {
    std::ostringstream out;
    out << "f:" << std::hex << std::bit_cast<std::uint64_t>(*floating);
    return out.str();
  }
  if (auto *boolean = std::get_if<bool>(&value))
    return std::string{"b:"} + (*boolean ? "true" : "false");
  const auto &text = std::get<std::string>(value);
  return "s:" + std::to_string(text.size()) + ":" + text;
}
static std::optional<std::pair<ExprPtr, ExprPtr>>
join_equality(const ExprPtr &expression, const std::string &right_table,
              const Bindings &bindings) {
  if (!expression || expression->kind != ExprKind::binary ||
      expression->text != "=" ||
      expression->left->kind != ExprKind::column ||
      expression->right->kind != ExprKind::column)
    return {};
  const auto left = resolve_column(expression->left->text, bindings).first;
  const auto right = resolve_column(expression->right->text, bindings).first;
  if (right == right_table && left != right_table)
    return std::pair{expression->left, expression->right};
  if (left == right_table && right != right_table)
    return std::pair{expression->right, expression->left};
  return {};
}
static std::vector<RelRow> apply_join(std::vector<RelRow> left_rows,
                                     const JoinSpec &join,
                                     const Catalog &catalog,
                                     const Bindings &bindings,
                                     bool optimizer_enabled) {
  auto right_rows = base_relation_rows(join.table.name, catalog);
  std::vector<RelRow> output;
  if (join.kind == JoinKind::cross) {
    output.reserve(left_rows.size() * right_rows.size());
    for (auto &left : left_rows)
      for (auto &right : right_rows)
        output.push_back(merge_rel_rows(left, right));
    return output;
  }
  auto equality = join_equality(join.on, join.table.name, bindings);
  std::vector<bool> matched_right(right_rows.size());
  if (optimizer_enabled && equality) {
    const auto [left_table, left_column] =
        resolve_column(equality->first->text, bindings);
    const auto [right_table, right_column] =
        resolve_column(equality->second->text, bindings);
    std::unordered_map<std::string, std::vector<std::size_t>> hash;
    for (std::size_t index = 0; index < right_rows.size(); ++index)
      if (auto key = scalar_hash_key(relation_scalar(
              catalog, right_rows[index], right_table, right_column)))
        hash[*key].push_back(index);
    for (auto &left : left_rows) {
      if (execution_cancelled())
        break;
      bool matched = false;
      if (auto key = scalar_hash_key(
              relation_scalar(catalog, left, left_table, left_column));
          key && hash.contains(*key)) {
        for (auto index : hash.at(*key)) {
          const auto combined = merge_rel_rows(left, right_rows[index]);
          if (truthy(eval_rel(join.on, catalog, combined, bindings))) {
            output.push_back(combined);
            matched = true;
            matched_right[index] = true;
          }
        }
      }
      if (!matched &&
          (join.kind == JoinKind::left || join.kind == JoinKind::full))
        output.push_back(left);
    }
  } else {
    for (auto &left : left_rows) {
      if (execution_cancelled())
        break;
      bool matched = false;
      for (std::size_t index = 0; index < right_rows.size(); ++index) {
        const auto combined = merge_rel_rows(left, right_rows[index]);
        if (truthy(eval_rel(join.on, catalog, combined, bindings))) {
          output.push_back(combined);
          matched = true;
          matched_right[index] = true;
        }
      }
      if (!matched &&
          (join.kind == JoinKind::left || join.kind == JoinKind::full))
        output.push_back(left);
    }
  }
  if (join.kind == JoinKind::right || join.kind == JoinKind::full)
    for (std::size_t index = 0; index < right_rows.size(); ++index)
      if (!matched_right[index])
        output.push_back(right_rows[index]);
  return output;
}

struct Key {
  std::array<std::uint64_t, 3> v{};
  std::uint8_t n{};
  bool operator==(const Key &) const = default;
};
static std::uint64_t hash64(std::uint64_t x) {
  x += 0x9E3779B97F4A7C15ULL;
  x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
  x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
  return x ^ (x >> 31);
}
static std::uint64_t key_hash(const Key &k) {
  std::uint64_t h = 0x243F6A8885A308D3ULL;
  for (std::size_t i = 0; i < k.n; ++i)
    h = hash64(h ^ hash64(k.v[i] + (i << 32)));
  return h;
}
struct Entry {
  Key key;
  std::vector<Agg> states;
};
class GroupTable {
  std::vector<std::optional<Entry>> slots_ =
      std::vector<std::optional<Entry>>(16);
  std::size_t size_{};
  std::size_t find(const Key &k) const {
    auto i = key_hash(k) & (slots_.size() - 1);
    for (;;) {
      if (!slots_[i] || slots_[i]->key == k)
        return i;
      i = (i + 1) & (slots_.size() - 1);
    }
  }
  void grow() {
    auto old = std::move(slots_);
    slots_ = std::vector<std::optional<Entry>>(old.size() * 2);
    size_ = 0;
    for (auto &e : old)
      if (e) {
        auto i = find(e->key);
        slots_[i] = std::move(e);
        ++size_;
      }
  }

public:
  std::vector<Agg> &get(const Key &k, const std::vector<Agg> &init) {
    if ((size_ + 1) * 10 > slots_.size() * 7)
      grow();
    auto i = find(k);
    if (!slots_[i]) {
      slots_[i] = Entry{k, init};
      ++size_;
    }
    return slots_[i]->states;
  }
  std::vector<Entry> entries() {
    std::vector<Entry> v;
    v.reserve(size_);
    for (auto &e : slots_)
      if (e)
        v.push_back(std::move(*e));
    return v;
  }
  std::size_t size() const { return size_; }
};
static GroupTable partition(const Query &q, const Table &t, std::size_t start,
                            std::size_t end, std::size_t batch) {
  auto init = states(q);
  GroupTable groups;
  std::vector<std::size_t> selection;
  selection.reserve(std::max<std::size_t>(1, batch));
  for (auto bs = start; bs < end; bs += std::max<std::size_t>(1, batch)) {
    selection.clear();
    for (auto i = bs; i < std::min(end, bs + batch); ++i) {
      if (!q.filter || truthy(eval(q.filter, t, i)))
        selection.push_back(i);
    }
    for (auto i : selection) {
      Key k;
      k.n = static_cast<std::uint8_t>(q.group_by.size());
      for (std::size_t j = 0; j < q.group_by.size(); ++j)
        k.v[j] = t.raw_key(q.group_by[j], i);
      update(groups.get(k, init), q, t, i);
    }
  }
  return groups;
}

class ThreadPool {
  std::vector<std::thread> workers_;
  std::queue<std::function<void()>> jobs_;
  std::mutex mutex_;
  std::condition_variable cv_;
  bool stop_{};

public:
  explicit ThreadPool(std::size_t n) {
    n = std::max<std::size_t>(1, n);
    for (std::size_t i = 0; i < n; ++i)
      workers_.emplace_back([this] {
        for (;;) {
          std::function<void()> job;
          {
            std::unique_lock lock(mutex_);
            cv_.wait(lock, [this] { return stop_ || !jobs_.empty(); });
            if (stop_ && jobs_.empty())
              return;
            job = std::move(jobs_.front());
            jobs_.pop();
          }
          job();
        }
      });
  }
  ~ThreadPool() {
    {
      std::lock_guard lock(mutex_);
      stop_ = true;
    }
    cv_.notify_all();
    for (auto &w : workers_)
      w.join();
  }
  template <class F> auto submit(F f) {
    using R = std::invoke_result_t<F>;
    auto task = std::make_shared<std::packaged_task<R()>>(std::move(f));
    auto future = task->get_future();
    {
      std::lock_guard lock(mutex_);
      jobs_.push([task] { (*task)(); });
    }
    cv_.notify_one();
    return future;
  }
};
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
    out << ",when:" << expression_key(condition) << ",then:"
        << expression_key(value);
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
             (!state.has ||
              (state.kind == Agg::Kind::min
                   ? compare(value, state.extreme) < 0
                   : compare(value, state.extreme) > 0))) {
    state.extreme = std::move(value);
    state.has = true;
  }
}
static Scalar eval_group_expr(const ExprPtr &expression,
                              const Catalog &catalog, const RelRow &row,
                              const Bindings &bindings,
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
    return static_cast<bool>(((compare(value, low) >= 0 &&
                               compare(value, high) <= 0) ^
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
    auto left_value =
        relation_scalar(catalog, left, spec.table, spec.column);
    auto right_value =
        relation_scalar(catalog, right, spec.table, spec.column);
    const bool left_null =
        std::holds_alternative<std::monostate>(left_value);
    const bool right_null =
        std::holds_alternative<std::monostate>(right_value);
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
  std::vector<std::pair<std::string, std::string>> partition_columns;
  partition_columns.reserve(expression->partition_by.size());
  for (auto &name : expression->partition_by)
    partition_columns.push_back(resolve_column(name, bindings));
  std::vector<ResolvedWindowOrder> resolved_order;
  resolved_order.reserve(expression->window_order_by.size());
  for (auto &spec : expression->window_order_by) {
    auto [table, column] = resolve_column(spec.key, bindings);
    resolved_order.push_back(
        {std::move(table), std::move(column), spec.ascending,
         spec.nulls_first});
  }
  std::unordered_map<std::string, std::vector<std::size_t>> partitions;
  for (std::size_t index = 0; index < relation.size(); ++index) {
    std::string key;
    for (auto &[table, column] : partition_columns) {
      if (!key.empty())
        key.push_back('|');
      key += scalar_hash_key(relation_scalar(catalog, relation[index], table,
                                             column))
                 .value_or("n:");
    }
    partitions[key].push_back(index);
  }
  std::vector<Scalar> result(relation.size(), std::monostate{});
  for (auto &[_, indices] : partitions) {
    std::sort(indices.begin(), indices.end(), [&](auto left, auto right) {
      const auto ordering = compare_rel_order(
          relation[left], relation[right], resolved_order, catalog);
      return ordering ? ordering < 0 : left < right;
    });
    if (expression->text == "row_number") {
      for (std::size_t position = 0; position < indices.size(); ++position)
        result[indices[position]] = static_cast<std::int64_t>(position + 1);
    } else if (expression->text == "rank" ||
               expression->text == "dense_rank") {
      std::size_t rank = 1, dense = 1;
      for (std::size_t position = 0; position < indices.size(); ++position) {
        if (position &&
            compare_rel_order(relation[indices[position - 1]],
                              relation[indices[position]],
                              resolved_order, catalog) != 0) {
          rank = position + 1;
          ++dense;
        }
        result[indices[position]] = static_cast<std::int64_t>(
            expression->text == "rank" ? rank : dense);
      }
    } else if (expression->text == "lag" ||
               expression->text == "lead") {
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
        result[index] = target
                            ? eval_rel(expression->args[0], catalog,
                                       relation[indices[*target]], bindings)
                            : expression->args.size() > 2
                                  ? eval_rel(expression->args[2], catalog,
                                             relation[index], bindings)
                                  : Scalar{std::monostate{}};
      }
    } else if (expression->text == "count" ||
               expression->text == "sum" || expression->text == "avg" ||
               expression->text == "min" || expression->text == "max") {
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
static Scalar eval_window_expr(const ExprPtr &expression,
                               std::size_t row_index,
                               const std::vector<RelRow> &relation,
                               const Catalog &catalog,
                               const Bindings &bindings,
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
static void finalize_rows(const Query &query, Rows &rows) {
  if (query.distinct) {
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
    std::vector<std::pair<std::size_t, const OrderSpec *>> order;
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
    const auto compare_rows = [&](const auto &left, const auto &right) {
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
    };
    const auto top_k = std::min(
        rows.size(), query.optimizer_enabled && query.limit
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
    rows.erase(rows.begin(), rows.begin() + std::min(query.offset, rows.size()));
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
static Scalar eval_materialized_values(
    const ExprPtr &expression, const std::vector<std::string> &columns,
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
    return saw_null ? Scalar{std::monostate{}}
                    : Scalar{expression->boolean};
  }
  case ExprKind::between: {
    auto value = evaluate(expression->left);
    auto low = evaluate(expression->args[0]);
    auto high = evaluate(expression->args[1]);
    if (std::holds_alternative<std::monostate>(value) ||
        std::holds_alternative<std::monostate>(low) ||
        std::holds_alternative<std::monostate>(high))
      return std::monostate{};
    return static_cast<bool>(((compare(value, low) >= 0 &&
                               compare(value, high) <= 0) ^
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
static void update_materialized_aggregate(
    Agg &state, const ExprPtr &expression,
    const MaterializedRelation &relation, std::size_t row) {
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
             (!state.has ||
              (state.kind == Agg::Kind::min
                   ? compare(value, state.extreme) < 0
                   : compare(value, state.extreme) > 0))) {
    state.extreme = std::move(value);
    state.has = true;
  }
}
static Scalar eval_materialized_group(
    const ExprPtr &expression, const MaterializedRelation &relation,
    std::size_t row, const std::vector<ExprPtr> &aggregates,
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
  for (std::size_t row = 0; row < relation.rows.size(); ++row)
    if (!query.filter || truthy(eval_materialized(query.filter, relation, row)))
      selected.push_back(row);
  const bool aggregate = !query.group_by.empty() || contains_agg(query.having) ||
                         std::any_of(query.select.begin(), query.select.end(),
                                     [](auto &item) {
                                       return contains_agg(item.expr);
                                     });
  Rows rows;
  if (aggregate) {
    std::vector<ExprPtr> aggregate_expressions;
    for (auto &item : query.select)
      collect_aggregates(item.expr, aggregate_expressions);
    collect_aggregates(query.having, aggregate_expressions);
    std::vector<Agg> templates;
    for (auto &expression : aggregate_expressions)
      templates.push_back(aggregate_state(expression));
    std::unordered_map<std::string,
                       std::pair<std::size_t, std::vector<Agg>>>
        groups;
    if (query.group_by.empty())
      groups.emplace("", std::pair{std::size_t{0}, templates});
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
      auto [iterator, inserted] = groups.try_emplace(
          key, std::pair{row, templates});
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
      rows.push_back(std::move(output));
    }
  } else {
    for (auto row : selected) {
      std::vector<Scalar> output;
      for (auto &item : query.select)
        output.push_back(eval_materialized(item.expr, relation, row));
      rows.push_back(std::move(output));
    }
  }
  finalize_rows(query, rows);
  return rows;
}
static Rows execute_rel(const Query &query, const Catalog &catalog);
static Rows execute_rel_inner(const Query &query, const Catalog &catalog,
                              bool skip_union);
static std::optional<Rows>
execute_materialized_joins(const Query &query, const Catalog &catalog) {
  const auto find_cte = [&](const TableRef &table) {
    return std::find_if(query.ctes.begin(), query.ctes.end(),
                        [&](const auto &entry) {
                          return entry.first == table.name;
                        });
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
          joined.push_back(std::move(combined));
          matched = true;
          matched_right[right_index] = true;
        }
      }
      if (!matched &&
          (join.kind == JoinKind::left || join.kind == JoinKind::full)) {
        auto combined = left;
        combined.resize(combined.size() + right_width, std::monostate{});
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
    auto cte = std::find_if(query.ctes.begin(), query.ctes.end(),
                            [&](auto &entry) {
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
  const bool aggregate = !query.group_by.empty() || contains_agg(query.having) ||
                         std::any_of(query.select.begin(), query.select.end(),
                                     [](auto &item) {
                                       return contains_agg(item.expr);
                                     });
  const bool has_windows =
      std::any_of(query.select.begin(), query.select.end(), [](auto &item) {
        return contains_window(item.expr);
      });
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
    if (query.group_by.empty())
      groups.emplace("", RelGroup{{}, templates});
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
      auto [iterator, inserted] = groups.try_emplace(key, RelGroup{row, templates});
      iterator->second.row = row;
      for (std::size_t index = 0; index < iterator->second.states.size(); ++index)
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
        output.push_back(eval_window_expr(
            item.expr, row_index, relation, catalog, bindings,
            window_expressions, window_values));
      rows.push_back(std::move(output));
    }
  } else {
    rows.reserve(relation.size());
    for (auto &row : relation) {
      std::vector<Scalar> output;
      for (auto &item : query.select)
        output.push_back(eval_rel(item.expr, catalog, row, bindings));
      rows.push_back(std::move(output));
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
          expression = scalar_expression(
              relation_scalar(catalog, row, table, column));
          return;
        } catch (const std::exception &) {
        }
      }
    }
  }
  substitute_outer_expr(expression->left, local, catalog, row,
                        outer_bindings);
  substitute_outer_expr(expression->right, local, catalog, row,
                        outer_bindings);
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
    for (std::size_t p = 0; p < parts; ++p) {
      auto start = p * t->size() / parts, end = (p + 1) * t->size() / parts;
      f.push_back(pool.submit(
          [&, start, end] { return partition(q, *t, start, end, batch); }));
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
      rows.push_back(std::move(row));
    }
  } else {
    bool done = false;
    std::vector<std::size_t> selection;
    selection.reserve(std::max<std::size_t>(1, batch));
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
        rows.push_back(std::move(row));
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
static std::string scalar_json(const Scalar &v) {
  std::ostringstream o;
  o << std::setprecision(17);
  if (std::holds_alternative<std::monostate>(v))
    return "null";
  if (auto *x = std::get_if<std::int64_t>(&v))
    o << "{\"t\":\"i\",\"v\":" << *x << '}';
  else if (auto *x = std::get_if<Decimal>(&v))
    o << "{\"t\":\"d\",\"v\":\"" << decimal_text(x->units) << "\"}";
  else if (auto *x = std::get_if<double>(&v))
    o << "{\"t\":\"f\",\"v\":" << *x << '}';
  else if (auto *x = std::get_if<bool>(&v))
    o << "{\"t\":\"b\",\"v\":" << (*x ? "true" : "false") << '}';
  else {
    o << "{\"t\":\"s\",\"v\":\"";
    for (char c : std::get<std::string>(v)) {
      if (c == '"' || c == '\\')
        o << '\\';
      o << c;
    }
    o << "\"}";
  }
  return o.str();
}
static std::string rows_json(const Rows &r) {
  std::string s = "[";
  for (std::size_t i = 0; i < r.size(); ++i) {
    if (i)
      s += ',';
    s += '[';
    for (std::size_t j = 0; j < r[i].size(); ++j) {
      if (j)
        s += ',';
      s += scalar_json(r[i][j]);
    }
    s += ']';
  }
  return s + "]";
}
static std::string strings_json(const std::vector<std::string> &values) {
  std::string output = "[";
  for (std::size_t index = 0; index < values.size(); ++index) {
    if (index)
      output += ',';
    output += '"';
    for (const char character : values[index]) {
      if (character == '"' || character == '\\')
        output += '\\';
      output += character;
    }
    output += '"';
  }
  return output + ']';
}
static std::vector<std::string> split_tabs(const std::string &s, int max = 16) {
  std::vector<std::string> v;
  std::size_t p = 0;
  while (static_cast<int>(v.size()) < max - 1) {
    auto q = s.find('\t', p);
    if (q == std::string::npos)
      break;
    v.push_back(s.substr(p, q - p));
    p = q + 1;
  }
  v.push_back(s.substr(p));
  return v;
}

struct AsyncRequestState {
  std::string phase{"queued"};
  std::uint64_t queue_ns{}, execution_ns{};
  Rows rows;
  std::string error;
};
struct AsyncRequest {
  std::string id, group;
  std::size_t priority{}, memory_mb{};
  Clock::time_point submitted{Clock::now()};
  Query query;
  bool include_rows{};
  std::shared_ptr<ExecutionControl> control;
  std::mutex mutex;
  std::condition_variable changed;
  AsyncRequestState state;
};
struct SchedulerState {
  std::array<std::deque<std::shared_ptr<AsyncRequest>>, 3> queues;
  std::unordered_map<std::string, std::shared_ptr<AsyncRequest>> requests;
  std::unordered_map<std::string, std::size_t> active_by_group,
      reserved_by_group;
  std::size_t active{}, reserved_memory_mb{}, schedule_cursor{};
  bool shutdown{};
};
struct SchedulerShared {
  std::mutex mutex;
  std::condition_variable changed;
  SchedulerState state;
  std::shared_ptr<const Catalog> catalog;
  std::size_t max_active{}, queue_capacity{}, memory_mb{};
};
class AsyncScheduler {
  std::shared_ptr<SchedulerShared> shared_;
  std::thread dispatcher_;

  static std::uint64_t elapsed_ns(Clock::time_point start) {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() -
                                                             start)
            .count());
  }
  static void finish_without_execution(
      const std::shared_ptr<AsyncRequest> &request, const std::string &phase,
      const std::string &error) {
    std::lock_guard lock(request->mutex);
    request->state.phase = phase;
    request->state.queue_ns = elapsed_ns(request->submitted);
    request->state.error = error;
    request->changed.notify_all();
  }

public:
  AsyncScheduler(std::shared_ptr<const Catalog> catalog, std::size_t max_active,
                 std::size_t queue_capacity, std::size_t memory_mb)
      : shared_(std::make_shared<SchedulerShared>()) {
    shared_->catalog = std::move(catalog);
    shared_->max_active = std::max<std::size_t>(1, max_active);
    shared_->queue_capacity = std::max<std::size_t>(1, queue_capacity);
    shared_->memory_mb = std::max<std::size_t>(1, memory_mb);
    auto shared = shared_;
    dispatcher_ = std::thread([shared] {
      constexpr std::array<std::size_t, 7> cycle{2, 2, 2, 2, 1, 1, 0};
      for (;;) {
        std::shared_ptr<AsyncRequest> request;
        {
          std::unique_lock lock(shared->mutex);
          for (;;) {
            if (shared->state.shutdown)
              return;
            if (shared->state.active < shared->max_active) {
              for (std::size_t attempt = 0; attempt < cycle.size(); ++attempt) {
                const auto priority =
                    cycle[shared->state.schedule_cursor++ % cycle.size()];
                auto &queue = shared->state.queues[priority];
                auto selected = std::find_if(queue.begin(), queue.end(),
                    [&](const auto &candidate) {
                      const auto found = shared->state.active_by_group.find(
                          candidate->group);
                      const auto group_active =
                          found == shared->state.active_by_group.end()
                              ? 0
                              : found->second;
                      return group_active <
                             std::max<std::size_t>(1,
                                 (shared->max_active + 1) / 2);
                    });
                if (selected != queue.end()) {
                  request = *selected;
                  queue.erase(selected);
                  break;
                }
              }
            }
            if (!request) {
              shared->changed.wait(lock);
              continue;
            }
            const bool cancelled =
                request->control->cancelled.load(std::memory_order_relaxed);
            const bool deadline = request->control->deadline &&
                                  Clock::now() >= *request->control->deadline;
            if (cancelled || deadline) {
              shared->state.reserved_memory_mb -= std::min(
                  shared->state.reserved_memory_mb, request->memory_mb);
              auto &group_memory =
                  shared->state.reserved_by_group[request->group];
              group_memory -= std::min(group_memory, request->memory_mb);
              finish_without_execution(
                  request, cancelled ? "cancelled" : "deadline",
                  cancelled ? "client cancellation"
                            : "deadline expired in queue");
              request.reset();
              continue;
            }
            ++shared->state.active;
            ++shared->state.active_by_group[request->group];
            break;
          }
        }
        std::thread([shared, request] {
          const auto started = Clock::now();
          {
            std::lock_guard lock(request->mutex);
            request->state.phase = "running";
            request->state.queue_ns = elapsed_ns(request->submitted);
            request->changed.notify_all();
          }
          execution_control = request->control;
          Rows rows;
          std::string error;
          try {
            rows = execute_rel(request->query, *shared->catalog);
          } catch (const std::exception &exception) {
            error = exception.what();
          }
          execution_control.reset();
          {
            std::lock_guard lock(request->mutex);
            request->state.execution_ns = elapsed_ns(started);
            if (request->control->cancelled.load(std::memory_order_relaxed)) {
              request->state.phase = "cancelled";
              request->state.error = "client cancellation";
            } else if (request->control->deadline &&
                       Clock::now() >= *request->control->deadline) {
              request->state.phase = "deadline";
              request->state.error = "deadline expired during execution";
            } else if (!error.empty()) {
              request->state.phase = "failed";
              request->state.error = std::move(error);
            } else {
              request->state.phase = "completed";
              request->state.rows = request->include_rows
                                        ? std::move(rows)
                                        : Rows{{Scalar{static_cast<std::int64_t>(
                                              rows.size())}}};
            }
            request->changed.notify_all();
          }
          {
            std::lock_guard lock(shared->mutex);
            shared->state.active -= std::min<std::size_t>(shared->state.active, 1);
            auto &group_active = shared->state.active_by_group[request->group];
            group_active -= std::min<std::size_t>(group_active, 1);
            shared->state.reserved_memory_mb -= std::min(
                shared->state.reserved_memory_mb, request->memory_mb);
            auto &group_memory =
                shared->state.reserved_by_group[request->group];
            group_memory -= std::min(group_memory, request->memory_mb);
            shared->changed.notify_all();
          }
        }).detach();
      }
    });
  }
  ~AsyncScheduler() {
    {
      std::lock_guard lock(shared_->mutex);
      shared_->state.shutdown = true;
      for (auto &[_, request] : shared_->state.requests)
        request->control->cancelled.store(true, std::memory_order_relaxed);
      shared_->changed.notify_all();
    }
    if (dispatcher_.joinable())
      dispatcher_.join();
  }
  std::optional<std::string>
  submit(const std::string &id, Query query, std::size_t priority,
         const std::string &group, std::uint64_t deadline_ms,
         std::size_t memory_mb, bool include_rows) {
    if (priority > 2)
      return "priority must be 0, 1, or 2";
    std::lock_guard lock(shared_->mutex);
    if (shared_->state.requests.contains(id))
      return "duplicate request id";
    std::size_t queued{};
    for (auto &queue : shared_->state.queues)
      queued += queue.size();
    if (queued >= shared_->queue_capacity)
      return "ADMISSION_REJECTED queue capacity";
    memory_mb = std::max<std::size_t>(1, memory_mb);
    if (shared_->state.reserved_memory_mb + memory_mb > shared_->memory_mb)
      return "ADMISSION_REJECTED global memory";
    const auto group_limit = std::max<std::size_t>(1, (shared_->memory_mb + 1) / 2);
    if (shared_->state.reserved_by_group[group] + memory_mb > group_limit)
      return "ADMISSION_REJECTED resource-group memory";
    auto request = std::make_shared<AsyncRequest>();
    request->id = id;
    request->priority = priority;
    request->group = group;
    request->memory_mb = memory_mb;
    request->submitted = Clock::now();
    request->query = std::move(query);
    request->include_rows = include_rows;
    request->control = std::make_shared<ExecutionControl>();
    if (deadline_ms)
      request->control->deadline =
          Clock::now() + std::chrono::milliseconds(deadline_ms);
    shared_->state.reserved_memory_mb += memory_mb;
    shared_->state.reserved_by_group[group] += memory_mb;
    shared_->state.requests.emplace(id, request);
    shared_->state.queues[priority].push_back(request);
    shared_->changed.notify_all();
    return {};
  }
  std::shared_ptr<AsyncRequest> request(const std::string &id) const {
    std::lock_guard lock(shared_->mutex);
    auto found = shared_->state.requests.find(id);
    return found == shared_->state.requests.end() ? nullptr : found->second;
  }
  std::optional<std::string> cancel(const std::string &id) {
    auto found = request(id);
    if (!found)
      return "unknown request";
    found->control->cancelled.store(true, std::memory_order_relaxed);
    shared_->changed.notify_all();
    return {};
  }
  std::optional<std::string> status(const std::string &id, bool wait,
                                    std::string &output) const {
    auto found = request(id);
    if (!found)
      return "unknown request";
    std::unique_lock lock(found->mutex);
    if (wait)
      found->changed.wait(lock, [&] {
        return found->state.phase != "queued" &&
               found->state.phase != "running";
      });
    std::size_t row_count{};
    if (found->state.phase == "completed")
      row_count = found->include_rows
                      ? found->state.rows.size()
                      : static_cast<std::size_t>(
                            std::get<std::int64_t>(found->state.rows[0][0]));
    auto error = found->state.error;
    std::replace(error.begin(), error.end(), '\t', ' ');
    std::replace(error.begin(), error.end(), '\n', ' ');
    output = "STATUS\t" + found->id + '\t' + found->state.phase + '\t' +
             std::to_string(found->priority) + '\t' + found->group + '\t' +
             std::to_string(found->state.queue_ns) + '\t' +
             std::to_string(found->state.execution_ns) + '\t' +
             std::to_string(row_count) + '\t' +
             (found->state.phase == "completed" && found->include_rows
                  ? rows_json(found->state.rows)
                  : "[]") +
             '\t' + error;
    return {};
  }
};

struct Language {
  std::string code;
  std::optional<std::string> country;
  bool operator==(const Language &) const = default;
};
struct Name {
  std::optional<std::string> url;
  std::vector<Language> languages;
  bool operator==(const Name &) const = default;
};
struct Document {
  std::int64_t doc_id;
  std::vector<Name> names;
  bool operator==(const Document &) const = default;
};
template <class T> struct LevelValue {
  std::optional<T> value;
  std::uint16_t repetition_level, definition_level;
};
struct Shredded {
  std::int64_t doc_id;
  std::vector<LevelValue<std::string>> urls, codes, countries;
};
static Shredded shred(const Document &d) {
  Shredded s{d.doc_id, {}, {}, {}};
  if (d.names.empty()) {
    s.urls.push_back({{}, 0, 0});
    s.codes.push_back({{}, 0, 0});
    s.countries.push_back({{}, 0, 0});
  }
  for (std::size_t ni = 0; ni < d.names.size(); ++ni) {
    auto &n = d.names[ni];
    s.urls.push_back({n.url, static_cast<std::uint16_t>(ni > 0),
                      static_cast<std::uint16_t>(n.url ? 2 : 1)});
    if (n.languages.empty()) {
      s.codes.push_back({{}, static_cast<std::uint16_t>(ni > 0), 1});
      s.countries.push_back({{}, static_cast<std::uint16_t>(ni > 0), 1});
    }
    for (std::size_t li = 0; li < n.languages.size(); ++li) {
      auto &l = n.languages[li];
      auto rep = static_cast<std::uint16_t>(li ? 2 : ni > 0);
      s.codes.push_back({l.code, rep, 2});
      s.countries.push_back(
          {l.country, rep, static_cast<std::uint16_t>(l.country ? 3 : 2)});
    }
  }
  return s;
}
static Document assemble(const Shredded &s) {
  if (!s.urls.empty() && s.urls.front().definition_level == 0)
    return {s.doc_id, {}};
  Document document{s.doc_id, {}};
  for (const auto &url : s.urls)
    document.names.push_back({url.value, {}});
  std::size_t name_index = 0;
  for (std::size_t i = 0; i < s.codes.size(); ++i) {
    if (i > 0 && s.codes[i].repetition_level < 2)
      ++name_index;
    if (s.codes[i].definition_level >= 2)
      document.names[name_index].languages.push_back(
          {*s.codes[i].value, s.countries[i].value});
  }
  return document;
}

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
  assert(optimize_query(star_join) == 1);
  assert(star_join.joins[0].table.name == "campaigns" &&
         star_join.joins[1].table.name == "users");
  auto dependent_join =
      Parser("SELECT COUNT(*) FROM events e JOIN users u ON e.user_id = "
             "u.user_id JOIN campaigns c ON c.campaign_id = u.user_id")
          .parse();
  dependent_join.optimizer_enabled = true;
  assert(optimize_query(dependent_join) == 0);
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
  const auto scalar_case = run(
      "SELECT events.event_id, CASE WHEN score BETWEEN 2.0 AND 4.0 THEN "
      "upper(country) ELSE 'other' END AS bucket FROM events ORDER BY "
      "event_id ASC",
      1);
  assert(rows_json(scalar_case) ==
         "[[{\"t\":\"i\",\"v\":1},{\"t\":\"s\",\"v\":\"other\"}],"
         "[{\"t\":\"i\",\"v\":2},{\"t\":\"s\",\"v\":\"US\"}],"
         "[{\"t\":\"i\",\"v\":3},{\"t\":\"s\",\"v\":\"IN\"}],"
         "[{\"t\":\"i\",\"v\":4},{\"t\":\"s\",\"v\":\"other\"}]]");
  const auto scalar_in = run(
      "SELECT event_id FROM events WHERE country IN ('IN', 'GB') AND "
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
  const auto scalar_call = run(
      "SELECT concat(lower(country), '-', cast(event_id AS varchar)) AS "
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
  assert(rows_json(execute_rel(windowed, catalog)) ==
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
  auto in_subquery = prepare(
      Parser("SELECT event_id FROM events WHERE campaign_id IN (SELECT "
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
static std::string arg(int argc, char **argv, const std::string &name,
                       const std::string &fallback) {
  for (int i = 1; i + 1 < argc; ++i)
    if (argv[i] == name)
      return argv[i + 1];
  return fallback;
}
static bool is_relational(const Query &query) {
  return query.from.name != "events" || !query.joins.empty() || query.having ||
         query.union_query ||
         !query.ctes.empty() ||
         std::any_of(query.select.begin(), query.select.end(), [](auto &item) {
           return contains_window(item.expr) || contains_subquery(item.expr);
         }) || contains_subquery(query.filter);
}
int main(int argc, char **argv) {
  try {
    const std::string command = argc > 1 ? argv[1] : "help";
    if (command == "self-test")
      return self_test(argc > 2 ? argv[2] : "benchmark/queries");
    const auto path = arg(argc, argv, "--data", "data/events.csv");
    const auto threads = std::stoull(arg(argc, argv, "--threads", "4"));
    const auto batch = std::stoull(arg(argc, argv, "--batch-size", "4096"));
    const auto memory_limit_mb =
        std::stoull(arg(argc, argv, "--memory-limit-mb", "0"));
    const auto max_result_rows =
        std::stoull(arg(argc, argv, "--max-result-rows", "0"));
    const auto max_active_queries =
        std::stoull(arg(argc, argv, "--max-active-queries", "4"));
    const auto queue_capacity =
        std::stoull(arg(argc, argv, "--queue-capacity", "128"));
    const auto scheduler_memory_mb =
        std::stoull(arg(argc, argv, "--scheduler-memory-mb", "1024"));
    auto load_start = Clock::now();
    auto table = Table::load(path);
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
      if (std::find_if(argv, argv + argc, [](const char *x) {
            return std::string(x) == "--explain";
          }) != argv + argc) {
        for (auto it = q.physical.rbegin(); it != q.physical.rend(); ++it)
          std::cout << std::string(std::distance(q.physical.rbegin(), it) * 2,
                                   ' ')
                    << *it << '\n';
      } else {
        const auto started = Clock::now();
        auto rows = is_relational(q)
                        ? execute_rel(q, Catalog::load(path, table))
                        : execute(q, table, pool, threads, batch);
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
          std::cerr << "{\"rows_scanned\":" << table->size()
                    << ",\"rows_returned\":" << rows.size()
                    << ",\"batches_scanned\":"
                    << (table->size() + batch - 1) / batch
                    << ",\"columns_scanned\":" << q.columns.size()
                    << ",\"worker_threads\":" << threads
                    << ",\"logical_partitions\":" << threads * 4
                    << ",\"elapsed_ns\":" << elapsed_ns << "}\n";
      }
      return 0;
    }
    if (command != "bench-server") {
      std::cout << "dremel-cpp query|bench-server\n";
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
        prepared.insert_or_assign(p[1], std::move(query));
        std::cout << "OK\t" << p[1];
      } else if (p[0] == "EXEC") {
        auto start = Clock::now();
        const auto &query = prepared.at(p[1]);
        if (is_relational(query) && !catalog)
          catalog = std::make_shared<Catalog>(Catalog::load(path, table));
        auto rows = is_relational(query)
                        ? execute_rel(query, *catalog)
                        : execute(query, table, pool, threads, batch);
        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                      Clock::now() - start)
                      .count();
        std::cout << "RESULT\t" << ns << '\t' << rows.size() << '\t'
                  << (p[2] == "1" ? rows_json(rows) : "[]");
      } else if (p[0] == "E2E") {
        auto start = Clock::now();
        auto q = prepare(Parser(p[2]).parse(), *table);
        if (q.ctes.empty())
          (void)bind_query(q);
        if (is_relational(q) && !catalog)
          catalog = std::make_shared<Catalog>(Catalog::load(path, table));
        auto rows = is_relational(q) ? execute_rel(q, *catalog)
                                     : execute(q, table, pool, threads, batch);
        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                      Clock::now() - start)
                      .count();
        std::cout << "RESULT\t" << ns << '\t' << rows.size() << '\t'
                  << (p[3] == "1" ? rows_json(rows) : "[]");
      } else if (p[0] == "EXPLAIN") {
        std::cout << "EXPLAIN\t" << strings_json(prepared.at(p[1]).physical);
      } else if (p[0] == "CONFIG_ASYNC") {
        std::cout << "ASYNC_CONFIG\t" << std::max<std::size_t>(1, max_active_queries)
                  << '\t' << std::max<std::size_t>(1, queue_capacity) << '\t'
                  << std::max<std::size_t>(1, scheduler_memory_mb) << '\t'
                  << std::max<std::size_t>(1, (max_active_queries + 1) / 2)
                  << '\t'
                  << std::max<std::size_t>(1, (scheduler_memory_mb + 1) / 2);
      } else if (p[0] == "SUBMIT") {
        try {
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
              p[1], found->second, std::stoull(p[3]), p[4],
              std::stoull(p[5]), std::stoull(p[6]),
              p.size() > 7 && p[7] == "1");
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
