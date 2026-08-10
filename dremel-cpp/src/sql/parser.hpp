#pragma once

#include "../core/types.hpp"

namespace dremel {

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
  std::pair<TableRef,
            std::optional<std::pair<std::string, std::shared_ptr<Query>>>>
  relation_ref() {
    if (peek().kind != TokenKind::lparen)
      return {table_ref(), {}};
    ++pos_;
    auto query = std::make_shared<Query>(subquery());
    (void)word("as");
    auto alias = next();
    if (alias.kind != TokenKind::word)
      throw std::runtime_error("derived table requires an alias");
    return {{alias.text, alias.text}, std::pair{alias.text, std::move(query)}};
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
        if (scale.kind != TokenKind::number || next().kind != TokenKind::rparen)
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
        if (cursor >= tokens_.size() || tokens_[cursor].kind != TokenKind::word)
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
        ctes.emplace_back(name, std::make_shared<Query>(std::move(cte_query)));
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
        std::vector<Token> left_tokens(tokens_.begin(),
                                       tokens_.begin() + index);
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
           : table == "users"     ? std::size_t{250'000}
           : table == "campaigns" ? std::size_t{5'000}
                                  : std::size_t{1'000};
  };
  auto estimated_rows = table_rows(q.from.name);
  const bool materialized_joins = !q.ctes.empty();
  for (auto &join : q.joins) {
    const std::string kind = join.kind == JoinKind::cross   ? "Cross"
                             : join.kind == JoinKind::inner ? "Inner"
                             : join.kind == JoinKind::left  ? "Left"
                             : join.kind == JoinKind::right ? "Right"
                                                            : "Full";
    q.logical.push_back(kind + "Join(" + join.table.name + ")");
    const bool equi = join.on && join.on->kind == ExprKind::binary &&
                      join.on->text == "=" && join.on->left && join.on->right &&
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
            : "NestedLoopJoinExec(type=" + kind + ";table=" + join.table.name +
                  ";estimated_rows=" + std::to_string(estimated_rows) + ")");
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
  if (std::any_of(q.select.begin(), q.select.end(),
                  [](auto &item) { return contains_subquery(item.expr); }) ||
      contains_subquery(q.filter)) {
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

} // namespace dremel
