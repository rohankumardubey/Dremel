#pragma once

#include "../execution/relational.hpp"

namespace dremel {

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

} // namespace dremel
