#pragma once

#include "../core/types.hpp"

namespace dremel {

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

} // namespace dremel
