#pragma once

#include "evaluator.hpp"
#include "memory.hpp"

namespace dremel {

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
  const auto size = table == "events"  ? catalog.events->size()
                    : table == "users" ? catalog.users.user_id.size()
                    : table == "campaigns"
                        ? catalog.campaigns.campaign_id.size()
                        : 0;
  account_query_memory(size * sizeof(RelRow), "relation materialization");
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
      expression->text != "=" || expression->left->kind != ExprKind::column ||
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
    if (right_rows.size() &&
        left_rows.size() >
            std::numeric_limits<std::size_t>::max() / right_rows.size())
      throw std::runtime_error(
          "RESOURCE_EXHAUSTED cross join cardinality overflow");
    account_query_memory(left_rows.size() * right_rows.size() * sizeof(RelRow),
                         "cross join output");
    output.reserve(left_rows.size() * right_rows.size());
    for (auto &left : left_rows)
      for (auto &right : right_rows)
        output.push_back(merge_rel_rows(left, right));
    return output;
  }
  auto equality = join_equality(join.on, join.table.name, bindings);
  account_query_memory(right_rows.size(), "join match bitmap");
  std::vector<bool> matched_right(right_rows.size());
  if (optimizer_enabled && equality) {
    const auto [left_table, left_column] =
        resolve_column(equality->first->text, bindings);
    const auto [right_table, right_column] =
        resolve_column(equality->second->text, bindings);
    std::unordered_map<std::string, std::vector<std::size_t>> hash;
    for (std::size_t index = 0; index < right_rows.size(); ++index)
      if (auto key = scalar_hash_key(relation_scalar(
              catalog, right_rows[index], right_table, right_column))) {
        if (!hash.contains(*key))
          account_query_memory(sizeof(std::string) + key->size() + 64,
                               "hash join build table");
        account_query_memory(sizeof(std::size_t),
                             "hash join build candidates");
        hash[*key].push_back(index);
      }
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
            account_query_memory(sizeof(RelRow), "join output");
            output.push_back(combined);
            matched = true;
            matched_right[index] = true;
          }
        }
      }
      if (!matched &&
          (join.kind == JoinKind::left || join.kind == JoinKind::full)) {
        account_query_memory(sizeof(RelRow), "join output");
        output.push_back(left);
      }
    }
  } else {
    for (auto &left : left_rows) {
      if (execution_cancelled())
        break;
      bool matched = false;
      for (std::size_t index = 0; index < right_rows.size(); ++index) {
        const auto combined = merge_rel_rows(left, right_rows[index]);
        if (truthy(eval_rel(join.on, catalog, combined, bindings))) {
          account_query_memory(sizeof(RelRow), "join output");
          output.push_back(combined);
          matched = true;
          matched_right[index] = true;
        }
      }
      if (!matched &&
          (join.kind == JoinKind::left || join.kind == JoinKind::full)) {
        account_query_memory(sizeof(RelRow), "join output");
        output.push_back(left);
      }
    }
  }
  if (join.kind == JoinKind::right || join.kind == JoinKind::full)
    for (std::size_t index = 0; index < right_rows.size(); ++index)
      if (!matched_right[index]) {
        account_query_memory(sizeof(RelRow), "join output");
        output.push_back(right_rows[index]);
      }
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
  std::vector<std::optional<Entry>> slots_;
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
    account_query_memory(old.size() * 2 * sizeof(std::optional<Entry>),
                         "hash aggregation table growth");
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
  GroupTable() {
    account_query_memory(16 * sizeof(std::optional<Entry>),
                         "hash aggregation table");
    slots_.resize(16);
  }
  std::vector<Agg> &get(const Key &k, const std::vector<Agg> &init) {
    if ((size_ + 1) * 10 > slots_.size() * 7)
      grow();
    auto i = find(k);
    if (!slots_[i]) {
      account_query_memory(sizeof(Entry) + init.size() * sizeof(Agg),
                           "hash aggregation group");
      slots_[i] = Entry{k, init};
      ++size_;
    }
    return slots_[i]->states;
  }
  std::vector<Entry> entries() {
    account_query_memory(size_ * sizeof(Entry), "aggregation merge buffer");
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
  account_query_memory(std::max<std::size_t>(1, batch) * sizeof(std::size_t),
                       "aggregation selection");
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

} // namespace dremel
