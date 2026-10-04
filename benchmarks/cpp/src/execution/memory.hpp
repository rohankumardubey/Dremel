#pragma once

#include "../core/types.hpp"

namespace dremel {

class QueryMemory {
  std::size_t limit_bytes_{};
  std::atomic_size_t accounted_bytes_{};
  std::atomic_size_t peak_accounted_bytes_{};

  void update_peak(std::size_t value) {
    auto peak = peak_accounted_bytes_.load(std::memory_order_relaxed);
    while (peak < value && !peak_accounted_bytes_.compare_exchange_weak(
                               peak, value, std::memory_order_relaxed,
                               std::memory_order_relaxed)) {
    }
  }

public:
  explicit QueryMemory(std::size_t limit_mb)
      : limit_bytes_(limit_mb > std::numeric_limits<std::size_t>::max() /
                                    (1024 * 1024)
                         ? std::numeric_limits<std::size_t>::max()
                         : limit_mb * 1024 * 1024) {}

  void account(std::size_t bytes, std::string_view operation) {
    if (!bytes)
      return;
    auto current = accounted_bytes_.load(std::memory_order_relaxed);
    for (;;) {
      if (bytes > std::numeric_limits<std::size_t>::max() - current)
        throw std::runtime_error(
            "RESOURCE_EXHAUSTED query memory accounting overflow in " +
            std::string(operation));
      const auto next = current + bytes;
      if (limit_bytes_ && next > limit_bytes_)
        throw std::runtime_error(
            "RESOURCE_EXHAUSTED " + std::string(operation) + " has " +
            std::to_string(current) + " accounted bytes and requested " +
            std::to_string(bytes) + " more; limit is " +
            std::to_string(limit_bytes_) + " bytes");
      if (accounted_bytes_.compare_exchange_weak(
              current, next, std::memory_order_relaxed,
              std::memory_order_relaxed)) {
        update_peak(next);
        return;
      }
    }
  }

  bool try_account(std::size_t bytes) {
    if (!bytes)
      return true;
    auto current = accounted_bytes_.load(std::memory_order_relaxed);
    for (;;) {
      if (bytes > std::numeric_limits<std::size_t>::max() - current)
        return false;
      const auto next = current + bytes;
      if (limit_bytes_ && next > limit_bytes_)
        return false;
      if (accounted_bytes_.compare_exchange_weak(
              current, next, std::memory_order_relaxed,
              std::memory_order_relaxed)) {
        update_peak(next);
        return true;
      }
    }
  }

  void release(std::size_t bytes) {
    auto current = accounted_bytes_.load(std::memory_order_relaxed);
    for (;;) {
      const auto next = bytes > current ? 0 : current - bytes;
      if (accounted_bytes_.compare_exchange_weak(
              current, next, std::memory_order_relaxed,
              std::memory_order_relaxed))
        return;
    }
  }

  std::size_t accounted_bytes() const {
    return accounted_bytes_.load(std::memory_order_relaxed);
  }
  std::size_t limit_bytes() const { return limit_bytes_; }
  std::size_t peak_accounted_bytes() const {
    return peak_accounted_bytes_.load(std::memory_order_relaxed);
  }
};

inline thread_local std::shared_ptr<QueryMemory> query_memory;

class QueryMemoryScope {
  std::shared_ptr<QueryMemory> previous_;

public:
  explicit QueryMemoryScope(std::shared_ptr<QueryMemory> memory)
      : previous_(std::exchange(query_memory, std::move(memory))) {}
  ~QueryMemoryScope() { query_memory = std::move(previous_); }
};

static void account_query_memory(std::size_t bytes,
                                 std::string_view operation) {
  if (query_memory)
    query_memory->account(bytes, operation);
}

static std::size_t row_bytes(const std::vector<Scalar> &row) {
  std::size_t bytes = sizeof(row) + row.capacity() * sizeof(Scalar);
  for (const auto &value : row)
    if (const auto *text = std::get_if<std::string>(&value))
      bytes += text->capacity();
  return bytes;
}

} // namespace dremel
