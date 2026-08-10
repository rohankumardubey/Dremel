#pragma once

#include <algorithm>
#include <array>
#include <atomic>
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

namespace dremel {

using Clock = std::chrono::steady_clock;
struct Decimal {
  std::int64_t units{};
  auto operator<=>(const Decimal &) const = default;
};
using Scalar = std::variant<std::monostate, std::int64_t, Decimal, double, bool,
                            std::string>;
using Rows = std::vector<std::vector<Scalar>>;

struct ExecutionControl {
  std::atomic_bool cancelled{};
  std::optional<Clock::time_point> deadline;
};
inline thread_local std::shared_ptr<ExecutionControl> execution_control;
static bool execution_cancelled() {
  return execution_control &&
         (execution_control->cancelled.load(std::memory_order_relaxed) ||
          (execution_control->deadline &&
           Clock::now() >= *execution_control->deadline));
}

} // namespace dremel
