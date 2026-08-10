#pragma once

#include "protocol.hpp"

namespace dremel {

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
  static void
  finish_without_execution(const std::shared_ptr<AsyncRequest> &request,
                           const std::string &phase, const std::string &error) {
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
                auto selected = std::find_if(
                    queue.begin(), queue.end(), [&](const auto &candidate) {
                      const auto found =
                          shared->state.active_by_group.find(candidate->group);
                      const auto group_active =
                          found == shared->state.active_by_group.end()
                              ? 0
                              : found->second;
                      return group_active <
                             std::max<std::size_t>(1, (shared->max_active + 1) /
                                                          2);
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
              finish_without_execution(request,
                                       cancelled ? "cancelled" : "deadline",
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
              request->state.rows =
                  request->include_rows
                      ? std::move(rows)
                      : Rows{{Scalar{static_cast<std::int64_t>(rows.size())}}};
            }
            request->changed.notify_all();
          }
          {
            std::lock_guard lock(shared->mutex);
            shared->state.active -=
                std::min<std::size_t>(shared->state.active, 1);
            auto &group_active = shared->state.active_by_group[request->group];
            group_active -= std::min<std::size_t>(group_active, 1);
            shared->state.reserved_memory_mb -=
                std::min(shared->state.reserved_memory_mb, request->memory_mb);
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
  std::optional<std::string> submit(const std::string &id, Query query,
                                    std::size_t priority,
                                    const std::string &group,
                                    std::uint64_t deadline_ms,
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
    const auto group_limit =
        std::max<std::size_t>(1, (shared_->memory_mb + 1) / 2);
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

} // namespace dremel
