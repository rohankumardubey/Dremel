#pragma once

namespace dremel {

class StreamMemoryReservation {
  std::shared_ptr<QueryMemory> memory_;
  std::size_t bytes_{};

public:
  StreamMemoryReservation(std::size_t bytes, std::string_view operation)
      : memory_(query_memory), bytes_(bytes) {
    account_query_memory(bytes_, operation);
  }
  StreamMemoryReservation(const StreamMemoryReservation &) = delete;
  ~StreamMemoryReservation() {
    if (memory_)
      memory_->release(bytes_);
  }
};

static bool streamable_result(const Query &query) {
  return query.from.name == "events" && query.joins.empty() &&
         query.group_by.empty() && !query.having && query.order_by.empty() &&
         !query.union_query && query.ctes.empty() && !query.distinct &&
         std::none_of(query.select.begin(), query.select.end(),
                      [](const auto &item) {
                        return contains_agg(item.expr) ||
                               contains_window(item.expr) ||
                               contains_subquery(item.expr);
                      }) &&
         !contains_subquery(query.filter);
}

template <class Sink> class ResultStreamer {
  const Query &query_;
  std::size_t batch_size_;
  std::size_t remaining_offset_;
  std::optional<std::size_t> remaining_limit_;
  Sink &sink_;
  ResultStreamMetrics metrics_;

  bool finished() const { return remaining_limit_ == 0; }

public:
  ResultStreamer(const Query &query, std::size_t batch_size, Sink &sink)
      : query_(query), batch_size_(std::max<std::size_t>(1, batch_size)),
        remaining_offset_(query.offset), remaining_limit_(query.limit),
        sink_(sink) {}

  void consume(const Table &table) {
    if (finished() || filter_always_false(query_.filter))
      return;
    for (std::size_t start = 0; start < table.size(); start += batch_size_) {
      if (execution_cancelled())
        throw std::runtime_error("query cancelled during result streaming");
      ++metrics_.batches_scanned;
      const auto capacity = std::min(batch_size_, table.size() - start);
      StreamMemoryReservation selection_memory(
          capacity * sizeof(std::size_t), "streaming scan selection");
      std::vector<std::size_t> selection;
      selection.reserve(capacity);
      for (auto row = start;
           row < std::min(table.size(), start + batch_size_); ++row)
        if (!query_.filter || truthy(eval(query_.filter, table, row)))
          selection.push_back(row);
      for (auto row_index : selection) {
        if (remaining_offset_) {
          --remaining_offset_;
          continue;
        }
        if (finished())
          return;
        std::vector<Scalar> row;
        row.reserve(query_.select.size());
        for (const auto &item : query_.select)
          row.push_back(eval(item.expr, table, row_index));
        StreamMemoryReservation row_memory(row_bytes(row),
                                           "streaming result row");
        metrics_.output_bytes += sink_(row);
        ++metrics_.rows_returned;
        if (remaining_limit_)
          --*remaining_limit_;
      }
    }
  }

  ResultStreamMetrics metrics() const { return metrics_; }
};

template <class Sink>
static ResultStreamMetrics stream_table_results(const Query &query,
                                                const Table &table,
                                                std::size_t batch_size,
                                                Sink &sink) {
  ResultStreamer<Sink> streamer(query, batch_size, sink);
  streamer.consume(table);
  return streamer.metrics();
}

template <class Sink>
static std::pair<ResultStreamMetrics, ParquetScanMetrics>
stream_parquet_results(const Query &query, const std::string &path,
                       std::size_t batch_size, std::size_t memory_limit_mb,
                       Sink &sink) {
  ResultStreamer<Sink> streamer(query, batch_size, sink);
  auto [dictionaries, scan] = stream_parquet_direct(
      path, query, batch_size, [&](std::shared_ptr<Table> table) {
        if (memory_limit_mb &&
            table->approximate_bytes() > memory_limit_mb * 1024 * 1024)
          throw std::runtime_error(
              "RESOURCE_EXHAUSTED streaming Parquet batch requires "
              "approximately " +
              std::to_string(table->approximate_bytes()) + " bytes");
        streamer.consume(*table);
      });
  (void)dictionaries;
  return {streamer.metrics(), scan};
}

} // namespace dremel
