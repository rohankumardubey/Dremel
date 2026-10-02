#pragma once

namespace dremel {

constexpr std::size_t sort_io_buffer_bytes = 4096;

class SortReservation {
  std::shared_ptr<QueryMemory> memory_;
  std::size_t bytes_{};

public:
  SortReservation(std::size_t bytes, std::string_view operation)
      : memory_(query_memory), bytes_(bytes) {
    if (!memory_)
      throw std::runtime_error(
          "external sort requires query memory accounting");
    memory_->account(bytes_, operation);
  }
  SortReservation(const SortReservation &) = delete;
  ~SortReservation() { memory_->release(bytes_); }
};

static bool spillable_sort(const Query &query) {
  return query.from.name == "events" && query.joins.empty() &&
         query.group_by.empty() && !query.having && !query.union_query &&
         query.ctes.empty() && !query.distinct && !query.order_by.empty() &&
         std::none_of(query.select.begin(), query.select.end(),
                      [](const auto &item) {
                        return contains_agg(item.expr) ||
                               contains_window(item.expr) ||
                               contains_subquery(item.expr);
                      }) &&
         !contains_subquery(query.filter);
}

template <std::unsigned_integral Value>
static void sort_append_le(std::vector<char> &output, Value value) {
  for (std::size_t index = 0; index < sizeof(Value); ++index)
    output.push_back(static_cast<char>((value >> (index * 8)) & 0xff));
}

static void sort_append_size(std::vector<char> &output, std::size_t value) {
  if (value > std::numeric_limits<std::uint32_t>::max())
    throw std::runtime_error("sort spill value exceeds 4 GiB");
  sort_append_le(output, static_cast<std::uint32_t>(value));
}

static std::vector<char> encode_sort_row(const std::vector<Scalar> &row) {
  std::vector<char> payload;
  sort_append_size(payload, row.size());
  for (const auto &value : row) {
    if (std::holds_alternative<std::monostate>(value)) {
      payload.push_back(0);
    } else if (const auto *integer = std::get_if<std::int64_t>(&value)) {
      payload.push_back(1);
      sort_append_le(payload, std::bit_cast<std::uint64_t>(*integer));
    } else if (const auto *decimal = std::get_if<Decimal>(&value)) {
      payload.push_back(2);
      sort_append_le(payload, std::bit_cast<std::uint64_t>(decimal->units));
    } else if (const auto *floating = std::get_if<double>(&value)) {
      payload.push_back(3);
      sort_append_le(payload, std::bit_cast<std::uint64_t>(*floating));
    } else if (const auto *boolean = std::get_if<bool>(&value)) {
      payload.push_back(4);
      payload.push_back(*boolean ? 1 : 0);
    } else if (const auto *text = std::get_if<std::string>(&value)) {
      payload.push_back(5);
      sort_append_size(payload, text->size());
      payload.insert(payload.end(), text->begin(), text->end());
    }
  }
  std::vector<char> encoded;
  encoded.reserve(payload.size() + 4);
  sort_append_size(encoded, payload.size());
  encoded.insert(encoded.end(), payload.begin(), payload.end());
  return encoded;
}

template <std::unsigned_integral Value>
static Value sort_take_le(const std::vector<char> &input, std::size_t &offset) {
  if (input.size() - std::min(input.size(), offset) < sizeof(Value))
    throw std::runtime_error("corrupt external sort row");
  Value value{};
  for (std::size_t index = 0; index < sizeof(Value); ++index)
    value |= static_cast<Value>(static_cast<unsigned char>(input[offset++]))
             << (index * 8);
  return value;
}

static std::vector<Scalar> decode_sort_row(const std::vector<char> &payload) {
  std::size_t offset{};
  const auto columns = sort_take_le<std::uint32_t>(payload, offset);
  std::vector<Scalar> row;
  row.reserve(columns);
  for (std::uint32_t column = 0; column < columns; ++column) {
    if (offset == payload.size())
      throw std::runtime_error("corrupt external sort scalar tag");
    const auto tag = static_cast<unsigned char>(payload[offset++]);
    switch (tag) {
    case 0:
      row.emplace_back(std::monostate{});
      break;
    case 1:
      row.emplace_back(std::bit_cast<std::int64_t>(
          sort_take_le<std::uint64_t>(payload, offset)));
      break;
    case 2:
      row.emplace_back(Decimal{std::bit_cast<std::int64_t>(
          sort_take_le<std::uint64_t>(payload, offset))});
      break;
    case 3:
      row.emplace_back(std::bit_cast<double>(
          sort_take_le<std::uint64_t>(payload, offset)));
      break;
    case 4:
      if (offset == payload.size())
        throw std::runtime_error("corrupt external sort boolean");
      row.emplace_back(payload[offset++] != 0);
      break;
    case 5: {
      const auto length = sort_take_le<std::uint32_t>(payload, offset);
      if (payload.size() - std::min(payload.size(), offset) < length)
        throw std::runtime_error("corrupt external sort string");
      row.emplace_back(std::string(payload.data() + offset, length));
      offset += length;
      break;
    }
    default:
      throw std::runtime_error("corrupt external sort scalar tag");
    }
  }
  if (offset != payload.size())
    throw std::runtime_error("corrupt external sort row length");
  return row;
}

static std::optional<std::pair<std::vector<Scalar>, std::uint64_t>>
read_sort_row(std::istream &input) {
  std::array<char, 4> length_bytes{};
  input.read(length_bytes.data(), length_bytes.size());
  const auto read = input.gcount();
  if (!read && input.eof())
    return std::nullopt;
  if (read != static_cast<std::streamsize>(length_bytes.size()))
    throw std::runtime_error("corrupt external sort record length");
  std::vector<char> length_vector(length_bytes.begin(), length_bytes.end());
  std::size_t offset{};
  const auto length = sort_take_le<std::uint32_t>(length_vector, offset);
  SortReservation payload_memory(length, "external sort encoded merge row");
  std::vector<char> payload(length);
  input.read(payload.data(), static_cast<std::streamsize>(payload.size()));
  if (input.gcount() != static_cast<std::streamsize>(payload.size()))
    throw std::runtime_error("corrupt external sort record payload");
  return std::pair{decode_sort_row(payload),
                   static_cast<std::uint64_t>(length) + 4};
}

static std::filesystem::path write_sort_run(
    const std::filesystem::path &workspace, std::size_t run_number,
    const OutputOrder &order, Rows &rows, std::size_t &reserved,
    SpillMetrics &metrics) {
  std::sort(rows.begin(), rows.end(), [&](const auto &left, const auto &right) {
    return query_row_less(left, right, order);
  });
  std::ostringstream name;
  name << "sort-run-" << std::setw(4) << std::setfill('0') << run_number
       << ".bin";
  const auto path = workspace / name.str();
  std::array<char, sort_io_buffer_bytes> buffer{};
  SortReservation buffer_memory(buffer.size(), "external sort write buffer");
  std::ofstream output;
  output.rdbuf()->pubsetbuf(buffer.data(), buffer.size());
  output.open(path, std::ios::binary | std::ios::trunc);
  if (!output)
    throw std::runtime_error("cannot create external sort run " +
                             path.string());
  for (const auto &row : rows) {
    auto encoded = encode_sort_row(row);
    SortReservation encoded_memory(encoded.size(),
                                   "external sort row encoding");
    output.write(encoded.data(), static_cast<std::streamsize>(encoded.size()));
    if (!output)
      throw std::runtime_error("cannot write external sort run " +
                               path.string());
    metrics.bytes_written += encoded.size();
  }
  output.flush();
  if (!output)
    throw std::runtime_error("cannot flush external sort run " +
                             path.string());
  query_memory->release(reserved);
  reserved = 0;
  rows.clear();
  ++metrics.files_created;
  return path;
}

class SortRunCursor {
  std::array<char, sort_io_buffer_bytes> buffer_{};
  std::ifstream input_;
  std::shared_ptr<QueryMemory> memory_;
  std::size_t current_bytes_{};

public:
  std::optional<std::vector<Scalar>> current;
  std::uint64_t bytes_read{};

  explicit SortRunCursor(const std::filesystem::path &path)
      : memory_(query_memory) {
    if (!memory_)
      throw std::runtime_error(
          "external sort requires query memory accounting");
    memory_->account(buffer_.size(), "external sort read buffer");
    try {
      input_.rdbuf()->pubsetbuf(buffer_.data(), buffer_.size());
      input_.open(path, std::ios::binary);
      if (!input_)
        throw std::runtime_error("cannot open external sort run " +
                                 path.string());
      advance();
    } catch (...) {
      memory_->release(buffer_.size() + current_bytes_);
      throw;
    }
  }
  SortRunCursor(const SortRunCursor &) = delete;
  ~SortRunCursor() { memory_->release(buffer_.size() + current_bytes_); }

  void advance() {
    memory_->release(current_bytes_);
    current_bytes_ = 0;
    current.reset();
    if (auto decoded = read_sort_row(input_)) {
      const auto bytes = row_bytes(decoded->first);
      memory_->account(bytes, "external sort merge row");
      current_bytes_ = bytes;
      current = std::move(decoded->first);
      bytes_read += decoded->second;
    }
  }
};

static void merge_sort_runs(const std::vector<std::filesystem::path> &paths,
                            const std::filesystem::path &output_path,
                            const OutputOrder &order, SpillMetrics &metrics) {
  std::vector<std::unique_ptr<SortRunCursor>> cursors;
  cursors.reserve(paths.size());
  for (const auto &path : paths)
    cursors.push_back(std::make_unique<SortRunCursor>(path));
  const auto heap_compare = [&](std::size_t left, std::size_t right) {
    if (query_row_less(*cursors[right]->current, *cursors[left]->current,
                       order))
      return true;
    if (query_row_less(*cursors[left]->current, *cursors[right]->current,
                       order))
      return false;
    return left > right;
  };
  std::priority_queue<std::size_t, std::vector<std::size_t>,
                      decltype(heap_compare)>
      heap(heap_compare);
  for (std::size_t index = 0; index < cursors.size(); ++index)
    if (cursors[index]->current)
      heap.push(index);
  std::array<char, sort_io_buffer_bytes> buffer{};
  SortReservation buffer_memory(buffer.size(),
                                "external sort merge write buffer");
  std::ofstream output;
  output.rdbuf()->pubsetbuf(buffer.data(), buffer.size());
  output.open(output_path, std::ios::binary | std::ios::trunc);
  if (!output)
    throw std::runtime_error("cannot create external sort merge run " +
                             output_path.string());
  while (!heap.empty()) {
    const auto index = heap.top();
    heap.pop();
    auto encoded = encode_sort_row(*cursors[index]->current);
    SortReservation encoded_memory(encoded.size(),
                                   "external sort merge encoding");
    output.write(encoded.data(), static_cast<std::streamsize>(encoded.size()));
    if (!output)
      throw std::runtime_error("cannot write external sort merge run " +
                               output_path.string());
    metrics.bytes_written += encoded.size();
    cursors[index]->advance();
    if (cursors[index]->current)
      heap.push(index);
  }
  output.flush();
  if (!output)
    throw std::runtime_error("cannot flush external sort merge run " +
                             output_path.string());
  for (const auto &cursor : cursors)
    metrics.bytes_read += cursor->bytes_read;
  ++metrics.files_created;
}

template <class Source, class Sink>
static std::tuple<ResultStreamMetrics, ParquetScanMetrics, SpillMetrics>
stream_external_sort_from_source(const Query &query, std::size_t batch_size,
                                 const std::string &spill_root, Source source,
                                 Sink &sink) {
  if (query.limit == 0)
    return {ResultStreamMetrics{}, ParquetScanMetrics{}, SpillMetrics{}};
  if (!query_memory)
    throw std::runtime_error(
        "external sort requires query memory accounting");
  const auto available = query_memory->limit_bytes() -
                         std::min(query_memory->accounted_bytes(),
                                  query_memory->limit_bytes());
  if (!query_memory->limit_bytes() || available < 1024 * 1024)
    throw std::runtime_error(
        "RESOURCE_EXHAUSTED external sort requires at least 1 MiB of "
        "available query memory");
  const auto run_budget = available / 2;
  SpillDirectory workspace(spill_root);
  std::vector<std::filesystem::path> paths;
  Rows rows;
  std::size_t reserved{};
  std::size_t largest_row{};
  ResultStreamMetrics stream;
  SpillMetrics spill{0, 0, 0, 0, 1};
  const auto batch = std::max<std::size_t>(1, batch_size);
  const auto order = output_order(query);
  const auto scan = source([&](const Table &table) {
    for (std::size_t start = 0; start < table.size(); start += batch) {
      if (execution_cancelled())
        throw std::runtime_error(
            "query cancelled during external sort run generation");
      ++stream.batches_scanned;
      for (auto row_index = start;
           row_index < std::min(table.size(), start + batch); ++row_index) {
        if (query.filter && !truthy(eval(query.filter, table, row_index)))
          continue;
        std::vector<Scalar> row;
        row.reserve(query.select.size());
        for (const auto &item : query.select)
          row.push_back(eval(item.expr, table, row_index));
        const auto bytes = row_bytes(row);
        largest_row = std::max(largest_row, bytes);
        if (!rows.empty() && reserved + bytes > run_budget)
          paths.push_back(write_sort_run(workspace.path(), paths.size(), order,
                                         rows, reserved, spill));
        if (bytes > run_budget)
          throw std::runtime_error(
              "RESOURCE_EXHAUSTED external sort row requires " +
              std::to_string(bytes) + "; run budget is " +
              std::to_string(run_budget) + " bytes");
        query_memory->account(bytes, "external sort run buffer");
        reserved += bytes;
        rows.push_back(std::move(row));
      }
    }
  });
  if (!rows.empty())
    paths.push_back(write_sort_run(workspace.path(), paths.size(), order, rows,
                                   reserved, spill));
  spill.partitions = paths.size();
  if (paths.empty()) {
    spill.passes = 0;
    return {stream, scan, spill};
  }
  const auto cursor_budget = available / 2;
  const auto bytes_per_cursor =
      sort_io_buffer_bytes + std::max<std::size_t>(1, largest_row);
  const auto fan_in =
      std::min<std::size_t>(32, cursor_budget / bytes_per_cursor);
  if (paths.size() > 1 && fan_in < 2)
    throw std::runtime_error(
        "RESOURCE_EXHAUSTED external sort cannot merge two rows within the "
        "query memory limit");
  while (paths.size() > std::max<std::size_t>(1, fan_in)) {
    std::vector<std::filesystem::path> next_paths;
    for (std::size_t start = 0, group = 0; start < paths.size();
         start += fan_in, ++group) {
      const auto end = std::min(paths.size(), start + fan_in);
      std::ostringstream name;
      name << "sort-merge-" << std::setw(2) << std::setfill('0')
           << spill.passes << '-' << std::setw(4) << group << ".bin";
      auto output = workspace.path() / name.str();
      merge_sort_runs(
          std::vector<std::filesystem::path>(paths.begin() + start,
                                             paths.begin() + end),
          output, order, spill);
      next_paths.push_back(std::move(output));
    }
    for (const auto &path : paths) {
      std::error_code error;
      if (!std::filesystem::remove(path, error) || error)
        throw std::runtime_error("cannot remove external sort run " +
                                 path.string() + ": " + error.message());
    }
    paths = std::move(next_paths);
    ++spill.passes;
  }
  std::vector<std::unique_ptr<SortRunCursor>> cursors;
  cursors.reserve(paths.size());
  for (const auto &path : paths)
    cursors.push_back(std::make_unique<SortRunCursor>(path));
  const auto heap_compare = [&](std::size_t left, std::size_t right) {
    if (query_row_less(*cursors[right]->current, *cursors[left]->current,
                       order))
      return true;
    if (query_row_less(*cursors[left]->current, *cursors[right]->current,
                       order))
      return false;
    return left > right;
  };
  std::priority_queue<std::size_t, std::vector<std::size_t>,
                      decltype(heap_compare)>
      heap(heap_compare);
  for (std::size_t index = 0; index < cursors.size(); ++index)
    if (cursors[index]->current)
      heap.push(index);
  auto remaining_offset = query.offset;
  auto remaining_limit = query.limit;
  for (;;) {
    if (execution_cancelled())
      throw std::runtime_error("query cancelled during external sort merge");
    if (heap.empty())
      break;
    const auto next = heap.top();
    heap.pop();
    if (remaining_offset) {
      --remaining_offset;
    } else if (remaining_limit != 0) {
      stream.output_bytes += sink(*cursors[next]->current);
      ++stream.rows_returned;
      if (remaining_limit)
        --*remaining_limit;
    }
    cursors[next]->advance();
    if (cursors[next]->current)
      heap.push(next);
    if (remaining_limit == 0)
      break;
  }
  for (const auto &cursor : cursors)
    spill.bytes_read += cursor->bytes_read;
  ++spill.passes;
  return {stream, scan, spill};
}

template <class Sink>
static std::pair<ResultStreamMetrics, SpillMetrics> stream_external_sort(
    const Query &query, const Table &table, std::size_t batch_size,
    const std::string &spill_root, Sink &sink) {
  auto source = [&](auto consume) {
    if (!filter_always_false(query.filter))
      consume(table);
    return ParquetScanMetrics{};
  };
  auto [stream, scan, spill] = stream_external_sort_from_source(
      query, batch_size, spill_root, source, sink);
  return {stream, spill};
}

template <class Sink>
static std::tuple<ResultStreamMetrics, ParquetScanMetrics, SpillMetrics>
stream_external_sort_parquet(const Query &query, const std::string &path,
                             std::size_t batch_size,
                             std::size_t memory_limit_mb,
                             const std::string &spill_root, Sink &sink) {
  auto source = [&](auto consume) {
    const auto [dictionaries, scan] = stream_parquet_direct(
        path, query, batch_size, [&](std::shared_ptr<Table> table) {
          if (memory_limit_mb &&
              table->approximate_bytes() > memory_limit_mb * 1024 * 1024)
            throw std::runtime_error(
                "RESOURCE_EXHAUSTED decoded Parquet batch requires " +
                std::to_string(table->approximate_bytes()) +
                " bytes; memory limit is " +
                std::to_string(memory_limit_mb) + " MiB");
          consume(*table);
        });
    return scan;
  };
  return stream_external_sort_from_source(query, batch_size, spill_root,
                                          source, sink);
}

} // namespace dremel
