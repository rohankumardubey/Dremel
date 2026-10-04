#pragma once

namespace dremel {

inline std::atomic_uint64_t spill_directory_sequence{};

class SpillDirectory {
  std::filesystem::path path_;

public:
  explicit SpillDirectory(const std::string &root) {
    std::error_code error;
    std::filesystem::create_directories(root, error);
    if (error)
      throw std::runtime_error("cannot create spill directory " + root +
                               ": " + error.message());
    for (std::size_t attempt = 0; attempt < 1000; ++attempt) {
      path_ = std::filesystem::path(root) /
              (".dremel-spill-" +
               std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                  Clock::now().time_since_epoch())
                                  .count()) +
               "-" +
               std::to_string(spill_directory_sequence.fetch_add(
                   1, std::memory_order_relaxed)));
      if (std::filesystem::create_directory(path_, error))
        return;
      if (error && error != std::errc::file_exists)
        throw std::runtime_error("cannot create spill workspace " +
                                 path_.string() + ": " + error.message());
      error.clear();
    }
    throw std::runtime_error("cannot allocate a unique spill workspace");
  }
  SpillDirectory(const SpillDirectory &) = delete;
  ~SpillDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  const std::filesystem::path &path() const { return path_; }
};

class QueryReservation {
  std::shared_ptr<QueryMemory> memory_;
  std::size_t bytes_{};

public:
  explicit QueryReservation(std::shared_ptr<QueryMemory> memory)
      : memory_(std::move(memory)) {}
  QueryReservation(const QueryReservation &) = delete;
  ~QueryReservation() { memory_->release(bytes_); }
  bool try_add(std::size_t bytes, std::size_t budget) {
    if (bytes > budget - std::min(bytes_, budget) ||
        !memory_->try_account(bytes))
      return false;
    bytes_ += bytes;
    return true;
  }
};

static bool spillable_aggregate(const Query &query) {
  return query.from.name == "events" && query.joins.empty() && !query.having &&
         !query.union_query && query.ctes.empty() &&
         std::none_of(query.select.begin(), query.select.end(),
                      [](const auto &item) {
                        return contains_window(item.expr) ||
                               contains_subquery(item.expr);
                      }) &&
         !contains_subquery(query.filter) && !query.group_by.empty() &&
         query.group_by.size() <= 3 && query.limit &&
         !query.order_by.empty() && !query.distinct;
}

static bool should_spill_aggregate(const Query &query, const Table &table,
                                   const std::string &spill_dir) {
  if (spill_dir.empty() || !query_memory || !query_memory->limit_bytes() ||
      !spillable_aggregate(query))
    return false;
  const auto available = query_memory->limit_bytes() -
                         std::min(query_memory->accounted_bytes(),
                                  query_memory->limit_bytes());
  return table.group_upper_bound(query.group_by) > available / 2 / 512;
}

static std::vector<Scalar> spill_group_output_row(
    const Query &query, const Table &table, const Key &key,
    const std::vector<Agg> &aggregate_states) {
  std::vector<Scalar> row;
  row.reserve(query.select.size());
  std::size_t aggregate_index = 0;
  for (const auto &item : query.select) {
    if (is_agg(item.expr)) {
      row.push_back(finish(aggregate_states[aggregate_index++]));
    } else if (item.expr->kind == ExprKind::column) {
      const auto found = std::find(query.group_by.begin(), query.group_by.end(),
                                   item.expr->text);
      row.push_back(table.key_scalar(
          item.expr->text,
          key.v[static_cast<std::size_t>(
              std::distance(query.group_by.begin(), found))]));
    } else {
      row.emplace_back(std::monostate{});
    }
  }
  return row;
}

static void retain_spill_top_k(const OutputOrder &order, std::size_t top_k,
                               Rows &rows, std::vector<Scalar> row) {
  if (!top_k)
    return;
  if (rows.size() < top_k) {
    account_query_memory(row_bytes(row), "spill result Top-K");
    rows.push_back(std::move(row));
    return;
  }
  const auto worst = static_cast<std::size_t>(std::distance(
      rows.begin(), std::max_element(rows.begin(), rows.end(),
                                     [&](const auto &left, const auto &right) {
                                       return query_row_less(left, right, order);
                                     })));
  if (!query_row_less(row, rows[worst], order))
    return;
  const auto old_bytes = row_bytes(rows[worst]);
  const auto new_bytes = row_bytes(row);
  if (new_bytes > old_bytes)
    account_query_memory(new_bytes - old_bytes, "spill result Top-K");
  else if (query_memory)
    query_memory->release(old_bytes - new_bytes);
  rows[worst] = std::move(row);
}

struct SpillKeyHash {
  std::size_t operator()(const Key &key) const {
    return static_cast<std::size_t>(key_hash(key));
  }
};

static std::pair<Rows, SpillMetrics>
execute_spilled_aggregate(const Query &query, const Table &table,
                          std::size_t batch, const std::string &spill_root) {
  if (!query_memory)
    throw std::runtime_error("spill requires query memory accounting");
  const auto available = query_memory->limit_bytes() -
                         std::min(query_memory->accounted_bytes(),
                                  query_memory->limit_bytes());
  if (available < 2 * 1024 * 1024)
    throw std::runtime_error(
        "RESOURCE_EXHAUSTED spill aggregation requires at least 2 MiB of "
        "available query memory");
  const auto partition_budget = available / 2;
  const auto estimated_groups =
      std::max<std::size_t>(1, table.group_upper_bound(query.group_by));
  const auto groups_per_partition =
      std::max<std::size_t>(1, partition_budget / 512);
  const auto required =
      std::max<std::size_t>(2, (estimated_groups + groups_per_partition - 1) /
                                  groups_per_partition);
  const auto partitions = std::min<std::size_t>(1024, std::bit_ceil(required));
  SpillDirectory workspace(spill_root);
  std::vector<std::filesystem::path> paths;
  paths.reserve(partitions);
  for (std::size_t partition = 0; partition < partitions; ++partition) {
    std::ostringstream name;
    name << "partition-" << std::setw(4) << std::setfill('0') << partition
         << ".bin";
    paths.push_back(workspace.path() / name.str());
  }
  std::vector<bool> created(partitions);
  std::vector<std::vector<std::uint64_t>> buckets(partitions);
  std::vector<std::unique_ptr<std::ofstream>> writers(partitions);
  const auto scratch_bytes =
      std::max<std::size_t>(1, batch) * sizeof(std::uint64_t) +
      partitions * (sizeof(std::vector<std::uint64_t>) + 4096);
  account_query_memory(scratch_bytes, "spill partition buffers");
  SpillMetrics metrics{0, partitions, 0, 0, 2};
  for (std::size_t start = 0; start < table.size();
       start += std::max<std::size_t>(1, batch)) {
    if (execution_cancelled())
      throw std::runtime_error("query cancelled during spill partitioning");
    for (auto &bucket : buckets)
      bucket.clear();
    for (auto row = start;
         row < std::min(table.size(), start + std::max<std::size_t>(1, batch));
         ++row) {
      if (query.filter && !truthy(eval(query.filter, table, row)))
        continue;
      Key key;
      key.n = static_cast<std::uint8_t>(query.group_by.size());
      for (std::size_t index = 0; index < query.group_by.size(); ++index)
        key.v[index] = table.raw_key(query.group_by[index], row);
      buckets[key_hash(key) & (partitions - 1)].push_back(row);
    }
    for (std::size_t partition = 0; partition < partitions; ++partition) {
      if (buckets[partition].empty())
        continue;
      if (!writers[partition]) {
        writers[partition] = std::make_unique<std::ofstream>(
            paths[partition], std::ios::binary | std::ios::trunc);
        if (!*writers[partition])
          throw std::runtime_error("cannot create spill partition " +
                                   paths[partition].string());
        created[partition] = true;
        ++metrics.files_created;
      }
      for (auto row : buckets[partition]) {
        auto encoded = row;
        if constexpr (std::endian::native == std::endian::big)
          encoded = std::byteswap(encoded);
        writers[partition]->write(reinterpret_cast<const char *>(&encoded),
                                  sizeof(encoded));
      }
      if (!*writers[partition])
        throw std::runtime_error("cannot write spill partition " +
                                 paths[partition].string());
      metrics.bytes_written += buckets[partition].size() * 8;
    }
  }
  for (auto &writer : writers)
    if (writer) {
      writer->flush();
      if (!*writer)
        throw std::runtime_error("cannot flush spill partition");
      writer.reset();
    }
  query_memory->release(scratch_bytes);
  buckets.clear();

  const auto initial = states(query);
  const auto group_reservation_bytes =
      sizeof(Key) + initial.size() * sizeof(Agg) + 128;
  const auto order = output_order(query);
  const auto top_k = *query.limit + query.offset;
  Rows rows;
  rows.reserve(std::min<std::size_t>(top_k, 1024));
  std::array<char, 64 * 1024> read_buffer{};
  account_query_memory(read_buffer.size(), "spill read buffer");
  for (std::size_t partition = 0; partition < partitions; ++partition) {
    if (!created[partition])
      continue;
    if (execution_cancelled())
      throw std::runtime_error("query cancelled during spill aggregation");
    std::ifstream input(paths[partition], std::ios::binary);
    if (!input)
      throw std::runtime_error("cannot read spill partition " +
                               paths[partition].string());
    QueryReservation reservation(query_memory);
    std::unordered_map<Key, std::vector<Agg>, SpillKeyHash> groups;
    while (input) {
      input.read(read_buffer.data(), read_buffer.size());
      const auto bytes = static_cast<std::size_t>(input.gcount());
      if (!bytes)
        break;
      if (bytes % 8)
        throw std::runtime_error("corrupt spill partition length");
      metrics.bytes_read += bytes;
      for (std::size_t offset = 0; offset < bytes; offset += 8) {
        std::uint64_t encoded{};
        std::memcpy(&encoded, read_buffer.data() + offset, sizeof(encoded));
        if constexpr (std::endian::native == std::endian::big)
          encoded = std::byteswap(encoded);
        const auto row = static_cast<std::size_t>(encoded);
        Key key;
        key.n = static_cast<std::uint8_t>(query.group_by.size());
        for (std::size_t index = 0; index < query.group_by.size(); ++index)
          key.v[index] = table.raw_key(query.group_by[index], row);
        if (!groups.contains(key)) {
          if (!reservation.try_add(group_reservation_bytes,
                                   partition_budget))
            throw std::runtime_error(
                "RESOURCE_EXHAUSTED spill partition " +
                std::to_string(partition) + " exceeded its " +
                std::to_string(partition_budget) + " byte memory budget");
          groups.emplace(key, initial);
        }
        update(groups.at(key), query, table, row);
      }
    }
    for (auto &[key, aggregate_states] : groups)
      retain_spill_top_k(
          order, top_k, rows,
          spill_group_output_row(query, table, key, aggregate_states));
  }
  query_memory->release(read_buffer.size());
  finalize_rows(query, rows);
  return {std::move(rows), metrics};
}

} // namespace dremel
