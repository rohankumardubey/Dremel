#pragma once

#include "catalog.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/statistics.h>

namespace dremel {

struct ParquetScanMetrics {
  std::size_t total_rows{}, rows_read{}, total_row_groups{}, row_groups_read{},
      total_columns{}, columns_read{}, compressed_bytes_read{}, batches_read{},
      peak_decoded_batch_bytes{};
  bool streaming_fallback{};
};

static std::shared_ptr<Table>
materialize_projected_arrow_table(const std::shared_ptr<arrow::Table> &source,
                                  const Table *dictionary_seed = nullptr);

template <class T> static T arrow_value(arrow::Result<T> result) {
  if (!result.ok())
    throw std::runtime_error(result.status().ToString());
  return std::move(result).ValueOrDie();
}

static void require_not_null(const arrow::Array &array, std::int64_t row,
                             const std::string &name) {
  if (array.IsNull(row))
    throw std::runtime_error("required column " + name + " contains null");
}

static std::shared_ptr<arrow::ChunkedArray>
required_column(const arrow::Table &table, const std::string &name) {
  auto column = table.GetColumnByName(name);
  if (!column)
    throw std::runtime_error("missing required column " + name);
  return column;
}

static void append_int64(const arrow::ChunkedArray &column,
                         std::vector<std::int64_t> &output,
                         const std::string &name) {
  if (column.type()->id() != arrow::Type::INT64)
    throw std::runtime_error("column " + name + " must be int64");
  for (const auto &chunk : column.chunks()) {
    const auto array = std::static_pointer_cast<arrow::Int64Array>(chunk);
    for (std::int64_t row = 0; row < array->length(); ++row) {
      require_not_null(*array, row, name);
      output.push_back(array->Value(row));
    }
  }
}

static void append_float64(const arrow::ChunkedArray &column,
                           std::vector<double> &output,
                           const std::string &name) {
  if (column.type()->id() != arrow::Type::DOUBLE)
    throw std::runtime_error("column " + name + " must be float64");
  for (const auto &chunk : column.chunks()) {
    const auto array = std::static_pointer_cast<arrow::DoubleArray>(chunk);
    for (std::int64_t row = 0; row < array->length(); ++row) {
      require_not_null(*array, row, name);
      output.push_back(array->Value(row));
    }
  }
}

static void append_boolean(const arrow::ChunkedArray &column,
                           std::vector<std::uint8_t> &output,
                           const std::string &name) {
  if (column.type()->id() != arrow::Type::BOOL)
    throw std::runtime_error("column " + name + " must be boolean");
  for (const auto &chunk : column.chunks()) {
    const auto array = std::static_pointer_cast<arrow::BooleanArray>(chunk);
    for (std::int64_t row = 0; row < array->length(); ++row) {
      require_not_null(*array, row, name);
      output.push_back(array->Value(row));
    }
  }
}

static void append_dictionary_strings(const arrow::ChunkedArray &column,
                                      Dictionary &dictionary,
                                      std::vector<std::uint32_t> &output,
                                      const std::string &name) {
  for (const auto &chunk : column.chunks()) {
    if (chunk->type_id() == arrow::Type::STRING) {
      const auto array = std::static_pointer_cast<arrow::StringArray>(chunk);
      for (std::int64_t row = 0; row < array->length(); ++row) {
        require_not_null(*array, row, name);
        output.push_back(dictionary.insert(array->GetString(row)));
      }
      continue;
    }
    if (chunk->type_id() != arrow::Type::DICTIONARY)
      throw std::runtime_error("column " + name +
                               " must be string or dictionary");
    const auto array = std::static_pointer_cast<arrow::DictionaryArray>(chunk);
    if (array->indices()->type_id() != arrow::Type::INT32 ||
        array->dictionary()->type_id() != arrow::Type::STRING)
      throw std::runtime_error("unsupported dictionary encoding for column " +
                               name);
    const auto indices =
        std::static_pointer_cast<arrow::Int32Array>(array->indices());
    const auto values =
        std::static_pointer_cast<arrow::StringArray>(array->dictionary());
    for (std::int64_t row = 0; row < array->length(); ++row) {
      require_not_null(*array, row, name);
      output.push_back(
          dictionary.insert(values->GetString(indices->Value(row))));
    }
  }
}

static void append_nullable_int64(const arrow::ChunkedArray &column,
                                  std::vector<std::int64_t> &output,
                                  std::vector<std::uint8_t> &definition,
                                  const std::string &name) {
  if (column.type()->id() != arrow::Type::INT64)
    throw std::runtime_error("column " + name + " must be int64");
  for (const auto &chunk : column.chunks()) {
    const auto array = std::static_pointer_cast<arrow::Int64Array>(chunk);
    for (std::int64_t row = 0; row < array->length(); ++row) {
      if (array->IsNull(row)) {
        output.push_back(0);
        definition.push_back(0);
      } else {
        output.push_back(array->Value(row));
        definition.push_back(1);
      }
    }
  }
}

static std::shared_ptr<Table>
materialize_arrow_table(const std::shared_ptr<arrow::Table> &source) {
  auto table = std::make_shared<Table>();
  append_int64(*required_column(*source, "event_id"), table->event_id,
               "event_id");
  append_int64(*required_column(*source, "user_id"), table->user_id, "user_id");
  append_int64(*required_column(*source, "timestamp"), table->timestamp,
               "timestamp");
  append_dictionary_strings(*required_column(*source, "country"),
                            table->country_dict, table->country, "country");
  append_dictionary_strings(*required_column(*source, "device"),
                            table->device_dict, table->device, "device");
  append_dictionary_strings(*required_column(*source, "event_type"),
                            table->event_dict, table->event_type, "event_type");
  append_int64(*required_column(*source, "duration_ms"), table->duration,
               "duration_ms");
  append_int64(*required_column(*source, "bytes"), table->bytes, "bytes");
  append_float64(*required_column(*source, "score"), table->score, "score");
  append_boolean(*required_column(*source, "success"), table->success,
                 "success");
  append_nullable_int64(*required_column(*source, "campaign_id"),
                        table->campaign, table->campaign_def, "campaign_id");
  const auto rows = static_cast<std::size_t>(source->num_rows());
  for (const auto size :
       {table->event_id.size(), table->user_id.size(), table->timestamp.size(),
        table->country.size(), table->device.size(), table->event_type.size(),
        table->duration.size(), table->bytes.size(), table->score.size(),
        table->success.size(), table->campaign.size(),
        table->campaign_def.size()})
    if (size != rows)
      throw std::runtime_error("Arrow columns have different row counts");
  return table;
}

static std::shared_ptr<Table>
materialize_projected_arrow_table(const std::shared_ptr<arrow::Table> &source,
                                  const Table *dictionary_seed) {
  auto table = std::make_shared<Table>();
  if (dictionary_seed) {
    table->country_dict = dictionary_seed->country_dict;
    table->device_dict = dictionary_seed->device_dict;
    table->event_dict = dictionary_seed->event_dict;
  }
  table->logical_rows = static_cast<std::size_t>(source->num_rows());
  const auto append_if_present = [&](const std::string &name, auto &&append) {
    if (const auto column = source->GetColumnByName(name))
      append(*column);
  };
  append_if_present("event_id", [&](const auto &column) {
    append_int64(column, table->event_id, "event_id");
  });
  append_if_present("user_id", [&](const auto &column) {
    append_int64(column, table->user_id, "user_id");
  });
  append_if_present("timestamp", [&](const auto &column) {
    append_int64(column, table->timestamp, "timestamp");
  });
  append_if_present("country", [&](const auto &column) {
    append_dictionary_strings(column, table->country_dict, table->country,
                              "country");
  });
  append_if_present("device", [&](const auto &column) {
    append_dictionary_strings(column, table->device_dict, table->device,
                              "device");
  });
  append_if_present("event_type", [&](const auto &column) {
    append_dictionary_strings(column, table->event_dict, table->event_type,
                              "event_type");
  });
  append_if_present("duration_ms", [&](const auto &column) {
    append_int64(column, table->duration, "duration_ms");
  });
  append_if_present("bytes", [&](const auto &column) {
    append_int64(column, table->bytes, "bytes");
  });
  append_if_present("score", [&](const auto &column) {
    append_float64(column, table->score, "score");
  });
  append_if_present("success", [&](const auto &column) {
    append_boolean(column, table->success, "success");
  });
  append_if_present("campaign_id", [&](const auto &column) {
    append_nullable_int64(column, table->campaign, table->campaign_def,
                          "campaign_id");
  });
  return table;
}

static std::string date_text(std::int32_t days) {
  const std::chrono::year_month_day date{
      std::chrono::sys_days{std::chrono::days{days}}};
  std::ostringstream output;
  output << std::setfill('0') << std::setw(4) << static_cast<int>(date.year())
         << '-' << std::setw(2) << static_cast<unsigned>(date.month()) << '-'
         << std::setw(2) << static_cast<unsigned>(date.day());
  return output.str();
}

static void append_strings(const arrow::ChunkedArray &column,
                           std::vector<std::string> &output,
                           const std::string &name) {
  Dictionary dictionary;
  std::vector<std::uint32_t> encoded;
  append_dictionary_strings(column, dictionary, encoded, name);
  output.reserve(output.size() + encoded.size());
  for (auto id : encoded)
    output.push_back(dictionary.values[id]);
}

static void append_dates(const arrow::ChunkedArray &column,
                         std::vector<std::string> &output,
                         const std::string &name) {
  if (column.type()->id() != arrow::Type::DATE32)
    throw std::runtime_error("column " + name + " must be date32");
  for (const auto &chunk : column.chunks()) {
    const auto array = std::static_pointer_cast<arrow::Date32Array>(chunk);
    for (std::int64_t row = 0; row < array->length(); ++row) {
      require_not_null(*array, row, name);
      output.push_back(date_text(array->Value(row)));
    }
  }
}

static void append_decimal(const arrow::ChunkedArray &column,
                           std::vector<std::int64_t> &output,
                           const std::string &name) {
  if (column.type()->id() != arrow::Type::DECIMAL128)
    throw std::runtime_error("column " + name + " must be decimal128");
  for (const auto &chunk : column.chunks()) {
    const auto array = std::static_pointer_cast<arrow::Decimal128Array>(chunk);
    for (std::int64_t row = 0; row < array->length(); ++row) {
      require_not_null(*array, row, name);
      output.push_back(
          static_cast<std::int64_t>(arrow::Decimal128(array->GetValue(row))));
    }
  }
}

static std::shared_ptr<arrow::Table>
read_interoperable_table(const std::string &path) {
  auto input = arrow_value(arrow::io::ReadableFile::Open(path));
  if (path.ends_with(".arrow")) {
    auto reader = arrow_value(arrow::ipc::RecordBatchFileReader::Open(input));
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.reserve(reader->num_record_batches());
    for (int index = 0; index < reader->num_record_batches(); ++index)
      batches.push_back(arrow_value(reader->ReadRecordBatch(index)));
    return arrow_value(
        arrow::Table::FromRecordBatches(reader->schema(), batches));
  }
  auto reader = arrow_value(
      parquet::arrow::OpenFile(input, arrow::default_memory_pool()));
  return arrow_value(reader->ReadTable());
}

static std::shared_ptr<Table> load_arrow_ipc(const std::string &path) {
  return materialize_arrow_table(read_interoperable_table(path));
}

static std::shared_ptr<Table> load_parquet(const std::string &path) {
  return materialize_arrow_table(read_interoperable_table(path));
}

static std::shared_ptr<Table> parquet_metadata_table(const std::string &path) {
  auto input = arrow_value(arrow::io::ReadableFile::Open(path));
  auto reader = arrow_value(
      parquet::arrow::OpenFile(input, arrow::default_memory_pool()));
  auto table = std::make_shared<Table>();
  table->logical_rows = static_cast<std::size_t>(
      reader->parquet_reader()->metadata()->num_rows());
  return table;
}

static const std::array<std::string, 11> event_columns{
    "event_id", "user_id", "timestamp", "country", "device", "event_type",
    "duration_ms", "bytes", "score", "success", "campaign_id"};

static std::set<std::string> direct_columns(const Query &query) {
  std::set<std::string> columns;
  const bool has_subquery =
      std::any_of(query.select.begin(), query.select.end(), [](const auto &item) {
        return contains_subquery(item.expr);
      }) ||
      contains_subquery(query.filter) || contains_subquery(query.having) ||
      std::any_of(query.joins.begin(), query.joins.end(), [](const auto &join) {
        return contains_subquery(join.on);
      });
  if (query.union_query || !query.ctes.empty() || has_subquery ||
      std::any_of(query.select.begin(), query.select.end(), [](const auto &item) {
        return item.expr && item.expr->kind == ExprKind::star;
      })) {
    columns.insert(event_columns.begin(), event_columns.end());
    return columns;
  }
  for (auto &qualified : query.columns) {
    const auto name = base_name(qualified);
    if (std::find(event_columns.begin(), event_columns.end(), name) !=
        event_columns.end())
      columns.insert(name);
  }
  return columns;
}

static std::optional<Scalar> parquet_literal(const ExprPtr &expression) {
  if (!expression)
    return {};
  if (expression->kind == ExprKind::integer)
    return Scalar{expression->integer};
  if (expression->kind == ExprKind::floating)
    return Scalar{expression->floating};
  if (expression->kind == ExprKind::boolean)
    return Scalar{expression->boolean};
  if (expression->kind == ExprKind::string)
    return Scalar{expression->text};
  return {};
}

static std::optional<std::pair<Scalar, Scalar>>
statistics_bounds(const std::shared_ptr<parquet::Statistics> &statistics) {
  if (!statistics || !statistics->HasMinMax())
    return {};
  switch (statistics->physical_type()) {
  case parquet::Type::BOOLEAN: {
    const auto values = std::static_pointer_cast<
        parquet::TypedStatistics<parquet::BooleanType>>(statistics);
    return std::pair{Scalar{values->min()}, Scalar{values->max()}};
  }
  case parquet::Type::INT64: {
    const auto values = std::static_pointer_cast<
        parquet::TypedStatistics<parquet::Int64Type>>(statistics);
    return std::pair{Scalar{values->min()}, Scalar{values->max()}};
  }
  case parquet::Type::DOUBLE: {
    const auto values = std::static_pointer_cast<
        parquet::TypedStatistics<parquet::DoubleType>>(statistics);
    return std::pair{Scalar{values->min()}, Scalar{values->max()}};
  }
  case parquet::Type::BYTE_ARRAY: {
    if (statistics->is_min_value_exact() != true ||
        statistics->is_max_value_exact() != true)
      return {};
    const auto values = std::static_pointer_cast<
        parquet::TypedStatistics<parquet::ByteArrayType>>(statistics);
    return std::pair{
        Scalar{std::string(static_cast<std::string_view>(values->min()))},
        Scalar{std::string(static_cast<std::string_view>(values->max()))}};
  }
  default:
    return {};
  }
}

static bool comparison_may_match(const std::string &operation,
                                 const Scalar &value, const Scalar &minimum,
                                 const Scalar &maximum) {
  const auto compare_values = [](const Scalar &left, const Scalar &right) {
    if ((std::holds_alternative<std::int64_t>(left) ||
         std::holds_alternative<double>(left)) &&
        (std::holds_alternative<std::int64_t>(right) ||
         std::holds_alternative<double>(right))) {
      const auto number = [](const Scalar &value) {
        if (const auto *integer = std::get_if<std::int64_t>(&value))
          return static_cast<long double>(*integer);
        return static_cast<long double>(std::get<double>(value));
      };
      const auto a = number(left), b = number(right);
      return a == b ? 0 : (a < b ? -1 : 1);
    }
    if (const auto *a = std::get_if<bool>(&left)) {
      const auto *b = std::get_if<bool>(&right);
      return !b || *a == *b ? 0 : (!*a ? -1 : 1);
    }
    if (const auto *a = std::get_if<std::string>(&left)) {
      const auto *b = std::get_if<std::string>(&right);
      return !b || *a == *b ? 0 : (*a < *b ? -1 : 1);
    }
    return 0;
  };
  const auto min_vs_value = compare_values(minimum, value);
  const auto max_vs_value = compare_values(maximum, value);
  if (operation == "=")
    return min_vs_value <= 0 && max_vs_value >= 0;
  if (operation == "!=")
    return !(min_vs_value == 0 && max_vs_value == 0);
  if (operation == "<")
    return min_vs_value < 0;
  if (operation == "<=")
    return min_vs_value <= 0;
  if (operation == ">")
    return max_vs_value > 0;
  if (operation == ">=")
    return max_vs_value >= 0;
  return true;
}

static bool row_group_may_match(
    const ExprPtr &expression, const parquet::RowGroupMetaData &row_group,
    const std::unordered_map<std::string, int> &indexes) {
  if (!expression)
    return true;
  if (expression->kind == ExprKind::boolean)
    return expression->boolean;
  if (expression->kind == ExprKind::null)
    return false;
  if (expression->kind == ExprKind::binary && expression->text == "and")
    return row_group_may_match(expression->left, row_group, indexes) &&
           row_group_may_match(expression->right, row_group, indexes);
  if (expression->kind == ExprKind::binary && expression->text == "or")
    return row_group_may_match(expression->left, row_group, indexes) ||
           row_group_may_match(expression->right, row_group, indexes);
  if (expression->kind == ExprKind::binary) {
    std::string column, operation = expression->text;
    std::optional<Scalar> value;
    if (expression->left && expression->left->kind == ExprKind::column &&
        (value = parquet_literal(expression->right)))
      column = expression->left->text;
    else if (expression->right &&
             expression->right->kind == ExprKind::column &&
             (value = parquet_literal(expression->left))) {
      column = expression->right->text;
      if (operation == "<")
        operation = ">";
      else if (operation == "<=")
        operation = ">=";
      else if (operation == ">")
        operation = "<";
      else if (operation == ">=")
        operation = "<=";
    } else
      return true;
    const auto found = indexes.find(base_name(column));
    if (found == indexes.end())
      return true;
    const auto bounds =
        statistics_bounds(row_group.ColumnChunk(found->second)->statistics());
    return !bounds || comparison_may_match(operation, *value, bounds->first,
                                            bounds->second);
  }
  if (expression->kind == ExprKind::between && !expression->boolean &&
      expression->left && expression->left->kind == ExprKind::column &&
      expression->args.size() == 2) {
    const auto low = parquet_literal(expression->args[0]);
    const auto high = parquet_literal(expression->args[1]);
    const auto found = indexes.find(base_name(expression->left->text));
    if (!low || !high || found == indexes.end())
      return true;
    const auto bounds =
        statistics_bounds(row_group.ColumnChunk(found->second)->statistics());
    return !bounds ||
           (comparison_may_match(">=", *low, bounds->first, bounds->second) &&
            comparison_may_match("<=", *high, bounds->first, bounds->second));
  }
  if (expression->kind == ExprKind::in_list && !expression->boolean &&
      expression->left && expression->left->kind == ExprKind::column) {
    const auto found = indexes.find(base_name(expression->left->text));
    if (found == indexes.end())
      return true;
    const auto bounds =
        statistics_bounds(row_group.ColumnChunk(found->second)->statistics());
    if (!bounds)
      return true;
    return std::any_of(expression->args.begin(), expression->args.end(),
                       [&](const auto &candidate) {
                         const auto value = parquet_literal(candidate);
                         return !value || comparison_may_match(
                                              "=", *value, bounds->first,
                                              bounds->second);
                       });
  }
  if (expression->kind == ExprKind::is_null && expression->left &&
      expression->left->kind == ExprKind::column) {
    const auto found = indexes.find(base_name(expression->left->text));
    if (found == indexes.end())
      return true;
    const auto statistics =
        row_group.ColumnChunk(found->second)->statistics();
    if (!statistics || !statistics->HasNullCount())
      return true;
    return expression->boolean
               ? statistics->null_count() < row_group.num_rows()
               : statistics->null_count() > 0;
  }
  return true;
}

struct ParquetSelection {
  std::vector<int> columns, row_groups;
  ParquetScanMetrics metrics;
};

static ParquetSelection parquet_scan_selection(const std::string &path,
                                               const Query &query) {
  auto input = arrow_value(arrow::io::ReadableFile::Open(path));
  auto reader = arrow_value(
      parquet::arrow::OpenFile(input, arrow::default_memory_pool()));
  const auto metadata = reader->parquet_reader()->metadata();
  const auto schema = metadata->schema();
  std::unordered_map<std::string, int> indexes;
  for (int column = 0; column < metadata->num_columns(); ++column)
    indexes[schema->Column(column)->name()] = column;
  ParquetSelection selection;
  for (auto &name : direct_columns(query))
    if (const auto found = indexes.find(name); found != indexes.end())
      selection.columns.push_back(found->second);
  for (int index = 0; index < metadata->num_row_groups(); ++index) {
    const auto row_group = metadata->RowGroup(index);
    const bool selected = row_group_may_match(query.filter, *row_group, indexes);
    if (!selected)
      continue;
    selection.row_groups.push_back(index);
    selection.metrics.rows_read +=
        static_cast<std::size_t>(row_group->num_rows());
    for (auto column : selection.columns)
      selection.metrics.compressed_bytes_read += static_cast<std::size_t>(
          std::max<std::int64_t>(0, row_group->ColumnChunk(column)
                                        ->total_compressed_size()));
  }
  selection.metrics.total_rows =
      static_cast<std::size_t>(metadata->num_rows());
  selection.metrics.total_row_groups =
      static_cast<std::size_t>(metadata->num_row_groups());
  selection.metrics.row_groups_read = selection.row_groups.size();
  selection.metrics.total_columns =
      static_cast<std::size_t>(metadata->num_columns());
  selection.metrics.columns_read = selection.columns.size();
  return selection;
}

static ParquetScanMetrics parquet_scan_plan(const std::string &path,
                                            const Query &query) {
  return parquet_scan_selection(path, query).metrics;
}

template <class T>
static void append_projected_values(std::vector<T> &target,
                                    std::vector<T> &source) {
  target.insert(target.end(), std::make_move_iterator(source.begin()),
                std::make_move_iterator(source.end()));
}

static void append_projected_table(Table &target, Table source) {
  target.logical_rows += source.logical_rows;
  target.country_dict = std::move(source.country_dict);
  target.device_dict = std::move(source.device_dict);
  target.event_dict = std::move(source.event_dict);
  append_projected_values(target.event_id, source.event_id);
  append_projected_values(target.user_id, source.user_id);
  append_projected_values(target.timestamp, source.timestamp);
  append_projected_values(target.duration, source.duration);
  append_projected_values(target.bytes, source.bytes);
  append_projected_values(target.campaign, source.campaign);
  append_projected_values(target.score, source.score);
  append_projected_values(target.country, source.country);
  append_projected_values(target.device, source.device);
  append_projected_values(target.event_type, source.event_type);
  append_projected_values(target.success, source.success);
  append_projected_values(target.campaign_def, source.campaign_def);
}

static std::pair<std::shared_ptr<Table>, ParquetScanMetrics>
load_parquet_direct(const std::string &path, const Query &query,
                    std::size_t batch_size) {
  auto selection = parquet_scan_selection(path, query);
  auto input = arrow_value(arrow::io::ReadableFile::Open(path));
  auto reader = arrow_value(
      parquet::arrow::OpenFile(input, arrow::default_memory_pool()));
  reader->set_batch_size(static_cast<std::int64_t>(
      std::max<std::size_t>(1, batch_size)));
  auto batches = arrow_value(reader->GetRecordBatchReader(
      selection.row_groups, selection.columns));
  auto table = std::make_shared<Table>();
  for (;;) {
    auto batch = arrow_value(batches->Next());
    if (!batch)
      break;
    const auto source =
        arrow_value(arrow::Table::FromRecordBatches({std::move(batch)}));
    auto projected = materialize_projected_arrow_table(source, table.get());
    append_projected_table(*table, std::move(*projected));
    ++selection.metrics.batches_read;
    selection.metrics.peak_decoded_batch_bytes = table->approximate_bytes();
  }
  return {std::move(table), selection.metrics};
}

template <class Consume>
static std::pair<std::shared_ptr<Table>, ParquetScanMetrics>
stream_parquet_direct(const std::string &path, const Query &query,
                      std::size_t batch_size, Consume consume) {
  auto selection = parquet_scan_selection(path, query);
  auto input = arrow_value(arrow::io::ReadableFile::Open(path));
  auto reader = arrow_value(
      parquet::arrow::OpenFile(input, arrow::default_memory_pool()));
  reader->set_batch_size(static_cast<std::int64_t>(
      std::max<std::size_t>(1, batch_size)));
  auto batches = arrow_value(reader->GetRecordBatchReader(
      selection.row_groups, selection.columns));
  auto dictionaries = std::make_shared<Table>();
  for (;;) {
    auto batch = arrow_value(batches->Next());
    if (!batch)
      break;
    const auto source =
        arrow_value(arrow::Table::FromRecordBatches({std::move(batch)}));
    auto table = materialize_projected_arrow_table(source, dictionaries.get());
    dictionaries->country_dict = table->country_dict;
    dictionaries->device_dict = table->device_dict;
    dictionaries->event_dict = table->event_dict;
    ++selection.metrics.batches_read;
    selection.metrics.peak_decoded_batch_bytes =
        std::max(selection.metrics.peak_decoded_batch_bytes,
                 table->approximate_bytes());
    consume(std::move(table));
  }
  dictionaries->logical_rows = selection.metrics.rows_read;
  return {std::move(dictionaries), selection.metrics};
}

static UsersTable load_users_interoperable(const std::string &path) {
  const auto source = read_interoperable_table(path);
  UsersTable users;
  append_int64(*required_column(*source, "user_id"), users.user_id, "user_id");
  append_strings(*required_column(*source, "segment"), users.segment,
                 "segment");
  append_dates(*required_column(*source, "signup_date"), users.signup_date,
               "signup_date");
  append_decimal(*required_column(*source, "lifetime_value"),
                 users.lifetime_value, "lifetime_value");
  append_strings(*required_column(*source, "region"), users.region, "region");
  std::vector<std::uint8_t> active;
  append_boolean(*required_column(*source, "active"), active, "active");
  users.active.reserve(active.size());
  for (auto value : active)
    users.active.push_back(value != 0);
  return users;
}

static CampaignsTable load_campaigns_interoperable(const std::string &path) {
  const auto source = read_interoperable_table(path);
  CampaignsTable campaigns;
  append_int64(*required_column(*source, "campaign_id"), campaigns.campaign_id,
               "campaign_id");
  append_strings(*required_column(*source, "campaign_name"),
                 campaigns.campaign_name, "campaign_name");
  append_decimal(*required_column(*source, "budget"), campaigns.budget,
                 "budget");
  append_dates(*required_column(*source, "start_date"), campaigns.start_date,
               "start_date");
  append_dates(*required_column(*source, "end_date"), campaigns.end_date,
               "end_date");
  append_strings(*required_column(*source, "channel"), campaigns.channel,
                 "channel");
  for (std::size_t row = 0; row < campaigns.campaign_id.size(); ++row)
    campaigns.index[campaigns.campaign_id[row]].push_back(row);
  return campaigns;
}

} // namespace dremel
