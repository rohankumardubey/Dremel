#pragma once

#include "catalog.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>

namespace dremel {

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
