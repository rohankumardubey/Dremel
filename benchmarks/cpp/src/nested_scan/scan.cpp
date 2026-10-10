// Official Apache Arrow/Parquet scan control, independent of the SQL reference
// engine.
#include <algorithm>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/byte_size.h>
#include <chrono>
#include <fcntl.h>
#include <iostream>
#include <map>
#include <numeric>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
#ifdef _WIN32
#include <io.h>
#include <sys/stat.h>
#else
#include <unistd.h>
#endif

template <class T> T unwrap(arrow::Result<T> value) {
  if (!value.ok())
    throw std::runtime_error(value.status().ToString());
  return std::move(value).ValueOrDie();
}
void check(const arrow::Status &status) {
  if (!status.ok())
    throw std::runtime_error(status.ToString());
}
std::vector<int> indexes(const std::string &text, int total) {
  if (text == "all") {
    std::vector<int> out(total);
    std::iota(out.begin(), out.end(), 0);
    return out;
  }
  if (text == "none")
    return {};
  std::vector<int> out;
  std::istringstream input(text);
  std::string part;
  std::set<int> seen;
  while (std::getline(input, part, ',')) {
    std::size_t consumed{};
    int value = std::stoi(part, &consumed);
    if (consumed != part.size() || value < 0 || value >= total ||
        !seen.insert(value).second)
      throw std::runtime_error("invalid or duplicate selection index");
    out.push_back(value);
  }
  if (out.empty() || text.back() == ',')
    throw std::runtime_error("empty selection");
  std::sort(out.begin(), out.end());
  return out;
}
void json_vector(const std::vector<int> &values) {
  std::cout << '[';
  for (std::size_t i = 0; i < values.size(); ++i) {
    if (i)
      std::cout << ',';
    std::cout << values[i];
  }
  std::cout << ']';
}

int main(int argc, char **argv) {
  try {
    std::map<std::string, std::string> options;
    for (int i = 1; i < argc; i += 2) {
      const std::string key = argv[i];
      if (i + 1 >= argc ||
          !std::set<std::string>{"--data", "--leaves", "--row-groups",
                                 "--batch-size", "--output"}
               .contains(key) ||
          !options.emplace(key, argv[i + 1]).second)
        throw std::runtime_error("invalid scan arguments");
    }
    const auto started = std::chrono::steady_clock::now();
    auto input = unwrap(arrow::io::ReadableFile::Open(options.at("--data")));
    auto reader =
        unwrap(parquet::arrow::OpenFile(input, arrow::default_memory_pool()));
    reader->set_use_threads(false);
    const auto batch_text =
        options.contains("--batch-size") ? options.at("--batch-size") : "4096";
    std::size_t consumed{};
    const auto batch_size = std::stoll(batch_text, &consumed);
    if (batch_size <= 0 || consumed != batch_text.size())
      throw std::runtime_error("batch size must be positive");
    reader->set_batch_size(batch_size);
    auto metadata = reader->parquet_reader()->metadata();
    auto columns =
        indexes(options.contains("--leaves") ? options.at("--leaves") : "all",
                metadata->num_columns());
    auto groups = indexes(
        options.contains("--row-groups") ? options.at("--row-groups") : "all",
        metadata->num_row_groups());
    auto batches = unwrap(reader->GetRecordBatchReader(groups, columns));
    std::shared_ptr<arrow::io::FileOutputStream> output;
    std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
    if (options.contains("--output")) {
#ifdef _WIN32
      const int fd = ::_open(options.at("--output").c_str(),
                             _O_CREAT | _O_EXCL | _O_WRONLY | _O_BINARY,
                             _S_IREAD | _S_IWRITE);
#else
      const int fd = ::open(options.at("--output").c_str(),
                            O_CREAT | O_EXCL | O_WRONLY, 0666);
#endif
      if (fd < 0)
        throw std::runtime_error("cannot create output (it may already exist)");
      auto stream = arrow::io::FileOutputStream::Open(fd);
      if (!stream.ok()) {
#ifdef _WIN32
        ::_close(fd);
#else
        ::close(fd);
#endif
        throw std::runtime_error(stream.status().ToString());
      }
      output = std::move(stream).ValueOrDie();
      writer = unwrap(arrow::ipc::MakeFileWriter(output, batches->schema()));
    }
    std::int64_t rows{}, count{}, peak{}, selected_rows{}, bytes{};
    for (int group : groups) {
      auto row_group = metadata->RowGroup(group);
      selected_rows += row_group->num_rows();
      for (int column : columns)
        bytes += row_group->ColumnChunk(column)->total_compressed_size();
    }
    for (;;) {
      auto batch = unwrap(batches->Next());
      if (!batch)
        break;
      rows += batch->num_rows();
      ++count;
      peak = std::max(peak, arrow::util::TotalBufferSize(*batch));
      if (writer)
        check(writer->WriteRecordBatch(*batch));
    }
    if (writer) {
      check(writer->Close());
      check(output->Close());
    }
    if (rows != selected_rows)
      throw std::runtime_error("selected row count was not fully decoded");
    const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             std::chrono::steady_clock::now() - started)
                             .count();
    std::cout << "{\"rows\":" << rows << ",\"batches\":" << count
              << ",\"elapsed_ns\":" << elapsed
              << ",\"peak_decoded_batch_bytes\":" << peak
              << ",\"selected_rows\":" << selected_rows
              << ",\"selected_compressed_bytes\":" << bytes
              << ",\"total_leaf_columns\":" << metadata->num_columns()
              << ",\"total_row_groups\":" << metadata->num_row_groups()
              << ",\"selected_leaf_columns\":";
    json_vector(columns);
    std::cout << ",\"row_groups\":";
    json_vector(groups);
    std::cout << ",\"max_definition_levels\":";
    std::vector<int> definitions, repetitions;
    for (int column : columns) {
      definitions.push_back(
          metadata->schema()->Column(column)->max_definition_level());
      repetitions.push_back(
          metadata->schema()->Column(column)->max_repetition_level());
    }
    json_vector(definitions);
    std::cout << ",\"max_repetition_levels\":";
    json_vector(repetitions);
    std::cout << "}\n";
  } catch (const std::exception &error) {
    std::cerr << "error: " << error.what() << '\n';
    return 1;
  }
}
