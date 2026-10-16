//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// writer/arrow_stream_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "writer/column_data_collection_serializer.hpp"

namespace duckdb {
namespace ext_nanoarrow {

struct ArrowFieldMetadata {
  idx_t column_index;
  vector<pair<string, string>> metadata;
};

struct ArrowStreamWriter {
  ArrowStreamWriter(const ClientProperties& options, FileSystem& fs,
                    const string& file_path, const vector<LogicalType>& logical_types,
                    const ArrowSchema& schema,
                    const vector<pair<string, string>>& metadata,
                    const vector<ArrowFieldMetadata>& field_metadata,
                    const ArrowIpcCompressionOptions& compression, bool file_format,
                    bool size_metadata);

  //! Removes the output when Finalize did not run, as parquet and csv writers do
  ~ArrowStreamWriter();

  void InitSchema(const ArrowSchema& schema, const vector<pair<string, string>>& metadata,
                  const vector<ArrowFieldMetadata>& field_metadata);

  void InitOutputFile(FileSystem& fs, const string& file_path);

  void WriteSchema();

  unique_ptr<ColumnDataCollectionSerializer> NewSerializer() const;

  void Flush(ColumnDataCollectionSerializer& serializer);

  void Finalize();

  idx_t NumberOfRowGroups() const;

  idx_t FileSize() const;

  static bool IsSizeMetadataKey(const string& key);

  //! Describes the type of every field, nested children included, under the
  //! <type_metadata_namespace>:type keys
  static void SetTypeMetadata(ArrowSchema& schema, const vector<LogicalType>& types,
                              const string& type_metadata_namespace);
  static bool IsTypeMetadataKey(const string& key, const string& type_metadata_namespace);
  //! Fills the statistics RETURN_STATS reports for this file once it is finalized
  void SetWrittenStatistics(CopyFunctionFileStatistics& statistics);

 private:
  void WriteFooter();
  //! Appends to whichever writer the output uses
  void WriteBytes(const_data_ptr_t data, idx_t size);
  ArrowIpcFileBlock WriteMessage(ColumnDataCollectionSerializer& serializer,
                                 const ArrowDictionaryFilter& write_dictionary = nullptr);
  //! Makes the dictionary the current one of its id, false when it already was
  bool UpdateDictionary(const ArrowDictionaryMessage& dictionary);
  idx_t TotalWritten() const;

  ClientProperties options;
  Allocator& allocator;
  ArrowIpcCompressionOptions compression;
  vector<LogicalType> logical_types;
  bool file_format;
  bool size_metadata;
  mutex lock;
  //! Writes on the async pool, so a remote output uploads several parts at once
  unique_ptr<AsyncFileWriter> async_writer;
  //! Local outputs, and SIZE_METADATA which patches the opening schema in place
  unique_ptr<BufferedFileWriter> writer;
  vector<ArrowIpcFileBlock> blocks;
  idx_t schema_message_size = 0;
  //! Set by RETURN_STATS, which also turns on the per batch accounting below
  optional_ptr<CopyFunctionFileStatistics> written_stats;
  //! The record batch totals SIZE_METADATA writes and RETURN_STATS reports
  ArrowBatchStats batch_stats{};
  Value footer_size_bytes;
  // Rotation checks read these while another thread may be flushing
  atomic<idx_t> row_group_count{0};
  atomic<idx_t> file_size{0};
  nanoarrow::UniqueSchema schema;
  bool has_dictionaries = false;
  //! The dictionary last written for each id
  unordered_map<int64_t, shared_ptr<nanoarrow::UniqueBuffer>> current_dictionaries;
};

}  // namespace ext_nanoarrow
}  // namespace duckdb
