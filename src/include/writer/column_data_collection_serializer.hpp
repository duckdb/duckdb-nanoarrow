//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// writer/column_data_collection_serializer.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "arrow_batch_stats.h"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/async_file_writer.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/main/client_properties.hpp"
#include "ipc/codecs.hpp"
#include "nanoarrow/nanoarrow_ipc.hpp"
#include "nanoarrow_errors.hpp"
#include "writer/arrow_dictionary_builder.hpp"

#include <functional>

namespace duckdb {
namespace ext_nanoarrow {

//! An encapsulated DictionaryBatch message, its body included
struct ArrowDictionaryMessage {
  int64_t id;
  shared_ptr<nanoarrow::UniqueBuffer> message;
};

//! Decides whether a dictionary message is written, all of them are without one
using ArrowDictionaryFilter = std::function<bool(const ArrowDictionaryMessage&)>;

class ColumnDataCollectionSerializer {
 public:
  ColumnDataCollectionSerializer(ClientProperties options, Allocator& allocator,
                                 ArrowIpcCompressionOptions compression = {},
                                 bool track_body_size = false);

  void Init(const ArrowSchema* schema, const vector<LogicalType>& logical_types);

  void SerializeSchema(const ArrowSchema* schema, idx_t reserved_size = 0);

  void SerializeFooter(nanoarrow::UniqueSchema schema,
                       const vector<ArrowIpcFileBlock>& blocks);

  //! A builder for one record batch of the schema given to Init
  unique_ptr<ArrowBatchBuilder> NewBatchBuilder(idx_t capacity) const;

  //! Serializes the record batch, preceded by a DictionaryBatch for every dictionary
  //! encoded field
  idx_t Serialize(ArrowBatchBuilder& builder);

  idx_t Serialize(const ColumnDataCollection& buffer);

  //! Writes the dictionary messages write_dictionary accepts, then the record batch,
  //! whose block is returned
  ArrowIpcFileBlock Flush(BufferedFileWriter& writer,
                          const ArrowDictionaryFilter& write_dictionary = nullptr);
  //! Hands the messages to the async writer, which frees them once written
  ArrowIpcFileBlock Flush(AsyncFileWriter& writer,
                          const ArrowDictionaryFilter& write_dictionary = nullptr);

  //! The dictionary messages of the last serialized record batch, in id order
  const vector<ArrowDictionaryMessage>& DictionaryMessages() const {
    return dictionary_messages;
  }

  //! The dictionary messages and the record batch, concatenated
  nanoarrow::UniqueBuffer TakeMessages();

  //! What the last serialized batch contributes to the file's record batch statistics,
  //! measured from the batch as written. Only set when tracking body sizes.
  const ArrowBatchMeasure& BatchMeasure() const { return batch_measure; }

  nanoarrow::UniqueBuffer GetHeader();

  nanoarrow::UniqueBuffer GetBody();

 private:
  ClientProperties options;
  Allocator& allocator;
  ArrowIpcCompressionOptions compression;
  bool track_body_size;
  ArrowBatchMeasure batch_measure{};
  ArrowExtensionTypes extension_types;
  const ArrowSchema* schema = nullptr;
  vector<LogicalType> logical_types;
  nanoarrow::ipc::UniqueEncoder encoder;
  nanoarrow::UniqueArrayView chunk_view;
  nanoarrow::UniqueBuffer header;
  nanoarrow::UniqueBuffer body;
  vector<ArrowDictionaryMessage> dictionary_messages;
  ArrowError error{};

  void SerializeDictionaries(ArrowArrayView& view, int64_t& next_id);
  void SerializeDictionary(int64_t id, ArrowArrayView& values);
};

//! The record batch schema. dictionary_columns are written dictionary encoded, which
//! like ENUM columns requires stream_format. View layouts are not supported
nanoarrow::UniqueSchema CreateArrowIpcSchema(const vector<LogicalType>& types,
                                             const vector<Identifier>& names,
                                             ClientProperties& options,
                                             const vector<idx_t>& dictionary_columns = {},
                                             bool stream_format = false);

//! The columns a DICTIONARY_ENCODE option names, as column names or lists of them
vector<idx_t> ReadDictionaryColumns(const vector<Value>& values,
                                    const vector<Identifier>& names);

//! Whether any field of the schema, nested ones included, is dictionary encoded
bool HasDictionaries(const ArrowSchema& schema);

}  // namespace ext_nanoarrow
}  // namespace duckdb
