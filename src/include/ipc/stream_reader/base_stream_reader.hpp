//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// ipc/stream_reader/base_stream_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "nanoarrow/nanoarrow.hpp"
#include "nanoarrow/nanoarrow_ipc.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "ipc/codecs.hpp"
#include "nanoarrow_errors.hpp"

#include "table_function/scan_arrow_ipc.hpp"

namespace duckdb {
namespace ext_nanoarrow {

struct ArrowIpcMessagePrefix {
  uint32_t continuation_token;
  int32_t metadata_size;
};

//! Base IPC Reader
class IPCStreamReader {
 public:
  virtual ~IPCStreamReader() = default;
  explicit IPCStreamReader(Allocator& allocator)
      : decoder(NewDuckDBArrowDecoder()), allocator(allocator) {};
  //! Gets the output schema, which is the file schema with projection pushdown being
  //! considered
  const ArrowSchema* GetOutputSchema();
  //! Gets the next batch
  bool GetNextBatch(ArrowArray* out);
  //! Gets the unique buffer to get the next batch
  virtual nanoarrow::UniqueBuffer GetUniqueBuffer() {
    throw InternalException("IPCStreamReader::GetUniqueBuffer not implemented");
  };

  //! Sets the projection pushdown for this reader, by top level column index
  void SetColumnProjection(const vector<idx_t>& column_indexes);
  //! Gets the base schema with no projection pushdown
  const ArrowSchema* GetBaseSchema();

  ArrowIpcMessageType ReadNextMessage(vector<ArrowIpcMessageType> expected_types,
                                      bool end_of_stream_ok = true);
  virtual ArrowIpcMessageType ReadNextMessage() {
    throw InternalException("IPCStreamReader::ReadNextMessage not implemented");
  }

 protected:
  virtual data_ptr_t ReadData(data_ptr_t ptr, idx_t size) {
    throw InternalException("IPCStreamReader::ReadData not implemented");
  }
  //! Decode Message is composed of 3 steps
  ArrowIpcMessageType DecodeMessage();
  //! 1. We decode the message metadata, and return the message_header_size
  idx_t DecodeMetadata();
  //! Validate and decode a complete header, returning true for end of stream
  bool DecodeHeaderBuffer(ArrowBufferView header);
  //! Decodes the schema message the decoder holds and prepares it for batches
  void DecodeSchema();
  //! Takes the schema of the footer the decoder decoded last, which files carry too
  void SchemaFromFooter();
  //! Prepares the decoder for batches of the base schema
  void SetSchema(const ArrowIpcDictionaryEncodings& dictionary_encodings);
  //! 2. We decode the message head, if message is finished we return true
  virtual bool DecodeHeader(idx_t message_header_size) {
    throw InternalException("IPCStreamReader::DecodeHead not implemented");
  }
  //! 3. We decode the message body
  virtual void DecodeBody() {
    throw InternalException("IPCStreamReader::DecodeBody not implemented");
  }

  bool HasProjection() const;
  //! Whether the decoder byte swaps buffers, which rewrites them outside the body
  bool NeedsEndianSwap() const;

  static ArrowBufferView AllocatedDataView(const_data_ptr_t data, int64_t size);

  static const char* MessageTypeString(ArrowIpcMessageType message_type);

  static int64_t CountFields(const ArrowSchema* schema);

  //! Keeps unaligned input headers alive in aligned storage for the decoder
  AllocatedData aligned_header;
  ArrowError error{};
  nanoarrow::ipc::UniqueDecoder decoder{};
  nanoarrow::ipc::UniqueDictionaries dictionaries{};
  //! The schema message endianness, which the decoder wipes before every message
  ArrowIpcEndianness stream_endianness = NANOARROW_IPC_ENDIANNESS_UNINITIALIZED;
  vector<int64_t> projected_fields;
  nanoarrow::UniqueSchema projected_schema;
  //! Schema without projection applied to it
  nanoarrow::UniqueSchema base_schema;

  //! Information on current buffer
  data_ptr_t cur_ptr{};
  int64_t cur_size{};

  //! Allocator used to allocate buffers with decoded arrow information
  Allocator& allocator;

  bool finished{false};

  ArrowIpcMessagePrefix message_prefix{};
  static constexpr uint32_t kContinuationToken = 0xFFFFFFFF;
};

}  // namespace ext_nanoarrow
}  // namespace duckdb
