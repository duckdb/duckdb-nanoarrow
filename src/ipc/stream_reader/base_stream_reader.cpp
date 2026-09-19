#include "ipc/stream_reader/base_stream_reader.hpp"
#include <cinttypes>
#include <iostream>

namespace duckdb {
namespace ext_nanoarrow {

const ArrowSchema* IPCStreamReader::GetBaseSchema() {
  if (base_schema->release) {
    return base_schema.get();
  }

  ReadNextMessage({NANOARROW_IPC_MESSAGE_TYPE_SCHEMA}, /*end_of_stream_ok*/ false);
  DecodeSchema();
  return base_schema.get();
}

void IPCStreamReader::DecodeSchema() {
  // Decode the schema and retain its dictionary encoding information.
  nanoarrow::ipc::UniqueDictionaryEncodings dictionary_encodings;
  THROW_NOT_OK(IOException, &error,
               ArrowIpcDecoderDecodeSchemaWithDictionaries(
                   decoder.get(), base_schema.get(), dictionary_encodings.get(), &error));
  SetSchema(*dictionary_encodings.get());
}

void IPCStreamReader::SchemaFromFooter() {
  // Encodings find their fields by address, so the schema moves rather than copies
  ArrowSchemaMove(&decoder->footer->schema, base_schema.get());
  SetSchema(decoder->footer->dictionaries);
}

void IPCStreamReader::SetSchema(const ArrowIpcDictionaryEncodings& dictionary_encodings) {
  if (decoder->feature_flags & NANOARROW_IPC_FEATURE_DICTIONARY_REPLACEMENT) {
    throw IOException("This stream uses unsupported feature DICTIONARY_REPLACEMENT");
  }

  THROW_NOT_OK(
      IOException, &error,
      ArrowIpcDictionariesInit(dictionaries.get(), &dictionary_encodings, &error));

  // Only the schema message carries this, later messages read back as uninitialized
  stream_endianness = decoder->endianness;

  // Set up the decoder to decode batches
  THROW_NOT_OK(IOException, &error,
               ArrowIpcDecoderSetEndianness(decoder.get(), decoder->endianness));
  THROW_NOT_OK(IOException, &error,
               ArrowIpcDecoderSetSchemaWithDictionaries(decoder.get(), base_schema.get(),
                                                        &dictionary_encodings, &error));
}

bool IPCStreamReader::HasProjection() const { return !projected_fields.empty(); }

bool IPCStreamReader::NeedsEndianSwap() const {
  // Mirrors nanoarrow, which swaps only for an explicit endianness that is not ours
  if (stream_endianness != NANOARROW_IPC_ENDIANNESS_LITTLE &&
      stream_endianness != NANOARROW_IPC_ENDIANNESS_BIG) {
    return false;
  }
  return stream_endianness != ArrowIpcSystemEndianness();
}

const ArrowSchema* IPCStreamReader::GetOutputSchema() {
  if (HasProjection()) {
    return projected_schema.get();
  } else {
    return GetBaseSchema();
  }
}

bool IPCStreamReader::GetNextBatch(ArrowArray* out) {
  const bool thread_safe_shared = ArrowSharedBufferIsThreadSafe();
  while (true) {
    ArrowIpcMessageType message_type =
        ReadNextMessage({NANOARROW_IPC_MESSAGE_TYPE_RECORD_BATCH,
                         NANOARROW_IPC_MESSAGE_TYPE_DICTIONARY_BATCH});
    if (message_type == NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED) {
      out->release = nullptr;
      return false;
    }

    if (message_type == NANOARROW_IPC_MESSAGE_TYPE_RECORD_BATCH) {
      break;
    }

    struct ArrowBufferView body_view = AllocatedDataView(cur_ptr, cur_size);
    nanoarrow::UniqueBuffer body_shared = GetUniqueBuffer();
    if (thread_safe_shared) {
      nanoarrow::UniqueBuffer shared;
      NANOARROW_THROW_NOT_OK(ArrowSharedBufferInit(shared.get(), body_shared.get()));
      THROW_NOT_OK(IOException, &error,
                   ArrowIpcDecoderDecodeDictionaryFromShared(
                       decoder.get(), shared.get(), NANOARROW_VALIDATION_LEVEL_FULL,
                       dictionaries.get(), &error));
    } else {
      THROW_NOT_OK(IOException, &error,
                   ArrowIpcDecoderDecodeDictionary(decoder.get(), body_view,
                                                   NANOARROW_VALIDATION_LEVEL_FULL,
                                                   dictionaries.get(), &error));
    }
  }

  struct ArrowBufferView body_view = AllocatedDataView(cur_ptr, cur_size);
  nanoarrow::UniqueBuffer body_shared = GetUniqueBuffer();
  nanoarrow::UniqueBuffer shared;
  NANOARROW_THROW_NOT_OK(ArrowSharedBufferInit(shared.get(), body_shared.get()));
  nanoarrow::UniqueArray array;
  if (HasProjection()) {
    NANOARROW_THROW_NOT_OK(ArrowArrayInitFromType(array.get(), NANOARROW_TYPE_STRUCT));
    NANOARROW_THROW_NOT_OK(
        ArrowArrayAllocateChildren(array.get(), GetOutputSchema()->n_children));

    if (thread_safe_shared) {
      for (int64_t i = 0; i < array->n_children; i++) {
        THROW_NOT_OK(
            IOException, &error,
            ArrowIpcDecoderDecodeArrayFromSharedWithDictionaries(
                decoder.get(), shared.get(), projected_fields[i], dictionaries.get(),
                array->children[i], NANOARROW_VALIDATION_LEVEL_FULL, &error));
      }
    } else {
      for (int64_t i = 0; i < array->n_children; i++) {
        THROW_NOT_OK(
            IOException, &error,
            ArrowIpcDecoderDecodeArrayWithDictionaries(
                decoder.get(), body_view, projected_fields[i], dictionaries.get(),
                array->children[i], NANOARROW_VALIDATION_LEVEL_FULL, &error));
      }
    }

    D_ASSERT(array->n_children > 0);
    array->length = array->children[0]->length;
    array->null_count = 0;
  } else if (thread_safe_shared) {
    THROW_NOT_OK(IOException, &error,
                 ArrowIpcDecoderDecodeArrayFromSharedWithDictionaries(
                     decoder.get(), shared.get(), -1, dictionaries.get(), array.get(),
                     NANOARROW_VALIDATION_LEVEL_FULL, &error));
  } else {
    THROW_NOT_OK(IOException, &error,
                 ArrowIpcDecoderDecodeArrayWithDictionaries(
                     decoder.get(), body_view, -1, dictionaries.get(), array.get(),
                     NANOARROW_VALIDATION_LEVEL_FULL, &error));
  }

  ArrowArrayMove(array.get(), out);
  return true;
}

void IPCStreamReader::SetColumnProjection(const vector<idx_t>& column_indexes) {
  if (column_indexes.empty()) {
    throw InternalException("Can't request zero fields projected from IpcStreamReader");
  }

  // Ensure we have a file schema to work with
  GetBaseSchema();

  nanoarrow::UniqueSchema schema;
  ArrowSchemaInit(schema.get());
  NANOARROW_THROW_NOT_OK(ArrowSchemaSetTypeStruct(
      schema.get(), UnsafeNumericCast<int64_t>(column_indexes.size())));

  // The decoder addresses a field by its index in a depth first walk of the schema
  vector<int64_t> flat_field_indexes;
  int64_t field_count = 0;
  for (int64_t i = 0; i < base_schema->n_children; i++) {
    flat_field_indexes.push_back(field_count);
    field_count += CountFields(base_schema->children[i]);
  }

  for (idx_t i = 0; i < column_indexes.size(); i++) {
    const auto col_idx = column_indexes[i];
    projected_fields.push_back(flat_field_indexes[col_idx]);
    NANOARROW_THROW_NOT_OK(
        ArrowSchemaDeepCopy(base_schema->children[col_idx], schema->children[i]));
  }
  projected_schema = std::move(schema);
}

idx_t IPCStreamReader::DecodeMetadata() {
  int32_t prefix_size;
  auto status = ArrowIpcDecoderPeekHeader(
      decoder.get(),
      AllocatedDataView(reinterpret_cast<const_data_ptr_t>(&message_prefix),
                        sizeof(message_prefix)),
      &prefix_size, &error);
  if (status != ENODATA) {
    THROW_NOT_OK(IOException, &error, status);
  }
  if (prefix_size != sizeof(message_prefix) ||
      decoder->header_size_bytes < static_cast<int64_t>(sizeof(message_prefix))) {
    throw IOException("Invalid Arrow IPC message prefix");
  }
  return decoder->header_size_bytes;
}

bool IPCStreamReader::DecodeHeaderBuffer(ArrowBufferView header) {
  // FlatBuffer verification requires alignment, which sliced input buffers may lack.
  if (reinterpret_cast<uintptr_t>(header.data.data) % alignof(uint64_t) != 0) {
    if (aligned_header.GetSize() < static_cast<idx_t>(header.size_bytes)) {
      aligned_header = allocator.Allocate(header.size_bytes);
    }
    std::memcpy(aligned_header.get(), header.data.data, header.size_bytes);
    header.data.data = aligned_header.get();
  }
  auto status = ArrowIpcDecoderVerifyHeader(decoder.get(), header, &error);
  if (status == ENODATA) {
    finished = true;
    return true;
  }
  THROW_NOT_OK(IOException, &error, status);
  if (decoder->body_size_bytes < 0) {
    throw IOException("Arrow IPC message body size must not be negative");
  }
  THROW_NOT_OK(IOException, &error,
               ArrowIpcDecoderDecodeHeader(decoder.get(), header, &error));
  return false;
}

ArrowIpcMessageType IPCStreamReader::DecodeMessage() {
  auto message_header_size = DecodeMetadata();
  if (DecodeHeader(message_header_size)) {
    return NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED;
  }
  DecodeBody();
  return decoder->message_type;
}

ArrowIpcMessageType IPCStreamReader::ReadNextMessage(
    vector<ArrowIpcMessageType> expected_types, bool end_of_stream_ok) {
  ArrowIpcMessageType actual_type = ReadNextMessage();
  if (end_of_stream_ok && actual_type == NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED) {
    return actual_type;
  }

  for (const auto expected_type : expected_types) {
    if (expected_type == actual_type) {
      return actual_type;
    }
  }

  std::stringstream expected_types_label;
  for (size_t i = 0; i < expected_types.size(); i++) {
    if (i > 0) {
      expected_types_label << " or ";
    }

    expected_types_label << MessageTypeString(expected_types[i]);
  }

  string actual_type_label;
  if (actual_type == NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED) {
    actual_type_label = "end of stream";
  } else {
    actual_type_label = MessageTypeString(actual_type);
  }

  throw IOException(string("Expected ") + expected_types_label.str() +
                    " Arrow IPC message but got " + actual_type_label);
}

int64_t IPCStreamReader::CountFields(const ArrowSchema* schema) {
  int64_t n_fields = 1;
  for (int64_t i = 0; i < schema->n_children; i++) {
    n_fields += CountFields(schema->children[i]);
  }
  return n_fields;
}

ArrowBufferView IPCStreamReader::AllocatedDataView(const_data_ptr_t data, int64_t size) {
  ArrowBufferView view{};
  view.data.data = data;
  view.size_bytes = size;
  return view;
}

const char* IPCStreamReader::MessageTypeString(ArrowIpcMessageType message_type) {
  switch (message_type) {
    case NANOARROW_IPC_MESSAGE_TYPE_SCHEMA:
      return "Schema";
    case NANOARROW_IPC_MESSAGE_TYPE_RECORD_BATCH:
      return "RecordBatch";
    case NANOARROW_IPC_MESSAGE_TYPE_DICTIONARY_BATCH:
      return "DictionaryBatch";
    case NANOARROW_IPC_MESSAGE_TYPE_TENSOR:
      return "Tensor";
    case NANOARROW_IPC_MESSAGE_TYPE_SPARSE_TENSOR:
      return "SparseTensor";
    case NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED:
      return "Uninitialized";
    default:
      return "";
  }
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
