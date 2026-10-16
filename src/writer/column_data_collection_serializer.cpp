#include "writer/column_data_collection_serializer.hpp"

#include <algorithm>
#include <cstring>
#include <utility>

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/bswap.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/numeric_utils.hpp"

namespace duckdb {

namespace ext_nanoarrow {

// The allocator must outlive the buffers it owns.
inline void InitArrowDuckBuffer(ArrowBuffer* buffer, Allocator& duck_allocator) {
  ArrowBufferInit(buffer);

  buffer->allocator.reallocate = [](ArrowBufferAllocator* allocator, uint8_t* ptr,
                                    int64_t old_size, int64_t new_size) -> uint8_t* {
    NANOARROW_DCHECK(allocator->private_data != nullptr);
    auto duck_allocator = static_cast<Allocator*>(allocator->private_data);
    if (ptr == nullptr && new_size > 0) {
      return duck_allocator->AllocateData(new_size);
    } else if (new_size == 0) {
      duck_allocator->FreeData(ptr, old_size);
      return nullptr;
    } else {
      return duck_allocator->ReallocateData(ptr, old_size, new_size);
    }
  };

  buffer->allocator.free = [](ArrowBufferAllocator* allocator, uint8_t* ptr,
                              int64_t old_size) {
    NANOARROW_DCHECK(allocator->private_data != nullptr);
    auto duck_allocator = static_cast<Allocator*>(allocator->private_data);
    duck_allocator->FreeData(ptr, old_size);
  };

  buffer->allocator.private_data = &duck_allocator;
}

static void CheckEncodableField(const ArrowSchema& field, const char* column,
                                bool stream_format) {
  if (field.dictionary) {
    if (!stream_format) {
      throw NotImplementedException(
          "Arrow IPC output does not support ENUM values in column \"%s\" of the file "
          "format, cast it to VARCHAR or write the stream format with FORMAT ARROWS",
          column);
    }
    CheckEncodableField(*field.dictionary, column, stream_format);
  }
  ArrowSchemaView view;
  ArrowError error{};
  THROW_NOT_OK(InternalException, &error, ArrowSchemaViewInit(&view, &field, &error));
  switch (view.type) {
    case NANOARROW_TYPE_STRING_VIEW:
    case NANOARROW_TYPE_BINARY_VIEW:
    case NANOARROW_TYPE_LIST_VIEW:
    case NANOARROW_TYPE_LARGE_LIST_VIEW:
      throw NotImplementedException(
          "Arrow IPC output does not support the Arrow view type \"%s\" in column "
          "\"%s\", reset arrow_output_version or the arrow view settings",
          field.format, column);
    default:
      break;
  }
  for (int64_t i = 0; i < field.n_children; i++) {
    CheckEncodableField(*field.children[i], column, stream_format);
  }
}

// Makes the field int32 indices into a dictionary of the values it described. The name,
// nullability and metadata stay on the field, where IPC keeps them
static void DictionaryEncodeField(ArrowSchema& field, const LogicalType& type) {
  if (type.id() == LogicalTypeId::ENUM) {
    // DuckDB writes ENUMs dictionary encoded already
    return;
  }
  ArrowDictionaryBuilder::CheckType(type, field, field.name);
  nanoarrow::UniqueSchema values;
  ArrowSchemaMove(&field, values.get());
  ArrowSchemaInit(&field);
  NANOARROW_THROW_NOT_OK(ArrowSchemaSetFormat(&field, "i"));
  NANOARROW_THROW_NOT_OK(ArrowSchemaSetName(&field, values->name));
  NANOARROW_THROW_NOT_OK(ArrowSchemaSetMetadata(&field, values->metadata));
  field.flags = values->flags;
  NANOARROW_THROW_NOT_OK(ArrowSchemaSetMetadata(values.get(), nullptr));
  NANOARROW_THROW_NOT_OK(ArrowSchemaAllocateDictionary(&field));
  ArrowSchemaMove(values.get(), field.dictionary);
}

nanoarrow::UniqueSchema CreateArrowIpcSchema(const vector<LogicalType>& types,
                                             const vector<Identifier>& names,
                                             ClientProperties& options,
                                             const vector<idx_t>& dictionary_columns,
                                             bool stream_format) {
  nanoarrow::UniqueSchema schema;
  ArrowConverter::ToArrowSchema(schema.get(), types, IdentifiersToStrings(names),
                                options);
  if (!dictionary_columns.empty()) {
    if (!stream_format) {
      throw BinderException(
          "DICTIONARY_ENCODE requires the Arrow IPC stream format, use FORMAT ARROWS or "
          "the .arrows extension");
    }
    // DuckDB owns the schema it exports, a nanoarrow owned copy can be changed
    nanoarrow::UniqueSchema duckdb_schema = std::move(schema);
    NANOARROW_THROW_NOT_OK(ArrowSchemaDeepCopy(duckdb_schema.get(), schema.get()));
    for (const auto column_index : dictionary_columns) {
      DictionaryEncodeField(*schema->children[column_index], types[column_index]);
    }
  }
  for (int64_t i = 0; i < schema->n_children; i++) {
    CheckEncodableField(*schema->children[i], schema->children[i]->name, stream_format);
  }
  return schema;
}

vector<idx_t> ReadDictionaryColumns(const vector<Value>& values,
                                    const vector<Identifier>& names) {
  vector<idx_t> result;
  auto add_column = [&](const Value& value) {
    if (value.IsNull()) {
      throw BinderException("DICTIONARY_ENCODE column names must not be NULL");
    }
    const auto column = value.ToString();
    idx_t column_index = 0;
    while (column_index < names.size() && names[column_index] != column) {
      column_index++;
    }
    if (column_index == names.size()) {
      throw BinderException(
          "Column \"%s\" in DICTIONARY_ENCODE is not among the written columns", column);
    }
    if (std::find(result.begin(), result.end(), column_index) != result.end()) {
      throw BinderException("Column \"%s\" is named more than once in DICTIONARY_ENCODE",
                            column);
    }
    result.push_back(column_index);
  };
  for (const auto& value : values) {
    if (!value.IsNull() && value.type().id() == LogicalTypeId::LIST) {
      for (const auto& child : ListValue::GetChildren(value)) {
        add_column(child);
      }
    } else {
      add_column(value);
    }
  }
  if (result.empty()) {
    throw BinderException("DICTIONARY_ENCODE requires at least one column name");
  }
  return result;
}

bool HasDictionaries(const ArrowSchema& schema) {
  if (schema.dictionary) {
    return true;
  }
  for (int64_t i = 0; i < schema.n_children; i++) {
    if (HasDictionaries(*schema.children[i])) {
      return true;
    }
  }
  return false;
}

ColumnDataCollectionSerializer::ColumnDataCollectionSerializer(
    ClientProperties options, Allocator& allocator,
    ArrowIpcCompressionOptions compression, bool track_body_size)
    : options(std::move(options)),
      allocator(allocator),
      compression(compression),
      track_body_size(track_body_size) {}

void ColumnDataCollectionSerializer::Init(const ArrowSchema* schema_p,
                                          const vector<LogicalType>& logical_types_p) {
  schema = schema_p;
  logical_types = logical_types_p;
  dictionary_messages.clear();
  header.reset();
  body.reset();
  encoder.reset();
  chunk_view.reset();

  InitArrowDuckBuffer(header.get(), allocator);
  InitArrowDuckBuffer(body.get(), allocator);
  NANOARROW_THROW_NOT_OK(ArrowIpcEncoderInit(encoder.get()));
  SetArrowIpcEncoderCompression(*encoder.get(), compression);
  THROW_NOT_OK(InternalException, &error,
               ArrowArrayViewInitFromSchema(chunk_view.get(), schema, &error));

  extension_types =
      ArrowTypeExtensionData::GetExtensionTypes(*options.client_context, logical_types);
}

void ColumnDataCollectionSerializer::SerializeSchema(const ArrowSchema* schema,
                                                     idx_t reserved_size) {
  header->size_bytes = 0;
  body->size_bytes = 0;
  THROW_NOT_OK(NotImplementedException, &error,
               ArrowIpcEncoderEncodeSchema(encoder.get(), schema, &error));
  NANOARROW_THROW_NOT_OK(
      ArrowIpcEncoderFinalizeBuffer(encoder.get(), true, header.get()));
  if (reserved_size) {
    if (reserved_size < static_cast<idx_t>(header->size_bytes)) {
      throw InternalException("Arrow IPC schema does not fit in its reserved message");
    }
    // Include padding in the message length to keep record batch offsets unchanged
    auto length = BSwapIfBE(NumericCast<int32_t>(reserved_size - 2 * sizeof(int32_t)));
    NANOARROW_THROW_NOT_OK(ArrowBufferAppendFill(
        header.get(), 0, NumericCast<int64_t>(reserved_size) - header->size_bytes));
    std::memcpy(header->data + sizeof(int32_t), &length, sizeof(length));
  }
}

void ColumnDataCollectionSerializer::SerializeFooter(
    nanoarrow::UniqueSchema schema, const vector<ArrowIpcFileBlock>& blocks) {
  header->size_bytes = 0;
  body->size_bytes = 0;
  nanoarrow::ipc::UniqueFooter footer;
  ArrowSchemaMove(schema.get(), &footer->schema);
  NANOARROW_THROW_NOT_OK(
      ArrowBufferAppend(&footer->record_batch_blocks, blocks.data(),
                        NumericCast<int64_t>(blocks.size() * sizeof(ArrowIpcFileBlock))));
  THROW_NOT_OK(InternalException, &error,
               ArrowIpcEncoderEncodeFooter(encoder.get(), footer.get(), &error));
  NANOARROW_THROW_NOT_OK(
      ArrowIpcEncoderFinalizeBuffer(encoder.get(), false, header.get()));
}

// Uncompressed body size, the buffers ArrowIpcEncoderCollectArray collects padded to 8
static int64_t PaddedBodySize(const ArrowArrayView& view) {
  int64_t size = 0;
  for (int64_t c = 0; c < view.n_children; c++) {
    const auto& child = *view.children[c];
    for (int64_t b = 0; b < child.array->n_buffers; b++) {
      size += AlignValue<int64_t>(child.buffer_views[b].size_bytes);
    }
    size += PaddedBodySize(child);
  }
  return size;
}

// A zero null count lets readers take every value as valid without a bitmap
static void DropUnusedValidityOf(ArrowArrayView& view) {
  if (view.null_count == 0 &&
      view.layout.buffer_type[0] == NANOARROW_BUFFER_TYPE_VALIDITY) {
    view.buffer_views[0].size_bytes = 0;
  }
}

static void DropUnusedValidity(ArrowArrayView& view) {
  for (int64_t c = 0; c < view.n_children; c++) {
    auto& child = *view.children[c];
    if (child.null_count == 0 &&
        child.layout.buffer_type[0] == NANOARROW_BUFFER_TYPE_VALIDITY) {
      child.buffer_views[0].size_bytes = 0;
    }
    DropUnusedValidity(child);
  }
}

unique_ptr<ArrowBatchBuilder> ColumnDataCollectionSerializer::NewBatchBuilder(
    idx_t capacity) const {
  D_ASSERT(schema);
  return make_uniq<ArrowBatchBuilder>(logical_types, *schema, capacity, options,
                                      extension_types);
}

// Ids in the order ArrowIpcDictionaryEncodingsAppendSchema gives them to the schema
void ColumnDataCollectionSerializer::SerializeDictionaries(ArrowArrayView& view,
                                                           int64_t& next_id) {
  if (view.dictionary) {
    SerializeDictionary(next_id++, *view.dictionary);
  }
  for (int64_t c = 0; c < view.n_children; c++) {
    SerializeDictionaries(*view.children[c], next_id);
  }
  if (view.dictionary) {
    SerializeDictionaries(*view.dictionary, next_id);
  }
}

void ColumnDataCollectionSerializer::SerializeDictionary(int64_t id,
                                                         ArrowArrayView& values) {
  DropUnusedValidityOf(values);
  DropUnusedValidity(values);
  auto message = make_shared_ptr<nanoarrow::UniqueBuffer>();
  nanoarrow::UniqueBuffer dictionary_body;
  InitArrowDuckBuffer(message->get(), allocator);
  InitArrowDuckBuffer(dictionary_body.get(), allocator);
  THROW_NOT_OK(InternalException, &error,
               ArrowIpcEncoderEncodeSimpleDictionaryBatch(encoder.get(), id, 0, &values,
                                                          dictionary_body.get(), &error));
  NANOARROW_THROW_NOT_OK(
      ArrowIpcEncoderFinalizeBuffer(encoder.get(), true, message->get()));
  NANOARROW_THROW_NOT_OK(ArrowBufferAppend(message->get(), dictionary_body->data,
                                           dictionary_body->size_bytes));
  dictionary_messages.push_back({id, std::move(message)});
}

idx_t ColumnDataCollectionSerializer::Serialize(ArrowBatchBuilder& builder) {
  auto array = builder.Finalize();
  header->size_bytes = 0;
  body->size_bytes = 0;
  dictionary_messages.clear();

  THROW_NOT_OK(duckdb::InternalException, &error,
               ArrowArrayViewSetArray(chunk_view.get(), array.get(), &error));
  DropUnusedValidity(*chunk_view.get());
  int64_t next_id = 0;
  SerializeDictionaries(*chunk_view.get(), next_id);
  if (track_body_size) {
    // Measured as written: uncompressed buffer sizes, the dropped bitmaps at size 0
    ArrowBatchStatsMeasure(chunk_view.get(), &batch_measure);
  }
  if (compression.type == NANOARROW_IPC_COMPRESSION_TYPE_NONE) {
    // One exact allocation, where growing buffer by buffer doubles and keeps the peaks
    NANOARROW_THROW_NOT_OK(
        ArrowBufferReserve(body.get(), PaddedBodySize(*chunk_view.get())));
  }
  THROW_NOT_OK(InternalException, &error,
               ArrowIpcEncoderEncodeSimpleRecordBatch(encoder.get(), chunk_view.get(),
                                                      body.get(), &error));
  NANOARROW_THROW_NOT_OK(
      ArrowIpcEncoderFinalizeBuffer(encoder.get(), true, header.get()));

  return 1;
}
idx_t ColumnDataCollectionSerializer::Serialize(const ColumnDataCollection& buffer) {
  if (buffer.Count() == 0) {
    return 0;
  }
  auto builder = NewBatchBuilder(buffer.Count());
  for (auto& chunk : buffer.Chunks()) {
    builder->Append(chunk);
  }
  return Serialize(*builder);
}

namespace {
//! Keeps an encoded buffer alive while the async writer writes a slice of it
class ArrowWriteBuffer final : public AsyncWriteBuffer {
 public:
  ArrowWriteBuffer(shared_ptr<nanoarrow::UniqueBuffer> buffer_p, idx_t offset, idx_t size)
      : buffer(std::move(buffer_p)), offset(offset), size(size) {}

  data_ptr_t Ptr() override { return (*buffer)->data + offset; }
  idx_t Size() const override { return size; }

 private:
  shared_ptr<nanoarrow::UniqueBuffer> buffer;
  idx_t offset;
  idx_t size;
};

//! Slices sized like parquet pages keep the async writer draining several at once
constexpr idx_t kAsyncWriteSliceBytes = 4 * 1024 * 1024;

void WriteSlices(AsyncFileWriter& writer, shared_ptr<nanoarrow::UniqueBuffer> shared) {
  const auto size = NumericCast<idx_t>((*shared)->size_bytes);
  for (idx_t offset = 0; offset < size; offset += kAsyncWriteSliceBytes) {
    writer.WriteData(make_uniq<ArrowWriteBuffer>(
        shared, offset, MinValue<idx_t>(kAsyncWriteSliceBytes, size - offset)));
  }
}
}  // namespace

ArrowIpcFileBlock ColumnDataCollectionSerializer::Flush(
    AsyncFileWriter& writer, const ArrowDictionaryFilter& write_dictionary) {
  // The messages register as one batch, so backpressure applies once, as parquet does
  auto batch = writer.StartBatch();
  for (const auto& dictionary : dictionary_messages) {
    if (!write_dictionary || write_dictionary(dictionary)) {
      WriteSlices(writer, dictionary.message);
    }
  }
  ArrowIpcFileBlock block{NumericCast<int64_t>(writer.GetTotalWritten()),
                          NumericCast<int32_t>(header->size_bytes), body->size_bytes};
  WriteSlices(writer, make_shared_ptr<nanoarrow::UniqueBuffer>(GetHeader()));
  if (body->size_bytes > 0) {
    WriteSlices(writer, make_shared_ptr<nanoarrow::UniqueBuffer>(GetBody()));
  }
  batch.Finish();
  return block;
}

ArrowIpcFileBlock ColumnDataCollectionSerializer::Flush(
    BufferedFileWriter& writer, const ArrowDictionaryFilter& write_dictionary) {
  for (const auto& dictionary : dictionary_messages) {
    if (!write_dictionary || write_dictionary(dictionary)) {
      const auto& message = *dictionary.message;
      writer.WriteData(message->data, NumericCast<idx_t>(message->size_bytes));
    }
  }
  ArrowIpcFileBlock block{NumericCast<int64_t>(writer.GetTotalWritten()),
                          NumericCast<int32_t>(header->size_bytes), body->size_bytes};
  writer.WriteData(header->data, header->size_bytes);
  writer.WriteData(body->data, body->size_bytes);
  return block;
}

nanoarrow::UniqueBuffer ColumnDataCollectionSerializer::TakeMessages() {
  nanoarrow::UniqueBuffer result;
  InitArrowDuckBuffer(result.get(), allocator);
  int64_t size = header->size_bytes + body->size_bytes;
  for (const auto& dictionary : dictionary_messages) {
    size += (*dictionary.message)->size_bytes;
  }
  NANOARROW_THROW_NOT_OK(ArrowBufferReserve(result.get(), size));
  for (const auto& dictionary : dictionary_messages) {
    const auto& message = *dictionary.message;
    ArrowBufferAppendUnsafe(result.get(), message->data, message->size_bytes);
  }
  ArrowBufferAppendUnsafe(result.get(), header->data, header->size_bytes);
  ArrowBufferAppendUnsafe(result.get(), body->data, body->size_bytes);
  header->size_bytes = 0;
  body->size_bytes = 0;
  dictionary_messages.clear();
  return result;
}
nanoarrow::UniqueBuffer ColumnDataCollectionSerializer::GetHeader() {
  auto result_header = std::move(header);
  InitArrowDuckBuffer(header.get(), allocator);
  return result_header;
}
nanoarrow::UniqueBuffer ColumnDataCollectionSerializer::GetBody() {
  auto result_body = std::move(body);
  InitArrowDuckBuffer(body.get(), allocator);
  return result_body;
}
}  // namespace ext_nanoarrow
}  // namespace duckdb
