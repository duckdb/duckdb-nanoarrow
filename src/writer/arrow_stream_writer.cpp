#include "writer/arrow_stream_writer.hpp"

#include <cstring>

#include "duckdb/common/bswap.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types.hpp"
#include "ipc/file_format.hpp"

namespace duckdb {

namespace ext_nanoarrow {

namespace {

constexpr char kTotalCompressedSize[] = "total_compressed_size";
constexpr char kTotalUncompressedSize[] = "total_uncompressed_size";
constexpr char kTypeKey[] = ":type";
constexpr char kTypeDetailsKey[] = ":type_details";
constexpr char kTypePrecisionKey[] = ":type_precision";
constexpr char kTypeScaleKey[] = ":type_scale";

void SetSchemaMetadata(ArrowSchema* schema,
                       const vector<pair<string, string>>& metadata) {
  nanoarrow::UniqueBuffer packed;
  NANOARROW_THROW_NOT_OK(ArrowMetadataBuilderInit(packed.get(), schema->metadata));
  for (const auto& item : metadata) {
    ArrowStringView key{item.first.data(), static_cast<int64_t>(item.first.size())};
    ArrowStringView value{item.second.data(), static_cast<int64_t>(item.second.size())};
    NANOARROW_THROW_NOT_OK(ArrowMetadataBuilderSet(packed.get(), key, value));
  }
  NANOARROW_THROW_NOT_OK(
      ArrowSchemaSetMetadata(schema, reinterpret_cast<char*>(packed->data)));
}

// The <namespace>:type value: DuckDB's name for the type without its parameters, or
// its alias such as JSON
string GetTypeName(const LogicalType& type) {
  return type.HasAlias() ? type.GetAlias() : LogicalTypeIdToString(type.id());
}

// The child types in the order Arrow lays out the children of the field
vector<LogicalType> GetTypeChildren(const LogicalType& type) {
  vector<LogicalType> children;
  switch (type.id()) {
    case LogicalTypeId::LIST:
      children.push_back(ListType::GetChildType(type));
      break;
    case LogicalTypeId::ARRAY:
      children.push_back(ArrayType::GetChildType(type));
      break;
    case LogicalTypeId::STRUCT:
    case LogicalTypeId::TUPLE:
      for (const auto& child : StructType::GetChildTypes(type)) {
        children.push_back(child.second);
      }
      break;
    case LogicalTypeId::MAP:
      children.push_back(MapType::KeyType(type));
      children.push_back(MapType::ValueType(type));
      break;
    case LogicalTypeId::UNION:
      for (idx_t member_index = 0; member_index < UnionType::GetMemberCount(type);
           member_index++) {
        children.push_back(UnionType::GetMemberType(type, member_index));
      }
      break;
    default:
      break;
  }
  return children;
}

// Adds <namespace>:type and <namespace>:type_details to the field, and recurses so
// every nested list, array, struct, map and union child is described as well
void AddTypeMetadata(ArrowSchema* schema, const LogicalType& type,
                     const string& type_metadata_namespace) {
  vector<pair<string, string>> metadata;
  metadata.emplace_back(type_metadata_namespace + kTypeKey, GetTypeName(type));
  metadata.emplace_back(type_metadata_namespace + kTypeDetailsKey, type.ToString());
  if (type.id() == LogicalTypeId::DECIMAL) {
    uint8_t width, scale;
    type.GetDecimalProperties(width, scale);
    metadata.emplace_back(type_metadata_namespace + kTypePrecisionKey, to_string(width));
    metadata.emplace_back(type_metadata_namespace + kTypeScaleKey, to_string(scale));
  }
  SetSchemaMetadata(schema, metadata);
  // Arrow keeps map keys and values under an entries struct, which has no DuckDB type
  ArrowSchema* parent = schema;
  if (type.id() == LogicalTypeId::MAP) {
    if (schema->n_children != 1) {
      throw InternalException("Arrow map for %s has no entries struct", type.ToString());
    }
    parent = schema->children[0];
  }
  const auto children = GetTypeChildren(type);
  if (static_cast<idx_t>(parent->n_children) != children.size()) {
    throw InternalException("Arrow children do not match the children of %s",
                            type.ToString());
  }
  for (idx_t child_index = 0; child_index < children.size(); child_index++) {
    AddTypeMetadata(parent->children[child_index], children[child_index],
                    type_metadata_namespace);
  }
}

}  // namespace

ArrowStreamWriter::ArrowStreamWriter(const ClientProperties& options_p, FileSystem& fs,
                                     const string& file_path,
                                     const vector<LogicalType>& logical_types,
                                     const ArrowSchema& schema_p,
                                     const vector<pair<string, string>>& metadata,
                                     const vector<ArrowFieldMetadata>& field_metadata,
                                     const ArrowIpcCompressionOptions& compression,
                                     bool file_format, bool size_metadata)
    : options(options_p),
      allocator(BufferAllocator::Get(*options.client_context)),
      compression(compression),
      logical_types(logical_types),
      file_format(file_format),
      size_metadata(size_metadata) {
  InitSchema(schema_p, metadata, field_metadata);
  InitOutputFile(fs, file_path);
}

void ArrowStreamWriter::InitSchema(const ArrowSchema& schema_p,
                                   const vector<pair<string, string>>& metadata,
                                   const vector<ArrowFieldMetadata>& field_metadata) {
  // Copy into a nanoarrow owned schema so the metadata set below is freed with it
  NANOARROW_THROW_NOT_OK(ArrowSchemaDeepCopy(&schema_p, schema.get()));
  if (!metadata.empty()) {
    SetSchemaMetadata(schema.get(), metadata);
  }
  for (const auto& field : field_metadata) {
    SetSchemaMetadata(schema->children[field.column_index], field.metadata);
  }
  if (size_metadata) {
    // Reserve space for the longest totals before writing any record batches
    const auto max_size = std::to_string(NumericLimits<int64_t>::Maximum());
    SetSchemaMetadata(schema.get(), {{kTotalCompressedSize, max_size},
                                     {kTotalUncompressedSize, max_size}});
  }
  has_dictionaries = HasDictionaries(*schema.get());
  ArrowError error{};
  THROW_NOT_OK(InternalException, &error,
               ArrowBatchStatsInit(&batch_stats, schema.get(), file_format, &error));
}

ArrowStreamWriter::~ArrowStreamWriter() {
  // Finalize closes the handle, so an open one here means the output is incomplete
  // The async writer aborts its own unfinished output when it is destroyed
  if (!writer || !writer->handle) {
    return;
  }
  try {
    writer->handle->AbortWrite();
  } catch (...) {
  }
}

void ArrowStreamWriter::InitOutputFile(FileSystem& fs, const string& file_path) {
  auto flags = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW;
  // Only a path this COPY creates may be removed when the write is aborted
  if (!fs.FileExists(file_path) && !fs.IsPipe(file_path)) {
    flags |= FileFlags::FILE_FLAGS_EXCLUSIVE_CREATE;
  }
  // Remote outputs upload parts at once through the async writer, local ones buffer
  if (size_metadata || !FileSystem::IsRemoteFile(file_path)) {
    writer = make_uniq<BufferedFileWriter>(fs, file_path.c_str(), flags);
    // WriteFooter patches offset 8 of a new regular file, fsspec needs OnDiskFile first
    if (size_metadata && (!writer->handle->OnDiskFile() ||
                          writer->handle->GetType() != FileType::FILE_TYPE_REGULAR ||
                          writer->handle->GetFileSize() != 0)) {
      throw IOException(
          "SIZE_METADATA requires a seekable local output to update the schema");
    }
  } else {
    async_writer =
        make_uniq<AsyncFileWriter>(*options.client_context, fs, file_path, flags);
  }
  if (file_format) {
    WriteBytes(const_data_ptr_cast(kArrowIPCFileMagic), kArrowIPCFileHeaderSize);
  }
}

void ArrowStreamWriter::WriteSchema() {
  auto serializer = NewSerializer();
  serializer->SerializeSchema(schema.get());
  schema_message_size = WriteMessage(*serializer).metadata_length;
  file_size = TotalWritten();
}

unique_ptr<ColumnDataCollectionSerializer> ArrowStreamWriter::NewSerializer() const {
  auto serializer = make_uniq<ColumnDataCollectionSerializer>(
      options, allocator, compression, size_metadata || written_stats);
  serializer->Init(schema.get(), logical_types);
  return serializer;
}

bool ArrowStreamWriter::UpdateDictionary(const ArrowDictionaryMessage& dictionary) {
  auto& current = current_dictionaries[dictionary.id];
  const auto& message = *dictionary.message;
  if (current && (*current)->size_bytes == message->size_bytes &&
      std::memcmp((*current)->data, message->data,
                  NumericCast<size_t>(message->size_bytes)) == 0) {
    return false;
  }
  current = dictionary.message;
  return true;
}

void ArrowStreamWriter::Flush(ColumnDataCollectionSerializer& serializer) {
  lock_guard<mutex> guard(lock);
  // A stream replaces a dictionary with the next one of its id, a repeat is redundant
  auto block = WriteMessage(serializer, [this](const ArrowDictionaryMessage& dictionary) {
    return UpdateDictionary(dictionary);
  });
  if (file_format) {
    blocks.push_back(block);
  }
  if (size_metadata || written_stats) {
    ArrowBatchStatsAdd(&batch_stats, &serializer.BatchMeasure(), block.metadata_length,
                       block.body_length,
                       compression.type != NANOARROW_IPC_COMPRESSION_TYPE_NONE);
  }
  ++row_group_count;
  file_size = TotalWritten();
}

void ArrowStreamWriter::Finalize() {
  uint8_t end_of_stream[] = {0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00};
  lock_guard<mutex> guard(lock);
  WriteBytes(end_of_stream, sizeof(end_of_stream));
  if (file_format) {
    WriteFooter();
  }
  file_size = TotalWritten();
  if (async_writer) {
    async_writer->Close();
  } else {
    writer->Close();
  }
  if (written_stats) {
    // Reported by name, like row_group_count for parquet
    written_stats->row_count = NumericCast<idx_t>(batch_stats.row_count);
    written_stats->file_size_bytes = file_size;
    written_stats->footer_size_bytes = footer_size_bytes;
    auto& extra_info = written_stats->extra_info;
    extra_info["record_batch_count"] = Value::UBIGINT(batch_stats.record_batch_count);
    extra_info["total_compressed_size"] =
        Value::UBIGINT(batch_stats.total_compressed_size);
    extra_info["total_uncompressed_size"] =
        Value::UBIGINT(batch_stats.total_uncompressed_size);
    // The bound covers record batches only, not the dictionaries a reader keeps
    extra_info["peak_read_memory_bytes"] =
        has_dictionaries
            ? Value(LogicalType::UBIGINT)
            : Value::UBIGINT(ArrowBatchStatsPeakReadMemoryBytes(&batch_stats));
    // Final even with SIZE_METADATA, which rewrites the schema at its reserved size
    extra_info["schema_size_bytes"] = Value::UBIGINT(schema_message_size);
  }
}

void ArrowStreamWriter::WriteBytes(const_data_ptr_t data, idx_t size) {
  if (async_writer) {
    async_writer->WriteData(data, size);
  } else {
    writer->WriteData(data, size);
  }
}

ArrowIpcFileBlock ArrowStreamWriter::WriteMessage(
    ColumnDataCollectionSerializer& serializer,
    const ArrowDictionaryFilter& write_dictionary) {
  return async_writer ? serializer.Flush(*async_writer, write_dictionary)
                      : serializer.Flush(*writer, write_dictionary);
}

idx_t ArrowStreamWriter::TotalWritten() const {
  return async_writer ? async_writer->GetTotalWritten() : writer->GetTotalWritten();
}

void ArrowStreamWriter::WriteFooter() {
  auto serializer = NewSerializer();
  nanoarrow::UniqueSchema footer_schema;
  NANOARROW_THROW_NOT_OK(ArrowSchemaDeepCopy(schema.get(), footer_schema.get()));
  if (size_metadata) {
    SetSchemaMetadata(
        footer_schema.get(),
        {{kTotalCompressedSize, std::to_string(batch_stats.total_compressed_size)},
         {kTotalUncompressedSize, std::to_string(batch_stats.total_uncompressed_size)}});
    // Update the opening schema to match the footer without changing its reserved size
    serializer->SerializeSchema(footer_schema.get(), schema_message_size);
    auto opening_schema = serializer->GetHeader();
    writer->Flush();
    const auto end_offset = writer->GetTotalWritten();
    writer->handle->Write(QueryContext(), opening_schema->data,
                          NumericCast<idx_t>(opening_schema->size_bytes),
                          kArrowIPCFileHeaderSize);
    writer->handle->Seek(end_offset);
  }
  serializer->SerializeFooter(std::move(footer_schema), blocks);
  auto footer = serializer->GetHeader();
  footer_size_bytes = Value::UBIGINT(NumericCast<uint64_t>(footer->size_bytes));
  const auto footer_size = BSwapIfBE(NumericCast<int32_t>(footer->size_bytes));
  WriteBytes(footer->data, footer->size_bytes);
  WriteBytes(const_data_ptr_cast(&footer_size), sizeof(footer_size));
  WriteBytes(const_data_ptr_cast(kArrowIPCFileMagic), kArrowIPCFileMagicSize);
}

void ArrowStreamWriter::SetTypeMetadata(ArrowSchema& schema,
                                        const vector<LogicalType>& types,
                                        const string& type_metadata_namespace) {
  if (static_cast<idx_t>(schema.n_children) != types.size()) {
    throw InternalException("Arrow schema does not match the written columns");
  }
  for (idx_t column_index = 0; column_index < types.size(); column_index++) {
    AddTypeMetadata(schema.children[column_index], types[column_index],
                    type_metadata_namespace);
  }
}

bool ArrowStreamWriter::IsTypeMetadataKey(const string& key,
                                          const string& type_metadata_namespace) {
  for (const auto suffix :
       {kTypeKey, kTypeDetailsKey, kTypePrecisionKey, kTypeScaleKey}) {
    if (key == type_metadata_namespace + suffix) {
      return true;
    }
  }
  return false;
}
void ArrowStreamWriter::SetWrittenStatistics(CopyFunctionFileStatistics& statistics) {
  written_stats = statistics;
}

bool ArrowStreamWriter::IsSizeMetadataKey(const string& key) {
  return key == kTotalCompressedSize || key == kTotalUncompressedSize;
}

idx_t ArrowStreamWriter::NumberOfRowGroups() const { return row_group_count; }

idx_t ArrowStreamWriter::FileSize() const { return file_size; }

}  // namespace ext_nanoarrow
}  // namespace duckdb
