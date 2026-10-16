
#include "write_arrow_stream.hpp"

#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "file_scanner/arrow_multi_file_info.hpp"

#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "nanoarrow/nanoarrow_ipc.hpp"

#include "nanoarrow_errors.hpp"
#include "table_function/read_arrow.hpp"
#include "utf8proc_wrapper.hpp"
#include "writer/arrow_stream_writer.hpp"

namespace duckdb {

namespace ext_nanoarrow {

namespace {

struct ArrowWriteBindData : public TableFunctionData {
  ClientProperties options;
  vector<LogicalType> sql_types;
  nanoarrow::UniqueSchema schema;
  vector<pair<string, string>> kv_metadata;
  vector<ArrowFieldMetadata> field_metadata;
  ArrowIpcCompressionOptions compression;
  bool file_format = true;
  bool size_metadata = false;
  idx_t row_group_size = 122880;
  string type_metadata_namespace;
};

struct ArrowWriteGlobalState : public GlobalFunctionData {
  unique_ptr<ArrowStreamWriter> writer;
};

// Reads the entries of a STRUCT option value as key/value pairs, blobs as raw bytes
vector<pair<string, string>> ReadMetadataPairs(const Value& kv_struct,
                                               const string& what) {
  auto& kv_struct_type = kv_struct.type();
  if (kv_struct.IsNull() || kv_struct_type.id() != LogicalTypeId::STRUCT) {
    throw BinderException("Expected %s to be a STRUCT", what);
  }
  vector<pair<string, string>> result;
  auto& values = StructValue::GetChildren(kv_struct);
  for (idx_t i = 0; i < values.size(); i++) {
    const auto& value = values[i];
    const auto& key = StructType::GetChildName(kv_struct_type, i).GetIdentifierName();
    if (value.IsNull()) {
      throw BinderException("Metadata value for key \"%s\" must not be NULL", key);
    }
    auto bytes = value.type().id() == LogicalTypeId::BLOB ? StringValue::Get(value)
                                                          : value.ToString();
    if (!Utf8Proc::IsValid(bytes.data(), bytes.size())) {
      throw BinderException("Metadata value for key \"%s\" is not valid UTF-8", key);
    }
    if (bytes.find('\0') != string::npos) {
      throw BinderException("Metadata value for key \"%s\" must not contain NUL bytes",
                            key);
    }
    result.emplace_back(key, std::move(bytes));
  }
  return result;
}

// Reads a STRUCT of column name to STRUCT of key/value pairs
vector<ArrowFieldMetadata> ReadFieldMetadata(const Value& columns,
                                             const vector<Identifier>& names) {
  if (columns.IsNull() || columns.type().id() != LogicalTypeId::STRUCT) {
    throw BinderException("Expected field_metadata argument to be a STRUCT");
  }
  vector<ArrowFieldMetadata> result;
  auto& values = StructValue::GetChildren(columns);
  for (idx_t i = 0; i < values.size(); i++) {
    const auto& column = StructType::GetChildName(columns.type(), i).GetIdentifierName();
    idx_t column_index = 0;
    while (column_index < names.size() && names[column_index] != column) {
      column_index++;
    }
    if (column_index == names.size()) {
      throw BinderException(
          "Column \"%s\" in field_metadata is not among the written columns", column);
    }
    auto what = StringUtil::Format("field_metadata entry for column \"%s\"", column);
    result.push_back({column_index, ReadMetadataPairs(values[i], what)});
  }
  return result;
}

unique_ptr<FunctionData> ArrowWriteBind(ClientContext& context,
                                        CopyFunctionBindInput& input,
                                        const vector<Identifier>& names,
                                        const vector<LogicalType>& sql_types) {
  D_ASSERT(names.size() == sql_types.size());
  auto bind_data = make_uniq<ArrowWriteBindData>();
  bind_data->options = context.GetClientProperties();
  // Arrow recommends .arrow for the file format and .arrows for the stream
  bind_data->file_format = !StringUtil::CIEquals(input.info.format, "arrows");
  vector<idx_t> dictionary_columns;

  for (auto& option : input.info.options) {
    const auto loption = StringUtil::Lower(option.first.GetIdentifierName());
    if (loption == "size_metadata") {
      if (option.second.size() > 1) {
        throw BinderException("SIZE_METADATA accepts at most one argument");
      }
      bind_data->size_metadata =
          option.second.empty() ||
          BooleanValue::Get(option.second[0].DefaultCastAs(LogicalType::BOOLEAN));
      continue;
    }
    if (loption == "dictionary_encode") {
      dictionary_columns = ReadDictionaryColumns(option.second, names);
      continue;
    }
    if (option.second.size() != 1) {
      throw BinderException("%s requires exactly one argument",
                            StringUtil::Upper(loption));
    }

    if (bind_data->compression.TrySetOption(loption, option.second[0])) {
      continue;
    }
    if (loption == "chunk_size") {
      bind_data->row_group_size = option.second[0].GetValue<uint64_t>();
    } else if (loption == "kv_metadata") {
      bind_data->kv_metadata =
          ReadMetadataPairs(option.second[0], "kv_metadata argument");
    } else if (loption == "field_metadata") {
      bind_data->field_metadata = ReadFieldMetadata(option.second[0], names);
    } else if (loption == "type_metadata_namespace") {
      if (option.second[0].IsNull()) {
        throw BinderException("TYPE_METADATA_NAMESPACE must not be NULL");
      }
      bind_data->type_metadata_namespace = option.second[0].ToString();
      if (bind_data->type_metadata_namespace.empty()) {
        throw BinderException("TYPE_METADATA_NAMESPACE must not be empty");
      }
      // The Arrow format reserves the ARROW namespace of custom_metadata keys
      const auto& type_metadata_namespace = bind_data->type_metadata_namespace;
      if (type_metadata_namespace == "ARROW" ||
          StringUtil::StartsWith(type_metadata_namespace, "ARROW:")) {
        throw BinderException(
            "TYPE_METADATA_NAMESPACE \"%s\" is in the ARROW namespace, which the Arrow "
            "format reserves",
            type_metadata_namespace);
      }
    }
  }
  bind_data->compression.Validate();
  bind_data->schema = CreateArrowIpcSchema(sql_types, names, bind_data->options,
                                           dictionary_columns, !bind_data->file_format);
  if (!bind_data->type_metadata_namespace.empty()) {
    const auto& type_metadata_namespace = bind_data->type_metadata_namespace;
    for (const auto& field : bind_data->field_metadata) {
      for (const auto& item : field.metadata) {
        if (ArrowStreamWriter::IsTypeMetadataKey(item.first, type_metadata_namespace)) {
          throw BinderException(
              "FIELD_METADATA key \"%s\" is reserved by TYPE_METADATA_NAMESPACE",
              item.first);
        }
      }
    }
    // DuckDB owns the metadata of the schema it exports, so copy into a nanoarrow owned
    // schema before adding to it. Every output file copies this schema.
    nanoarrow::UniqueSchema schema;
    NANOARROW_THROW_NOT_OK(ArrowSchemaDeepCopy(bind_data->schema.get(), schema.get()));
    ArrowStreamWriter::SetTypeMetadata(*schema.get(), sql_types, type_metadata_namespace);
    bind_data->schema = std::move(schema);
  }

  if (bind_data->size_metadata) {
    if (!bind_data->file_format) {
      throw BinderException(
          "SIZE_METADATA requires the Arrow IPC file format, use FORMAT ARROW or the "
          ".arrow extension");
    }
    if (FileSystem::IsRemoteFile(input.info.file_path)) {
      throw BinderException(
          "SIZE_METADATA requires a seekable local output to update the schema");
    }
    for (const auto& item : bind_data->kv_metadata) {
      if (ArrowStreamWriter::IsSizeMetadataKey(item.first)) {
        throw BinderException("KV_METADATA key \"%s\" is written by SIZE_METADATA",
                              item.first);
      }
    }
  }

  bind_data->sql_types = sql_types;

  return std::move(bind_data);
}

unique_ptr<GlobalFunctionData> ArrowWriteInitializeGlobal(ClientContext& context,
                                                          FunctionData& bind_data,
                                                          const string& file_path) {
  auto global_state = make_uniq<ArrowWriteGlobalState>();
  auto& arrow_bind = bind_data.Cast<ArrowWriteBindData>();

  auto& fs = FileSystem::GetFileSystem(context);
  global_state->writer = make_uniq<ArrowStreamWriter>(
      arrow_bind.options, fs, file_path, arrow_bind.sql_types, *arrow_bind.schema.get(),
      arrow_bind.kv_metadata, arrow_bind.field_metadata, arrow_bind.compression,
      arrow_bind.file_format, arrow_bind.size_metadata);
  global_state->writer->WriteSchema();
  return std::move(global_state);
}

void ArrowWriteFinalize(ClientContext& context, FunctionData& bind_data,
                        GlobalFunctionData& gstate) {
  auto& global_state = gstate.Cast<ArrowWriteGlobalState>();
  global_state.writer->Finalize();
}

void ArrowWriteGetWrittenStatistics(ClientContext& context, FunctionData& bind_data,
                                    GlobalFunctionData& gstate,
                                    CopyFunctionFileStatistics& statistics) {
  auto& global_state = gstate.Cast<ArrowWriteGlobalState>();
  global_state.writer->SetWrittenStatistics(statistics);
}

// Batch copies still ask for a local state, which prepare_batch and flush_batch never use
unique_ptr<LocalFunctionData> ArrowWriteInitializeLocal(ExecutionContext& context,
                                                        FunctionData& bind_data_p) {
  return make_uniq<LocalFunctionData>();
}

CopyFunctionExecutionMode ArrowWriteExecutionMode(bool preserve_insertion_order,
                                                  bool supports_batch_index) {
  if (!preserve_insertion_order) {
    return CopyFunctionExecutionMode::PARALLEL_COPY_TO_FILE;
  }
  if (supports_batch_index) {
    return CopyFunctionExecutionMode::BATCH_COPY_TO_FILE;
  }
  return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
}

optional_idx ArrowWriteDesiredBatchSize(ClientContext& context,
                                        FunctionData& bind_data_p) {
  auto& bind_data = bind_data_p.Cast<ArrowWriteBindData>();
  return optional_idx(bind_data.row_group_size);
}

idx_t ArrowWriteFileSizeBytes(GlobalFunctionData& gstate) {
  auto& global_state = gstate.Cast<ArrowWriteGlobalState>();
  // A file with no row groups would rotate forever
  if (global_state.writer->NumberOfRowGroups() == 0) {
    return 0;
  }
  return global_state.writer->FileSize();
}

struct ArrowWriteBatchData : public PreparedBatchData {
  unique_ptr<ColumnDataCollectionSerializer> serializer;
};

// Batch preparation runs concurrently and only reads the shared schema.
unique_ptr<PreparedBatchData> ArrowWritePrepareBatch(
    ClientContext& context, FunctionData& bind_data, GlobalFunctionData& gstate,
    unique_ptr<ColumnDataCollection> collection) {
  auto& global_state = gstate.Cast<ArrowWriteGlobalState>();

  auto batch = make_uniq<ArrowWriteBatchData>();
  batch->serializer = global_state.writer->NewSerializer();
  batch->serializer->Serialize(*collection);
  collection->Reset();

  return std::move(batch);
}

void ArrowWriteFlushBatch(ClientContext& context, FunctionData& bind_data,
                          GlobalFunctionData& gstate, PreparedBatchData& batch_p) {
  auto& global_state = gstate.Cast<ArrowWriteGlobalState>();
  auto& batch = batch_p.Cast<ArrowWriteBatchData>();
  global_state.writer->Flush(*batch.serializer);
}

}  // namespace

void RegisterArrowStreamCopyFunction(ExtensionLoader& loader) {
  CopyFunction function("arrows");
  function.copy_to_bind = ArrowWriteBind;
  function.copy_to_initialize_global = ArrowWriteInitializeGlobal;
  function.copy_to_initialize_local = ArrowWriteInitializeLocal;
  function.copy_to_finalize = ArrowWriteFinalize;
  function.copy_to_get_written_statistics = ArrowWriteGetWrittenStatistics;
  function.execution_mode = ArrowWriteExecutionMode;
  function.copy_from_bind = MultiFileFunction<ArrowMultiFileInfo>::MultiFileBindCopy;
  function.copy_from_function = ReadArrowStreamFunction();
  function.prepare_batch = ArrowWritePrepareBatch;
  function.flush_batch = ArrowWriteFlushBatch;
  function.desired_batch_size = ArrowWriteDesiredBatchSize;
  function.file_size_bytes = ArrowWriteFileSizeBytes;

  function.extension = "arrows";
  loader.RegisterFunction(function);

  function.name = "arrow";
  function.extension = "arrow";
  loader.RegisterFunction(function);
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
