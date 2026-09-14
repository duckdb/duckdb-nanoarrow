#include "writer/to_arrow_ipc.hpp"

#include "ipc/codecs.hpp"
#include "writer/column_data_collection_serializer.hpp"

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

namespace ext_nanoarrow {

struct ToArrowIpcFunctionData : public TableFunctionData {
  ToArrowIpcFunctionData() = default;
  nanoarrow::UniqueSchema schema;
  vector<LogicalType> logical_types;
  const idx_t chunk_size = ToArrowIPCFunction::DEFAULT_CHUNK_SIZE * STANDARD_VECTOR_SIZE;
  ArrowIpcCompressionOptions compression;
};

struct ToArrowIpcGlobalState : public GlobalTableFunctionState {
  ToArrowIpcGlobalState() : sent_schema(false) {}
  atomic<bool> sent_schema;
  mutex lock;
};

struct ToArrowIpcLocalState : public LocalTableFunctionState {
  unique_ptr<ArrowAppender> appender;
  unique_ptr<ColumnDataCollectionSerializer> serializer;
  idx_t current_count = 0;
  bool checked_schema = false;
};

unique_ptr<LocalTableFunctionState> ToArrowIPCFunction::InitLocal(
    ExecutionContext& context, TableFunctionInitInput& input,
    GlobalTableFunctionState* global_state) {
  auto local_state = make_uniq<ToArrowIpcLocalState>();
  auto properties = context.client.GetClientProperties();
  auto& data = input.bind_data->Cast<ToArrowIpcFunctionData>();
  local_state->serializer = make_uniq<ColumnDataCollectionSerializer>(
      properties, BufferAllocator::Get(context.client), data.compression);
  // Init() allocates an encoder and an array view, so it belongs here and not
  // in Function(), which runs for every incoming chunk.
  local_state->serializer->Init(data.schema.get(), data.logical_types);
  return std::move(local_state);
}

unique_ptr<GlobalTableFunctionState> ToArrowIPCFunction::InitGlobal(
    ClientContext& context, TableFunctionInitInput& input) {
  return make_uniq<ToArrowIpcGlobalState>();
}

unique_ptr<FunctionData> ToArrowIPCFunction::Bind(ClientContext& context,
                                                  TableFunctionBindInput& input,
                                                  vector<LogicalType>& return_types,
                                                  vector<string>& names) {
  auto result = make_uniq<ToArrowIpcFunctionData>();

  // The binder only lets the declared parameters through, all of them compression options
  for (auto& kv : input.named_parameters) {
    if (kv.second.IsNull()) {
      throw BinderException("Cannot use NULL as function argument");
    }
    result->compression.TrySetOption(kv.first, kv.second);
  }
  result->compression.Validate();

  // Set return schema
  return_types.emplace_back(LogicalType::BLOB);
  names.emplace_back("ipc");
  return_types.emplace_back(LogicalType::BOOLEAN);
  names.emplace_back("header");

  // Create the Arrow schema
  auto properties = context.GetClientProperties();
  result->logical_types = input.input_table_types;
  ArrowConverter::ToArrowSchema(result->schema.get(), input.input_table_types,
                                input.input_table_names, properties);
  return std::move(result);
}

void SerializeArray(const ToArrowIpcLocalState& local_state,
                    nanoarrow::UniqueBuffer& arrow_serialized_ipc_buffer) {
  ArrowArray finalized = local_state.appender->Finalize();
  nanoarrow::UniqueArray arr(&finalized);
  local_state.serializer->Serialize(*arr.get());
  arrow_serialized_ipc_buffer = local_state.serializer->GetHeader();
  auto body = local_state.serializer->GetBody();
  NANOARROW_THROW_NOT_OK(
      ArrowBufferAppend(arrow_serialized_ipc_buffer.get(), body->data, body->size_bytes));
}

void InsertMessageToChunk(nanoarrow::UniqueBuffer& arrow_serialized_ipc_buffer,
                          DataChunk& output) {
  const auto ptr = reinterpret_cast<const char*>(arrow_serialized_ipc_buffer->data);
  const auto len = arrow_serialized_ipc_buffer->size_bytes;
  const auto wrapped_buffer =
      make_buffer<ArrowStringVectorBuffer>(std::move(arrow_serialized_ipc_buffer));
  auto& vector = output.data[0];
  StringVector::AddBuffer(vector, wrapped_buffer);
  const auto data_ptr = reinterpret_cast<string_t*>(vector.GetData());
  *data_ptr = string_t(ptr, len);
  output.SetCardinality(1);
  output.Verify();
}

OperatorResultType ToArrowIPCFunction::Function(ExecutionContext& context,
                                                TableFunctionInput& data_p,
                                                DataChunk& input, DataChunk& output) {
  nanoarrow::UniqueBuffer arrow_serialized_ipc_buffer;
  auto& data = data_p.bind_data->Cast<ToArrowIpcFunctionData>();
  auto& local_state = data_p.local_state->Cast<ToArrowIpcLocalState>();
  auto& global_state = data_p.global_state->Cast<ToArrowIpcGlobalState>();

  bool sending_schema = false;

  bool caching_disabled =
      PhysicalOperator::SelectOperatorCachingMode(context) == OperatorCachingMode::NONE;

  if (!local_state.checked_schema) {
    if (!global_state.sent_schema) {
      lock_guard<mutex> init_lock(global_state.lock);
      if (!global_state.sent_schema) {
        // This run will send the schema, other threads can just send the
        // buffers
        global_state.sent_schema = true;
        sending_schema = true;
      }
    }
    local_state.checked_schema = true;
  }

  if (sending_schema) {
    local_state.serializer->SerializeSchema(data.schema.get());
    arrow_serialized_ipc_buffer = local_state.serializer->GetHeader();
    output.data[1].SetValue(0, Value::BOOLEAN(true));
  } else {
    if (!local_state.appender) {
      local_state.appender = make_uniq<ArrowAppender>(
          input.GetTypes(), data.chunk_size, context.client.GetClientProperties(),
          ArrowTypeExtensionData::GetExtensionTypes(context.client, input.GetTypes()));
    }

    // Append input chunk
    local_state.appender->Append(input, 0, input.size(), input.size());
    local_state.current_count += input.size();

    // If chunk size is reached, we can flush to IPC blob
    if (caching_disabled || local_state.current_count >= data.chunk_size) {
      SerializeArray(local_state, arrow_serialized_ipc_buffer);
      // Reset appender
      local_state.appender.reset();
      local_state.current_count = 0;

      // This is a data message, hence we set the second column to false
      output.data[1].SetValue(0, Value::BOOLEAN(false));
    } else {
      return OperatorResultType::NEED_MORE_INPUT;
    }
  }
  InsertMessageToChunk(arrow_serialized_ipc_buffer, output);
  if (sending_schema) {
    return OperatorResultType::HAVE_MORE_OUTPUT;
  } else {
    return OperatorResultType::NEED_MORE_INPUT;
  }
}

OperatorFinalizeResultType ToArrowIPCFunction::FunctionFinal(ExecutionContext& context,
                                                             TableFunctionInput& data_p,
                                                             DataChunk& output) {
  auto& local_state = data_p.local_state->Cast<ToArrowIpcLocalState>();

  if (local_state.appender) {
    // Flush remaining buffered data first, then come back to emit EOS
    nanoarrow::UniqueBuffer arrow_serialized_ipc_buffer;
    SerializeArray(local_state, arrow_serialized_ipc_buffer);
    InsertMessageToChunk(arrow_serialized_ipc_buffer, output);
    output.data[1].SetValue(0, Value::BOOLEAN(false));
    local_state.appender.reset();
    return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
  }

  // Emit the Arrow IPC end-of-stream marker (continuation token + 0-length metadata)
  uint8_t end_of_stream[] = {0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00};
  nanoarrow::UniqueBuffer eos_buffer;
  ArrowBufferInit(eos_buffer.get());
  ArrowBufferAppend(eos_buffer.get(), end_of_stream, sizeof(end_of_stream));
  InsertMessageToChunk(eos_buffer, output);
  output.data[1].SetValue(0, Value::BOOLEAN(false));

  return OperatorFinalizeResultType::FINISHED;
}

TableFunction ToArrowIPCFunction::GetFunction() {
  TableFunction fun("to_arrow_ipc", {LogicalType::TABLE}, nullptr, Bind, InitGlobal,
                    InitLocal);
  fun.in_out_function = Function;
  fun.in_out_function_final = FunctionFinal;
  fun.named_parameters["compression"] = LogicalType::VARCHAR;
  fun.named_parameters["compression_level"] = LogicalType::BIGINT;
  return fun;
}

void ToArrowIPCFunction::RegisterToIPCFunction(ExtensionLoader& loader) {
  const auto function = GetFunction();
  loader.RegisterFunction(function);
}
}  // namespace ext_nanoarrow
}  // namespace duckdb
