#include "writer/to_arrow_ipc.hpp"

#include "ipc/codecs.hpp"
#include "writer/column_data_collection_serializer.hpp"

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

namespace ext_nanoarrow {

struct ToArrowIpcFunctionData : public TableFunctionData {
  ToArrowIpcFunctionData() = default;
  ClientProperties options;
  ArrowIpcCompressionOptions compression;
  nanoarrow::UniqueSchema schema;
  vector<LogicalType> logical_types;
  const idx_t chunk_size = ToArrowIPCFunction::DEFAULT_CHUNK_SIZE * STANDARD_VECTOR_SIZE;
};

struct ToArrowIpcGlobalState : public GlobalTableFunctionState {
  ToArrowIpcGlobalState() : sent_schema(false) {}
  atomic<bool> sent_schema;
  mutex lock;
};

struct ToArrowIpcLocalState : public LocalTableFunctionState {
  unique_ptr<ArrowBatchBuilder> builder;
  unique_ptr<ColumnDataCollectionSerializer> serializer;
  idx_t current_count = 0;
  bool checked_schema = false;
};

unique_ptr<LocalTableFunctionState> ToArrowIPCFunction::InitLocal(
    ExecutionContext& context, TableFunctionInitInput& input,
    GlobalTableFunctionState* global_state) {
  auto local_state = make_uniq<ToArrowIpcLocalState>();
  auto& data = input.bind_data->Cast<ToArrowIpcFunctionData>();
  local_state->serializer = make_uniq<ColumnDataCollectionSerializer>(
      data.options, BufferAllocator::Get(context.client), data.compression);
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
                                                  vector<Identifier>& names) {
  auto result = make_uniq<ToArrowIpcFunctionData>();
  vector<idx_t> dictionary_columns;
  for (auto& kv : input.named_parameters) {
    if (kv.second.IsNull()) {
      throw BinderException("Cannot use NULL as function argument");
    }
    if (StringUtil::CIEquals(kv.first.GetIdentifierName(), "dictionary_encode")) {
      dictionary_columns = ReadDictionaryColumns({kv.second}, input.input_table_names);
      continue;
    }
    result->compression.TrySetOption(kv.first.GetIdentifierName(), kv.second);
  }
  result->compression.Validate();

  return_types.emplace_back(LogicalType::BLOB);
  names.emplace_back("ipc");
  return_types.emplace_back(LogicalType::BOOLEAN);
  names.emplace_back("header");

  result->options = context.GetClientProperties();
  result->logical_types = input.input_table_types;
  // Each row holds whole messages: a record batch comes with its dictionaries
  result->schema = CreateArrowIpcSchema(input.input_table_types, input.input_table_names,
                                        result->options, dictionary_columns, true);
  return std::move(result);
}

void SerializeArray(const ToArrowIpcLocalState& local_state,
                    nanoarrow::UniqueBuffer& arrow_serialized_ipc_buffer) {
  local_state.serializer->Serialize(*local_state.builder);
  arrow_serialized_ipc_buffer = local_state.serializer->TakeMessages();
}

void InsertMessageToChunk(nanoarrow::UniqueBuffer& arrow_serialized_ipc_buffer,
                          DataChunk& output) {
  const auto ptr = reinterpret_cast<const char*>(arrow_serialized_ipc_buffer->data);
  const auto len = arrow_serialized_ipc_buffer->size_bytes;
  auto& vector = output.data[0];
  auto writer = FlatVector::Writer<string_t>(vector, 1);
  StringVector::AddAuxiliaryData(
      vector, make_uniq<ArrowStringVectorBuffer>(std::move(arrow_serialized_ipc_buffer)));
  writer.WriteStringRef(string_t(ptr, len));
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
        global_state.sent_schema = true;
        sending_schema = true;
      }
    }
    local_state.checked_schema = true;
  }

  if (sending_schema) {
    local_state.serializer->SerializeSchema(data.schema.get());
    arrow_serialized_ipc_buffer = local_state.serializer->GetHeader();
    output.data[1].Append(Value::BOOLEAN(true));
  } else {
    if (!local_state.builder) {
      local_state.builder = local_state.serializer->NewBatchBuilder(data.chunk_size);
    }

    local_state.builder->Append(input);
    local_state.current_count += input.size();

    if (caching_disabled || local_state.current_count >= data.chunk_size) {
      SerializeArray(local_state, arrow_serialized_ipc_buffer);
      local_state.builder.reset();
      local_state.current_count = 0;

      output.data[1].Append(Value::BOOLEAN(false));
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

  if (local_state.builder) {
    nanoarrow::UniqueBuffer arrow_serialized_ipc_buffer;
    SerializeArray(local_state, arrow_serialized_ipc_buffer);
    InsertMessageToChunk(arrow_serialized_ipc_buffer, output);

    output.data[1].Append(Value::BOOLEAN(false));
  }

  return OperatorFinalizeResultType::FINISHED;
}

TableFunction ToArrowIPCFunction::GetFunction() {
  TableFunction fun("to_arrow_ipc", {LogicalType::TABLE}, nullptr, Bind, InitGlobal,
                    InitLocal);
  fun.in_out_function = Function;
  fun.in_out_function_final = FunctionFinal;
  fun.GetSignature().WithTypedKwargs("options", [](TypedKwargs& options) {
    options.Add("compression", LogicalType::VARCHAR)
        .Add("compression_level", LogicalType::BIGINT)
        .Add("dictionary_encode", LogicalType::LIST(LogicalType::VARCHAR));
  });
  return fun;
}

void ToArrowIPCFunction::RegisterToIPCFunction(ExtensionLoader& loader) {
  const auto function = GetFunction();
  loader.RegisterFunction(function);
}
}  // namespace ext_nanoarrow
}  // namespace duckdb
