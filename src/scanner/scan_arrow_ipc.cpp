
#include "table_function/scan_arrow_ipc.hpp"
#include "ipc/stream_factory.hpp"

#include "duckdb/function/table/arrow.hpp"

#include "ipc/stream_reader/base_stream_reader.hpp"

#include "duckdb/function/function.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

namespace ext_nanoarrow {

struct ScanArrowIPCFunction : ArrowTableFunction {
  static unique_ptr<FunctionData> ScanArrowIPCBind(ClientContext& context,
                                                   TableFunctionBindInput& input,
                                                   vector<LogicalType>& return_types,
                                                   vector<Identifier>& names) {
    // Create a vector with all the buffers and their sizes
    vector<ArrowIPCBuffer> buffers;
    const auto buffer_ptr_list = ListValue::GetChildren(input.inputs[0]);
    for (auto& buffer_ptr_struct : buffer_ptr_list) {
      auto unpacked = StructValue::GetChildren(buffer_ptr_struct);
      buffers.emplace_back(unpacked[0].GetPointer(), unpacked[1].GetValue<uint64_t>());
    }

    auto stream_factory = make_shared_ptr<BufferIPCStreamFactory>(context, buffers);
    stream_factory->InitReader();
    auto res = make_uniq<ArrowScanFunctionData>(stream_factory);
    stream_factory->GetSchema(res->schema_root.arrow_schema);

    PopulateArrowTableSchema(context, res->arrow_table, res->schema_root.arrow_schema);
    names = StringsToIdentifiers(res->arrow_table.GetNames());
    return_types = res->arrow_table.GetTypes();
    res->all_types = return_types;
    if (return_types.empty()) {
      throw InvalidInputException(
          "Provided table/dataframe must have at least one column");
    }

    return std::move(res);
  }

  static TableFunction Function() {
    child_list_t<LogicalType> make_buffer_struct_children{{"ptr", LogicalType::POINTER},
                                                          {"size", LogicalType::UBIGINT}};
    TableFunction scan_arrow_ipc_func(
        "scan_arrow_ipc",
        {LogicalType::LIST(LogicalType::STRUCT(make_buffer_struct_children))},
        ArrowScanFunction, ScanArrowIPCBind, ArrowScanInitGlobal, ArrowScanInitLocal);

    scan_arrow_ipc_func.cardinality = ArrowScanCardinality;
    scan_arrow_ipc_func.projection_pushdown = true;
    scan_arrow_ipc_func.filter_pushdown = false;
    scan_arrow_ipc_func.filter_prune = false;

    return scan_arrow_ipc_func;
  }
};

void ScanArrowIPC::RegisterReadArrowStream(ExtensionLoader& loader) {
  auto function = ScanArrowIPCFunction::Function();
  loader.RegisterFunction(function);
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
