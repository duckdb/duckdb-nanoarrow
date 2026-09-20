#include "table_function/read_arrow.hpp"

#include <inttypes.h>

#include "file_scanner/arrow_multi_file_info.hpp"
#include "zstd.h"

#include "duckdb/common/radix.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "nanoarrow/nanoarrow.hpp"
#include "nanoarrow/nanoarrow_ipc.hpp"

#include "ipc/stream_factory.hpp"
#include "ipc/stream_reader/base_stream_reader.hpp"
#include "nanoarrow_errors.hpp"

// read_arrow() implementation
//
// This version uses the ArrowIpcDecoder directly. instead of nanoarrow's
// ArrowArrayStream wrapper. This lets it use DuckDB's allocator at the
// expense of a bit more verbosity. Because we can apply the projection
// it reduces some of the verbosity of the actual DuckDB part (although the
// ArrayStreamReader from nanoarrow could support a projection, which
// would handle that too).
//
// I like this version better than the simpler one, and there are more parts
// that could get optimized here (whereas with the array stream version you
// don't have much control).

namespace duckdb {

namespace ext_nanoarrow {

struct ReadArrowStream : ArrowTableFunction {
  static TableFunction Function() {
    MultiFileFunction<ArrowMultiFileInfo> read_arrow("read_arrow");
    read_arrow.projection_pushdown = true;
    read_arrow.filter_pushdown = false;
    read_arrow.filter_prune = false;
    return static_cast<TableFunction>(read_arrow);
  }

  static unique_ptr<TableRef> ScanReplacement(ClientContext& context,
                                              ReplacementScanInput& input,
                                              optional_ptr<ReplacementScanData> data) {
    auto table_name = ReplacementScan::GetFullPath(input);
    if (!ReplacementScan::CanReplace(table_name, {"arrows", "arrow"})) {
      return nullptr;
    }

    auto table_function = make_uniq<TableFunctionRef>();
    vector<unique_ptr<ParsedExpression>> children;
    auto table_name_expr = ConstantExpression::String(table_name);
    children.push_back(std::move(table_name_expr));
    auto function_expr = make_uniq<FunctionExpression>("read_arrow", std::move(children));
    table_function->function = std::move(function_expr);

    if (!FileSystem::HasGlob(table_name)) {
      auto& fs = FileSystem::GetFileSystem(context);
      table_function->alias = Identifier(fs.ExtractBaseName(table_name));
    }

    return std::move(table_function);
  }
};

TableFunction ReadArrowStreamFunction() { return ReadArrowStream::Function(); }

void RegisterReadArrowStream(ExtensionLoader& loader) {
  auto function = ReadArrowStream::Function();
  loader.RegisterFunction(function);
  // So we can accept a list of paths as well e.g., ['file_1.arrow','file_2.arrow']
  function.arguments = {LogicalType::LIST(LogicalType::VARCHAR)};
  loader.RegisterFunction(function);
  auto& config = DBConfig::GetConfig(loader.GetDatabaseInstance());
  config.replacement_scans.emplace_back(ReadArrowStream::ScanReplacement);
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
