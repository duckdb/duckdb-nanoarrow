//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// table_function/arrow_ipc_function_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table/arrow.hpp"
#include "ipc/stream_factory.hpp"

namespace duckdb {
namespace ext_nanoarrow {
//! Our FunctionData is the same as the ArrowScanFunctionData, which keeps the
//! ArrowIPCStreamFactory alive, with typed access to it
struct ArrowIPCFunctionData : public ArrowScanFunctionData {
  explicit ArrowIPCFunctionData(shared_ptr<ArrowIPCStreamFactory> factory)
      : ArrowScanFunctionData(std::move(factory)) {}

  ArrowIPCStreamFactory& IPCFactory() const {
    return factory->Cast<ArrowIPCStreamFactory>();
  }
};
}  // namespace ext_nanoarrow
}  // namespace duckdb
