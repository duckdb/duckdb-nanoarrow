//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// ipc/ipc_stream_factory.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "ipc/array_stream.hpp"

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "table_function/scan_arrow_ipc.hpp"

namespace duckdb {
namespace ext_nanoarrow {

class IPCFileStreamReader;

class ArrowStreamFactory {
  ArrowStreamFactory() {};
};
//! Hands DuckDB's Arrow scan the stream of one reader, which the scan data keeps alive
class ArrowIPCStreamFactory : public ArrowScanFactory {
 public:
  explicit ArrowIPCStreamFactory(Allocator& allocator);

  //! Moves the reader into the stream, so a factory produces once
  unique_ptr<ArrowArrayStreamWrapper> ProduceStream(
      ArrowStreamParameters& parameters) override;
  //! The projected top level columns in output order, empty when nothing is projected
  static vector<idx_t> ProjectedColumnIndexes(const ArrowStreamParameters& parameters);

  //! Copies the schema of the reader
  void GetSchema(ArrowSchema& schema) override;

  //! Opens the file, wraps it in the ArrowIpcInputStream, and wraps it in
  //! the ArrowArrayStream reader.
  virtual void InitReader() {
    throw NotImplementedException("ArrowIPCStreamFactory::InitReader not implemented");
  }

  Allocator& allocator;
  unique_ptr<IPCStreamReader> reader;
  ArrowError error{};
};

class BufferIPCStreamFactory final : public ArrowIPCStreamFactory {
 public:
  explicit BufferIPCStreamFactory(ClientContext& context,
                                  const vector<ArrowIPCBuffer>& buffers);
  void InitReader() override;

  vector<ArrowIPCBuffer> buffers;
};

class FileIPCStreamFactory final : public ArrowIPCStreamFactory {
 public:
  //! The file info from the glob keeps its size, so opening it needs no request
  FileIPCStreamFactory(ClientContext& context, OpenFileInfo file);
  void InitReader() override;
  //! Opens another reader with its own handle, for a scan that keeps its own position
  unique_ptr<IPCFileStreamReader> OpenReader() const;

  FileSystem& fs;
  TaskScheduler& scheduler;
  OpenFileInfo file;
};
}  // namespace ext_nanoarrow
}  // namespace duckdb
