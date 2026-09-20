#include "ipc/stream_factory.hpp"

#include <iostream>
#include <utility>

#include "duckdb/common/map.hpp"
#include "ipc/stream_reader/ipc_buffer_stream_reader.hpp"
#include "ipc/stream_reader/ipc_file_stream_reader.hpp"

namespace duckdb {
namespace ext_nanoarrow {
ArrowIPCStreamFactory::ArrowIPCStreamFactory(Allocator& allocator_p)
    : allocator(allocator_p) {}

unique_ptr<ArrowArrayStreamWrapper> ArrowIPCStreamFactory::ProduceStream(
    ArrowStreamParameters& parameters) {
  if (!reader) {
    throw InternalException("IpcStreamReader was not initialized or was already moved");
  }

  const auto column_indexes = ProjectedColumnIndexes(parameters);
  if (!column_indexes.empty()) {
    reader->SetColumnProjection(column_indexes);
  }

  auto out = make_uniq<ArrowArrayStreamWrapper>();
  IpcArrayStream(std::move(reader)).ToArrayStream(&out->arrow_array_stream);
  return out;
}

vector<idx_t> ArrowIPCStreamFactory::ProjectedColumnIndexes(
    const ArrowStreamParameters& parameters) {
  vector<idx_t> column_indexes;
  if (parameters.projected_columns.columns.empty()) {
    return column_indexes;
  }
  // Arrow field names may repeat, so take the bound indexes in output position order
  const auto& filter_to_col = parameters.projected_columns.filter_to_col;
  const map<idx_t, idx_t> ordered(filter_to_col.begin(), filter_to_col.end());
  for (const auto& entry : ordered) {
    column_indexes.push_back(entry.second);
  }
  return column_indexes;
}

void ArrowIPCStreamFactory::GetSchema(ArrowSchema& schema) {
  if (!reader) {
    throw InternalException("IpcStreamReader is no longer valid");
  }

  NANOARROW_THROW_NOT_OK(ArrowSchemaDeepCopy(reader->GetBaseSchema(), &schema));
}

BufferIPCStreamFactory::BufferIPCStreamFactory(ClientContext& context,
                                               const vector<ArrowIPCBuffer>& buffers_p)
    : ArrowIPCStreamFactory(BufferAllocator::Get(context)), buffers(buffers_p) {}

void BufferIPCStreamFactory::InitReader() {
  if (reader) {
    throw InternalException("ArrowArrayStream or IpcStreamReader already initialized");
  }
  reader = make_uniq<IPCBufferStreamReader>(buffers, allocator);
}

FileIPCStreamFactory::FileIPCStreamFactory(ClientContext& context, OpenFileInfo file)
    : ArrowIPCStreamFactory(BufferAllocator::Get(context)),
      fs(FileSystem::GetFileSystem(context)),
      scheduler(TaskScheduler::GetScheduler(context)),
      file(std::move(file)) {}

void FileIPCStreamFactory::InitReader() {
  if (reader) {
    throw InternalException("ArrowArrayStream or IpcStreamReader already initialized");
  }
  reader = OpenReader();
}

unique_ptr<IPCFileStreamReader> FileIPCStreamFactory::OpenReader() const {
  unique_ptr<FileHandle> handle = fs.OpenFile(file, FileOpenFlags::FILE_FLAGS_READ);
  return make_uniq<IPCFileStreamReader>(fs, std::move(handle), allocator, &scheduler);
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
