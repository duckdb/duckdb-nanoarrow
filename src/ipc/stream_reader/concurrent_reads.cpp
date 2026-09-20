#include "ipc/stream_reader/concurrent_reads.hpp"

#include <cstring>

#include "duckdb/common/exception.hpp"

namespace duckdb {
namespace ext_nanoarrow {

namespace {

//! The first batch holds a schema and the start of the first body
constexpr idx_t kFirstBatchBytes = 1024 * 1024;
//! Two batches of this size bound the memory one remote stream holds
constexpr idx_t kMaxBatchBytes = 128 * 1024 * 1024;
//! Each concurrent read of a batch, large enough that transfer outweighs the round trip
constexpr idx_t kChunkBytes = 4 * 1024 * 1024;
//! The most bytes of one read, which pread rejects from 2 GiB on macOS
constexpr idx_t kMaxReadBytes = 1024 * 1024 * 1024;

//! Runs one read on the executor's pool
class FileReadTask : public BaseExecutorTask {
 public:
  FileReadTask(TaskExecutor& executor, FileHandle& handle, const FileRead& read)
      : BaseExecutorTask(executor), handle(handle), read(read) {}

  void ExecuteTask() override { ReadAt(handle, read); }

 private:
  FileHandle& handle;
  FileRead read;
};

}  // namespace

void ReadAt(FileHandle& handle, const FileRead& read) {
  for (idx_t done = 0; done < read.size; done += kMaxReadBytes) {
    const auto size = MinValue<idx_t>(kMaxReadBytes, read.size - done);
    handle.Read(read.target + done, size, read.location + done);
  }
}

void ScheduleReads(TaskExecutor& executor, FileHandle& handle,
                   const vector<FileRead>& reads) {
  for (const auto& read : reads) {
    executor.ScheduleTask(make_uniq<FileReadTask>(executor, handle, read));
  }
}

void ReadConcurrently(TaskScheduler& scheduler, FileHandle& handle,
                      const vector<FileRead>& reads) {
  TaskExecutor executor(scheduler, TaskSchedulerType::ASYNC);
  ScheduleReads(executor, handle, reads);
  executor.WorkOnTasks();
}

RemoteReadAhead::RemoteReadAhead(FileHandle& handle, idx_t file_size,
                                 Allocator& allocator, TaskScheduler& scheduler)
    : handle(handle),
      file_size(file_size),
      allocator(allocator),
      scheduler(scheduler),
      next_batch_size(kFirstBatchBytes) {}

RemoteReadAhead::~RemoteReadAhead() {
  // Queued chunks of a stream nobody reads further are cancelled, not downloaded
  Discard(current);
  Discard(next);
}

bool RemoteReadAhead::Contains(const Batch& batch, idx_t location) {
  return location >= batch.begin && location < batch.end;
}

void RemoteReadAhead::Launch(Batch& batch, idx_t begin) {
  batch.begin = begin;
  batch.end = begin + MinValue<idx_t>(next_batch_size, file_size - begin);
  next_batch_size = MinValue<idx_t>(next_batch_size * 2, kMaxBatchBytes);
  batch.data = allocator.Allocate(batch.end - batch.begin);
  vector<FileRead> chunks;
  for (idx_t chunk = batch.begin; chunk < batch.end; chunk += kChunkBytes) {
    const auto size = MinValue<idx_t>(kChunkBytes, batch.end - chunk);
    chunks.push_back(FileRead{batch.data.get() + (chunk - batch.begin), size, chunk});
  }
  batch.executor = make_uniq<TaskExecutor>(scheduler, TaskSchedulerType::ASYNC);
  ScheduleReads(*batch.executor, handle, chunks);
  launched_batches++;
}

void RemoteReadAhead::Await(Batch& batch) {
  if (batch.executor) {
    auto executor = std::move(batch.executor);
    executor->WorkOnTasks();
  }
}

void RemoteReadAhead::Discard(Batch& batch) {
  if (batch.executor) {
    auto executor = std::move(batch.executor);
    executor->CancelAndDrain();
  }
  batch = Batch();
}

void RemoteReadAhead::Advance() {
  if (Contains(next, offset)) {
    Await(next);
    std::swap(current, next);
    Discard(next);
  } else {
    // A seek past both batches restarts the fetch where it landed
    Discard(current);
    Discard(next);
    Launch(current, offset);
    Await(current);
  }
  // A reader wanting only the schema stops in the first batch, so it fetches no more
  if (current.end < file_size && launched_batches > 1) {
    Launch(next, current.end);
  }
}

void RemoteReadAhead::Read(data_ptr_t target, idx_t size) {
  while (size > 0) {
    if (offset >= file_size) {
      throw SerializationException("not enough data in file to deserialize result");
    }
    if (!Contains(current, offset)) {
      Advance();
    }
    const auto available = MinValue<idx_t>(current.end - offset, size);
    std::memcpy(target, current.data.get() + (offset - current.begin), available);
    target += available;
    offset += available;
    size -= available;
  }
}

void RemoteReadAhead::Seek(idx_t location) {
  // A body skipped past the end means the stream was cut short, as a read would report
  if (location > file_size) {
    throw SerializationException("not enough data in file to deserialize result");
  }
  offset = location;
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
