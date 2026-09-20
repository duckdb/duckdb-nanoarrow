//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// ipc/stream_reader/concurrent_reads.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/parallel/task_executor.hpp"

namespace duckdb {
namespace ext_nanoarrow {

//! A read of the file into memory
struct FileRead {
  data_ptr_t target;
  idx_t size;
  idx_t location;
};

//! Runs one read in pieces, since a single read of 2 GiB or more fails on macOS
void ReadAt(FileHandle& handle, const FileRead& read);
//! Schedules each read as a task of the executor, which runs on the executor's pool
void ScheduleReads(TaskExecutor& executor, FileHandle& handle,
                   const vector<FileRead>& reads);
//! Runs the reads at the same time on the async pool, so they wait one round trip
void ReadConcurrently(TaskScheduler& scheduler, FileHandle& handle,
                      const vector<FileRead>& reads);

//! Reads a remote file in order while the next bytes download on the async pool
class RemoteReadAhead {
 public:
  RemoteReadAhead(FileHandle& handle, idx_t file_size, Allocator& allocator,
                  TaskScheduler& scheduler);
  //! Cancels the chunks not started yet and waits for the running ones
  ~RemoteReadAhead();

  //! Reads the next bytes, throwing past the end like BufferedFileReader does
  void Read(data_ptr_t target, idx_t size);
  //! Moves without copying, throwing past the end like Read
  void Seek(idx_t location);
  idx_t Offset() const { return offset; }

 private:
  //! A range of the file fetched as several concurrent reads
  struct Batch {
    idx_t begin = 0;
    idx_t end = 0;
    AllocatedData data;
    unique_ptr<TaskExecutor> executor;
  };

  static bool Contains(const Batch& batch, idx_t location);
  //! Starts fetching the bytes that follow begin, growing each batch up to the limit
  void Launch(Batch& batch, idx_t begin);
  //! Waits for the fetches of a batch, running queued ones on this thread
  static void Await(Batch& batch);
  //! Cancels what a batch has not fetched yet and frees it
  static void Discard(Batch& batch);
  //! Makes current hold the offset, moving to the next batch or refetching
  void Advance();

  FileHandle& handle;
  const idx_t file_size;
  Allocator& allocator;
  TaskScheduler& scheduler;
  idx_t offset = 0;
  //! The size of the next batch, which starts small so reading a schema stays cheap
  idx_t next_batch_size;
  idx_t launched_batches = 0;
  Batch current;
  Batch next;
};

}  // namespace ext_nanoarrow
}  // namespace duckdb
