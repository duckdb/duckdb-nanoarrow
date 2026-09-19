//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// ipc/stream_reader/ipc_file_stream_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "ipc/stream_reader/base_stream_reader.hpp"
#include "ipc/stream_reader/concurrent_reads.hpp"

namespace duckdb {
namespace ext_nanoarrow {

//! A half open byte range inside a record batch body
struct BodyRange {
  idx_t begin;
  idx_t end;
};

//! IPC File
class IPCFileStreamReader final : public IPCStreamReader {
 public:
  //! A scheduler lets the reads of one fetch of a remote file run at the same time
  IPCFileStreamReader(FileSystem& fs, unique_ptr<FileHandle> handle, Allocator& allocator,
                      optional_ptr<TaskScheduler> scheduler = nullptr);

  ArrowIpcMessageType ReadNextMessage() override;

  //! Publishes the offset of each message it reaches, for progress read on another thread
  void TrackProgress(shared_ptr<atomic<idx_t>> offset);

  //! The size of the file being read, for estimating a row count without a footer
  idx_t FileSize() { return file_reader.FileSize(); }
  //! Remote reads pay a round trip each, so they are merged and fetched differently
  bool IsRemote() const { return remote; }

  //! Reads the footer once, false when the file has none and the scan stays sequential
  bool TryReadFooter();
  //! The footer TryReadFooter decoded, with its size and magic
  ArrowBufferView FooterWindow() const;
  //! Takes the schema from the footer another reader of the file read, instead of reading
  void LoadFooter(ArrowBufferView window);
  //! The record batch blocks named by the footer, empty when there is no footer
  const vector<ArrowIpcFileBlock>& RecordBatchBlocks() const {
    return record_batch_blocks;
  }
  //! The dictionary batch blocks named by the footer
  const vector<ArrowIpcFileBlock>& DictionaryBlocks() const { return dictionary_blocks; }
  //! Decodes these dictionary blocks, which the record batch blocks need first
  void LoadDictionaries(const vector<ArrowIpcFileBlock>& blocks);
  //! Reads only the record batches of these footer blocks, then reports the end
  void SetBlocks(const ArrowIpcFileBlock* begin, const ArrowIpcFileBlock* end);
  //! Reads the blocks left to scan into memory from any thread, whole bodies if asked
  void FetchBlocks(bool whole = false);
  //! The most bytes FetchBlocks reads, for budgeting reads scheduled ahead
  idx_t BlockBytes() const;
  //! Whether batch lengths can be read from the headers alone
  bool CanCountWithoutBodies();
  //! Reads only headers from here on, so fetching skips the bodies
  void CountOnly();
  //! Reads the length of the next record batch and skips its body, false at the end
  bool NextBatchLength(idx_t& length);

 private:
  //! Where one claimed block starts in memory once fetched
  struct FetchedBlock {
    shared_ptr<AllocatedData> data;
    data_ptr_t ptr = nullptr;
  };

  BufferedFileReader file_reader;
  AllocatedData message_header;
  shared_ptr<AllocatedData> message_body;
  //! Pipes and character devices must keep the sequential read
  bool positional = false;
  bool remote = false;
  //! Runs the reads of a remote file at the same time, none for a local one
  optional_ptr<TaskScheduler> scheduler;
  //! The claimed blocks in memory, empty until FetchBlocks runs
  vector<FetchedBlock> fetched_blocks;
  idx_t fetched_index = 0;
  shared_ptr<atomic<idx_t>> progress_offset;
  vector<ArrowIpcFileBlock> record_batch_blocks;
  vector<ArrowIpcFileBlock> dictionary_blocks;
  bool footer_read = false;
  //! The footer, its size and the magic, for claim readers to take the schema from
  AllocatedData footer_window;
  //! Whether the file starts with the magic of the file format, which has a footer
  bool file_magic = false;
  //! The claimed blocks still to read, both null outside a block scan
  const ArrowIpcFileBlock* next_block = nullptr;
  const ArrowIpcFileBlock* end_block = nullptr;
  //! Counting reads headers only, so regular files seek past the bodies
  bool skip_bodies = false;
  //! Stands in for bodies a count skips, whose views only need offsets inside it
  AllocatedData unread_body;
  //! The flat index of the field whose view gives the batch length, negative when none
  int64_t count_field = -1;

  void EnsureInputStreamAligned();
  //! Whether the body can be read with one positional read instead of the buffered reader
  bool CanReadBodyPositionally(idx_t body_start, idx_t body_size);
  //! Reads a whole body, small ones through the buffer and large ones positionally
  void ReadBodyPositionally(idx_t body_start, idx_t body_size);
  //! Reads only the buffers the projection needs, returns false to read the whole body
  bool TryReadProjectedBody(idx_t body_start, idx_t body_size);
  //! The merged body ranges the projection needs, false when the whole body is cheaper
  bool ProjectedRanges(const_data_ptr_t base, idx_t body_size, vector<BodyRange>& merged);
  //! Ranges closer than this are read together
  idx_t CoalesceGap() const;
  //! Verifies and decodes a footer window into the decoder
  static bool DecodeFooter(ArrowIpcDecoder& footer_decoder, ArrowBufferView window);
  //! Plans one read per run of neighbouring blocks, or of only their headers
  void PlanRuns(const vector<idx_t>& indexes, bool headers_only,
                vector<FetchedBlock>& fetched, vector<FileRead>& reads);
  //! Plans the body ranges a projection needs once the block header is in memory
  void PlanProjectedBlock(const ArrowIpcFileBlock& block, FetchedBlock& fetched,
                          vector<FileRead>& reads);
  //! Runs planned reads, at the same time when the file is remote
  void ReadAll(const vector<FileRead>& reads);
  //! Points the current body at the scratch space a count reads no bytes into
  void SetUnreadBody(idx_t size);
  //! Decodes the header of a fetched block, false for an end of stream marker
  bool DecodeBlockHeader(const ArrowIpcFileBlock& block, const FetchedBlock& fetched);
  ArrowIpcMessageType DecodeFetchedBlock(const ArrowIpcFileBlock& block,
                                         const FetchedBlock& fetched);

  data_ptr_t ReadData(data_ptr_t ptr, idx_t size) override;
  bool DecodeHeader(idx_t message_header_size) override;
  void DecodeBody() override;
  nanoarrow::UniqueBuffer GetUniqueBuffer() override;
};

}  // namespace ext_nanoarrow
}  // namespace duckdb
