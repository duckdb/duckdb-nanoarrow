#include "ipc/stream_reader/ipc_file_stream_reader.hpp"

#include <algorithm>

#include "duckdb/common/file_system.hpp"
#include "ipc/file_format.hpp"

namespace duckdb {
namespace ext_nanoarrow {

namespace {

//! Ranges nearer than this are read together, sized for one seek on a local disk
constexpr idx_t kCoalesceGapBytes = 64 * 1024;
//! Small bodies are read whole, several reads cost more than the bytes they save
constexpr idx_t kMinBodyBytesForRanges = 1024 * 1024;
//! A remote header read is a round trip, which costs more than this many unneeded bytes
constexpr idx_t kRemoteMinBodyBytesForRanges = 8 * 1024 * 1024;
//! Reading most of the body is the whole body read with extra system calls
constexpr double kMaxProjectedFraction = 0.8;
//! Enough for the footer of most files, so reading it costs one request
constexpr idx_t kFooterReadBytes = 64 * 1024;
//! A remote request costs a round trip, which is worth about this many bytes of transfer
constexpr idx_t kRemoteCoalesceGapBytes = 1024 * 1024;
//! One remote connection moves about 90 MB a second, so a larger read is cut into pieces
constexpr idx_t kRemoteMaxReadBytes = 32 * 1024 * 1024;

//! Makes the discovery decode take the uncompressed path, which only does arithmetic
struct CodecOverride {
  explicit CodecOverride(ArrowIpcDecoder& decoder)
      : decoder(decoder), saved(decoder.codec) {
    decoder.codec = NANOARROW_IPC_COMPRESSION_TYPE_NONE;
  }
  ~CodecOverride() { decoder.codec = saved; }
  ArrowIpcDecoder& decoder;
  ArrowIpcCompressionType saved;
};

//! Collects the body ranges of one decoded field, children only and never dictionaries
bool CollectFieldRanges(const ArrowArrayView& view, const_data_ptr_t base,
                        idx_t body_size, vector<BodyRange>& ranges) {
  for (int i = 0; i < NANOARROW_MAX_FIXED_BUFFERS; i++) {
    if (view.layout.buffer_type[i] == NANOARROW_BUFFER_TYPE_NONE) {
      break;
    }
    const auto& buffer = view.buffer_views[i];
    // An empty buffer carries no pointer to take an offset from
    if (buffer.size_bytes <= 0 || buffer.data.data == nullptr) {
      continue;
    }
    if (buffer.data.as_uint8 < base) {
      return false;
    }
    const auto begin = static_cast<idx_t>(buffer.data.as_uint8 - base);
    const auto size = static_cast<idx_t>(buffer.size_bytes);
    if (begin > body_size || size > body_size - begin) {
      return false;
    }
    ranges.push_back(BodyRange{begin, begin + size});
  }
  // A dictionary is decoded from its own batch body, so its buffers are not in ours
  for (int64_t i = 0; i < view.n_children; i++) {
    if (!CollectFieldRanges(*view.children[i], base, body_size, ranges)) {
      return false;
    }
  }
  return true;
}

IOException UnexpectedToken(uint32_t token) {
  return IOException("Expected continuation token (0xFFFFFFFF) but got " +
                     std::to_string(token));
}

//! Whether bytes end with the magic that closes every file, which a cut file lacks
bool EndsWithMagic(const_data_ptr_t end) {
  return std::memcmp(end - kArrowIPCFileMagicSize, kArrowIPCFileMagic,
                     kArrowIPCFileMagicSize) == 0;
}

//! Whether nanoarrow can size the buffers of a view without its int64 arithmetic wrapping
bool LengthFits(const ArrowArrayView& view) {
  // Validation multiplies the length by each buffer width and by the fixed list size
  constexpr auto kMax = NumericLimits<int64_t>::Maximum() - 8;
  for (int i = 0; i < NANOARROW_MAX_FIXED_BUFFERS; i++) {
    const auto bits = view.layout.element_size_bits[i];
    if (bits > 0 && view.length > kMax / bits) {
      return false;
    }
  }
  const auto children = view.layout.child_size_elements;
  if (children > 0 && view.length > kMax / children) {
    return false;
  }
  for (int64_t i = 0; i < view.n_children; i++) {
    if (!LengthFits(*view.children[i])) {
      return false;
    }
  }
  return true;
}

//! Whether a field or any of its children is dictionary encoded
bool HasDictionary(const ArrowSchema& schema) {
  if (schema.dictionary) {
    return true;
  }
  for (int64_t i = 0; i < schema.n_children; i++) {
    if (HasDictionary(*schema.children[i])) {
      return true;
    }
  }
  return false;
}

//! Copies blocks out of the decoder, which frees its own copy on the next message
bool CopyFooterBlocks(const ArrowBuffer& source, idx_t file_size,
                      vector<ArrowIpcFileBlock>& blocks) {
  const auto count = static_cast<idx_t>(source.size_bytes) / sizeof(ArrowIpcFileBlock);
  blocks.resize(count);
  if (count > 0) {
    std::memcpy(blocks.data(), source.data, count * sizeof(ArrowIpcFileBlock));
  }
  // A block naming bytes outside the file would send a positional read anywhere
  for (const auto& block : blocks) {
    // Messages start aligned after the leading magic, and a scan seeks straight to them
    if (block.offset < static_cast<int64_t>(kArrowIPCFileHeaderSize) ||
        block.offset % 8 != 0 || block.body_length < 0 ||
        block.metadata_length < static_cast<int32_t>(sizeof(ArrowIpcMessagePrefix))) {
      return false;
    }
    // Each length is bounded by what is left, so their sum cannot wrap around
    const auto offset = static_cast<idx_t>(block.offset);
    const auto metadata_length = static_cast<idx_t>(block.metadata_length);
    if (offset > file_size || metadata_length > file_size - offset ||
        static_cast<idx_t>(block.body_length) > file_size - offset - metadata_length) {
      return false;
    }
  }
  return true;
}

}  // namespace
IPCFileStreamReader::IPCFileStreamReader(FileSystem& fs, unique_ptr<FileHandle> handle,
                                         Allocator& allocator,
                                         optional_ptr<TaskScheduler> scheduler)
    : IPCStreamReader(allocator), file_reader(fs, std::move(handle)) {
  // Regular and remote files read at an offset, and asking a remote one its type opens it
  remote = FileSystem::IsRemoteFile(file_reader.handle->GetPath());
  positional = remote || file_reader.handle->GetType() == FileType::FILE_TYPE_REGULAR;
  if (remote) {
    this->scheduler = scheduler;
  }
}

bool IPCFileStreamReader::TryReadFooter() {
  if (footer_read) {
    return !record_batch_blocks.empty();
  }
  footer_read = true;
  if (!positional) {
    return false;
  }
  const auto file_size = file_reader.FileSize();
  if (file_size < kArrowIPCFileHeaderSize + kArrowIPCFileFooterTailSize) {
    file_end_read = true;
    missing_footer = true;
    return false;
  }
  // A remote file pays a round trip per read, so its name says whether it is a stream
  if (remote) {
    if (StringUtil::EndsWith(file_reader.handle->GetPath(), ".arrows")) {
      return false;
    }
  } else {
    char magic[kArrowIPCFileMagicSize];
    file_reader.handle->Read(magic, kArrowIPCFileMagicSize, 0);
    if (std::memcmp(magic, kArrowIPCFileMagic, kArrowIPCFileMagicSize) != 0) {
      return false;
    }
  }
  if (base_schema->release) {
    throw InternalException("TryReadFooter must run before the schema is read");
  }

  // One read usually holds the whole footer, which saves a round trip on remote files
  const auto end_size = MinValue<idx_t>(file_size, kFooterReadBytes);
  // A mismatch is reported by formatting the end of the buffer, so keep a NUL past it
  auto end = allocator.Allocate(end_size + 1);
  end.get()[end_size] = 0;
  file_reader.handle->Read(end.get(), end_size, file_size - end_size);
  file_end_read = true;
  // The footer size may be corrupt, but a whole file always ends with the magic
  if (!EndsWithMagic(end.get() + end_size)) {
    missing_footer = true;
    return false;
  }

  auto scratch = NewDuckDBArrowDecoder();
  ArrowError footer_error{};
  if (ArrowIpcDecoderPeekFooter(
          scratch.get(),
          AllocatedDataView(end.get() + end_size - kArrowIPCFileFooterTailSize,
                            kArrowIPCFileFooterTailSize),
          &footer_error) != NANOARROW_OK) {
    return false;
  }
  // Verification adds the tail to this size in int32, so the sum must fit there too
  const auto footer_size = static_cast<int64_t>(scratch->header_size_bytes);
  if (footer_size <= 0 ||
      static_cast<idx_t>(footer_size) > file_size - kArrowIPCFileFooterTailSize ||
      footer_size > NumericLimits<int32_t>::Maximum() -
                        static_cast<int64_t>(kArrowIPCFileFooterTailSize)) {
    return false;
  }

  // Verification rejects a window that is not the footer plus the tail
  const auto window_size = static_cast<idx_t>(footer_size) + kArrowIPCFileFooterTailSize;
  // The verifier checks alignment, so the window gets its own allocation
  auto window = allocator.Allocate(window_size + 1);
  window.get()[window_size] = 0;
  if (window_size <= end_size) {
    std::memcpy(window.get(), end.get() + end_size - window_size, window_size);
  } else {
    file_reader.handle->Read(window.get(), window_size, file_size - window_size);
  }
  const auto window_view =
      AllocatedDataView(window.get(), static_cast<int64_t>(window_size));
  if (!DecodeFooter(*scratch.get(), window_view)) {
    return false;
  }

  const auto& footer = *scratch->footer;
  if (!CopyFooterBlocks(footer.record_batch_blocks, file_size, record_batch_blocks) ||
      !CopyFooterBlocks(footer.dictionary_blocks, file_size, dictionary_blocks) ||
      record_batch_blocks.empty()) {
    record_batch_blocks.clear();
    dictionary_blocks.clear();
    return false;
  }
  footer_window = std::move(window);
  // The footer is the schema of record of a file, as other readers of the format take it
  LoadFooter(FooterWindow());
  return true;
}

bool IPCFileStreamReader::DecodeFooter(ArrowIpcDecoder& footer_decoder,
                                       ArrowBufferView window) {
  ArrowError footer_error{};
  return ArrowIpcDecoderPeekFooter(
             &footer_decoder,
             AllocatedDataView(
                 window.data.as_uint8 + window.size_bytes - kArrowIPCFileFooterTailSize,
                 kArrowIPCFileFooterTailSize),
             &footer_error) == NANOARROW_OK &&
         ArrowIpcDecoderVerifyFooter(&footer_decoder, window, &footer_error) ==
             NANOARROW_OK &&
         ArrowIpcDecoderDecodeFooter(&footer_decoder, window, &footer_error) ==
             NANOARROW_OK;
}

ArrowBufferView IPCFileStreamReader::FooterWindow() const {
  // The allocation keeps a NUL past the window, which is not part of it
  return AllocatedDataView(footer_window.get(),
                           static_cast<int64_t>(footer_window.GetSize()) - 1);
}

void IPCFileStreamReader::LoadFooter(ArrowBufferView window) {
  if (!DecodeFooter(*decoder.get(), window)) {
    throw InternalException("LoadFooter needs a footer that TryReadFooter decoded");
  }
  SchemaFromFooter();
}

void IPCFileStreamReader::ReadFileEnd() {
  file_end_read = true;
  const auto file_size = file_reader.FileSize();
  if (file_size < kArrowIPCFileHeaderSize + kArrowIPCFileFooterTailSize) {
    missing_footer = true;
    return;
  }
  char magic[kArrowIPCFileMagicSize];
  file_reader.handle->Read(magic, kArrowIPCFileMagicSize,
                           file_size - kArrowIPCFileMagicSize);
  missing_footer =
      !EndsWithMagic(reinterpret_cast<const_data_ptr_t>(magic) + kArrowIPCFileMagicSize);
}

void IPCFileStreamReader::TrackProgress(shared_ptr<atomic<idx_t>> offset) {
  progress_offset = std::move(offset);
}

nanoarrow::UniqueBuffer IPCFileStreamReader::GetUniqueBuffer() {
  // A fetched block shares its allocation with neighbours, so wrap where the body starts
  nanoarrow::UniqueBuffer out;
  if (message_body) {
    nanoarrow::BufferInitWrapped(out.get(), message_body, cur_ptr, cur_size);
  }
  return out;
}
bool IPCFileStreamReader::DecodeHeader(const idx_t message_header_size) {
  // The size comes from the file, so it is checked before that much is allocated
  CheckInFile(SequentialOffset(), message_header_size - sizeof(message_prefix));
  if (message_header.GetSize() < message_header_size) {
    message_header = allocator.Allocate(message_header_size);
  }
  std::memcpy(message_header.get(), &message_prefix, sizeof(message_prefix));
  ReadData(message_header.get() + sizeof(message_prefix),
           message_header_size - sizeof(message_prefix));

  return DecodeHeaderBuffer(AllocatedDataView(message_header.get(), message_header_size));
}

void IPCFileStreamReader::CheckInFile(idx_t start, idx_t size) {
  // A pipe has no size, so its sequential read reports the truncation instead
  if (!positional) {
    return;
  }
  const auto file_size = file_reader.FileSize();
  if (start > file_size || size > file_size - start) {
    throw IOException("Arrow IPC stream is truncated, it ends inside a message");
  }
}

void IPCFileStreamReader::ReadBodyPositionally(idx_t body_start, idx_t body_size) {
  // A body smaller than the buffer reads through it, which also buffers the next header
  if (body_size < FILE_BUFFER_SIZE) {
    ReadData(message_body->get(), body_size);
    return;
  }
  // Reading the header usually buffered the start of the body, so copy it, not reread it
  const auto buffered =
      MinValue<idx_t>(file_reader.read_data - file_reader.offset, body_size);
  ReadData(message_body->get(), buffered);
  // Positional reads replace the 4 KB reads the buffered reader would issue
  ReadAt(*file_reader.handle, FileRead{message_body->get() + buffered,
                                       body_size - buffered, body_start + buffered});
  file_reader.Seek(body_start + body_size);
}

bool IPCFileStreamReader::ProjectedRanges(const_data_ptr_t base, idx_t body_size,
                                          vector<BodyRange>& merged) {
  if (!HasProjection() || body_size < kMinBodyBytesForRanges) {
    return false;
  }
  // Dictionary batches are decoded whole, and only a record batch has projected fields
  if (decoder->message_type != NANOARROW_IPC_MESSAGE_TYPE_RECORD_BATCH) {
    return false;
  }
  // Unions carried an extra buffer before V5, which shifts every later buffer index
  if (decoder->metadata_version < NANOARROW_IPC_METADATA_VERSION_V5) {
    return false;
  }
  // A swap rewrites buffers into scratch space, so their addresses stop meaning offsets
  if (NeedsEndianSwap()) {
    return false;
  }

  const auto body_view = AllocatedDataView(base, static_cast<int64_t>(body_size));
  vector<BodyRange> ranges;
  {
    // Compressed buffers are found at the same offsets, only their contents differ
    CodecOverride codec_override(*decoder.get());
    for (const auto field_index : projected_fields) {
      ArrowArrayView* view = nullptr;
      if (ArrowIpcDecoderDecodeArrayViewWithDictionaries(decoder.get(), body_view,
                                                         field_index, dictionaries->get(),
                                                         &view, &error) != NANOARROW_OK) {
        return false;
      }
      if (!CollectFieldRanges(*view, base, body_size, ranges)) {
        return false;
      }
    }
  }
  if (ranges.empty()) {
    return false;
  }

  std::sort(ranges.begin(), ranges.end(),
            [](const BodyRange& a, const BodyRange& b) { return a.begin < b.begin; });
  const auto gap = CoalesceGap();
  for (const auto& range : ranges) {
    if (!merged.empty() && range.begin <= merged.back().end + gap) {
      if (range.end > merged.back().end) {
        merged.back().end = range.end;
      }
    } else {
      merged.push_back(range);
    }
  }

  idx_t projected_bytes = 0;
  for (const auto& range : merged) {
    projected_bytes += range.end - range.begin;
  }
  return static_cast<double>(projected_bytes) <=
         static_cast<double>(body_size) * kMaxProjectedFraction;
}

bool IPCFileStreamReader::TryReadProjectedBody(idx_t body_start, idx_t body_size) {
  vector<BodyRange> merged;
  if (!ProjectedRanges(message_body->get(), body_size, merged)) {
    return false;
  }
  for (const auto& range : merged) {
    ReadAt(*file_reader.handle,
           FileRead{message_body->get() + range.begin, range.end - range.begin,
                    body_start + range.begin});
  }
  file_reader.Seek(body_start + body_size);
  return true;
}

void IPCFileStreamReader::DecodeBody() {
  message_body.reset();
  cur_ptr = nullptr;
  cur_size = 0;
  if (decoder->body_size_bytes <= 0) {
    return;
  }
  EnsureInputStreamAligned();
  // The padding belongs to the previous message, so take the offset after it
  const auto body_start = SequentialOffset();
  const auto body_size = static_cast<idx_t>(decoder->body_size_bytes);
  CheckInFile(body_start, body_size);
  // A swap reads the buffers it rewrites, and a compressed body must be decoded to count
  if (skip_bodies && !NeedsEndianSwap() &&
      decoder->codec == NANOARROW_IPC_COMPRESSION_TYPE_NONE) {
    // The read ahead fetches every byte anyway, and seeking drops a small body's buffer
    if (scheduler) {
      SequentialSeek(body_start + body_size);
      SetUnreadBody(body_size);
      return;
    }
    if (body_size >= FILE_BUFFER_SIZE && positional) {
      file_reader.Seek(body_start + body_size);
      SetUnreadBody(body_size);
      return;
    }
  }
  message_body = make_shared_ptr<AllocatedData>(allocator.Allocate(body_size));
  if (scheduler) {
    SequentialRead(message_body->get(), body_size);
  } else if (positional) {
    if (!TryReadProjectedBody(body_start, body_size)) {
      ReadBodyPositionally(body_start, body_size);
    }
  } else {
    ReadData(message_body->get(), body_size);
  }
  cur_ptr = message_body->get();
  cur_size = static_cast<int64_t>(body_size);
}

void IPCFileStreamReader::SetUnreadBody(idx_t size) {
  // The view takes offsets inside the body and reads no byte, so untouched pages back it
  if (unread_body.GetSize() < size) {
    unread_body = Allocator::DefaultAllocator().Allocate(size);
  }
  cur_ptr = unread_body.get();
  cur_size = static_cast<int64_t>(size);
}

data_ptr_t IPCFileStreamReader::ReadData(data_ptr_t ptr, idx_t size) {
  SequentialRead(ptr, size);
  return ptr;
}

void IPCFileStreamReader::SequentialRead(data_ptr_t target, idx_t size) {
  if (!scheduler) {
    file_reader.ReadData(target, size);
    return;
  }
  // A remote stream is read in order, so the bytes after the current ones download early
  if (!read_ahead) {
    read_ahead = make_uniq<RemoteReadAhead>(*file_reader.handle, file_reader.FileSize(),
                                            allocator, *scheduler);
  }
  read_ahead->Read(target, size);
}

idx_t IPCFileStreamReader::SequentialOffset() {
  return read_ahead ? read_ahead->Offset() : file_reader.CurrentOffset();
}

void IPCFileStreamReader::SequentialSeek(idx_t location) {
  if (read_ahead) {
    read_ahead->Seek(location);
  } else {
    file_reader.Seek(location);
  }
}

bool IPCFileStreamReader::CanCountWithoutBodies() {
  GetBaseSchema();
  int64_t flat_index = 0;
  for (int64_t i = 0; i < base_schema->n_children; i++) {
    // A skipped dictionary batch leaves its fields without the values the view needs
    if (!HasDictionary(*base_schema->children[i])) {
      count_field = flat_index;
      return true;
    }
    flat_index += CountFields(base_schema->children[i]);
  }
  return false;
}

void IPCFileStreamReader::CountOnly() {
  if (count_field < 0 && !CanCountWithoutBodies()) {
    throw InternalException("NextBatchLength needs a field without dictionaries");
  }
  skip_bodies = true;
}

bool IPCFileStreamReader::NextBatchLength(idx_t& length) {
  CountOnly();
  ArrowIpcMessageType message_type;
  do {
    message_type =
        IPCStreamReader::ReadNextMessage({NANOARROW_IPC_MESSAGE_TYPE_RECORD_BATCH,
                                          NANOARROW_IPC_MESSAGE_TYPE_DICTIONARY_BATCH});
    if (message_type == NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED) {
      return false;
    }
  } while (message_type != NANOARROW_IPC_MESSAGE_TYPE_RECORD_BATCH);

  // The view takes offsets inside an unread plain body and decodes a compressed one
  const auto body_view = AllocatedDataView(cur_ptr, cur_size);
  ArrowArrayView* view = nullptr;
  THROW_NOT_OK(
      IOException, &error,
      ArrowIpcDecoderDecodeArrayViewWithDictionaries(
          decoder.get(), body_view, count_field, dictionaries->get(), &view, &error));
  if (!LengthFits(*view)) {
    throw IOException("Arrow IPC record batch length is too large");
  }
  // The buffer sizes bound the rows, so a header cannot claim more than the body holds
  THROW_NOT_OK(IOException, &error,
               ArrowArrayViewValidate(view, NANOARROW_VALIDATION_LEVEL_MINIMAL, &error));
  length = static_cast<idx_t>(view->length);
  return true;
}

idx_t IPCFileStreamReader::CoalesceGap() const {
  return remote ? kRemoteCoalesceGapBytes : kCoalesceGapBytes;
}

idx_t IPCFileStreamReader::BlockBytes() const {
  idx_t bytes = 0;
  for (auto block = next_block; block != end_block; block++) {
    bytes += static_cast<idx_t>(block->metadata_length);
    if (!skip_bodies) {
      bytes += static_cast<idx_t>(block->body_length);
    }
  }
  return bytes;
}

void IPCFileStreamReader::FetchBlocks(bool whole) {
  const auto count = static_cast<idx_t>(end_block - next_block);
  // Blocks are published after every read succeeded, so a failed fetch leaves none
  fetched_blocks.clear();
  fetched_index = 0;
  vector<FetchedBlock> fetched(count);
  // Every column needs the whole body, which one read per run of blocks covers
  const bool projected =
      !whole && !skip_bodies && HasProjection() && !NeedsEndianSwap() &&
      projected_fields.size() < static_cast<idx_t>(GetBaseSchema()->n_children);
  vector<idx_t> whole_blocks;
  vector<idx_t> ranged_blocks;
  const auto min_ranged_body =
      remote ? kRemoteMinBodyBytesForRanges : kMinBodyBytesForRanges;
  for (idx_t i = 0; i < count; i++) {
    // Several reads of a small body cost more than the bytes they would save
    if (projected && static_cast<idx_t>(next_block[i].body_length) >= min_ranged_body) {
      ranged_blocks.push_back(i);
    } else {
      whole_blocks.push_back(i);
    }
  }
  vector<FileRead> reads;
  PlanRuns(whole_blocks, skip_bodies, fetched, reads);
  // The header says where the projected buffers are, so it comes first
  PlanRuns(ranged_blocks, true, fetched, reads);
  ReadAll(reads);
  reads.clear();
  for (const auto i : ranged_blocks) {
    PlanProjectedBlock(next_block[i], fetched[i], reads);
  }
  ReadAll(reads);
  fetched_blocks = std::move(fetched);
}

void IPCFileStreamReader::PlanRuns(const vector<idx_t>& indexes, bool headers_only,
                                   vector<FetchedBlock>& fetched,
                                   vector<FileRead>& reads) {
  const auto gap = CoalesceGap();
  auto end_of = [&](const ArrowIpcFileBlock& block) {
    auto end =
        static_cast<idx_t>(block.offset) + static_cast<idx_t>(block.metadata_length);
    return headers_only ? end : end + static_cast<idx_t>(block.body_length);
  };
  idx_t i = 0;
  while (i < indexes.size()) {
    // Blocks next to each other in the file are read together, gaps included
    const auto begin = static_cast<idx_t>(next_block[indexes[i]].offset);
    auto end = end_of(next_block[indexes[i]]);
    idx_t j = i + 1;
    for (; j < indexes.size(); j++) {
      const auto& block = next_block[indexes[j]];
      const auto offset = static_cast<idx_t>(block.offset);
      if (offset < end || offset - end > gap) {
        break;
      }
      end = end_of(block);
    }
    auto data = make_shared_ptr<AllocatedData>(allocator.Allocate(end - begin));
    reads.push_back(FileRead{data->get(), end - begin, begin});
    for (idx_t k = i; k < j; k++) {
      const auto offset = static_cast<idx_t>(next_block[indexes[k]].offset);
      fetched[indexes[k]] = FetchedBlock{data, data->get() + (offset - begin)};
    }
    i = j;
  }
}

void IPCFileStreamReader::PlanProjectedBlock(const ArrowIpcFileBlock& block,
                                             FetchedBlock& fetched,
                                             vector<FileRead>& reads) {
  const auto metadata_size = static_cast<idx_t>(block.metadata_length);
  const auto body_size = static_cast<idx_t>(block.body_length);
  auto data =
      make_shared_ptr<AllocatedData>(allocator.Allocate(metadata_size + body_size));
  std::memcpy(data->get(), fetched.ptr, metadata_size);
  fetched = FetchedBlock{data, data->get()};

  vector<BodyRange> ranges;
  const auto body = data->get() + metadata_size;
  // The header decodes here only to find the buffers, the scan decodes it again
  if (!DecodeBlockHeader(block, fetched) || !ProjectedRanges(body, body_size, ranges)) {
    ranges.clear();
    ranges.push_back(BodyRange{0, body_size});
  }
  const auto body_start = static_cast<idx_t>(block.offset) + metadata_size;
  for (const auto& range : ranges) {
    reads.push_back(
        FileRead{body + range.begin, range.end - range.begin, body_start + range.begin});
  }
}

void IPCFileStreamReader::ReadAll(const vector<FileRead>& reads) {
  // Local reads are cheap one after another, remote ones each wait a round trip
  if (scheduler) {
    // A large body would download on one connection while the others sit idle
    const auto pieces = SplitReads(reads, kRemoteMaxReadBytes);
    if (pieces.size() > 1) {
      ReadConcurrently(*scheduler, *file_reader.handle, pieces);
      return;
    }
  }
  for (const auto& read : reads) {
    ReadAt(*file_reader.handle, read);
  }
}

bool IPCFileStreamReader::DecodeBlockHeader(const ArrowIpcFileBlock& block,
                                            const FetchedBlock& fetched) {
  std::memcpy(&message_prefix, fetched.ptr, sizeof(message_prefix));
  if (message_prefix.continuation_token != kContinuationToken) {
    throw UnexpectedToken(message_prefix.continuation_token);
  }
  const auto header_size = DecodeMetadata();
  if (header_size > static_cast<idx_t>(block.metadata_length)) {
    throw IOException("Arrow IPC message header is larger than its footer block");
  }
  // An end of stream marker has no body, and a footer block should never name one
  return !DecodeHeaderBuffer(AllocatedDataView(fetched.ptr, header_size));
}

ArrowIpcMessageType IPCFileStreamReader::DecodeFetchedBlock(
    const ArrowIpcFileBlock& block, const FetchedBlock& fetched) {
  if (!DecodeBlockHeader(block, fetched)) {
    return NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED;
  }
  // A dictionary batch among the record batches would change dictionaries readers share
  if (!reading_dictionaries &&
      decoder->message_type == NANOARROW_IPC_MESSAGE_TYPE_DICTIONARY_BATCH) {
    throw IOException(
        "Arrow IPC footer names a dictionary batch as a record batch block");
  }
  message_body.reset();
  cur_ptr = nullptr;
  cur_size = 0;
  if (decoder->body_size_bytes > 0) {
    const auto body_size = static_cast<idx_t>(decoder->body_size_bytes);
    if (body_size > static_cast<idx_t>(block.body_length)) {
      throw IOException("Arrow IPC message body is larger than its footer block");
    }
    if (skip_bodies && !NeedsEndianSwap() &&
        decoder->codec == NANOARROW_IPC_COMPRESSION_TYPE_NONE) {
      SetUnreadBody(body_size);
    } else if (skip_bodies) {
      // Only the header was fetched, and a swapped or compressed body is read to count it
      message_body = make_shared_ptr<AllocatedData>(allocator.Allocate(body_size));
      ReadAt(*file_reader.handle,
             FileRead{message_body->get(), body_size,
                      static_cast<idx_t>(block.offset) +
                          static_cast<idx_t>(block.metadata_length)});
      cur_ptr = message_body->get();
      cur_size = static_cast<int64_t>(body_size);
    } else {
      message_body = fetched.data;
      cur_ptr = fetched.ptr + block.metadata_length;
      cur_size = static_cast<int64_t>(body_size);
    }
  }
  return decoder->message_type;
}

bool IPCFileStreamReader::NeedsDictionaries() {
  return HasDictionary(*GetOutputSchema());
}

void IPCFileStreamReader::LoadDictionaries(const vector<ArrowIpcFileBlock>& blocks) {
  if (blocks.empty()) {
    return;
  }
  SetBlocks(blocks.data(), blocks.data() + blocks.size());
  // Dictionaries are decoded whole, so a projection has no ranges to read
  FetchBlocks(true);
  // Only dictionary blocks are named, so the read decodes them all and reaches the end
  nanoarrow::UniqueArray none;
  reading_dictionaries = true;
  if (GetNextBatch(none.get())) {
    throw IOException("Arrow IPC footer names a record batch as a dictionary block");
  }
  reading_dictionaries = false;
}

void IPCFileStreamReader::SetBlocks(const ArrowIpcFileBlock* begin,
                                    const ArrowIpcFileBlock* end) {
  next_block = begin;
  end_block = end;
  finished = false;
  fetched_blocks.clear();
  fetched_index = 0;
}

ArrowIpcMessageType IPCFileStreamReader::ReadNextMessage() {
  if (next_block) {
    if (next_block == end_block) {
      // The decoded arrays keep what they use, so the claim's buffers can go now
      fetched_blocks.clear();
      message_body.reset();
      return NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED;
    }
    // A scan that scheduled no reads ahead fetches its blocks on the first message
    if (fetched_blocks.empty()) {
      FetchBlocks();
    }
    // TryReadFooter checked that the block lies inside the file on an aligned offset
    const auto& block = *next_block++;
    return DecodeFetchedBlock(block, fetched_blocks[fetched_index++]);
  }
  if (finished) {
    return NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED;
  }
  if (progress_offset) {
    progress_offset->store(SequentialOffset());
  }

  // If there is no more data to be read, we're done!
  idx_t message_start = SequentialOffset();
  try {
    EnsureInputStreamAligned();
    message_start = SequentialOffset();
    SequentialRead(reinterpret_cast<data_ptr_t>(&message_prefix), sizeof(message_prefix));

    // Read the embedded stream after the file header.
    if (SequentialOffset() == kArrowIPCFileHeaderSize &&
        std::memcmp(kArrowIPCFileMagic, &message_prefix, kArrowIPCFileHeaderSize) == 0) {
      // A file ends with its footer, so one without it is a partial copy
      if (positional && !file_end_read) {
        ReadFileEnd();
      }
      if (missing_footer) {
        throw IOException("Arrow IPC file \"%s\" is truncated, it has no footer",
                          file_reader.handle->GetPath());
      }
      uint32_t token;
      do {
        SequentialRead(reinterpret_cast<data_ptr_t>(&token), sizeof(token));
      } while (token != kContinuationToken);
      // Read the metadata size
      message_prefix.continuation_token = kContinuationToken;
      SequentialRead(reinterpret_cast<data_ptr_t>(&message_prefix.metadata_size),
                     sizeof(message_prefix.metadata_size));
    } else if (message_prefix.continuation_token != kContinuationToken) {
      throw UnexpectedToken(message_prefix.continuation_token);
    }
  } catch (SerializationException& e) {
    // A stream may stop at a message boundary, but a pipe has no size to compare against
    if (SequentialOffset() > message_start || message_start < file_reader.FileSize()) {
      throw IOException("Arrow IPC stream is truncated, it ends inside a message prefix");
    }
    finished = true;
    return NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED;
  }

  try {
    return DecodeMessage();
  } catch (SerializationException& e) {
    throw IOException("Arrow IPC stream is truncated, it ends inside a message");
  }
}

void IPCFileStreamReader::EnsureInputStreamAligned() {
  uint8_t padding[8];
  int padding_bytes = 8 - (SequentialOffset() % 8);
  if (padding_bytes != 8) {
    SequentialRead(padding, padding_bytes);
  }
  D_ASSERT((SequentialOffset() % 8) == 0);
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
