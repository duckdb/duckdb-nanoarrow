#include "duckdb/function/table/arrow.hpp"
#include "duckdb/parallel/callback_async_task.hpp"

#include "file_scanner/arrow_file_scan.hpp"
#include "file_scanner/arrow_multi_file_info.hpp"
#include "ipc/stream_reader/ipc_file_stream_reader.hpp"

namespace duckdb {
namespace ext_nanoarrow {

namespace {
//! Claims below this many body bytes would spend more on setup than on decoding
constexpr idx_t kMinClaimBodyBytes = 1024 * 1024;
//! A remote claim is fetched with few requests, so it holds more to keep them large
constexpr idx_t kRemoteMinClaimBodyBytes = 16 * 1024 * 1024;
atomic<idx_t> next_scan_id{1};
}  // namespace

ArrowFileScan::ArrowFileScan(ClientContext& context, const OpenFileInfo& file)
    : BaseFileReader(file), scan_id(next_scan_id++) {
  factory = make_uniq<FileIPCStreamFactory>(context, file);

  factory->InitReader();
  auto& reader = static_cast<IPCFileStreamReader&>(*factory->reader);
  // A remote footer carries the schema, so reading it first skips the start of the file
  const bool has_footer = reader.TryReadFooter();
  factory->GetFileSchema(schema_root);
  ArrowTableFunction::PopulateArrowTableSchema(context, arrow_table,
                                               schema_root.arrow_schema);
  names = arrow_table.GetNames();
  types = arrow_table.GetTypes();
  if (types.empty()) {
    throw InvalidInputException("Provided table/dataframe must have at least one column");
  }
  columns = MultiFileColumnDefinition::ColumnsFromNamesAndTypes(
      StringsToIdentifiers(names), types);

  // Scans move this reader away, so progress and estimates keep their own copies
  file_size = reader.FileSize();
  reader.TrackProgress(progress_offset);
  count_without_bodies = reader.CanCountWithoutBodies();
  if (has_footer) {
    PlanClaims(reader.RecordBatchBlocks(),
               reader.IsRemote() ? kRemoteMinClaimBodyBytes : kMinClaimBodyBytes);
    dictionary_blocks = reader.DictionaryBlocks();
    const auto window = reader.FooterWindow();
    footer_window = factory->allocator.Allocate(static_cast<idx_t>(window.size_bytes));
    std::memcpy(footer_window.get(), window.data.data,
                static_cast<size_t>(window.size_bytes));
  }
}

void ArrowFileScan::PlanClaims(const vector<ArrowIpcFileBlock>& file_blocks,
                               idx_t min_claim_bytes) {
  idx_t begin = 0;
  idx_t body_bytes = 0;
  for (idx_t i = 0; i < file_blocks.size(); i++) {
    body_bytes += static_cast<idx_t>(file_blocks[i].body_length);
    if (body_bytes >= min_claim_bytes) {
      claims.push_back(BlockRange{begin, i + 1});
      begin = i + 1;
      body_bytes = 0;
    }
  }
  if (begin < file_blocks.size()) {
    claims.push_back(BlockRange{begin, file_blocks.size()});
  }
  // Even one claim reads its blocks with few large reads instead of following the stream
  blocks = file_blocks;
}

idx_t ArrowFileScan::ClaimCount() const { return MaxValue<idx_t>(claims.size(), 1); }

string ArrowFileScan::GetReaderType() const { return "ARROW"; }

const vector<string>& ArrowFileScan::GetNames() { return names; }
const vector<LogicalType>& ArrowFileScan::GetTypes() { return types; }

bool ArrowFileScan::TryInitializeScan(ClientContext& context,
                                      GlobalTableFunctionState& gstate_p,
                                      LocalTableFunctionState& lstate_p) {
  if (!claims.empty()) {
    // Claims go out in file order under the multi file lock, which keeps batch order
    if (next_claim >= claims.size()) {
      return false;
    }
    lstate_p.Cast<ArrowFileLocalState>().claim_index = next_claim++;
    return true;
  }
  auto& gstate = gstate_p.Cast<ArrowFileGlobalState>();
  if (gstate.files.find(file_list_idx.GetIndex()) != gstate.files.end()) {
    // Without a footer the batches can only be found by reading the stream in order
    return false;
  }
  gstate.files.insert(file_list_idx.GetIndex());
  return true;
}

void ArrowFileScan::InitializeScanData(ArrowFileLocalState& lstate,
                                       stream_factory_produce_t producer,
                                       uintptr_t producer_data) {
  lstate.local_arrow_function_data =
      make_uniq<ArrowScanFunctionData>(producer, producer_data);
  // A memberwise copy shares the release pointer, so a second scan would free it twice
  NANOARROW_THROW_NOT_OK(
      ArrowSchemaDeepCopy(&schema_root.arrow_schema,
                          &lstate.local_arrow_function_data->schema_root.arrow_schema));
  lstate.local_arrow_function_data->arrow_table = arrow_table;
  // Global column indexes and projection ids do not address this file's schema
  auto local_column_indexes = column_indexes;
  if (local_column_indexes.empty()) {
    // Only virtual or constant columns are read, so any file column yields the rows
    local_column_indexes.emplace_back(0);
  }
  const vector<idx_t> no_projection_ids;
  lstate.init_input = make_uniq<TableFunctionInitInput>(*lstate.local_arrow_function_data,
                                                        std::move(local_column_indexes),
                                                        no_projection_ids, filters);
}

void ArrowFileScan::StartScan(ClientContext& context, ArrowFileLocalState& lstate) {
  lstate.local_arrow_global_state =
      ArrowTableFunction::ArrowScanInitGlobal(context, *lstate.init_input);
  lstate.local_arrow_local_state = ArrowTableFunction::ArrowScanInitLocalInternal(
      context, *lstate.init_input, lstate.local_arrow_global_state.get());
  lstate.table_function_input = make_uniq<TableFunctionInput>(
      lstate.local_arrow_function_data.get(), lstate.local_arrow_local_state.get(),
      lstate.local_arrow_global_state.get());
}

unique_ptr<ArrowArrayStreamWrapper> ArrowFileScan::ProduceBlocks(
    uintptr_t local_state, ArrowStreamParameters& parameters) {
  // PrepareScan already gave the reader the projection these parameters carry
  auto& lstate = *reinterpret_cast<ArrowFileLocalState*>(local_state);
  auto out = make_uniq<ArrowArrayStreamWrapper>();
  IpcArrayStream(*lstate.block_reader).ToArrayStream(&out->arrow_array_stream);
  return out;
}

void ArrowFileScan::PrepareScan(ClientContext& context,
                                GlobalTableFunctionState& gstate_p,
                                LocalTableFunctionState& lstate_p) {
  // Reading waits for Scan, so ScheduleIO can fetch the claim on the async pool first
  auto& lstate = lstate_p.Cast<ArrowFileLocalState>();
  lstate.scan_started = false;
  lstate.count_reader = nullptr;
  lstate.count_owned_reader.reset();
  lstate.count_rows_left = 0;
  // Only virtual or constant columns are read, so the headers alone give the rows
  const bool count_only = column_indexes.empty() && count_without_bodies;
  if (claims.empty()) {
    lstate.block_scan_id = 0;
    if (count_only) {
      if (!factory->reader) {
        throw InternalException("ArrowFileScan counted a file whose reader was moved");
      }
      lstate.count_owned_reader.reset(
          static_cast<IPCFileStreamReader*>(factory->reader.release()));
      lstate.count_reader = lstate.count_owned_reader.get();
      return;
    }
    InitializeScanData(lstate, &FileIPCStreamFactory::Produce,
                       reinterpret_cast<uintptr_t>(factory.get()));
    return;
  }
  // A state keeps its reader for the next claim of the same file
  if (lstate.block_scan_id != scan_id) {
    // The previous scan borrows the previous reader, so it goes first
    lstate.table_function_input.reset();
    lstate.local_arrow_local_state.reset();
    lstate.local_arrow_global_state.reset();
    lstate.block_scan_id = 0;
    lstate.block_reader = factory->OpenReader();
    // The schema comes from the footer copy, so a claim costs no read of the file start
    lstate.block_reader->LoadFooter(ArrowBufferView{
        {footer_window.get()}, static_cast<int64_t>(footer_window.GetSize())});
    // Each reader decodes the dictionaries once, since a file cannot replace them
    if (!count_only) {
      lstate.block_reader->LoadDictionaries(dictionary_blocks);
    }
    InitializeScanData(lstate, &ArrowFileScan::ProduceBlocks,
                       reinterpret_cast<uintptr_t>(&lstate));
    // The fetch needs the projection before the scan that would push it starts
    if (!count_only) {
      vector<idx_t> projection;
      for (const auto column_id : lstate.init_input->column_ids) {
        if (column_id != COLUMN_IDENTIFIER_ROW_ID) {
          projection.push_back(column_id);
        }
      }
      if (!projection.empty()) {
        lstate.block_reader->SetColumnProjection(projection);
      }
    }
    lstate.block_scan_id = scan_id;
  }
  const auto& claim = claims[lstate.claim_index];
  lstate.block_reader->SetBlocks(blocks.data() + claim.begin, blocks.data() + claim.end);
  if (count_only) {
    lstate.block_reader->CountOnly();
    lstate.count_reader = lstate.block_reader.get();
  }
}

AsyncResult ArrowFileScan::ScheduleIO(ClientContext& context,
                                      GlobalTableFunctionState& gstate_p,
                                      LocalTableFunctionState& lstate_p) {
  auto& lstate = lstate_p.Cast<ArrowFileLocalState>();
  // Without a footer the reads follow the stream, so they cannot be planned ahead
  if (claims.empty()) {
    return SourceResultType::HAVE_MORE_OUTPUT;
  }
  auto& reader = *lstate.block_reader;
  vector<unique_ptr<AsyncTask>> io_tasks;
  io_tasks.push_back(make_uniq<CallbackAsyncTask>([&reader] { reader.FetchBlocks(); },
                                                  reader.BlockBytes()));
  return AsyncResult::FromTasks(std::move(io_tasks), TaskSchedulerType::ASYNC);
}

AsyncResult ArrowFileScan::Scan(ClientContext& context,
                                GlobalTableFunctionState& global_state,
                                LocalTableFunctionState& local_state, DataChunk& chunk) {
  auto& lstate = local_state.Cast<ArrowFileLocalState>();
  if (!lstate.scan_started) {
    lstate.scan_started = true;
    if (!lstate.count_reader) {
      StartScan(context, lstate);
    }
  }
  if (lstate.count_reader) {
    while (lstate.count_rows_left == 0) {
      if (!lstate.count_reader->NextBatchLength(lstate.count_rows_left)) {
        FinishClaim(lstate);
        return SourceResultType::FINISHED;
      }
    }
    const auto count = MinValue<idx_t>(lstate.count_rows_left, STANDARD_VECTOR_SIZE);
    chunk.SetChildCardinality(count);
    lstate.count_rows_left -= count;
    return SourceResultType::HAVE_MORE_OUTPUT;
  }
  ArrowTableFunction::ArrowScanFunction(context, *lstate.table_function_input, chunk);
  if (chunk.size() == 0) {
    FinishClaim(lstate);
    return SourceResultType::FINISHED;
  }
  return SourceResultType::HAVE_MORE_OUTPUT;
}

void ArrowFileScan::FinishClaim(ArrowFileLocalState& lstate) {
  // A finished state can wait in a pool, so the last batch it scanned is freed here
  lstate.table_function_input.reset();
  lstate.local_arrow_local_state.reset();
  lstate.local_arrow_global_state.reset();
  lstate.count_reader = nullptr;
  lstate.count_owned_reader.reset();
}

double ArrowFileScan::GetProgressInFile(ClientContext& context) {
  if (!claims.empty()) {
    const auto started = MinValue<idx_t>(next_claim.load(), claims.size());
    return 100.0 * static_cast<double>(started) / static_cast<double>(claims.size());
  }
  if (file_size == 0) {
    return 100;
  }
  const auto offset = MinValue<idx_t>(progress_offset->load(), file_size);
  return 100.0 * static_cast<double>(offset) / static_cast<double>(file_size);
}

idx_t ArrowFileScan::EstimatedRowCount() {
  // Without reading the footer the row count is the file size over the row width
  idx_t row_width = 0;
  for (const auto& type : types) {
    auto width = GetTypeIdSize(type.InternalType());
    // A variable length value stores an offset here and its bytes elsewhere
    row_width += type.InternalType() == PhysicalType::VARCHAR ? width + 16 : width;
  }
  if (row_width == 0) {
    return 0;
  }
  return file_size / row_width;
}

shared_ptr<BaseUnionData> ArrowFileScan::GetUnionData(idx_t file_idx) {
  auto data = make_shared_ptr<BaseUnionData>(OpenFileInfo(GetFileName()));
  data->names = GetNames();
  data->types = GetTypes();
  return data;
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
