//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// file_scanner/arrow_file_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "ipc/stream_factory.hpp"

#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/parallel/async_result.hpp"

namespace duckdb {
namespace ext_nanoarrow {

struct ArrowFileLocalState;

//! This class refers to an Arrow File Scan
class ArrowFileScan : public BaseFileReader {
 public:
  ArrowFileScan(ClientContext& context, const OpenFileInfo& file);
  //! Each scan takes a deep copy of the schema, so this one releases its own
  ~ArrowFileScan() override = default;

  //! Factory of this stream
  unique_ptr<FileIPCStreamFactory> factory;

  string GetReaderType() const override;

  const vector<string>& GetNames();
  const vector<LogicalType>& GetTypes();
  ArrowSchemaWrapper schema_root;
  ArrowTableSchema arrow_table;

  bool TryInitializeScan(ClientContext& context, GlobalTableFunctionState& gstate,
                         LocalTableFunctionState& lstate) override;
  void PrepareScan(ClientContext& context, GlobalTableFunctionState& gstate,
                   LocalTableFunctionState& lstate) override;
  //! Fetches the blocks of a claim on the async pool, before Scan decodes them
  AsyncResult ScheduleIO(ClientContext& context, GlobalTableFunctionState& gstate,
                         LocalTableFunctionState& lstate) override;
  AsyncResult Scan(ClientContext& context, GlobalTableFunctionState& global_state,
                   LocalTableFunctionState& local_state, DataChunk& chunk) override;
  double GetProgressInFile(ClientContext& context) override;

  shared_ptr<BaseUnionData> GetUnionData(idx_t file_idx) override;

  //! Rows estimated from the file size and the average width of a row, 0 when unknown
  idx_t EstimatedRowCount();
  //! How many scans can run on this file at once
  idx_t ClaimCount() const;

 private:
  //! A range of footer blocks that one scan reads
  struct BlockRange {
    idx_t begin;
    idx_t end;
  };

  //! Groups the footer blocks into claims of at least this many body bytes
  void PlanClaims(const vector<ArrowIpcFileBlock>& file_blocks, idx_t min_claim_bytes);
  void InitializeScanData(ArrowFileLocalState& lstate, stream_factory_produce_t producer,
                          uintptr_t producer_data);
  void StartScan(ClientContext& context, ArrowFileLocalState& lstate);
  static void FinishClaim(ArrowFileLocalState& lstate);
  static unique_ptr<ArrowArrayStreamWrapper> ProduceBlocks(
      uintptr_t local_state, ArrowStreamParameters& parameters);

  vector<string> names;
  vector<LogicalType> types;
  //! Tells readers apart in reused scan states, where an address could repeat
  const idx_t scan_id;
  vector<ArrowIpcFileBlock> blocks;
  vector<ArrowIpcFileBlock> dictionary_blocks;
  //! The footer, which claim readers take the schema from instead of reading the file
  AllocatedData footer_window;
  vector<BlockRange> claims;
  //! Whether a scan that reads no file column can count rows from the headers
  bool count_without_bodies = false;
  idx_t file_size = 0;
  //! Where the sequential scan has got to, written by whichever reader it moved into
  shared_ptr<atomic<idx_t>> progress_offset = make_shared_ptr<atomic<idx_t>>(0);
  //! Handed out under the multi file lock, read without it for progress
  atomic<idx_t> next_claim{0};
};
}  // namespace ext_nanoarrow
}  // namespace duckdb
