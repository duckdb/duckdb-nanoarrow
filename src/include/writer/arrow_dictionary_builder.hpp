//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// writer/arrow_dictionary_builder.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/main/client_properties.hpp"
#include "nanoarrow/nanoarrow.hpp"

namespace duckdb {
class ArrowAppender;

namespace ext_nanoarrow {

using ArrowExtensionTypes =
    unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>>;

//! Dictionary encodes the values of one column of a record batch: int32 indices into the
//! distinct values, which DuckDB's Arrow conversion writes as the dictionary
class ArrowDictionaryBuilder {
 public:
  virtual ~ArrowDictionaryBuilder() = default;

  //! Throws a BinderException unless a column of this type can be dictionary encoded
  static void CheckType(const LogicalType& type, const ArrowSchema& values,
                        const string& column);
  static unique_ptr<ArrowDictionaryBuilder> Create(const LogicalType& type);

  virtual void Append(Vector& input, idx_t count) = 0;
  //! Moves the indices, with the dictionary attached, into out. Called once
  virtual void Finalize(ClientProperties& options,
                        const shared_ptr<ArrowTypeExtensionData>& extension_type,
                        ArrowArray* out) = 0;
};

//! Builds one record batch for a schema whose dictionary encoded columns are not ENUMs,
//! which DuckDB's ArrowAppender writes like every other column
class ArrowBatchBuilder {
 public:
  ArrowBatchBuilder(const vector<LogicalType>& types, const ArrowSchema& schema,
                    idx_t capacity, ClientProperties options,
                    const ArrowExtensionTypes& extension_types);
  ~ArrowBatchBuilder();

  void Append(DataChunk& input);
  idx_t Count() const { return row_count; }
  //! The record batch as a struct array. Called once
  nanoarrow::UniqueArray Finalize();

  //! Whether the schema's column at index is dictionary encoded by ArrowBatchBuilder
  static bool IsDictionaryColumn(const ArrowSchema& schema, const LogicalType& type,
                                 idx_t index);

 private:
  ClientProperties options;
  ArrowExtensionTypes extension_types;
  idx_t column_count;
  //! The columns ArrowAppender writes, and their types
  vector<column_t> appended_columns;
  vector<LogicalType> appended_types;
  unique_ptr<ArrowAppender> appender;
  DataChunk appended_chunk;
  //! Per column, null for the columns ArrowAppender writes
  vector<unique_ptr<ArrowDictionaryBuilder>> dictionaries;
  idx_t row_count = 0;
};

}  // namespace ext_nanoarrow
}  // namespace duckdb
