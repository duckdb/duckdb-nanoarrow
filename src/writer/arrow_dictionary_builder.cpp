#include "writer/arrow_dictionary_builder.hpp"

#include <cstring>
#include <type_traits>

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/string_heap.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "nanoarrow_errors.hpp"

namespace duckdb {
namespace ext_nanoarrow {

namespace {

//! Values are equal when their bytes are, so -0.0, 0.0 and each NaN stay distinct
template <class T>
struct FixedWidthValues {
  static hash_t HashValue(const T& value) {
    return Hash(const_char_ptr_cast(&value), sizeof(T));
  }
  static bool Equals(const T& left, const T& right) {
    return std::memcmp(&left, &right, sizeof(T)) == 0;
  }
  static T Store(const T& value, StringHeap&) { return value; }
};

struct StringValues {
  static hash_t HashValue(const string_t& value) {
    return Hash(value.GetData(), value.GetSize());
  }
  static bool Equals(const string_t& left, const string_t& right) {
    return left == right;
  }
  static string_t Store(const string_t& value, StringHeap& heap) {
    return value.IsInlined() ? value : heap.AddBlob(value);
  }
};

template <class T, class OPS>
class TypedDictionaryBuilder final : public ArrowDictionaryBuilder {
  //! vector<bool> packs bits, the values are copied into a DuckDB vector as bytes
  using Stored =
      typename std::conditional<std::is_same<T, bool>::value, uint8_t, T>::type;

 public:
  explicit TypedDictionaryBuilder(LogicalType type_p)
      : type(std::move(type_p)), slots(kInitialSlots, 0) {}

  void Append(Vector& input, idx_t count) override {
    UnifiedVectorFormat format;
    input.ToUnifiedFormat(count, format);
    const auto data = UnifiedVectorFormat::GetData<T>(format);
    auto& remap = DictionaryRemap(input, count);
    NANOARROW_THROW_NOT_OK(
        ArrowBufferReserve(indices.get(), NumericCast<int64_t>(count * sizeof(int32_t))));
    if (null_count > 0) {
      NANOARROW_THROW_NOT_OK(
          ArrowBitmapReserve(validity.get(), NumericCast<int64_t>(count)));
    }
    for (idx_t i = 0; i < count; i++) {
      const auto index = format.sel->get_index(i);
      if (!format.validity.RowIsValid(index)) {
        AppendValidity(false, count - i);
        const int32_t null_index = 0;
        ArrowBufferAppendUnsafe(indices.get(), &null_index, sizeof(null_index));
        row_count++;
        continue;
      }
      int32_t code;
      if (index < remap.size()) {
        // A DuckDB dictionary entry is looked up once, whichever rows select it
        if (remap[index] < 0) {
          remap[index] = Lookup(data[index]);
        }
        code = remap[index];
      } else {
        code = Lookup(data[index]);
      }
      AppendValidity(true, count - i);
      ArrowBufferAppendUnsafe(indices.get(), &code, sizeof(code));
      row_count++;
    }
  }

  void Finalize(ClientProperties& options,
                const shared_ptr<ArrowTypeExtensionData>& extension_type,
                ArrowArray* out) override {
    NANOARROW_THROW_NOT_OK(ArrowArrayInitFromType(out, NANOARROW_TYPE_INT32));
    ArrowBufferMove(indices.get(), ArrowArrayBuffer(out, 1));
    if (null_count > 0) {
      ArrowBitmapMove(validity.get(), ArrowArrayValidityBitmap(out));
    }
    out->length = NumericCast<int64_t>(row_count);
    out->null_count = NumericCast<int64_t>(null_count);
    ArrowError error{};
    THROW_NOT_OK(InternalException, &error,
                 ArrowArrayFinishBuilding(out, NANOARROW_VALIDATION_LEVEL_NONE, &error));
    // Attached once the indices are built, nanoarrow cannot finish an array DuckDB owns
    NANOARROW_THROW_NOT_OK(ArrowArrayAllocateDictionary(out));
    FinalizeValues(options, extension_type, out->dictionary);
  }

 private:
  static constexpr idx_t kInitialSlots = 1024;

  //! Our dictionary index of each entry of a DuckDB dictionary, -1 until it is looked up.
  //! Empty when hashing every row is cheaper
  vector<int32_t>& DictionaryRemap(const Vector& input, idx_t count) {
    remap_scratch.clear();
    if (input.GetVectorType() != VectorType::DICTIONARY_VECTOR) {
      return remap_scratch;
    }
    const auto size = DictionaryVector::DictionarySize(input);
    if (!size.IsValid()) {
      return remap_scratch;
    }
    const auto& id = DictionaryVector::DictionaryId(input);
    if (id.empty() && size.GetIndex() > count) {
      return remap_scratch;
    }
    // An id names the same entries in every chunk, so the remap carries over
    auto& remap = id.empty() ? remap_scratch : remaps[id];
    if (remap.size() != size.GetIndex()) {
      remap.assign(size.GetIndex(), -1);
    }
    return remap;
  }

  int32_t Lookup(const T& value) {
    const auto hash = OPS::HashValue(value);
    const auto mask = slots.size() - 1;
    auto slot = hash & mask;
    for (; slots[slot] != 0; slot = (slot + 1) & mask) {
      const auto entry = slots[slot] - 1;
      if (hashes[entry] == hash && OPS::Equals(static_cast<T>(values[entry]), value)) {
        return NumericCast<int32_t>(entry);
      }
    }
    if (values.size() >= static_cast<idx_t>(NumericLimits<int32_t>::Maximum())) {
      throw InvalidInputException(
          "DICTIONARY_ENCODE cannot index more than %d distinct values in a record "
          "batch",
          NumericLimits<int32_t>::Maximum());
    }
    values.push_back(static_cast<Stored>(OPS::Store(value, heap)));
    hashes.push_back(hash);
    slots[slot] = NumericCast<uint32_t>(values.size());
    if (values.size() * 2 > slots.size()) {
      Grow();
    }
    return NumericCast<int32_t>(values.size() - 1);
  }

  void Grow() {
    slots.assign(slots.size() * 2, 0);
    const auto mask = slots.size() - 1;
    for (idx_t entry = 0; entry < hashes.size(); entry++) {
      auto slot = hashes[entry] & mask;
      while (slots[slot] != 0) {
        slot = (slot + 1) & mask;
      }
      slots[slot] = NumericCast<uint32_t>(entry + 1);
    }
  }

  //! The validity bitmap is only kept from the first NULL on, row_count rows precede it.
  //! Space for this row and the rest of the chunk, remaining rows, is reserved
  void AppendValidity(bool valid, idx_t remaining) {
    if (null_count == 0) {
      if (valid) {
        return;
      }
      NANOARROW_THROW_NOT_OK(ArrowBitmapReserve(
          validity.get(), NumericCast<int64_t>(row_count + remaining)));
      ArrowBitmapAppendUnsafe(validity.get(), 1, NumericCast<int64_t>(row_count));
    }
    if (!valid) {
      null_count++;
    }
    ArrowBitmapAppendUnsafe(validity.get(), valid ? 1 : 0, 1);
  }

  //! DuckDB's Arrow conversion of the distinct values, so they match the schema
  void FinalizeValues(ClientProperties& options,
                      const shared_ptr<ArrowTypeExtensionData>& extension_type,
                      ArrowArray* out) {
    ArrowExtensionTypes extension_types;
    if (extension_type) {
      extension_types.emplace(0, extension_type);
    }
    const vector<LogicalType> types{type};
    ArrowAppender appender(types, MaxValue<idx_t>(values.size(), 1), options,
                           extension_types);
    DataChunk chunk;
    chunk.Initialize(Allocator::DefaultAllocator(), types);
    for (idx_t offset = 0; offset < values.size(); offset += STANDARD_VECTOR_SIZE) {
      const auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, values.size() - offset);
      chunk.Reset();
      std::memcpy(FlatVector::GetDataMutable<T>(chunk.data[0]), values.data() + offset,
                  count * sizeof(T));
      chunk.SetCardinality(count);
      appender.Append(chunk, 0, count, count);
    }
    ArrowArray appended = appender.Finalize();
    ArrowArrayMove(appended.children[0], out);
    appended.release(&appended);
  }

  LogicalType type;
  //! The distinct values in index order, strings kept in the heap
  vector<Stored> values;
  vector<hash_t> hashes;
  StringHeap heap;
  //! Open addressing table of value index + 1, 0 for an empty slot
  vector<uint32_t> slots;
  unordered_map<string, vector<int32_t>> remaps;
  vector<int32_t> remap_scratch;
  nanoarrow::UniqueBuffer indices;
  nanoarrow::UniqueBitmap validity;
  idx_t row_count = 0;
  idx_t null_count = 0;
};

template <class T>
unique_ptr<ArrowDictionaryBuilder> CreateFixedWidth(const LogicalType& type) {
  return make_uniq<TypedDictionaryBuilder<T, FixedWidthValues<T>>>(type);
}

bool IsSupportedPhysicalType(PhysicalType type) {
  switch (type) {
    case PhysicalType::BOOL:
    case PhysicalType::INT8:
    case PhysicalType::INT16:
    case PhysicalType::INT32:
    case PhysicalType::INT64:
    case PhysicalType::UINT8:
    case PhysicalType::UINT16:
    case PhysicalType::UINT32:
    case PhysicalType::UINT64:
    case PhysicalType::INT128:
    case PhysicalType::UINT128:
    case PhysicalType::FLOAT:
    case PhysicalType::DOUBLE:
    case PhysicalType::INTERVAL:
    case PhysicalType::VARCHAR:
      return true;
    default:
      return false;
  }
}

}  // namespace

void ArrowDictionaryBuilder::CheckType(const LogicalType& type, const ArrowSchema& values,
                                       const string& column) {
  if (!IsSupportedPhysicalType(type.InternalType()) || values.n_children != 0 ||
      values.dictionary) {
    throw BinderException("DICTIONARY_ENCODE does not support column \"%s\" of type %s",
                          column, type.ToString());
  }
  if (type.id() == LogicalTypeId::TIME_TZ) {
    // Distinct values with different offsets would convert to equal dictionary values
    throw BinderException(
        "DICTIONARY_ENCODE does not support column \"%s\" of type %s, whose Arrow "
        "conversion drops the time zone offset",
        column, type.ToString());
  }
  // The extension name sits on the field, readers resolve the dictionary type without it
  ArrowStringView extension_name{nullptr, 0};
  NANOARROW_THROW_NOT_OK(ArrowMetadataGetValue(
      values.metadata, ArrowCharView("ARROW:extension:name"), &extension_name));
  if (extension_name.data) {
    throw BinderException(
        "DICTIONARY_ENCODE does not support column \"%s\" of type %s, which is written "
        "as the Arrow extension type %s",
        column, type.ToString(),
        string(extension_name.data, NumericCast<idx_t>(extension_name.size_bytes)));
  }
}

unique_ptr<ArrowDictionaryBuilder> ArrowDictionaryBuilder::Create(
    const LogicalType& type) {
  switch (type.InternalType()) {
    case PhysicalType::BOOL:
      return CreateFixedWidth<bool>(type);
    case PhysicalType::INT8:
      return CreateFixedWidth<int8_t>(type);
    case PhysicalType::INT16:
      return CreateFixedWidth<int16_t>(type);
    case PhysicalType::INT32:
      return CreateFixedWidth<int32_t>(type);
    case PhysicalType::INT64:
      return CreateFixedWidth<int64_t>(type);
    case PhysicalType::UINT8:
      return CreateFixedWidth<uint8_t>(type);
    case PhysicalType::UINT16:
      return CreateFixedWidth<uint16_t>(type);
    case PhysicalType::UINT32:
      return CreateFixedWidth<uint32_t>(type);
    case PhysicalType::UINT64:
      return CreateFixedWidth<uint64_t>(type);
    case PhysicalType::INT128:
      return CreateFixedWidth<hugeint_t>(type);
    case PhysicalType::UINT128:
      return CreateFixedWidth<uhugeint_t>(type);
    case PhysicalType::FLOAT:
      return CreateFixedWidth<float>(type);
    case PhysicalType::DOUBLE:
      return CreateFixedWidth<double>(type);
    case PhysicalType::INTERVAL:
      return CreateFixedWidth<interval_t>(type);
    case PhysicalType::VARCHAR:
      return make_uniq<TypedDictionaryBuilder<string_t, StringValues>>(type);
    default:
      throw InternalException("No dictionary builder for type %s", type.ToString());
  }
}

bool ArrowBatchBuilder::IsDictionaryColumn(const ArrowSchema& schema,
                                           const LogicalType& type, idx_t index) {
  return schema.children[index]->dictionary && type.id() != LogicalTypeId::ENUM;
}

ArrowBatchBuilder::ArrowBatchBuilder(const vector<LogicalType>& types,
                                     const ArrowSchema& schema, idx_t capacity,
                                     ClientProperties options_p,
                                     const ArrowExtensionTypes& extension_types_p)
    : options(std::move(options_p)),
      extension_types(extension_types_p),
      column_count(types.size()) {
  D_ASSERT(static_cast<idx_t>(schema.n_children) == types.size());
  dictionaries.resize(types.size());
  ArrowExtensionTypes appended_extension_types;
  for (idx_t i = 0; i < types.size(); i++) {
    if (IsDictionaryColumn(schema, types[i], i)) {
      dictionaries[i] = ArrowDictionaryBuilder::Create(types[i]);
      continue;
    }
    auto extension_type = extension_types.find(i);
    if (extension_type != extension_types.end()) {
      appended_extension_types.emplace(appended_columns.size(), extension_type->second);
    }
    appended_columns.push_back(i);
    appended_types.push_back(types[i]);
  }
  if (!appended_columns.empty()) {
    appender = make_uniq<ArrowAppender>(appended_types, capacity, options,
                                        appended_extension_types);
    if (appended_columns.size() < column_count) {
      appended_chunk.InitializeEmpty(appended_types);
    }
  }
}

ArrowBatchBuilder::~ArrowBatchBuilder() = default;

void ArrowBatchBuilder::Append(DataChunk& input) {
  const auto count = input.size();
  if (appender) {
    if (appended_columns.size() == column_count) {
      appender->Append(input, 0, count, count);
    } else {
      appended_chunk.ReferenceColumns(input, appended_columns);
      appender->Append(appended_chunk, 0, count, count);
    }
  }
  for (idx_t i = 0; i < column_count; i++) {
    if (dictionaries[i]) {
      dictionaries[i]->Append(input.data[i], count);
    }
  }
  row_count += count;
}

nanoarrow::UniqueArray ArrowBatchBuilder::Finalize() {
  nanoarrow::UniqueArray result;
  if (appended_columns.size() == column_count) {
    ArrowArray finalized = appender->Finalize();
    ArrowArrayMove(&finalized, result.get());
    return result;
  }
  NANOARROW_THROW_NOT_OK(ArrowArrayInitFromType(result.get(), NANOARROW_TYPE_STRUCT));
  NANOARROW_THROW_NOT_OK(
      ArrowArrayAllocateChildren(result.get(), NumericCast<int64_t>(column_count)));
  result->length = NumericCast<int64_t>(row_count);
  result->null_count = 0;
  if (appender) {
    ArrowArray appended = appender->Finalize();
    for (idx_t i = 0; i < appended_columns.size(); i++) {
      ArrowArrayMove(appended.children[i], result->children[appended_columns[i]]);
    }
    // Releases nothing but DuckDB's root, every child was moved out
    appended.release(&appended);
  }
  for (idx_t i = 0; i < column_count; i++) {
    if (!dictionaries[i]) {
      continue;
    }
    auto extension_type = extension_types.find(i);
    dictionaries[i]->Finalize(options,
                              extension_type == extension_types.end()
                                  ? shared_ptr<ArrowTypeExtensionData>()
                                  : extension_type->second,
                              result->children[i]);
  }
  return result;
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
