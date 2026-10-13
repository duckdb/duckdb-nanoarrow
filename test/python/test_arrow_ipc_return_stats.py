import os

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

# Batches of different sizes and buffer layouts, so the pair peak depends on neighbours
QUERIES = {
    "integers": "SELECT i AS id, i * 2 AS twice FROM range({rows}) t(i)",
    "strings": "SELECT i AS id, repeat('x', (i % 97) * (i // 2048 + 1)) AS s FROM range({rows}) t(i)",
    "nested": "SELECT [i, NULL, i + 1] AS l, {'a': i, 'b': 'v' || i} AS st, MAP {i: 'm' || i} AS m"
    " FROM range({rows}) t(i)",
    "nulls": "SELECT CASE WHEN i % 3 = 0 THEN NULL ELSE i END AS n, (i / 7)::DECIMAL(18,3) AS d,"
    " i % 2 = 0 AS b FROM range({rows}) t(i)",
}
COMPRESSIONS = [None, "zstd", "lz4"]


def pad8(size):
    return (size + 7) & ~7


def next_pow2(size):
    return 1 if size <= 1 else 1 << (size - 1).bit_length()


def buffer_sizes(batch):
    """Every buffer of every column of a batch, nested children included"""
    sizes = []
    for column in batch.columns:
        sizes.extend(0 if buffer is None else buffer.size for buffer in column.buffers())
    return sizes


def dropped_validity(array):
    """The bitmaps a reader that keeps one per field allocates for fields written without
    one: ceil(length / 8) bytes each, rounded up to a power of two of at least 2"""
    size = 0
    has_validity = not (pa.types.is_null(array.type) or pa.types.is_union(array.type))
    if has_validity and len(array) > 0 and array.buffers()[0] is None:
        size += next_pow2(max((len(array) + 7) // 8, 2))
    if pa.types.is_struct(array.type):
        size += sum(dropped_validity(array.field(i)) for i in range(array.type.num_fields))
    elif pa.types.is_list(array.type) or pa.types.is_large_list(array.type) \
            or pa.types.is_fixed_size_list(array.type) or pa.types.is_map(array.type):
        size += dropped_validity(array.values)
    return size


def type_buffer_count(t):
    """Buffers an array of type t holds, its children's included, by Arrow's layouts"""
    if pa.types.is_null(t):
        return 0
    if pa.types.is_struct(t):
        return 1 + sum(type_buffer_count(t.field(i).type) for i in range(t.num_fields))
    if pa.types.is_map(t):
        return 2 + 1 + type_buffer_count(t.key_type) + type_buffer_count(t.item_type)
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        return 2 + type_buffer_count(t.value_type)
    if pa.types.is_fixed_size_list(t):
        return 1 + type_buffer_count(t.value_type)
    if pa.types.is_union(t):
        own = 2 if t.mode == "dense" else 1
        return own + sum(type_buffer_count(t.field(i).type) for i in range(t.num_fields))
    if pa.types.is_string(t) or pa.types.is_binary(t) or pa.types.is_large_string(t) \
            or pa.types.is_large_binary(t):
        return 3
    return 2


def schema_buffer_count(schema):
    """Buffers per batch, the root struct's validity included as the writer counts it"""
    return 1 + sum(type_buffer_count(field.type) for field in schema)


def schema_message_size(data, start):
    """The size of the schema message at start: the continuation marker, the int32 length
    and that many bytes of flatbuffer and padding"""
    assert data[start:start + 4] == b"\xff\xff\xff\xff"
    length = int.from_bytes(data[start + 4:start + 8], 'little')
    assert data[start + 8 + length:start + 8 + length + 4] == b"\xff\xff\xff\xff"
    return 8 + length


def expected_stats(path, compressed_output):
    """Recomputes the statistics from the written file, as the requirements define them"""
    with open(path, 'rb') as f:
        data = f.read()
    reader = ipc.open_file(path)
    messages = ipc.MessageReader.open_stream(pa.py_buffer(data[8:]))
    messages.read_next_message()  # the schema message
    bodies = []
    on_disk = []
    uncompressed = []
    previous_peak = 0
    pair_peak = 0
    for index in range(reader.num_record_batches):
        message = messages.read_next_message()
        bodies.append(message.body.size)
        on_disk.append(len(message.serialize()))
        sizes = buffer_sizes(reader.get_batch(index))
        uncompressed.append(sum(pad8(size) for size in sizes))
        if compressed_output:
            peak = next_pow2(on_disk[-1]) + sum(next_pow2(pad8(size)) for size in sizes)
        else:
            # A file reader reads the metadata in the same allocation as the body
            peak = next_pow2(on_disk[-1])
        peak += sum(dropped_validity(column) for column in reader.get_batch(index).columns)
        pair_peak = max(pair_peak, peak + previous_peak)
        previous_peak = peak
    return {
        "count": reader.read_all().num_rows,
        "file_size_bytes": len(data),
        "footer_size_bytes": int.from_bytes(data[-10:-6], 'little'),
        "record_batch_count": reader.num_record_batches,
        "total_compressed_size": sum(bodies),
        "total_uncompressed_size": sum(uncompressed),
        "peak_read_memory_bytes": max(pair_peak, 64) + 63 * (schema_buffer_count(reader.schema) + 2),
        # The schema message follows the 8-byte ARROW1 header
        "schema_size_bytes": schema_message_size(data, 8),
    }


def copy_with_stats(connection, query, path, compression, extra=""):
    options = "FORMAT ARROW, CHUNK_SIZE 2048, RETURN_STATS"
    if compression:
        options += f", COMPRESSION '{compression}'"
    rows = connection.execute(f"COPY ({query}) TO '{path}' ({options}{extra})").fetchall()
    columns = [description[0] for description in connection.description]
    return [dict(zip(columns, row)) for row in rows]


def returned_stats(row):
    stats = {key: row[key] for key in ("count", "file_size_bytes", "footer_size_bytes")}
    stats.update({key: int(value) for key, value in row["extra_info"].items()})
    return stats


@pytest.mark.parametrize("compression", COMPRESSIONS)
@pytest.mark.parametrize("rows", [1, 2048, 10000])
@pytest.mark.parametrize("shape", QUERIES)
def test_return_stats_match_the_file(connection, tmp_path, shape, rows, compression):
    path = str(tmp_path / f"{shape}.arrow")
    (row,) = copy_with_stats(connection, QUERIES[shape].replace("{rows}", str(rows)), path, compression)
    assert row["filename"] == path
    assert returned_stats(row) == expected_stats(path, compression is not None)


@pytest.mark.parametrize("compression", ["zstd", "lz4"])
def test_return_stats_totals_match_size_metadata(connection, tmp_path, compression):
    path = str(tmp_path / "sizes.arrow")
    query = QUERIES["strings"].replace("{rows}", "10000")
    (row,) = copy_with_stats(connection, query, path, compression, ", SIZE_METADATA")
    metadata = ipc.open_file(path).schema.metadata
    assert int(row["extra_info"]["total_compressed_size"]) == int(metadata[b"total_compressed_size"])
    assert int(row["extra_info"]["total_uncompressed_size"]) == int(metadata[b"total_uncompressed_size"])


@pytest.mark.parametrize("compression", COMPRESSIONS)
def test_return_stats_schema_size_with_size_metadata(connection, tmp_path, compression):
    path = str(tmp_path / "sizes.arrow")
    query = QUERIES["strings"].replace("{rows}", "10000")
    (row,) = copy_with_stats(connection, query, path, compression, ", SIZE_METADATA")
    with open(path, 'rb') as f:
        data = f.read()
    schema_size = int(row["extra_info"]["schema_size_bytes"])
    # The schema as it is on disk, after the totals are written into it
    assert schema_size == schema_message_size(data, 8)
    messages = ipc.MessageReader.open_stream(pa.py_buffer(data[8 + schema_size:]))
    assert messages.read_next_message().type == "record batch"
    # The totals take room in the schema, so it is larger than without them
    (plain,) = copy_with_stats(connection, query, str(tmp_path / "plain.arrow"), compression)
    assert schema_size > int(plain["extra_info"]["schema_size_bytes"])


def test_return_stats_one_row_per_file(connection, tmp_path):
    directory = tmp_path / "parts"
    query = QUERIES["integers"].replace("{rows}", "10000")
    rows = copy_with_stats(connection, query, str(directory), None, ", ROW_GROUPS_PER_FILE 2")
    assert len(rows) == len(os.listdir(directory)) == 3
    assert sum(row["count"] for row in rows) == 10000
    for row in rows:
        assert returned_stats(row) == expected_stats(row["filename"], False)
    # Every file of one COPY starts with the same schema message
    assert len({row["extra_info"]["schema_size_bytes"] for row in rows}) == 1


def test_return_stats_stream_has_no_footer(connection, tmp_path):
    path = str(tmp_path / "stream.arrows")
    connection.execute(f"COPY (SELECT 42 AS i) TO '{path}' (FORMAT ARROWS, RETURN_STATS)")
    columns = [description[0] for description in connection.description]
    (row,) = [dict(zip(columns, values)) for values in connection.fetchall()]
    assert row["count"] == 1
    assert row["file_size_bytes"] == os.path.getsize(path)
    assert row["footer_size_bytes"] is None


@pytest.mark.parametrize("compression", COMPRESSIONS)
def test_return_stats_stream_schema_size(connection, tmp_path, compression):
    path = str(tmp_path / "stream.arrows")
    query = QUERIES["nested"].replace("{rows}", "10000")
    options = "FORMAT ARROWS, CHUNK_SIZE 2048, RETURN_STATS"
    if compression:
        options += f", COMPRESSION '{compression}'"
    connection.execute(f"COPY ({query}) TO '{path}' ({options})")
    columns = [description[0] for description in connection.description]
    (row,) = [dict(zip(columns, values)) for values in connection.fetchall()]
    with open(path, 'rb') as f:
        data = f.read()
    schema_size = int(row["extra_info"]["schema_size_bytes"])
    # A stream starts with the schema message
    assert schema_size == schema_message_size(data, 0)
    messages = ipc.MessageReader.open_stream(pa.py_buffer(data[schema_size:]))
    assert messages.read_next_message().type == "record batch"
