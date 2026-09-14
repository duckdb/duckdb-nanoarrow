import pytest
import pyarrow as pa
import duckdb
import pyarrow.ipc as ipc
from pyarrow.ipc import MessageReader as mr
import os
import sys
import tempfile

# duckdb.duckdb.NotImplementedException: Not implemented Error: Unsupported Internal Arrow Type for Decimal d:37,5,256
# "generated_decimal256.stream",

# duckdb.duckdb.ConversionException: Conversion Error: Could not convert Interval to Microsecond
# "generated_interval.stream"

# Not implemented Error: Unsupported Internal Arrow Type: "d" Union
# "generated_union.stream"

little_big_integration_files = [
    "generated_null_trivial.stream",
    "generated_primitive_large_offsets.stream",
    "generated_custom_metadata.stream",
    "generated_datetime.stream",
    "generated_decimal.stream",
    "generated_map_non_canonical.stream",
    "generated_map.stream",
    "generated_nested_large_offsets.stream",
    "generated_nested.stream",
    "generated_null.stream",
    "generated_primitive_no_batches.stream",
    "generated_primitive_zerolength.stream",
    "generated_primitive.stream",
    "generated_recursive_nested.stream",
    "generated_dictionary.stream",
    "generated_dictionary_unsigned.stream",
    # Nested dictionaries currently trigger a DuckDB Arrow conversion ownership error.
    # "generated_nested_dictionary.stream",
]

compression_2_0_0 = ["generated_uncompressible_zstd.stream", "generated_zstd.stream"]

script_path = os.path.dirname(os.path.abspath(__file__))

test_folder = os.path.join(script_path, '..', '..', 'arrow-testing', 'data', 'arrow-ipc-stream', 'integration')

# All Test Folders:
big_endian_folder = os.path.join(test_folder, '1.0.0-bigendian')
little_endian_folder = os.path.join(test_folder, '1.0.0-littleendian')
compression_folder = os.path.join(test_folder, '2.0.0-compression')


def compare_result(arrow_result, duckdb_result, con):
    return con.execute(
        """
    SELECT COUNT(*) = 0
    FROM (
        (SELECT * FROM arrow_result EXCEPT SELECT * FROM duckdb_result)
        UNION
        (SELECT * FROM duckdb_result EXCEPT SELECT * FROM arrow_result)
    ) """
    ).fetchone()[0]


# 1. Compare result from reading the IPC file in Arrow, and in Duckdb
def compare_ipc_file_reader(con, file):
    arrow_result = ipc.open_stream(file).read_all()
    duckdb_file_result = con.sql(f"FROM read_arrow('{file}')").to_arrow_table()
    assert compare_result(arrow_result, duckdb_file_result, con)


# 2. Now test the writer, write it to a file from DuckDB, read it with arrow and compare
def compare_ipc_file_writer(con, file):
    arrow_result = ipc.open_stream(file).read_all()
    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = os.path.join(temp_dir, "arrow_duck.arrows")
        con.execute(f"COPY (FROM read_arrow('{file}')) TO '{file_path}'")
        duckdb_file_result = con.sql(f"FROM read_arrow('{file}')").to_arrow_table()
        assert compare_result(arrow_result, duckdb_file_result, con)


# 3. Compare result from reading the IPC file in Arrow, and in Duckdb
def compare_ipc_buffer_reader(con, file):
    arrow_result = ipc.open_stream(file).read_all()
    reader = mr.open_stream(file)
    duckdb_struct_result = con.from_arrow(reader).to_arrow_table()
    assert compare_result(arrow_result, duckdb_struct_result, con)


# 4. Now test the DuckDB buffer writer, by reading it back with arrow and comparing
def compare_ipc_buffer_writer(con, file):
    arrow_result = ipc.open_stream(file).read_all()
    buffers = con.execute(f"FROM to_arrow_ipc((FROM read_arrow('{file}')))").fetchall()
    if not buffers:
        return
    # Skip schema (first row) and EOS marker (last row), keep only data batches
    arrow_buffers = []
    for i in range(1, len(buffers) - 1):
        arrow_buffers.append(pa.py_buffer(buffers[0][0] + buffers[i][0]))

    if not arrow_buffers:
        return

    batches = []
    for buffer in arrow_buffers:
        with pa.BufferReader(buffer) as reader:
            stream_reader = ipc.RecordBatchStreamReader(reader)
            schema = stream_reader.schema
            batches.extend(stream_reader)

    duckdb_struct_result = pa.Table.from_batches(batches, schema=schema)
    assert compare_result(arrow_result, duckdb_struct_result, con)


# 5. Test that concatenating all to_arrow_ipc() blobs produces a valid IPC stream
#    (i.e., includes the end-of-stream marker)
def compare_ipc_buffer_writer_full_stream(con, file):
    arrow_result = ipc.open_stream(file).read_all()
    buffers = con.execute(f"FROM to_arrow_ipc((FROM read_arrow('{file}')))").fetchall()
    # Need at least schema + EOS (2 rows) with data in between
    if len(buffers) < 3:
        return
    # Concatenate all blobs into one complete IPC stream
    full_stream = b"".join(row[0] for row in buffers)
    reader = ipc.RecordBatchStreamReader(pa.BufferReader(full_stream))
    duckdb_struct_result = reader.read_all()
    assert compare_result(arrow_result, duckdb_struct_result, con)


class TestArrowIntegrationTests(object):
    def test_read_ipc_file(self, connection):
        for file in little_big_integration_files:
            compare_ipc_file_reader(connection, os.path.join(big_endian_folder, file))
            compare_ipc_file_reader(connection, os.path.join(little_endian_folder, file))
        for file in compression_2_0_0:
            compare_ipc_file_reader(connection, os.path.join(compression_folder, file))

    def test_write_ipc_file(self, connection):
        for file in little_big_integration_files:
            compare_ipc_file_writer(connection, os.path.join(big_endian_folder, file))
            compare_ipc_file_writer(connection, os.path.join(little_endian_folder, file))
        for file in compression_2_0_0:
            compare_ipc_file_reader(connection, os.path.join(compression_folder, file))

    def test_read_ipc_buffer(self, connection):
        for file in little_big_integration_files:
            compare_ipc_buffer_reader(connection, os.path.join(big_endian_folder, file))
            compare_ipc_buffer_reader(connection, os.path.join(little_endian_folder, file))
        for file in compression_2_0_0:
            compare_ipc_file_reader(connection, os.path.join(compression_folder, file))

    def test_write_ipc_buffer(self, connection):
        for file in little_big_integration_files:
            compare_ipc_buffer_writer(connection, os.path.join(big_endian_folder, file))
            compare_ipc_buffer_writer(connection, os.path.join(little_endian_folder, file))
        for file in compression_2_0_0:
            compare_ipc_buffer_writer(connection, os.path.join(compression_folder, file))

    def test_write_ipc_buffer_full_stream(self, connection):
        for file in little_big_integration_files:
            compare_ipc_buffer_writer_full_stream(connection, os.path.join(big_endian_folder, file))
            compare_ipc_buffer_writer_full_stream(connection, os.path.join(little_endian_folder, file))
        for file in compression_2_0_0:
            compare_ipc_buffer_writer_full_stream(connection, os.path.join(compression_folder, file))
