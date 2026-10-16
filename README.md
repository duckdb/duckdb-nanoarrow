# nanoarrow for DuckDB

This extension, nanoarrow, allows you to read Arrow IPC streams and files. It serves a similar purpose as the now-deprecated [Arrow DuckDB core extension](https://github.com/duckdb/arrow).
However, it comes with the added functionality to query Arrow IPC files and is much better tested. This extension is released as a DuckDB Community Extension.
For compatibility reasons with the previous Arrow core extension, this extension is also aliased as `arrow`.

You can install and load it as:

```sql
-- arrow would also be a suitable name
INSTALL nanoarrow FROM community;
LOAD nanoarrow;
```

## Usage
Below is a complete example of how to use our extension to read an Arrow IPC file.
In addition to our extension, you will also need the `httpfs` extension installed and loaded to fetch the data directly from GitHub.

```sql
LOAD httpfs;
LOAD nanoarrow;
SELECT
    commit, message
  FROM
    'https://github.com/apache/arrow-experiments/raw/refs/heads/main/data/arrow-commits/arrow-commits.arrows'
  LIMIT 10;
```

```
┌───────────────────────────┬───────────────────────────────────────────────────────────────────────────────────────────┐
│          commit           │                                          message                                          │
│          varchar          │                                          varchar                                          │
├───────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
│ 49cdb0fe4e98fda19031c86…  │ GH-40370: [C++] Define ARROW_FORCE_INLINE for non-MSVC builds (#40372)                    │
│ 1d966e98e41ce817d1f8c51…  │ GH-40386: [Python] Fix except clauses (#40387)                                            │
│ 96f26a89bd73997f7532643…  │ GH-40227: [R] ensure executable files in `create_package_with_all_dependencies` (#40232)  │
│ ee1a8c39a55f3543a82fed9…  │ GH-40366: [C++] Remove const qualifier from Buffer::mutable_span_as (#40367)              │
│ 3d467ac7bfae03cf2db0980…  │ GH-20127: [Python][CI] Remove legacy hdfs tests from hdfs and hypothesis setup (#40363)   │
│ ef6ea6beed071ed070daf03…  │ GH-40345: [FlightRPC][C++][Java][Go] Add URI scheme to reuse connection (#40084)          │
│ 53e0c745ad491af98a5bf18…  │ GH-40153: [C++][Python] Fix test_gdb failures on 32-bit (#40293)                          │
│ 3ba6d286caad328b8572a3b…  │ GH-40059: [C++][Python] Basic conversion of RecordBatch to Arrow Tensor (#40064)          │
│ 4ce9a5edd2710fb8bf0c642…  │ GH-40153: [Python] Make `Tensor.__getbuffer__` work on 32-bit platforms (#40294)          │
│ 2445975162905bd8d9a42ff…  │ GH-40334: [C++][Gandiva] Add missing OpenSSL dependency to encrypt_utils_test.cc (#40338) │
├───────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────┤
│ 10 rows                                                                                                     2 columns │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

In the remainder of this section, we cover the supported parameters and usages for our IPC readers/writers.

### IPC Files

#### Write
Writing Arrow IPC output is done using the COPY statement. Below is a simple example of how you can use DuckDB to create an IPC file.

```sql
COPY (SELECT 42 as foofy, 'string' as stringy) TO "test.arrow";
```

`COPY` follows the Arrow file extension convention. `.arrow` files and `FORMAT ARROW` produce the Arrow IPC file format, including a footer for random access with readers such as `pyarrow.ipc.open_file`. `.arrows` files and `FORMAT ARROWS` produce the Arrow IPC streaming format, which the `to_arrow_ipc` buffer function also emits as messages.
For a different filename extension, specify the format explicitly:

```sql
COPY (SELECT 42 as foofy, 'string' as stringy) TO "test.ipc" (FORMAT ARROWS);
```

The Copy function of the Copy To Arrow File operation accepts the following parameters:
* `row_group_size`: The size of a row group. By default, the value is 122,880. A lower value may reduce performance but can be beneficial for streaming. It is important to note that this value is not exact, a slightly higher value divisible by 2,048 (DuckDB's standard vector size) may be used as the actual row group size.
* `chunk_size`: An alias for the `row_group_size` parameter.
* `row_group_size_bytes`: The size of row groups in bytes.
* `row_groups_per_file`: The maximum number of row groups per file. If this option is set, multiple files can be generated in a single `COPY` call. This means the specified path will create a directory, and the `row_group_size` parameter will also be used to determine the partition sizes.
* `kv_metadata`: Key-value metadata to be added to the file schema.
* `field_metadata`: Key-value metadata to be added to individual fields of the file schema, as a struct of column name to struct of metadata, e.g. `FIELD_METADATA {'id': {'unit': 'count'}}`. The keys are merged with the metadata DuckDB attaches to the field, so `ARROW:extension:name` and `ARROW:extension:metadata` can be set to tag a field with an extension type. For both options, values must be valid UTF-8 without NUL bytes and not `NULL`.
* `type_metadata_namespace`: A namespace string, neither empty nor `NULL` and outside the `ARROW` namespace that the Arrow format reserves, that describes the DuckDB type of every field, including nested list, array, struct, map and union children. `<namespace>:type` holds the type name without its parameters, for example `DECIMAL`, `UUID`, `LIST`, `ARRAY`, `STRUCT` or `MAP`. `<namespace>:type_details` holds the full type as `DESCRIBE` spells it, for example `DECIMAL(10,2)`, `INTEGER[3]` or `STRUCT(a INTEGER)[]`. Decimal fields additionally carry `<namespace>:type_precision` and `<namespace>:type_scale`. The entries struct that holds map keys and values gets no keys, since it is not a DuckDB type. For example `TYPE_METADATA_NAMESPACE 'VENDOR'` tags an `INTEGER[]` column with `VENDOR:type` set to `LIST` and `VENDOR:type_details` set to `INTEGER[]`, and its element with `INTEGER`. The keys are merged with the metadata DuckDB already attaches to a field, so an extension type is preserved, and `field_metadata` must not set any of the four keys of the namespace. No type metadata is written when the option is omitted.
* `compression`: The codec used to compress the record batch bodies: `uncompressed` (the default), `zstd` or `lz4`.
* `compression_level`: The compression level for the selected codec. For `zstd` this ranges from -131072 to 22 (default: 3, negative levels favour speed), for `lz4` from -65536 to 12 (default: 0, i.e. the fast mode; 3 and above use LZ4HC, negative levels select an acceleration). Requires `compression`.
* `dictionary_encode`: Column names, as strings or a list, whose values are written dictionary encoded: each record batch stores `int32` indices into the distinct values of the column in that batch, which a preceding dictionary batch holds. A dictionary is written again only when it differs from the one before it, so a column with few distinct values gets much smaller, and readers such as pyarrow load it as a dictionary array. Columns of any type without children can be encoded, except types written as an Arrow extension type, such as `UUID` with `arrow_lossless_conversion`, whose type a reader would lose, and `TIMETZ`, whose Arrow conversion drops the offset. Values are distinct when their bits differ, so `-0.0` and `0.0`, and each `NaN`, are separate `FLOAT` and `DOUBLE` dictionary values, which pandas rejects as categorical categories. Requires the stream format (`FORMAT ARROWS` or the `.arrows` extension), whose readers support a dictionary being replaced between batches. `ENUM` columns are always written dictionary encoded, and are only supported in streams.

```sql
COPY (FROM events) TO 'events.arrows' (DICTIONARY_ENCODE ('country', 'status'));
```

* `size_metadata`: Disabled by default. Adds `total_compressed_size` and `total_uncompressed_size` to schema metadata as decimal byte counts summed over the record batch bodies of each output file, including buffer padding. Excludes message headers, the schema, and file framing. The compressed total includes compression prefixes and padding as stored on disk. The uncompressed total counts the original buffers with padding. Both totals are equal for uncompressed output. Requires `FORMAT ARROW` or the `.arrow` extension and a seekable local output so the opening schema can be updated to match the footer.

```sql
COPY (SELECT * FROM range(10000)) TO 'sizes.arrow' (SIZE_METADATA);
```

The totals can be read with `SELECT key, value FROM arrow_kv_metadata('sizes.arrow') WHERE field_path IS NULL` or through `pyarrow.ipc.open_file('sizes.arrow').schema.metadata`.

If `row_group_size_bytes` and either `chunk_size` or `row_group_size` are used, the row groups will be defined by the smallest of these parameters.

For example, to write a zstd-compressed stream:

```sql
COPY (SELECT 42 as foofy, 'string' as stringy) TO "test.arrows" (COMPRESSION 'zstd');
```

`RETURN_STATS` makes `COPY` return one row per written file instead of the row count. `count`, `file_size_bytes` and `footer_size_bytes` describe the file (`footer_size_bytes` is `NULL` for a stream, which has no footer). Record batch statistics are in `extra_info`, so they are also available for remote outputs, where `size_metadata` is not:
* `record_batch_count`: The number of record batches in the file.
* `total_compressed_size`: The size of all record batch bodies as stored, including compression prefixes and buffer padding. It excludes message headers, the schema, the dictionary batches of `dictionary_encode` and `ENUM` columns, and file framing, and is counted the same way as `size_metadata`.
* `total_uncompressed_size`: The size of all record batch bodies after decompression, including buffer padding, without dictionary batches. It equals `total_compressed_size` for uncompressed output.
* `peak_read_memory_bytes`: An upper bound on the memory a reader needs at any point while decoding the file, `NULL` for a stream with dictionary encoded columns, whose dictionaries it does not count. It assumes the reader allocates in powers of two, as arrow-java does, and holds two adjacent record batches at once. It counts the metadata a file reader reads with each batch, the validity bitmaps such a reader allocates for columns written without one, and up to 63 bytes of alignment per allocation, so it can also size an arena that aligns each allocation to 64 bytes.
* `schema_size_bytes`: The size of the schema message as written, including its continuation marker, length prefix and padding. The record batches follow it and span `file_size_bytes - footer_size_bytes - 26 - schema_size_bytes` bytes in a file and `file_size_bytes - 8 - schema_size_bytes` in a stream, which includes the dictionary batches of `dictionary_encode` columns. Without `size_metadata` the files of one `COPY` share it, so their record batches can be joined into one stream, since each file writes every dictionary before its first record batch. A file with no record batches has an empty span, so a reader can skip it.

```sql
COPY (SELECT * FROM range(10000)) TO 's3://bucket/stats.arrow' (RETURN_STATS);
```

#### Read
You can consume the file using the `read_arrow` scanner. For example, to read the file we just created, you could run:
```sql
FROM read_arrow('test.arrow');
```

Similar to the copy function, the extension also registers `.arrows` and `.arrow` as valid extensions for the Arrow IPC format. This means that a replacement scan can be applied if that is the file extension, so the following would also be a valid query:
```sql
FROM 'test.arrow';
```

Files and streams whose record batch bodies are compressed with `zstd` or `lz4` (the two codecs allowed by the Arrow IPC format) are read transparently.

Besides single-file reading, our extension also fully supports multi-file reading, including all valid multi-file options.

If we were to create a second test file using:
```sql
COPY (SELECT 42 as foofy, 'string' as stringy) TO "test_2.arrow" (FORMAT ARROW);
```

We can then run a query that reads both files using a glob pattern or a list of file paths:

```sql
-- Glob
FROM read_arrow('*.arrow')

-- List
FROM read_arrow(['test.arrow','test_2.arrow'])
```

When reading multiple files, the following parameters are also supported:
* `union_by_name`: If the schemas of the files differ, setting `union_by_name` allows DuckDB to construct the schema by aligning columns with the same name.
* `filename`: If set to `True`, this will add a column with the name of the file that generated each row.
* `hive_partitioning`: Enables reading data from a Hive-partitioned dataset and applies partition filtering.

The key-value metadata of the schema and of its fields can be read with `arrow_kv_metadata`, which accepts the same file paths, globs and lists as `read_arrow`. The `field_path` column lists the field names from the top-level column down to the field and is `NULL` for schema-level entries:
```sql
SELECT field_path, key, value FROM arrow_kv_metadata('test.arrows');
```
> [!NOTE]
> [Arrow IPC files](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format) and [Arrow IPC streams](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format) are both standard Arrow formats. This extension reads both regardless of filename extension, and `COPY` writes a file for `.arrow` or `FORMAT ARROW` and a stream for `.arrows` or `FORMAT ARROWS`.
### IPC Stream Buffers
Similar to the old core Arrow extension, this extension also allows direct production and consumption of the Arrow IPC streaming format from in-memory buffers in both Python and Node.js.
In this section, we will demonstrate how to use the Python API, but you can find many tests that serve as examples for both [Node.js](https://github.com/paleolimbot/duckdb-nanoarrow/tree/main/test/nodejs) and [Python](https://github.com/paleolimbot/duckdb-nanoarrow/tree/main/test/python).

Our extension can create Arrow IPC buffers using the `to_arrow_ipc` function. This function returns two columns: one containing the serialized data as a `BLOB`, and another `BOOL` column indicating which tuples contain the header information of the messages. For example, consider the following table in our DuckDB database:
```python
import pyarrow as pa
import duckdb
import pyarrow.ipc as ipc

connection = duckdb.connect()
connection.execute("CREATE TABLE T (f0 integer, f1 varchar, f2 bool )")
connection.execute("INSERT INTO T values (1, 'foo', true),(2, 'bar', NULL), (3, 'baz', false), (4, NULL, true) ")
```

We can then obtain our buffers by simply issuing a `to_arrow_ipc` call, like this:

```python
buffers = connection.execute("FROM to_arrow_ipc((FROM T))").fetchall()
```
In this case, our buffers will contain two tuples: the first is the header of our message, and the second is the data. The record batch bodies can be compressed with the same `compression` and `compression_level` options as `COPY`, given as named parameters, e.g. `to_arrow_ipc((FROM T), compression := 'zstd')`. `dictionary_encode := ['country', 'status']`, a list of column names, writes those columns dictionary encoded, as `COPY` does. A data tuple of a result with dictionary encoded columns, `ENUM` columns included, holds the dictionary batches of its record batch before it. To convert this into an Arrow table, we simply concatenate the tuples and use the `ipc.RecordBatchStreamReader`. For example, you can read them as follows:


```python
batches = []
with pa.BufferReader(pa.py_buffer(buffers[0][0] + buffers[1][0])) as reader:
     stream_reader = ipc.RecordBatchStreamReader(reader)
     schema = stream_reader.schema
     batches.extend(stream_reader)
arrow_table = pa.Table.from_batches(batches, schema=schema)
```

To read buffers with DuckDB, you must use the Python function `from_arrow`. Continuing from our example, we would first need to convert our Arrow table into the Arrow IPC format.
```python
batch = arrow_table.to_batches()[0]
sink = pa.BufferOutputStream()
with pa.ipc.new_stream(sink, batch.schema) as writer:
     writer.write_batch(batch)
buffer = sink.getvalue()
buf_reader = pa.BufferReader(buffer)
msg_reader = ipc.MessageReader.open_stream(buf_reader)
```

After this, the following query will return a DuckDB relation with the deserialized Arrow IPC:

```python
connection.from_arrow(msg_reader)
```

## Building

To build the extension, clone the repository with submodules:

``` shell
git clone --recurse-submodules https://github.com/paleolimbot/duckdb-nanoarrow.git
```

...or if you forget to clone the submodules/you're using VSCode to do your checkout, you can run:

``` shell
git submodule init
git submodule update --checkout
```

A quick-and-dirty way to get your build up and running is to run `make`:

```sh
make

```
The main binaries that will be built are:

```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/nanoarrow/nanoarrow.duckdb_extension
```

- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `nanoarrow.duckdb_extension` is the loadable binary as it would be distributed.

If you'd like to use VSCode with the integration provided by the CMake/clangd extension, you
can run:

``` shell
cp CMakeUserPresets.json duckdb/
```

...and ensure that `.vscode/settings.json` contains:

``` json
{
    "cmake.sourceDirectory": "${workspaceFolder}/duckdb"
}
```

Then choose *Developer: Reload window* from the command palette and choose the
*Extension (Debug build)* preset.

## Running the extension

To run the extension code, simply start the shell with `./build/release/duckdb`
(if you're using `make` to build) or `./build/duckdb` (if you're using CMake
via VSCode).

Now we can use the features from the extension directly in DuckDB.

## Running the tests

Different tests can be created for DuckDB extensions. Tests are written in
SQL  `./test/sql`. These SQL tests can be run using `make test` (if using
make) or `./test_local.sh` (if using CMake via VSCode).

## Running the benchmarks

Benchmarks live in `./benchmark/micro` and run with DuckDB's benchmark runner,
which `BUILD_BENCHMARK=1 make release` builds with the extension linked in.

``` shell
build/release/benchmark/benchmark_runner benchmark/micro/read_large_batches.benchmark
```

The `Regression` workflow builds the runner for the base branch and for the pull
request, then runs the benchmarks in `.github/regression/micro.csv` with DuckDB's
`scripts/regression/test_runner.py` through `.github/regression/run.py`. It fails
when a benchmark errors or returns a wrong result, when the geometric mean of all
of them is 10% slower, or when a single benchmark is 10% slower in two runs in a
row, since the second run filters out runner noise. The same comparison runs
locally against two runners built from different checkouts.

``` shell
python3 .github/regression/run.py \
  --old ../base/build/release/benchmark/benchmark_runner \
  --new build/release/benchmark/benchmark_runner \
  --benchmarks .github/regression/micro.csv
```

## Debugging

You can debug an interactive SQL session by launching it with `gdb` or `lldb`:

``` shell
lldb build/duckdb
```

...or you can use the CodeLLDB extension (Command Palette: *LLDB: Attach to process*)
to launch a VSCode interactive debugger launched in a terminal.
