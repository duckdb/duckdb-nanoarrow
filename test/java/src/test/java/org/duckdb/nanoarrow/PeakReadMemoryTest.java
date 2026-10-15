package org.duckdb.nanoarrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.netty.NettyAllocationManager;
import org.apache.arrow.memory.unsafe.UnsafeAllocationManager;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Writes files with COPY ... (RETURN_STATS) and reads them back with arrow-java, which allocates in powers of two.
 * Reading the whole file, one record batch after another, must never need more memory than the
 * peak_read_memory_bytes the COPY returned, with the netty or the unsafe allocator.
 */
class PeakReadMemoryTest {
	private static final Path SHELL = Path.of(System.getProperty("duckdb.shell", "../../build/release/duckdb"));
	private static final Path EXTENSION = Path.of(
	    System.getProperty("duckdb.extension", "../../build/release/extension/nanoarrow/nanoarrow.duckdb_extension"));

	// Cover fixed width, variable width, nested, and columns without nulls, whose validity bitmap the writer leaves
	// out but arrow-java allocates
	private static final String[][] SHAPES = {
	    {"integers", "SELECT i AS id, i * 2 AS twice FROM range({rows}) t(i)"},
	    {"strings_with_nulls", "SELECT CASE WHEN i % 7 = 0 THEN NULL ELSE repeat('x', (i % 50)::INTEGER) END AS s "
	                           + "FROM range({rows}) t(i)"},
	    {"strings_without_nulls", "SELECT md5(i::VARCHAR) AS s, i::VARCHAR AS t FROM range({rows}) t(i)"},
	    {"nested", "SELECT {'a': i, 'b': [i, i + 1], 'c': {'d': i::VARCHAR}} AS st, [i::DOUBLE] AS l "
	               + "FROM range({rows}) t(i)"},
	    {"mixed", "SELECT i % 2 = 0 AS b, (i * 1.25)::DECIMAL(18,2) AS d, DATE '2024-01-01' + (i % 365)::INTEGER AS dt, "
	              + "CASE WHEN i % 3 = 0 THEN NULL ELSE i END AS n FROM range({rows}) t(i)"},
	};
	// Cases where the bound is easiest to get wrong: {name, query, rows per batch}. Each runs at its own size.
	private static final String[][] PATHOLOGICAL = {
	    // No batch at all, so only the minimum applies
	    {"empty", "SELECT i AS id, 'x' AS s FROM range(0) t(i)", "2048"},
	    // One row per batch, every buffer at the 2-byte minimum allocation
	    {"one_row_batches", "SELECT i AS id, i::VARCHAR AS s, i % 2 = 0 AS b FROM range(64) t(i)", "1"},
	    // A data buffer of exactly 2^16 bytes, and one 8 bytes past it, which rounds up to 2^17
	    {"power_of_two", "SELECT i AS v FROM range(8192) t(i)", "8192"},
	    {"just_over_power_of_two", "SELECT i AS v FROM range(8193) t(i)", "8193"},
	    // A few rows holding megabytes each, so one buffer dominates
	    {"huge_values", "SELECT i AS id, repeat(chr(65 + i::INTEGER), 3000000 + i::INTEGER) AS s FROM range(3) t(i)",
	     "1"},
	    // A tiny batch next to a large one, and a large batch followed by a one row batch
	    {"tiny_then_large", "SELECT CASE WHEN i < 2048 THEN 'a' ELSE repeat('x', 3000) END AS s FROM range(4096) t(i)",
	     "2048"},
	    {"large_then_tiny", "SELECT repeat('x', 3000) AS s FROM range(2049) t(i)", "2048"},
	    // Hundreds of buffers per batch, each with up to 63 bytes of alignment
	    {"wide", "SELECT " + columns(300, "i + %d AS c%d") + " FROM range(3000) t(i)", "2048"},
	    {"wide_strings", "SELECT " + columns(100, "(i + %d)::VARCHAR AS c%d") + " FROM range(3000) t(i)", "2048"},
	    // Columns of nothing but NULL, of several types
	    {"all_nulls", "SELECT NULL::INTEGER AS a, NULL::VARCHAR AS b, NULL::INTEGER[] AS c, "
	                  + "NULL::STRUCT(x INTEGER) AS d FROM range(5000) t(i)", "2048"},
	    // Six levels of nesting through struct, list and map
	    {"deep_nesting", "SELECT {'a': [{'b': [MAP {i: [{'c': i::VARCHAR}]}]}]} AS deep FROM range(3000) t(i)", "2048"},
	    // Lists with many elements each, so the child arrays are much longer than the batch
	    {"long_lists", "SELECT range(i % 1000) AS l FROM range(3000) t(i)", "2048"},
	    // A union, whose children each get every row
	    {"union", "SELECT CASE WHEN i % 2 = 0 THEN union_value(n := i)::UNION(n BIGINT, s VARCHAR) "
	              + "ELSE union_value(s := i::VARCHAR)::UNION(n BIGINT, s VARCHAR) END AS u FROM range(3000) t(i)",
	     "2048"},
	    // Data that zstd and lz4 cannot shrink
	    {"incompressible", "SELECT md5(i::VARCHAR) || md5((i * 7919)::VARCHAR) AS s, hash(i) AS h FROM range(5000) t(i)",
	     "2048"},
	};
	private static final String[] COMPRESSIONS = {"uncompressed", "zstd", "lz4"};
	private static final String[] FORMATS = {"arrow", "arrows"};
	private static final long[] ROWS = {1, 10000};

	static Stream<Arguments> cases() {
		List<Arguments> cases = new ArrayList<>();
		for (String[] shape : SHAPES) {
			for (long rows : ROWS) {
				add(cases, shape[0], shape[1].replace("{rows}", Long.toString(rows)), 2048);
			}
		}
		for (String[] shape : PATHOLOGICAL) {
			add(cases, shape[0], shape[1], Long.parseLong(shape[2]));
		}
		return cases.stream();
	}

	private static void add(List<Arguments> cases, String shape, String query, long chunkSize) {
		for (String compression : COMPRESSIONS) {
			for (String format : FORMATS) {
				for (String allocator : new String[] {"netty", "unsafe"}) {
					cases.add(Arguments.of(shape, query, chunkSize, compression, format, allocator));
				}
			}
		}
	}

	/** count select list entries from a format with the index in it twice, e.g. "i + %d AS c%d" */
	private static String columns(int count, String format) {
		List<String> columns = new ArrayList<>();
		for (int c = 0; c < count; c++) {
			columns.add(String.format(format, c, c));
		}
		return String.join(", ", columns);
	}

	@ParameterizedTest(name = "{0} {3} {4} {5}")
	@MethodSource("cases")
	void readingNeverNeedsMoreThanThePeak(String shape, String query, long chunkSize, String compression,
	                                      String format, String allocator, @TempDir Path directory) throws Exception {
		Path file = directory.resolve(shape + "." + format);
		long rows = Long.parseLong(duckdb("SELECT count(*) FROM (" + query + ")"));
		String options = "FORMAT " + format + ", CHUNK_SIZE " + chunkSize + ", RETURN_STATS" +
		                 (compression.equals("uncompressed") ? "" : ", COMPRESSION '" + compression + "'");
		String[] stats = duckdb("WITH s AS (COPY (" + query + ") TO '" + file + "' (" + options +
		                        ")) SELECT count, extra_info['peak_read_memory_bytes']::UBIGINT FROM s")
		                     .split(",");
		assertEquals(rows, Long.parseLong(stats[0]));
		long peakReadMemory = Long.parseLong(stats[1]);

		AllocationManager.Factory factory =
		    allocator.equals("netty") ? NettyAllocationManager.FACTORY : UnsafeAllocationManager.FACTORY;
		long readRows = 0;
		long peak;
		try (RootAllocator root = new RootAllocator(
		         RootAllocator.configBuilder().allocationManagerFactory(factory).build());
		     BufferAllocator reading = root.newChildAllocator("reading", 0, Long.MAX_VALUE);
		     FileInputStream in = new FileInputStream(file.toFile());
		     ArrowReader reader = format.equals("arrow")
		                              ? new ArrowFileReader(in.getChannel(), reading, CommonsCompressionFactory.INSTANCE)
		                              : new ArrowStreamReader(new BufferedInputStream(in), reading,
		                                                      CommonsCompressionFactory.INSTANCE)) {
			VectorSchemaRoot batch = reader.getVectorSchemaRoot();
			while (reader.loadNextBatch()) {
				readRows += batch.getRowCount();
			}
			peak = reading.getPeakMemoryAllocation();
		}
		assertEquals(rows, readRows);
		assertTrue(peak <= peakReadMemory,
		           "reading took " + peak + " bytes, more than the peak_read_memory_bytes of " + peakReadMemory);
	}

	/** Runs one statement in the duckdb shell, with the unsigned extension build loaded, and returns its single CSV row */
	private static String duckdb(String sql) throws IOException, InterruptedException {
		Process process = new ProcessBuilder(SHELL.toString(), "-unsigned", "-csv", "-noheader", "-bail", "-cmd",
		                                     "LOAD '" + EXTENSION.toAbsolutePath() + "'", "-c", sql)
		                      .redirectErrorStream(true)
		                      .start();
		String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
		assertTrue(process.waitFor(5, TimeUnit.MINUTES), "duckdb did not finish");
		assertEquals(0, process.exitValue(), output);
		return output;
	}
}
