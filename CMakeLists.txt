cmake_minimum_required(VERSION 3.5)
include(FetchContent)

# Set extension name here
set(TARGET_NAME nanoarrow)

set(NANOARROW_IPC ON)
set(NANOARROW_NAMESPACE "DuckDBExt${TARGET_NAME}")
# nanoarrow main with IPC body compression (#935) and the IPC dictionary decoding fixes
# (#957, #960), move to a release tag once one ships it
fetchcontent_declare(nanoarrow
                     URL "https://github.com/apache/arrow-nanoarrow/archive/ea215b000788ec2e94fc88bad5249290c94b01cd.zip"
                     URL_HASH SHA256=7459578d315cae675c92f12da123ce5dd4c62b99ede37cf746dc218221d7c2e6
)
# nanoarrow compresses and decompresses itself with liblz4 and libzstd, from vcpkg in CI.
# Builds without vcpkg, such as DuckDB-Wasm, can opt out of the codecs: compressed IPC
# bodies are then rejected when read and never written
option(NANOARROW_IPC_COMPRESSION "Read and write zstd and lz4 compressed IPC bodies" ON)
if(NANOARROW_IPC_COMPRESSION)
  set(NANOARROW_IPC_WITH_ZSTD ON)
  # DuckDB's bundled zstd is namespaced C++ that nanoarrow's C sources cannot link against
  find_path(ZSTD_INCLUDE_DIR zstd.h REQUIRED)
  # nanoarrow looks lz4 up through pkgconfig unless these variables are set
  find_path(LZ4_INCLUDE_DIR lz4frame.h REQUIRED)
  find_library(LZ4_LIBRARY NAMES lz4 liblz4 REQUIRED)
  get_filename_component(LZ4_LIBRARY_DIR "${LZ4_LIBRARY}" DIRECTORY)
  set(NANOARROW_IPC_WITH_LZ4 ON)
  set(LZ4_INCLUDE_DIRS "${LZ4_INCLUDE_DIR}")
  set(LZ4_LIBRARY_DIRS "${LZ4_LIBRARY_DIR}")
  set(LZ4_LIBRARIES "${LZ4_LIBRARY}")
else()
  set(NANOARROW_IPC_WITH_ZSTD OFF)
  set(NANOARROW_IPC_WITH_LZ4 OFF)
endif()
# DuckDB instruments only its C++ flags, so nanoarrow's C sources take the same sanitizer
if(ENABLE_THREAD_SANITIZER)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=thread")
elseif(ENABLE_SANITIZER AND (FORCE_ASSERT OR RELEASE_SANITIZER))
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address")
endif()
# Both libraries link into the extension transitively through nanoarrow_ipc (CMake 3.24)
fetchcontent_makeavailable(nanoarrow)
# DuckDB puts its namespaced zstd.h on every target, nanoarrow must see the real one first
foreach(nanoarrow_ipc_target nanoarrow_ipc_static nanoarrow_ipc_shared)
  if(TARGET ${nanoarrow_ipc_target})
    # The imported zstd target exports the same directory, this keeps it out of isystem
    set_target_properties(${nanoarrow_ipc_target} PROPERTIES NO_SYSTEM_FROM_IMPORTED ON)
    target_include_directories(${nanoarrow_ipc_target} BEFORE
                               PRIVATE "${ZSTD_INCLUDE_DIR}")
  endif()
endforeach()

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/file_scanner/arrow_file_scan.cpp
    src/file_scanner/arrow_multi_file_info.cpp
    src/ipc/array_stream.cpp
    src/ipc/codecs.cpp
    src/ipc/stream_factory.cpp
    src/ipc/stream_reader/base_stream_reader.cpp
    src/ipc/stream_reader/ipc_file_stream_reader.cpp
    src/ipc/stream_reader/ipc_buffer_stream_reader.cpp
    src/ipc/stream_reader/concurrent_reads.cpp
    src/scanner/arrow_kv_metadata.cpp
    src/scanner/read_arrow.cpp
    src/scanner/scan_arrow_ipc.cpp
    src/nanoarrow_extension.cpp
    src/writer/arrow_stream_writer.cpp
    src/writer/column_data_collection_serializer.cpp
    src/writer/write_arrow_stream.cpp
    src/writer/to_arrow_ipc.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link nanoarrow in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} nanoarrow::nanoarrow nanoarrow::nanoarrow_ipc)
target_link_libraries(${LOADABLE_EXTENSION_NAME} nanoarrow::nanoarrow
                      nanoarrow::nanoarrow_ipc)

install(TARGETS ${EXTENSION_NAME}
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
