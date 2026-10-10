cmake_minimum_required(VERSION 3.20)
project(dremel_cpp LANGUAGES CXX)
set(DREMEL_CXX_STANDARD "26" CACHE STRING "C++ language standard for the engine")
set(CMAKE_CXX_STANDARD ${DREMEL_CXX_STANDARD})
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
find_package(Threads REQUIRED)
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

set(DREMEL_ENGINE_HEADERS
    src/app/cli.hpp
    src/core/types.hpp
    src/execution/aggregate.hpp
    src/execution/evaluator.hpp
    src/execution/memory.hpp
    src/execution/relational.hpp
    src/execution/scalar.hpp
    src/nested/levels.hpp
    src/optimizer/optimizer.hpp
    src/server/protocol.hpp
    src/server/scheduler.hpp
    src/sql/parser.hpp
    src/storage/catalog.hpp
    src/storage/interoperable.hpp
    src/tests/self_test.hpp)

add_executable(dremel-cpp src/main.cpp ${DREMEL_ENGINE_HEADERS})
target_compile_options(dremel-cpp PRIVATE $<$<CONFIG:Release>:-O3> $<$<CONFIG:Release>:-DNDEBUG>)
target_link_libraries(dremel-cpp PRIVATE Threads::Threads Arrow::arrow_shared Parquet::parquet_shared)
add_executable(dremel-cpp-tests src/main.cpp ${DREMEL_ENGINE_HEADERS})
target_compile_options(dremel-cpp-tests PRIVATE -UNDEBUG)
target_link_libraries(dremel-cpp-tests PRIVATE Threads::Threads Arrow::arrow_shared Parquet::parquet_shared)
enable_testing()
add_test(NAME dremel_cpp_tests COMMAND dremel-cpp-tests self-test ${CMAKE_SOURCE_DIR}/../workloads/queries)

add_executable(dremel-nested-scan src/nested_scan/scan.cpp)
target_compile_options(dremel-nested-scan PRIVATE $<$<CONFIG:Release>:-O3>)
target_link_libraries(dremel-nested-scan PRIVATE Arrow::arrow_shared Parquet::parquet_shared)
