cmake_minimum_required(VERSION 3.20)

project(
    PulseGrid
    VERSION 0.1.0
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(pulsegrid
    src/shm_region.cpp
)

target_include_directories(pulsegrid
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
)

target_compile_options(pulsegrid
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
        -Wconversion
        -Wshadow
)

add_executable(pulsegrid_producer
    apps/producer.cpp
)

target_link_libraries(pulsegrid_producer
    PRIVATE pulsegrid
)

add_executable(pulsegrid_consumer
    apps/consumer.cpp
)

target_link_libraries(pulsegrid_consumer
    PRIVATE pulsegrid
)

include(CTest)

if(BUILD_TESTING)
    find_package(GTest REQUIRED)

    add_executable(pulsegrid_tests
        tests/ring_buffer_test.cpp
        tests/wraparound_test.cpp
        tests/shm_region_test.cpp
        tests/shm_queue_process_test.cpp
        tests/shm_queue_batch_test.cpp
        tests/state_store_test.cpp
    )

    target_link_libraries(pulsegrid_tests
        PRIVATE
            pulsegrid
            GTest::gtest_main
    )

    include(GoogleTest)
    gtest_discover_tests(pulsegrid_tests)
endif()

# Benchmarks

add_executable(
    pulsegrid_shm_throughput
    bench/shm_throughput.cpp
)

target_link_libraries(
    pulsegrid_shm_throughput
    PRIVATE
        pulsegrid
)

target_compile_options(
    pulsegrid_shm_throughput
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)

add_executable(
    pulsegrid_uds_throughput
    bench/uds_throughput.cpp
)

target_link_libraries(
    pulsegrid_uds_throughput
    PRIVATE
        pulsegrid
)

target_compile_options(
    pulsegrid_uds_throughput
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)

add_executable(
    pulsegrid_shm_latency
    bench/shm_latency.cpp
)

target_link_libraries(
    pulsegrid_shm_latency
    PRIVATE
        pulsegrid
)

target_compile_options(
    pulsegrid_shm_latency
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)

add_executable(
    pulsegrid_uds_latency
    bench/uds_latency.cpp
)

target_link_libraries(
    pulsegrid_uds_latency
    PRIVATE
        pulsegrid
)

target_compile_options(
    pulsegrid_uds_latency
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)

add_executable(
    pulsegrid_uds_batched_throughput
    bench/uds_batched_throughput.cpp
)

target_link_libraries(
    pulsegrid_uds_batched_throughput
    PRIVATE
        pulsegrid
)

target_compile_options(
    pulsegrid_uds_batched_throughput
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)

add_executable(
    pulsegrid_shm_batched_throughput
    bench/shm_batched_throughput.cpp
)

target_link_libraries(
    pulsegrid_shm_batched_throughput
    PRIVATE
        pulsegrid
)

target_compile_options(
    pulsegrid_shm_batched_throughput
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)
