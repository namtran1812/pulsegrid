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
    )

    target_link_libraries(pulsegrid_tests
        PRIVATE
            pulsegrid
            GTest::gtest_main
    )

    include(GoogleTest)
    gtest_discover_tests(pulsegrid_tests)
endif()
