cmake_minimum_required (VERSION 2.8)

project(tsdb-unittest)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -g -w -std=c++11")
message(STATUS "CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS}")

FIND_PACKAGE(Boost COMPONENTS system filesystem iostreams unit_test_framework)
if (NOT Boost_FOUND)
    message(FATAL_ERROR "Fatal error: Boost (version >= 1.55) required.")
else()
    message(STATUS "Setting up BOOST")
    message(STATUS " Includes - ${Boost_INCLUDE_DIRS}")
    message(STATUS " Library  - ${Boost_LIBRARY_DIRS}")
    message(STATUS " boost_system ${Boost_SYSTEM_LIBRARY}")
    message(STATUS " boost_filesystem ${Boost_FILESYSTEM_LIBRARY}")
    message(STATUS " boost_iostreams ${Boost_IOSTREAMS_LIBRARY}")
    message(STATUS " boost_unit_test_framework ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY}")
    include_directories(${Boost_INCLUDE_DIRS})
    link_directories(${Boost_LIBRARY_DIRS})
endif (NOT Boost_FOUND)

find_package(GTest REQUIRED)
if (NOT GTest_FOUND)
    message(FATAL_ERROR "Fatal error: GTest not found!")
endif (NOT GTest_FOUND)

#find_package(GMock REQUIRED)
#if (NOT GMock_FOUND)
#    message(FATAL_ERROR "Fatal error: GMock not found!")
#endif (NOT GMock_FOUND)

find_package(Threads REQUIRED)

include_directories(${GTEST_INCLUDE_DIRS})
include_directories(..)

add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../base"  "${CMAKE_CURRENT_BINARY_DIR}/base")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../block"  "${CMAKE_CURRENT_BINARY_DIR}/block")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../chunk"  "${CMAKE_CURRENT_BINARY_DIR}/chunk")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../compact"  "${CMAKE_CURRENT_BINARY_DIR}/compact")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../db"  "${CMAKE_CURRENT_BINARY_DIR}/db")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../external"  "${CMAKE_CURRENT_BINARY_DIR}/external")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../head"  "${CMAKE_CURRENT_BINARY_DIR}/head")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../index"  "${CMAKE_CURRENT_BINARY_DIR}/index")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../label"  "${CMAKE_CURRENT_BINARY_DIR}/label")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../querier"  "${CMAKE_CURRENT_BINARY_DIR}/querier")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../tombstone"  "${CMAKE_CURRENT_BINARY_DIR}/tombstone")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../wal"  "${CMAKE_CURRENT_BINARY_DIR}/wal")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../tsdbutil"  "${CMAKE_CURRENT_BINARY_DIR}/tsdbutil")

add_executable(UnitTest 
    db_bench.cpp
    db_test.cpp
    unittest_main.cpp
)

target_link_libraries(
    UnitTest 
    Base 
    Block
    Chunk
    Compact
    DB
    External
    Head
    Index
    Label
    #QL
    #QL_LEXER
    Querier
    Tombstone
    TSDBUtil
    Wal
    ${Boost_IOSTREAMS_LIBRARY}
    ${Boost_FILESYSTEM_LIBRARY}
    ${Boost_SYSTEM_LIBRARY}
    ${GTEST_BOTH_LIBRARIES}
    ${CMAKE_THREAD_LIBS_INIT}
)