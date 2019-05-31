cmake_minimum_required (VERSION 2.8)

project(tsdb)

set(CMAKE_CXX_STANDARD 17)

set(TOPDIR ${PROJECT_SOURCE_DIR})

include_directories(${TOPDIR})

add_subdirectory("${TOPDIR}/base"  "${CMAKE_CURRENT_BINARY_DIR}/base")
add_subdirectory("${TOPDIR}/block"  "${CMAKE_CURRENT_BINARY_DIR}/block")
add_subdirectory("${TOPDIR}/chunk"  "${CMAKE_CURRENT_BINARY_DIR}/chunk")
add_subdirectory("${TOPDIR}/compact"  "${CMAKE_CURRENT_BINARY_DIR}/compact")
add_subdirectory("${TOPDIR}/db"  "${CMAKE_CURRENT_BINARY_DIR}/db")
add_subdirectory("${TOPDIR}/external"  "${CMAKE_CURRENT_BINARY_DIR}/external")
add_subdirectory("${TOPDIR}/head"  "${CMAKE_CURRENT_BINARY_DIR}/head")
add_subdirectory("${TOPDIR}/index"  "${CMAKE_CURRENT_BINARY_DIR}/index")
add_subdirectory("${TOPDIR}/label"  "${CMAKE_CURRENT_BINARY_DIR}/label")
add_subdirectory("${TOPDIR}/querier"  "${CMAKE_CURRENT_BINARY_DIR}/querier")
add_subdirectory("${TOPDIR}/tombstone"  "${CMAKE_CURRENT_BINARY_DIR}/tombstone")
add_subdirectory("${TOPDIR}/wal"  "${CMAKE_CURRENT_BINARY_DIR}/wal")
add_subdirectory("${TOPDIR}/tsdbutil"  "${CMAKE_CURRENT_BINARY_DIR}/tsdbutil")

