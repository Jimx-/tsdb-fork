cmake_minimum_required(VERSION 3.5)
project(tsdb)

set(CMAKE_CXX_STANDARD 17)

set(TOPDIR ${PROJECT_SOURCE_DIR})
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${TOPDIR}/cmake")

FIND_PACKAGE(Boost COMPONENTS system filesystem iostreams)
if (NOT Boost_FOUND)
    message(FATAL_ERROR "Fatal error: Boost (version >= 1.55) required.")
else()
    message(STATUS "Setting up BOOST")
    message(STATUS " Includes - ${Boost_INCLUDE_DIRS}")
    message(STATUS " Library  - ${Boost_LIBRARY_DIRS}")
    message(STATUS " boost_system ${Boost_SYSTEM_LIBRARY}")
    message(STATUS " boost_filesystem ${Boost_FILESYSTEM_LIBRARY}")
    message(STATUS " boost_iostreams ${Boost_IOSTREAMS_LIBRARY}")
    include_directories(${Boost_INCLUDE_DIRS})
    link_directories(${Boost_LIBRARY_DIRS})
endif (NOT Boost_FOUND)

add_subdirectory(promql)
add_subdirectory(cxxopts)

include_directories(
    ${TOPDIR}
    ${TOPDIR}/promql/include
    ${TOPDIR}/tagtree/include
    ${TOPDIR}/cxxopts/include    
)

add_subdirectory(tagtree)

set(SOURCE_FILES
    main.cpp)
            
set(HEADER_FILES )

set(EXT_SOURCE_FILES )

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

set(LIBRARIES
    pthread
    atomic
    simple-web-server
    tagtree
    promql
    -Wl,--whole-archive
    Base
    Block
    Chunk
    Compact
    DB
    External
    Head
    Index
    Label
    Querier
    Tombstone
    TSDBUtil
    Wal
    -Wl,--no-whole-archive
    ${Boost_IOSTREAMS_LIBRARY}
    ${Boost_FILESYSTEM_LIBRARY}
    ${Boost_SYSTEM_LIBRARY}
)
 
add_executable(tsdb ${SOURCE_FILES} ${HEADER_FILES} ${EXT_SOURCE_FILES})
target_link_libraries(tsdb ${LIBRARIES}) 

add_executable(tsbs tsbs.cpp base_data_loader.cpp insert_data_loader.cpp genzipf.c mixed_data_loader.cpp)
target_link_libraries(tsbs ${LIBRARIES})

add_executable(nebench nebench.cpp)
target_link_libraries(nebench ${LIBRARIES})
