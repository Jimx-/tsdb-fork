include_directories(..)
#aux_source_directory(. DIR_LIB_SRCS)
file(GLOB DIR_LIB_SRCS
    ulid_uint128.cpp
    xxhash.cpp
)

add_library (External ${DIR_LIB_SRCS})