include_directories(..)
aux_source_directory(. DIR_LIB_SRCS)
#file(GLOB DIR_LIB_SRCS
#DBUtils.cpp)
add_library (DB ${DIR_LIB_SRCS})