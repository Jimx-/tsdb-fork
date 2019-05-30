include_directories(..)
aux_source_directory(. DIR_LIB_SRCS)

add_library (TSDBUtil ${DIR_LIB_SRCS})