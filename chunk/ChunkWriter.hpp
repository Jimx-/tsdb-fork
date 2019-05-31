#ifndef CHUNKWRITER_H
#define CHUNKWRITER_H

#include <deque>
#include <stdio.h>

#include "block/ChunkWriterInterface.hpp"
#include "chunk/ChunkMeta.hpp"

namespace tsdb {
namespace chunk {

class ChunkWriter : public block::ChunkWriterInterface {
private:
    std::string dir; // "[ulid]/chunks"
    std::deque<FILE*> files;
    std::deque<int> seqs;
    uint64_t pos;
    uint64_t chunk_size;

public:
    // Implicit construct from const char *
    ChunkWriter(const std::string& dir);

    FILE* tail();

    // finalize_tail writes all pending data to the current tail file and close
    // it
    void finalize_tail();

    void cut();

    void write(const uint8_t* bytes, int size);

    void write_chunks(const std::vector<std::shared_ptr<ChunkMeta>>& chunks);

    uint64_t seq();

    void close();

    ~ChunkWriter();
};

} // namespace chunk
} // namespace tsdb

#endif
