#ifndef HEADCHUNKREADER_H
#define HEADCHUNKREADER_H

#include "block/ChunkReaderInterface.hpp"
#include "chunk/ChunkInterface.hpp"

namespace tsdb {
namespace head {

class Head;

class HeadChunkReader : public block::ChunkReaderInterface {
private:
    Head* head;
    int64_t min_time;
    int64_t max_time;

public:
    HeadChunkReader(Head* head, int64_t min_time, int64_t max_time);

    std::pair<std::shared_ptr<chunk::ChunkInterface>, bool>
    chunk(const tagtree::TSID& tsid, uint64_t ref);

    // Will not use.
    bool error() { return false; }
    uint64_t size() { return 0; }
};

} // namespace head
} // namespace tsdb

#endif
