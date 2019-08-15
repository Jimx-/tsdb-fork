#ifndef CHUNKREADER_H
#define CHUNKREADER_H

#include <deque>
#include <limits>
#include <stdint.h>

#include "block/ChunkReaderInterface.hpp"
#include "chunk/ChunkInterface.hpp"
#include "tsdbutil/ByteSlice.hpp"

namespace tsdb {
namespace chunk {

// TODO(Alec), more chunk types.
class ChunkReader : public block::ChunkReaderInterface {
private:
    std::deque<std::shared_ptr<tsdbutil::ByteSlice>> bs;

    bool err_;

    uint64_t size_;

public:
    // Implicit construct from const char *
    ChunkReader(const std::string& dir);

    // Validate the back of bs after each push_back
    bool validate();

    // Will return EmptyChunk when error
    std::pair<std::shared_ptr<ChunkInterface>, bool> chunk(tagtree::TSID,
                                                           uint64_t ref);

    bool error();

    uint64_t size();
};

} // namespace chunk
} // namespace tsdb

#endif
