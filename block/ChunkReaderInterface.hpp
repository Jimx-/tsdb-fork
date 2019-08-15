#ifndef CHUNKREADERINTERFACE_H
#define CHUNKREADERINTERFACE_H

#include <stdint.h>

#include "chunk/ChunkInterface.hpp"
#include "tagtree/tsid.h"

namespace tsdb {

namespace block {

class ChunkReaderInterface {
public:
    virtual std::pair<std::shared_ptr<chunk::ChunkInterface>, bool>
    chunk(tagtree::TSID tsid, uint64_t ref) = 0;
    virtual bool error() = 0;
    virtual uint64_t size() = 0;
    virtual ~ChunkReaderInterface() = default;
};

} // namespace block

} // namespace tsdb

#endif
