#ifndef INDEXWRITERINTERFACE
#define INDEXWRITERINTERFACE

#include <deque>
#include <stdint.h>
#include <unordered_set>

#include "chunk/ChunkMeta.hpp"
#include "index/PostingsInterface.hpp"
#include "label/Label.hpp"

namespace tsdb {

namespace block {

class IndexWriterInterface {
public:
    // 0 succeed, -1 error
    virtual int add_series(
        const tagtree::TSID& tsid,
        const std::vector<std::shared_ptr<chunk::ChunkMeta>>& chunks) = 0;

    virtual ~IndexWriterInterface() {}
};

} // namespace block

} // namespace tsdb

#endif
