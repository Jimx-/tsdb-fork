#ifndef HEADINDEXREADER_H
#define HEADINDEXREADER_H

#include "block/IndexReaderInterface.hpp"
#include "tsdbutil/StringTuplesInterface.hpp"

namespace tsdb {
namespace head {

class Head;

class HeadIndexReader : public block::IndexReaderInterface {
private:
    Head* head;
    int64_t min_time;
    int64_t max_time;

public:
    HeadIndexReader(Head* head, int64_t min_time, int64_t max_time);

    std::pair<std::unique_ptr<index::PostingsInterface>, bool>
    get_all_postings();

    bool series(tagtree::TSID tsid,
                std::vector<std::shared_ptr<chunk::ChunkMeta>>& chunks);

    bool error();
    uint64_t size();
};

} // namespace head
} // namespace tsdb

#endif
