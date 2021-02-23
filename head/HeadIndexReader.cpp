#include <iostream>
#include <set>
#include <vector>

#include "base/Logging.hpp"
#include "head/Head.hpp"
#include "head/HeadIndexReader.hpp"
#include "head/HeadUtils.hpp"
#include "tsdbutil/ListStringTuples.hpp"

namespace tsdb {
namespace head {

HeadIndexReader::HeadIndexReader(Head* head, int64_t min_time, int64_t max_time)
    : head(head), min_time(min_time), max_time(max_time)
{}

// postings returns the postings list iterator for the label pair.
std::pair<std::unique_ptr<index::PostingsInterface>, bool>
HeadIndexReader::get_all_postings()
{
    auto p = head->posting_list->all();
    if (p)
        return {std::move(p), true};
    else
        return {nullptr, false};
}

bool HeadIndexReader::series(
    tagtree::TSID tsid, std::vector<std::shared_ptr<chunk::ChunkMeta>>& chunks)
{
    std::shared_ptr<MemSeries> s = head->series->get_by_id(tsid);
    if (!s) {
        // LOG_ERROR << "not existed, series id: " << tsid;
        return false;
    }

    base::MutexLockGuard series_lock(s->mutex_);
    int i = 0;
    for (auto const& chk : s->chunks) {
        // Do not expose chunks that are outside of the specified range.
        if (!chk->overlap_closed(min_time, max_time)) {
            i++;
            continue;
        }
        chunks.push_back(std::shared_ptr<chunk::ChunkMeta>(new chunk::ChunkMeta(
            s->chunk_id(i), chk->min_time, chk->max_time)));
        ++i;
    }

    return true;
}

uint64_t HeadIndexReader::size() { return 0; }

// TODO(Alec).
bool HeadIndexReader::error() { return false; }

} // namespace head
} // namespace tsdb
