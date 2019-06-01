#include "compact/CompactionChunkSeriesSet.hpp"
#include "base/Logging.hpp"

namespace tsdb {
namespace compact {

CompactionChunkSeriesSet::CompactionChunkSeriesSet(
    const std::shared_ptr<block::IndexReaderInterface>& ir,
    const std::shared_ptr<block::ChunkReaderInterface>& cr,
    const std::shared_ptr<tombstone::TombstoneReaderInterface>& tr,
    std::unique_ptr<index::PostingsInterface>&& p)
    : p(std::move(p)), ir(ir), cr(cr), tr(tr),
      csm(new querier::ChunkSeriesMeta()), err_()
{}

bool CompactionChunkSeriesSet::next() const
{
    if (!p->next()) return false;

    csm->clear();
    csm->tsid = p->at();
    try {
        csm->intervals = tr->get(p->at());
    } catch (const std::out_of_range& e) {
    }

    if (!ir->series(p->at(), csm->chunks)) {
        err_.wrap("Error get series " + p->at().to_string());
        // LOG_DEBUG << "next err: " << err_.error();
        return false;
    }

    // Remove completely deleted chunks.
    if (!csm->intervals.empty()) {
        std::vector<std::shared_ptr<chunk::ChunkMeta>>::iterator it =
            csm->chunks.begin();
        while (it != csm->chunks.end()) {
            if (tombstone::is_subrange((*it)->min_time, (*it)->max_time,
                                       csm->intervals))
                it = csm->chunks.erase(it);
            else
                ++it;
        }
    }

    // Read real chunk for each chunk meta.
    for (int i = 0; i < csm->chunks.size(); i++) {
        bool succeed;
        std::tie(csm->chunks[i]->chunk, succeed) =
            cr->chunk(csm->tsid, csm->chunks[i]->ref);
        if (!succeed) {
            err_.wrap("Chunk " + std::to_string(csm->chunks[i]->ref) +
                      "not found");
            return false;
        }
    }

    return true;
}

// Need DeleteIterator to filter the real chunk.
const std::shared_ptr<querier::ChunkSeriesMeta>&
CompactionChunkSeriesSet::at() const
{
    return csm;
}

bool CompactionChunkSeriesSet::error() const
{
    if (err_)
        return true;
    else
        return false;
}

error::Error CompactionChunkSeriesSet::error_detail() const { return err_; }

} // namespace compact
} // namespace tsdb
