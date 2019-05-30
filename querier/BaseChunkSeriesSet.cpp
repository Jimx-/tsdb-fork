#include "querier/BaseChunkSeriesSet.hpp"

namespace tsdb{
namespace querier{

// BaseChunkSeriesSet loads the label set and chunk references for a postings
// list from an index. It filters out series that have labels set that should be unset
//
// The chunk pointer in ChunkMeta is not set
// NOTE(Alec), BaseChunkSeriesSet fine-grained filters the chunks using tombstone.
BaseChunkSeriesSet::BaseChunkSeriesSet(const std::shared_ptr<block::IndexReaderInterface> & ir, 
        const std::shared_ptr<tombstone::TombstoneReaderInterface> & tr, 
        const std::initializer_list<std::shared_ptr<label::MatcherInterface> > & list): ir(ir), tr(tr), cm(new ChunkSeriesMeta()){
    std::tie(p, err_) = postings_for_matchers(ir, list);
    err_ = !err_;
    if(err_){
        this->p.reset();
        this->ir.reset();
        this->tr.reset();
        this->cm.reset();
    }
}

// next() always called before at().
const std::shared_ptr<ChunkSeriesMeta> & BaseChunkSeriesSet::at() const{
    return cm;
}

bool BaseChunkSeriesSet::next() const{
    if(err_)
        return false;

    while(p->next()){
        uint64_t ref = p->at();

        cm->clear();
        // Get labels and deque of ChunkMeta of the corresponding series.
        if(!ir->series(ref, cm->lset, cm->chunks)){
            // TODO, ErrNotFound
            // err_ = true;
            // return false;
            continue;
        }

        // Get Intervals from MemTombstones
        try{
            // LOG_INFO << ref;
            cm->intervals = tr->get(ref);
        }
        catch(const std::out_of_range & e){}

        if(!(cm->intervals).empty()){
            std::deque<std::shared_ptr<chunk::ChunkMeta> >::iterator it = cm->chunks.begin();
            while(it != cm->chunks.end()){
                if(tombstone::is_subrange((*it)->min_time, (*it)->max_time, cm->intervals))
                    it = cm->chunks.erase(it);
                else
                    ++ it;
            }
        }
        return true;
    }
    return false;
}

bool BaseChunkSeriesSet::error() const{
    return err_;
}

}}