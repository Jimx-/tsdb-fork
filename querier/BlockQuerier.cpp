#include "base/Logging.hpp"
#include "querier/BaseChunkSeriesSet.hpp"
#include "querier/BlockSeriesSet.hpp"
#include "querier/BlockQuerier.hpp"
#include "querier/EmptySeriesSet.hpp"
#include "querier/PopulatedChunkSeriesSet.hpp"
#include "tsdbutil/StringTuplesInterface.hpp"

namespace tsdb{
namespace querier{

BlockQuerier::BlockQuerier(const std::shared_ptr<block::BlockInterface> & block, int64_t min_time, int64_t max_time): min_time(min_time), max_time(max_time){
    bool succeed_;
    std::tie(indexr, succeed_) = block->index();
    if(!succeed_){
        // LOG_ERROR << "Error getting block index";
        err_.set("error get block index");
        return;
    }
    std::tie(chunkr, succeed_) = block->chunks();
    if(!succeed_){
        // LOG_ERROR << "Error getting block chunks";
        err_.set("error get block chunks");
        return;
    }
    std::tie(tombstones, succeed_) = block->tombstones();
    if(!succeed_){
        // LOG_ERROR << "Error getting block tombstones";
        err_.set("error get block tombstones");
        return;
    }
}

std::shared_ptr<SeriesSetInterface> BlockQuerier::select(const std::initializer_list<std::shared_ptr<label::MatcherInterface> > & l) const{
    std::shared_ptr<ChunkSeriesSetInterface> base(new BaseChunkSeriesSet(indexr, tombstones, l));
    if(base->error()){
        // Happens when it cannot find the matching postings.
        // LOG_ERROR << "Error get BaseChunkSeriesSet";
        return nullptr;
    }
    return std::shared_ptr<SeriesSetInterface>(new BlockSeriesSet(
        std::shared_ptr<ChunkSeriesSetInterface>(new PopulatedChunkSeriesSet(
            base,
            chunkr,
            min_time,
            max_time
        )),
        min_time,
        max_time
    ));
}

std::deque<std::string> BlockQuerier::label_values(const std::string & s) const{
    std::unique_ptr<tsdbutil::StringTuplesInterface> tpls = indexr->label_values({s});
    std::deque<std::string> d;
    for(int i = 0; i < tpls->len(); i ++){
        d.push_back(tpls->at(i));
    }
    return d;
}

std::deque<std::string> BlockQuerier::label_names() const{
    return indexr->label_names();
}

}}