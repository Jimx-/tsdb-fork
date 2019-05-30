#ifndef BASECHUNKSERIESSET_H
#define BASECHUNKSERIESSET_H

#include "block/IndexReaderInterface.hpp"
#include "index/PostingsInterface.hpp"
#include "querier/ChunkSeriesSetInterface.hpp"
#include "querier/QuerierUtils.hpp"
#include "tombstone/MemTombstones.hpp"

namespace tsdb{
namespace querier{

// BaseChunkSeriesSet loads the label set and chunk references for a postings
// list from an index. It filters out series that have labels set that should be unset
//
// The chunk pointer in ChunkMeta is not set.
// NOTE(Alec), BaseChunkSeriesSet fine-grained filters the chunks using tombstone.
class BaseChunkSeriesSet: public ChunkSeriesSetInterface{
    private:
        std::unique_ptr<index::PostingsInterface> p;
        std::shared_ptr<block::IndexReaderInterface> ir;
        std::shared_ptr<tombstone::TombstoneReaderInterface> tr;

        std::shared_ptr<ChunkSeriesMeta> cm;
        mutable bool err_;

    public:
        BaseChunkSeriesSet(const std::shared_ptr<block::IndexReaderInterface> & ir, 
                const std::shared_ptr<tombstone::TombstoneReaderInterface> & tr = std::shared_ptr<tombstone::TombstoneReaderInterface>(new tombstone::MemTombstones()), 
                const std::initializer_list<std::shared_ptr<label::MatcherInterface> > & list = {});

        // next() always called before at().
        const std::shared_ptr<ChunkSeriesMeta> & at() const;

        bool next() const;

        bool error() const;
};

}}

#endif