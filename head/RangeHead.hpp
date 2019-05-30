#ifndef RANGEHEAD_H
#define RANGEHEAD_H

#include "block/BlockInterface.hpp"
#include "head/Head.hpp"
#include "head/HeadChunkReader.hpp"
#include "head/HeadIndexReader.hpp"
#include "tombstone/MemTombstones.hpp"

namespace tsdb{
namespace head{

class RangeHead: public block::BlockInterface{
    private:
        std::shared_ptr<Head> head;
        int64_t min_time;
        int64_t max_time;

    public:
        RangeHead(const std::shared_ptr<Head> & head, int64_t min_time, int64_t max_time): head(head), min_time(min_time), max_time(max_time){}

        // index returns an IndexReader over the block's data.
        std::pair<std::shared_ptr<block::IndexReaderInterface>, bool> index() const{
            return {std::shared_ptr<block::IndexReaderInterface>(new HeadIndexReader(head.get(), std::max(min_time, head->MinTime()), max_time)), true};
        }

        // chunks returns a ChunkReader over the block's data.
        std::pair<std::shared_ptr<block::ChunkReaderInterface>, bool> chunks() const{
            return {std::shared_ptr<block::ChunkReaderInterface>(new HeadChunkReader(head.get(), std::max(min_time, head->MinTime()), max_time)), true};
        }

        // tombstones returns a TombstoneReader over the block's deleted data.
        std::pair<std::shared_ptr<tombstone::TombstoneReaderInterface>, bool> tombstones() const{
            return {std::shared_ptr<tombstone::TombstoneReaderInterface>(new tombstone::MemTombstones()), true};
        }

        int64_t MaxTime(){
            return max_time;
        }

        int64_t MinTime(){
            return min_time;
        }

        // TODO(Alec).
        error::Error error() const{ return error::Error(); }
};

}
}

#endif