#include "block/ChunkReaderInterface.hpp"
#include "head/Head.hpp"
#include "head/HeadChunk.hpp"
#include "head/HeadChunkReader.hpp"
#include "head/HeadUtils.hpp"

namespace tsdb{
namespace head{

HeadChunkReader::HeadChunkReader(Head * head, int64_t min_time, int64_t max_time): head(head), min_time(min_time), max_time(max_time){}

std::pair<std::shared_ptr<chunk::ChunkInterface>, bool> HeadChunkReader::chunk(uint64_t ref){
    uint64_t sid, cid;
    std::tie(sid, cid) = unpack_chunk_id(ref);

    std::shared_ptr<MemSeries> s = head->series->get_by_id(sid);
    if(!s)
        // This means that the series has been garbage collected.
        return {nullptr, false};
    
    base::MutexLockGuard series_lock(s->mutex_);
    std::shared_ptr<MemChunk> c = s->chunk(cid);

    // This means that the chunk has been garbage collected or is outside
    // the specified range.
    if(!c || !c->overlap_closed(min_time, max_time))
        return {nullptr, false};

    return {std::shared_ptr<chunk::ChunkInterface>(new HeadChunk(s, c->chunk, cid)), true};
}

}}