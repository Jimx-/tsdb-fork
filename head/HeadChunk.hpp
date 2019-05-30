#ifndef HEADCHUNK_H
#define HEADCHUNK_H

#include "chunk/ChunkInterface.hpp"
#include "head/MemSeries.hpp"

namespace tsdb{
namespace head{

// This wrap chunk in MemSeries::chunks to provide iterator.
// The reason of this class is to lock MemSeries when calling iterator().
class HeadChunk: public chunk::ChunkInterface{
    // NOTE Can only have one appender at the same time.
    private:
        std::shared_ptr<MemSeries> s;
        std::shared_ptr<chunk::ChunkInterface> c;
        int cid;

    public:
        HeadChunk(const std::shared_ptr<MemSeries> & s, const std::shared_ptr<chunk::ChunkInterface> & c, int cid): s(s), c(c), cid(cid){}

        const uint8_t * bytes(){ return c->bytes(); }
        uint8_t encoding(){ return c->encoding(); }

        // (Alec): will not append data in this class (should be done in MemSeries).
        std::unique_ptr<chunk::ChunkAppenderInterface> appender(){ return nullptr; }
        
        std::unique_ptr<chunk::ChunkIteratorInterface> iterator(){
            base::MutexLockGuard series_lock(s->mutex_);
            return s->iterator(cid);
        }
        
        int num_samples(){ return c->num_samples(); }
        uint64_t size(){ return c->size(); }
};

}
}

#endif