#ifndef MEMSERIES_H
#define MEMSERIES_H

#include "base/Mutex.hpp"
#include "chunk/ChunkIteratorInterface.hpp"
#include "chunk/ChunkMeta.hpp"
#include "db/DBUtils.hpp"
#include "head/HeadUtils.hpp"
#include "label/Label.hpp"

#include <deque>

namespace tsdb {
namespace head {

typedef chunk::ChunkMeta MemChunk;

class MemIterator : public chunk::ChunkIteratorInterface {
private:
    std::unique_ptr<ChunkIteratorInterface> iterator;
    mutable int id; // chunk id.
    int total;
    Sample sample_buf[4];

public:
    MemIterator(const std::shared_ptr<MemChunk>& m, const Sample* buf);
    std::pair<int64_t, double> at() const;
    bool next() const;
    bool error() const;
};

// TODO(Alec), should come up with some other ways of recording chunk ids when
// introducing UPDATE and Random Delete.
class MemSeries {
public:
    base::MutexLock mutex_;
    tagtree::TSID tsid;
    // std::unique_ptr<MemChunk> head_chunk;
    std::deque<std::shared_ptr<MemChunk>> chunks;
    int64_t chunk_range;
    int64_t first_chunk;
    int64_t next_at; // Timestamp at which to cut the next chunk
    Sample sample_buf[4];
    bool pending_commit;
    std::unique_ptr<chunk::ChunkAppenderInterface> appender;

    MemSeries(const tagtree::TSID& tsid, int64_t chunk_range);

    int64_t min_time();
    int64_t max_time();

    std::shared_ptr<MemChunk> head();

    // Return (success, created_chunk)
    std::pair<bool, bool> append(int64_t timestamp, double value);

    std::shared_ptr<MemChunk> cut(int64_t timestamp);

    std::deque<std::shared_ptr<chunk::ChunkMeta>> chunks_meta();

    void reset();

    error::Error appendable(int64_t timestamp, double value);

    std::shared_ptr<MemChunk> chunk(int id);

    int64_t chunk_id(int pos);

    int truncate_chunk_before(int64_t timestamp);

    // No need to worry about the MemSeries invalidation problem if gc
    // because the iterator is in safe mode which will iterate on a copy anyway.
    std::unique_ptr<chunk::ChunkIteratorInterface> iterator(int id);
};

} // namespace head
} // namespace tsdb

#endif
