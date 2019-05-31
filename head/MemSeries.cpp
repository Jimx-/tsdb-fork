#include "head/MemSeries.hpp"
#include "base/Logging.hpp"
#include "base/TSDBException.hpp"
#include "chunk/XORAppender.hpp"
#include "chunk/XORChunk.hpp"
#include "chunk/XORIterator.hpp"
#include "db/DBUtils.hpp"

namespace tsdb {
namespace head {

MemIterator::MemIterator(const std::shared_ptr<MemChunk>& m, const Sample* buf)
{
    iterator = m->chunk->iterator();
    id = -1;
    total = m->chunk->num_samples();
    sample_buf[0] = buf[0];
    sample_buf[1] = buf[1];
    sample_buf[2] = buf[2];
    sample_buf[3] = buf[3];
}

std::pair<int64_t, double> MemIterator::at() const
{
    if (total - id > 4) return iterator->at();
    return {sample_buf[4 - (total - id)].t, sample_buf[4 - (total - id)].v};
}

bool MemIterator::next() const
{
    if (id + 1 >= total) return false;
    ++id;
    if (total - id > 4) return iterator->next();
    return true;
}

bool MemIterator::error() const { return iterator->error(); }

MemSeries::MemSeries(const common::TSID& tsid, int64_t chunk_range)
    : mutex_(), tsid(tsid), chunk_range(chunk_range), first_chunk(0),
      next_at(std::numeric_limits<int64_t>::min()), pending_commit(false)
{}

int64_t MemSeries::min_time()
{
    if (chunks.empty()) return std::numeric_limits<int64_t>::min();
    return chunks[0]->min_time;
}

int64_t MemSeries::max_time()
{
    if (chunks.empty()) return std::numeric_limits<int64_t>::min();
    return chunks.back()->max_time;
}

std::shared_ptr<MemChunk> MemSeries::head()
{
    if (chunks.empty()) return nullptr;
    return chunks.back();
}

// Return (success, created_chunk)
std::pair<bool, bool> MemSeries::append(int64_t timestamp, double value)
{

    bool chunk_created = false;
    std::shared_ptr<MemChunk> h = head();
    if (!h) {
        h = cut(timestamp);
        chunk_created = true;
    }
    int num_samples = h->chunk->num_samples();

    // TODO(Alec), add out of order data.
    if (h->max_time >= timestamp) return {false, chunk_created};

    // If we reach 25% of a chunk's desired sample count, set a definitive time
    // at which to start the next chunk.
    // At latest it must happen at the timestamp set when the chunk was cut.
    // If it already reaches 1/4, then we may cut it earlier to avoid the too
    // long chunk
    if (num_samples == SAMPLES_PER_CHUNK / 4)
        next_at = compute_chunk_end_time(h->min_time, h->max_time, next_at);

    if (timestamp >= next_at) {
        h = cut(timestamp);
        chunk_created = true;
    }

    appender->append(timestamp, value);

    h->max_time = timestamp;

    sample_buf[0] = sample_buf[1];
    sample_buf[1] = sample_buf[2];
    sample_buf[2] = sample_buf[3];
    sample_buf[3].reset(timestamp, value);

    return {true, chunk_created};
}

std::shared_ptr<MemChunk> MemSeries::cut(int64_t timestamp)
{
    chunks.emplace_back(new MemChunk(
        std::hash<common::TSID>()(tsid),
        std::shared_ptr<chunk::ChunkInterface>(new chunk::XORChunk()),
        timestamp, std::numeric_limits<int64_t>::min()));

    // Set upper bound on when the next chunk must be started. An earlier
    // timestamp may be chosen dynamically at a later point.
    //
    // NOTE(Alec), this can make the chunk not cross the boundary of
    // chunk_range. And because the HeadChunkReader of RangeHead only checks
    // ChunkMeta::overlap_closed() , which will not exclude the
    // crossing-boundary chunks. Thus, the LeveledCompactor::write() will not
    // fail because of crossing boundary.
    next_at = db::range_for_timestamp(timestamp, chunk_range);

    appender.reset();

    try {
        appender = chunks.back()->chunk->appender();
    } catch (const base::TSDBException& e) {
        LOG_ERROR << "MemSeries cut, error: " << e.what();
        exit(1);
    }
    return chunks.back();
}

std::deque<std::shared_ptr<chunk::ChunkMeta>> MemSeries::chunks_meta()
{
    return chunks;
}

void MemSeries::reset()
{
    chunks.clear();
    first_chunk = 0;
    next_at = std::numeric_limits<int64_t>::min();
    sample_buf[0].reset();
    sample_buf[1].reset();
    sample_buf[2].reset();
    sample_buf[3].reset();
    pending_commit = false;
    appender.reset();
}

error::Error MemSeries::appendable(int64_t timestamp, double value)
{
    if (chunks.empty()) return error::Error();
    if (timestamp > chunks.back()->max_time)
        return error::Error();
    else
        return ErrOutOfOrderSample;
}

std::shared_ptr<MemChunk> MemSeries::chunk(int id)
{
    int idx = id - first_chunk;
    if (idx < 0 || idx >= chunks.size()) return nullptr;
    return chunks[idx];
}

int64_t MemSeries::chunk_id(int pos) { return first_chunk + pos; }

int MemSeries::truncate_chunk_before(int64_t timestamp)
{
    int last = first_chunk;
    while (!chunks.empty() && chunks.front()->max_time < timestamp) {
        chunks.pop_front();
        ++first_chunk;
    }
    return first_chunk - last;
}

// No need to worry about the MemSeries invalidation problem if gc
// because the iterator is in safe mode which will iterate on a copy anyway.
std::unique_ptr<chunk::ChunkIteratorInterface> MemSeries::iterator(int id)
{
    std::shared_ptr<MemChunk> c = chunk(id);
    if (!c) return nullptr;
    if (id - first_chunk < chunks.size()) return c->chunk->iterator();

    // Serve the last 4 samples for the last chunk from the sample buffer
    // as their compressed bytes may be mutated by added samples.
    return std::unique_ptr<chunk::ChunkIteratorInterface>(
        new MemIterator(c, sample_buf));
}

} // namespace head
} // namespace tsdb
