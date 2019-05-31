#ifndef HEAD_HPP
#define HEAD_HPP

#include <stdint.h>
#include <unordered_map>
#include <unordered_set>

#include "base/Atomic.hpp"
#include "base/Error.hpp"
#include "base/Mutex.hpp"
#include "base/ThreadPool.hpp"
#include "base/WaitGroup.hpp"
#include "block/BlockInterface.hpp"
#include "db/AppenderInterface.hpp"
#include "head/StripeSeries.hpp"
#include "index/MemPostings.hpp"
#include "tsdbutil/tsdbutils.hpp"
#include "wal/WAL.hpp"

namespace tsdb {
namespace head {

// Head handles reads and writes of time series data within a time window.
// TODO(Alec), add metrics to monitor Head.
class Head : public block::BlockInterface {
public:
    int64_t chunk_range;
    std::unique_ptr<wal::WAL> wal;

    base::AtomicInt64 min_time;
    base::AtomicInt64 max_time;
    base::AtomicInt64 valid_time; // Shouldn't be lower than the max_time of the
                                  // last persisted block
    base::AtomicUInt64 last_series_id;

    // All series addressable by hash or id
    std::unique_ptr<StripeSeries> series;

    base::RWMutexLock mutex_;
    std::unordered_set<std::string> symbols;
    std::unordered_map<std::string, std::unordered_set<std::string>>
        label_values; // label name to possible values

    std::unique_ptr<index::MemPostings> posting_list;

    std::shared_ptr<base::ThreadPool> pool_;

    error::Error err_;

    Head(int64_t chunk_range, std::unique_ptr<wal::WAL>&& wal,
         const std::shared_ptr<base::ThreadPool>& pool_);

    // The samples before valid_time will be appended.
    //
    // init loads data from the write ahead log and prepares the head for
    // writes. It should be called before using an appender so that limits the
    // ingested samples to the head min valid time.
    error::Error init(int64_t min_valid_time);

    void update_min_max_time(int64_t mint, int64_t maxt);

    // The samples will be destructed automatically after this function
    // finishes.
    void process_wal_samples(const std::vector<tsdbutil::RefSample>& samples,
                             base::AtomicUInt64* unknown_refs,
                             base::WaitGroup* wg);

    wal::CorruptionError load_wal(wal::SegmentReader* reader);

    std::unique_ptr<db::AppenderInterface> head_appender();

    std::unique_ptr<db::AppenderInterface> appender();

    // index returns an IndexReader over the block's data.
    std::pair<std::shared_ptr<block::IndexReaderInterface>, bool> index() const;

    // chunks returns a ChunkReader over the block's data.
    std::pair<std::shared_ptr<block::ChunkReaderInterface>, bool>
    chunks() const;

    // tombstones returns a TombstoneReader over the block's deleted data.
    std::pair<std::shared_ptr<tombstone::TombstoneReaderInterface>, bool>
    tombstones() const;

    int64_t MinTime() const
    {
        return const_cast<base::AtomicInt64*>(&min_time)->get();
    }
    int64_t MaxTime() const
    {
        return const_cast<base::AtomicInt64*>(&max_time)->get();
    }

    // init_time initializes a head with the first timestamp. This only needs to
    // be called for a completely fresh head with an empty WAL. Returns true if
    // the initialization took an effect.
    bool init_time(int64_t t);

    bool overlap_closed(int64_t mint, int64_t maxt) const
    {
        // The block itself is a half-open interval
        // [pb.meta.MinTime, pb.meta.MaxTime).
        return MinTime() <= maxt && mint < MaxTime();
    }

    // If there are 2 threads calling this function at the same time,
    // it can be the situation that the 2 threads both generate an id.
    // But only one will be finally push into StripeSeries, and the other id
    // and its corresponding std::shared_ptr<MemSeries> will be abandoned.
    //
    // In a word, this function is thread-safe.
    std::pair<std::shared_ptr<MemSeries>, bool>
    get_or_create(const common::TSID& tsid);

    // chunkRewrite re-writes the chunks which overlaps with deleted ranges
    // and removes the samples in the deleted ranges.
    // Chunks is deleted if no samples are left at the end.
    error::Error chunk_rewrite(const common::TSID& tsid,
                               const tombstone::Intervals& dranges);

    // del all samples in the range of [mint, maxt] for series that satisfy the
    // given label matchers.
    //
    // Head::chunk_rewrite()
    // WAL::log()
    error::Error del(int64_t mint, int64_t maxt,
                     const std::unordered_set<common::TSID>& tsids);

    void gc();

    error::Error truncate(int64_t mint);

    // void close() const{
    //     if(wal)
    //         wal.reset();
    // }

    error::Error error() const { return err_; }

    ~Head()
    {
        // LOG_DEBUG << "~Head";
    }
};

} // namespace head
} // namespace tsdb

#endif
