#include <algorithm>
#include <boost/bind.hpp>
#include <iostream>
#include <limits>

#include "head/Head.hpp"
#include "head/HeadAppender.hpp"
#include "head/HeadChunkReader.hpp"
#include "head/HeadIndexReader.hpp"
#include "head/InitAppender.hpp"
#include "querier/ChunkSeriesIterator.hpp"
#include "querier/QuerierUtils.hpp"
#include "tombstone/MemTombstones.hpp"
#include "tsdbutil/RecordDecoder.hpp"
#include "tsdbutil/RecordEncoder.hpp"
#include "wal/checkpoint.hpp"

namespace tsdb {
namespace head {

Head::Head(int64_t chunk_range, std::unique_ptr<wal::WAL>&& wal,
           const std::shared_ptr<base::ThreadPool>& pool_)
    : chunk_range(chunk_range), wal(std::move(wal)), pool_(pool_)
{
    if (chunk_range < 1) {
        err_.set("invalid chunk range " + std::to_string(chunk_range));
        return;
    }
    min_time.getAndSet(std::numeric_limits<int64_t>::max());
    max_time.getAndSet(std::numeric_limits<int64_t>::min());
    series = std::unique_ptr<StripeSeries>(new StripeSeries());
    posting_list =
        std::unique_ptr<index::MemPostings>(new index::MemPostings());
}

// init loads data from the write ahead log and prepares the head for writes.
// It should be called before using an appender so that
// limits the ingested samples to the head min valid time.
error::Error Head::init(int64_t min_valid_time)
{
    valid_time.getAndSet(min_valid_time);
    if (!wal) return error::Error();

    // Backfill the checkpoint first if it exists.
    std::pair<std::pair<std::string, int>, error::Error> lp =
        wal::last_checkpoint(wal->dir());
    if (lp.second && lp.second != "not found")
        return error::wrap(lp.second, "find last checkpoint");
    if (!lp.second) {
        wal::SegmentReader reader({wal::SegmentRange(
            lp.first.first, 0, std::numeric_limits<int>::max())});
        if (reader.error())
            return error::wrap(reader.error(), "open checkpoint");

        // A corrupted checkpoint is a hard error for now and requires user
        // intervention. There's likely little data that can be recovered
        // anyway.
        wal::CorruptionError cerr = load_wal(&reader);
        if (cerr) return error::wrap(cerr.err_, "backfill checkpoint");
        ++lp.first.second;
    }

    wal::CorruptionError cerr;
    {
        // Backfill segments from the last checkpoint onwards
        wal::SegmentReader reader(
            {wal::SegmentRange(wal->dir(), lp.first.second, -1)});
        if (reader.error())
            return error::wrap(reader.error(), "open WAL segments");

        cerr = load_wal(&reader);
    }
    if (!cerr) {
        posting_list->ensure_order(pool_);
        gc();
        return error::Error();
    }
    LOG_WARN << "msg=\"encountered WAL error, attempting repair\" err="
             << cerr.error().error();
    error::Error err = wal->repair(cerr);
    if (err) return error::wrap(err, "repair corrupted WAL");

    posting_list->ensure_order(pool_);
    gc();
    return error::Error();
}

void Head::update_min_max_time(int64_t mint, int64_t maxt)
{
    while (true) {
        int64_t lt = min_time.get();
        if (mint >= lt || valid_time.get() >= mint) break;
        if (min_time.cas(lt, mint)) break;
    }
    while (true) {
        int64_t ht = max_time.get();
        if (maxt <= ht) break;
        if (max_time.cas(ht, maxt)) break;
    }
}

// The samples before valid_time will be appended.
// The samples will be destructed automatically after this function finishes.
void Head::process_wal_samples(const std::vector<tsdbutil::RefSample>& samples,
                               base::AtomicUInt64* unknown_refs,
                               base::WaitGroup* wg)
{
    // Mitigate lock contention in StripeSeries::get_by_id().
    std::unordered_map<tagtree::TSID, std::shared_ptr<MemSeries>> series_map;

    int64_t mint = std::numeric_limits<int64_t>::max();
    int64_t maxt = std::numeric_limits<int64_t>::min();

    int64_t min_valid_time = valid_time.get();
    for (const tsdbutil::RefSample& s : samples) {
        if (s.t < min_valid_time) continue;
        std::shared_ptr<MemSeries> ms;
        if (series_map.find(s.tsid) == series_map.end()) {
            ms = series->get_by_id(s.tsid);
            if (!ms) {
                unknown_refs->increment();
                continue;
            }
            series_map[s.tsid] = ms;
        } else
            ms = series_map[s.tsid];
        ms->append(s.t, s.v);
        if (s.t > maxt) maxt = s.t;
        if (s.t < mint) mint = s.t;
    }
    update_min_max_time(mint, maxt);
    wg->done();
}

// NOTICE(Alec), when loading RefSeries, RecordDecoder will sort the lset of
// each RefSeries.
wal::CorruptionError Head::load_wal(wal::SegmentReader* reader)
{
    // Track number of samples that referenced a series we don't know about
    // for error reporting.
    base::AtomicUInt64 unknown_refs;

    // Count total tasks sent to pool_.
    base::WaitGroup wg;

    // Store all Stone for chunk_rewrite.
    tombstone::MemTombstones all_stones;

    // Partition samples by ref % partition_num.
    int partition_num = 8;

    std::vector<tsdbutil::RefSeries> rseries;
    std::vector<tsdbutil::RefSample> rsamples;
    std::vector<tsdbutil::Stone> rstones;

    std::vector<std::vector<tsdbutil::RefSample>> shards;
    shards.resize(partition_num);

    while (reader->next()) {
        std::pair<uint8_t*, int> rec_pair = reader->record();

        if (rec_pair.second < 1)
            return wal::CorruptionError(reader->segment(), reader->offset(),
                                        error::Error("record length < 1"));
        tsdbutil::RECORD_ENTRY_TYPE type = rec_pair.first[0];
        if (type == tsdbutil::RECORD_SERIES) {
            // LOG_DEBUG << "RECORD_SERIES start";
            rseries.clear();
            error::Error err = tsdbutil::RecordDecoder::series(
                rec_pair.first, rec_pair.second, rseries);
            if (err)
                return wal::CorruptionError(reader->segment(), reader->offset(),
                                            error::Error("decode series"));

            for (tsdbutil::RefSeries& s : rseries) {
                // LOG_DEBUG << s.lset.front().label << " " <<
                // s.lset.front().value << " " << s.lset.back().label << " " <<
                // s.lset.back().value;
                get_or_create(s.tsid);
            }
            // LOG_DEBUG << "RECORD_SERIES finished";
        } else if (type == tsdbutil::RECORD_SAMPLES) {
            // LOG_DEBUG << "RECORD_SAMPLES start";
            rsamples.clear();
            error::Error err = tsdbutil::RecordDecoder::samples(
                rec_pair.first, rec_pair.second, rsamples);
            if (err)
                return wal::CorruptionError(reader->segment(), reader->offset(),
                                            error::Error("decode samples"));

            for (int i = 0; i < partition_num; ++i)
                shards[i].clear();

            for (tsdbutil::RefSample& s : rsamples)
                shards[std::hash<tagtree::TSID>()(s.tsid) % partition_num]
                    .push_back(s);

            for (int i = 0; i < partition_num; ++i) {
                if (!shards[i].empty()) {
                    wg.add(1);
                    pool_->run(boost::bind(&Head::process_wal_samples, this,
                                           shards[i], &unknown_refs, &wg));
                }
            }
            // LOG_DEBUG << "RECORD_SAMPLES finished";
        } else if (type == tsdbutil::RECORD_TOMBSTONES) {
            // LOG_DEBUG << "RECORD_TOMBSTONES start";
            rstones.clear();
            error::Error err = tsdbutil::RecordDecoder::tombstones(
                rec_pair.first, rec_pair.second, rstones);
            if (err)
                return wal::CorruptionError(reader->segment(), reader->offset(),
                                            error::Error("decode tombstones"));

            for (tsdbutil::Stone& s : rstones) {
                for (tombstone::Interval& itvl : s.itvls) {
                    // if(s.ref == 2)
                    // LOG_DEBUG << s.ref << " " << itvl.min_time << " " <<
                    // itvl.max_time;
                    if (itvl.max_time < valid_time.get()) continue;
                    all_stones.add_interval(s.tsid, itvl);
                }
            }

        } else
            return wal::CorruptionError(
                reader->segment(), reader->offset(),
                error::Error("invalid record type " + std::to_string(type)));

        // Wait till all task finished.
        wg.wait();
    }
    if (reader->error()) return reader->cerror();

    error::Error err =
        all_stones.iter(static_cast<std::function<error::Error(
                            const tagtree::TSID&, const tombstone::Intervals&)>>(
            [this](const tagtree::TSID& tsid, const tombstone::Intervals& itv) {
                return this->chunk_rewrite(tsid, itv);
            }));

    if (err)
        return wal::CorruptionError(
            -1, 0, error::Error("deleting samples from tombstones"));

    if (unknown_refs.get() > 0)
        LOG_WARN << "msg=\"unknown series references count="
                 << unknown_refs.get();
    return wal::CorruptionError();
}

std::unique_ptr<db::AppenderInterface> Head::head_appender()
{
    return std::unique_ptr<db::AppenderInterface>(new HeadAppender(
        const_cast<Head*>(this),
        // Set the minimum valid time to whichever is greater the head min valid
        // time or the compaciton window. This ensures that no samples will be
        // added within the compaction window to avoid races.
        std::max(valid_time.get(), MaxTime() - chunk_range / 2),
        std::numeric_limits<int64_t>::max(),
        std::numeric_limits<int64_t>::min()));
}

std::unique_ptr<db::AppenderInterface> Head::appender()
{
    // The head cache might not have a starting point yet. The init appender
    // picks up the first appended timestamp as the base.
    // LOG_DEBUG << MinTime() << " " << std::numeric_limits<int64_t>::max();
    if (MinTime() == std::numeric_limits<int64_t>::max()) {
        return std::unique_ptr<db::AppenderInterface>(
            new InitAppender(const_cast<Head*>(this)));
    }
    return head_appender();
}

// index returns an IndexReader over the block's data.
std::pair<std::shared_ptr<block::IndexReaderInterface>, bool>
Head::index() const
{
    return {std::shared_ptr<block::IndexReaderInterface>(
                new HeadIndexReader(const_cast<Head*>(this), MinTime(),
                                    std::numeric_limits<int64_t>::max())),
            true};
}

// chunks returns a ChunkReader over the block's data.
std::pair<std::shared_ptr<block::ChunkReaderInterface>, bool>
Head::chunks() const
{
    return {std::shared_ptr<block::ChunkReaderInterface>(
                new HeadChunkReader(const_cast<Head*>(this), MinTime(),
                                    std::numeric_limits<int64_t>::max())),
            true};
}

// tombstones returns a TombstoneReader over the block's deleted data.
std::pair<std::shared_ptr<tombstone::TombstoneReaderInterface>, bool>
Head::tombstones() const
{
    return {std::shared_ptr<tombstone::TombstoneReaderInterface>(
                new tombstone::MemTombstones()),
            true};
}

// init_time initializes a head with the first timestamp. This only needs to be
// called for a completely fresh head with an empty WAL. Returns true if the
// initialization took an effect.
bool Head::init_time(int64_t t)
{
    if (!min_time.cas(std::numeric_limits<int64_t>::max(), t)) return false;
    // Ensure that max time is initialized to at least the min time we just set.
    // Concurrent appenders may already have set it to a higher value.
    max_time.cas(std::numeric_limits<int64_t>::min(), t);
    return true;
}

// If there are 2 threads calling this function at the same time,
// it can be the situation that the 2 threads both generate an id.
// But only one will be finally push into StripeSeries, and the other id
// and its corresponding std::shared_ptr<MemSeries> will be abandoned.
//
// In a word, this function is thread-safe.
std::pair<std::shared_ptr<MemSeries>, bool>
Head::get_or_create(const tagtree::TSID& tsid)
{
    std::shared_ptr<MemSeries> s = series->get_by_id(tsid);
    if (s) return {s, false};
    // Optimistically assume that we are the first one to create the series.

    std::shared_ptr<MemSeries> s1(new MemSeries(tsid, chunk_range));

    std::pair<std::shared_ptr<MemSeries>, bool> s2 =
        series->get_or_set(tsid, s1);
    if (!s2.second) return {s2.first, false};

    posting_list->add(tsid);

    return {s2.first, true};
}

// chunkRewrite re-writes the chunks which overlaps with deleted ranges
// and removes the samples in the deleted ranges.
// Chunks is deleted if no samples are left at the end.
error::Error Head::chunk_rewrite(const tagtree::TSID& tsid,
                                 const tombstone::Intervals& dranges)
{
    if (dranges.empty()) {
        return error::Error();
    }

    std::shared_ptr<MemSeries> ms = series->get_by_id(tsid);
    if (!ms) {
        return error::Error();
    }

    base::MutexLockGuard lock(ms->mutex_);
    if (ms->chunks.empty()) {
        return error::Error();
    }

    std::vector<std::shared_ptr<MemChunk>> chks;
    for (auto chk : ms->chunks) {
        chks.push_back(chk);
    }

    if (chks.empty()) return error::Error();
    querier::ChunkSeriesIterator it(chks, dranges, chks.front()->min_time,
                                    chks.back()->max_time);
    // if(ref == 0){
    //     for(auto & iv: dranges)
    //         LOG_DEBUG  << iv.min_time << ", " << iv.max_time;
    // }
    ms->reset();
    while (it.next()) {
        std::pair<int64_t, double> p = it.at();
        std::pair<bool, bool> ap = ms->append(p.first, p.second);
        if (!ap.first) {
            // TODO(Alec), make it clear.
            LOG_WARN << "msg=\"failed to add sample during delete\"";
        }
    }

    return error::Error();
}

// del all samples in the range of [mint, maxt] for series that satisfy the
// given label matchers.
//
// Head::chunk_rewrite()
// WAL::log()
error::Error Head::del(int64_t mint, int64_t maxt,
                       const std::unordered_set<tagtree::TSID>& tsids)
{
    // Do not delete anything beyond the currently valid range.
    std::pair<int64_t, int64_t> tp =
        tsdbutil::clamp_interval(mint, maxt, MinTime(), MaxTime());
    if (tp.first > tp.second)
        return error::Error("given range outside the head range");

    std::shared_ptr<block::IndexReaderInterface> ir(
        new HeadIndexReader(this, tp.first, tp.second));

    std::vector<tsdbutil::Stone> stones;
    bool dirty = false;
    // LOG_DEBUG << "tp1: " << tp.first << " " << tp.second;
    for (auto&& p : tsids) {
        // LOG_DEBUG << "next() " << tp.first << " " << tp.second;
        std::shared_ptr<MemSeries> s = series->get_by_id(p);
        if (!s)
            return error::Error("error StripeSeries::get_by_id " +
                                p.to_string());

        int64_t t0 = s->min_time();
        int64_t t1 = s->max_time();
        if (t0 == std::numeric_limits<int64_t>::min() ||
            t1 == std::numeric_limits<int64_t>::min())
            continue;
        // Delete only until the current values and not beyond.
        tp = tsdbutil::clamp_interval(mint, maxt, t0, t1);
        if (tp.first > tp.second) continue;
        if (wal)
            stones.emplace_back(p,
                                tombstone::Intervals({{tp.first, tp.second}}));
        // LOG_DEBUG << "tp: " << tp.first << " " << tp.second;
        error::Error err = chunk_rewrite(p, {{tp.first, tp.second}});
        if (err) return error::Error("error delete samples");
        dirty = true;
    }
    if (wal) {
        // Although we don't store the stones in the head
        // we need to write them to the WAL to mark these as deleted
        // after a restart while loading the WAL.
        // for(auto & s: stones){
        //     for(auto &i: s.itvls)
        //         LOG_DEBUG << s.ref << " " << i.min_time << " " << i.max_time;
        // }
        std::vector<uint8_t> rec;
        tsdbutil::RecordEncoder::tombstones(stones, rec);
        error::Error err = wal->log(rec);
        if (err) return error::wrap(err, "error wal::log");
    }
    if (dirty) gc();
    return error::Error();
}

void Head::gc()
{
    // Only data strictly lower than this timestamp must be deleted.
    int64_t mint = MinTime();
    // LOG_DEBUG << mint;
    // Drop old chunks and remember series IDs and hashes if they can be
    // deleted entirely.
    std::pair<std::unordered_set<tagtree::TSID>, int> rm_pair = series->gc(mint);
    if (rm_pair.first.size() == 0) {
        LOG_DEBUG << "head::gc() nothing to gc";
        return;
    }
    // for(auto i: rm_pair.first)
    //     LOG_DEBUG << i;
    // LOG_DEBUG << posting_list->size();
    // Remove deleted series IDs from the postings lists.
    posting_list->del(rm_pair.first);
    // LOG_DEBUG << posting_list->size();
}

error::Error Head::truncate(int64_t mint)
{
    bool initialized =
        (MinTime() == std::numeric_limits<int64_t>::max()) ? true : false;
    if (MinTime() >= mint && !initialized) return error::Error();
    min_time.getAndSet(mint);
    valid_time.getAndSet(mint);
    // LOG_DEBUG << "MinTime: " << MinTime();
    // Ensure that max time is at least as high as min time.
    if (MaxTime() < mint) max_time.cas(MaxTime(), mint);

    // This was an initial call to Truncate after loading blocks on startup.
    // We haven't read back the WAL yet, so do not attempt to truncate it.
    if (initialized) return error::Error();

    auto t0 = std::chrono::high_resolution_clock::now();
    gc();
    LOG_INFO << "msg=\"head GC completed\" MinTime=" << MinTime()
             << " duration="
             << std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::high_resolution_clock::now() - t0)
                    .count()
             << "ms";

    if (!wal) return error::Error();
    t0 = std::chrono::high_resolution_clock::now();
    std::pair<std::pair<int, int>, error::Error> segs =
        wal->segments(wal->dir());
    if (segs.second) return error::wrap(segs.second, "get segment range");
    --segs.first.second; // Never consider last segment for checkpoint.
    if (segs.first.second < 0) return error::Error();
    // The lower third of segments should contain mostly obsolete samples.
    // If we have less than three segments, it's not worth checkpointing yet.
    segs.first.second =
        segs.first.first + (segs.first.second - segs.first.first) / 3;
    if (segs.first.second <= segs.first.first) return error::Error();
    std::pair<wal::CheckpointStats, error::Error> ckp = wal::checkpoint(
        wal.get(), segs.first.first, segs.first.second,
        [this](const tagtree::TSID& tsid) -> bool {
            if (this->series->get_by_id(tsid))
                return true;
            else
                return false;
        },
        mint);
    if (ckp.second) return error::wrap(ckp.second, "create checkpoint");
    error::Error err = wal->truncate(segs.first.second + 1);
    if (err) {
        // If truncating fails, we'll just try again at the next checkpoint.
        // Leftover segments will just be ignored in the future if there's a
        // checkpoint that supersedes them.
        LOG_ERROR << "msg=\"truncating segments failed\" err=" << err.error();
    }
    err = wal::delete_checkpoints(wal->dir(), segs.first.second);
    if (err) {
        // Leftover old checkpoints do not cause problems down the line beyond
        // occupying disk space.
        // They will just be ignored since a higher checkpoint exists.
        LOG_ERROR << "msg=\"delete old checkpoints\" err=" << err.error();
    }
    LOG_INFO << "msg=\"WAL checkpoint complete\" first=" << segs.first.first
             << " last=" << segs.first.second << " duration="
             << std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::high_resolution_clock::now() - t0)
                    .count()
             << "ms";
    return error::Error();
}

} // namespace head
} // namespace tsdb
