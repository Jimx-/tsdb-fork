#include <boost/bind.hpp>
#include <boost/filesystem.hpp>
#include <limits>

#include <sched.h>

#include "base/Logging.hpp"
#include "base/TimeStamp.hpp"
#include "base/WaitGroup.hpp"
#include "block/Block.hpp"
#include "compact/LeveledCompactor.hpp"
#include "db/DB.hpp"
#include "db/DBAppender.hpp"
#include "db/DBUtils.hpp"
#include "head/RangeHead.hpp"
#include "querier/BlockQuerier.hpp"
#include "querier/Querier.hpp"
#include "tsdbutil/tsdbutils.hpp"
#include "wal/WAL.hpp"

namespace tsdb {
namespace db {

DB::DB(const std::string& dir_, const Options& options)
    : dir_(dir_), opts(options), compactc(new base::Channel<char>()),
      donec(new base::Channel<char>()), stopc(new base::Channel<char>()),
      compact_cancel(new base::Channel<char>()), auto_compact(true),
      pool_(new base::ThreadPool("DB ThreadPool"))
{
    boost::filesystem::create_directories(dir_);

    if (!opts.no_lock_file) {
        lockf = base::FLock(tsdbutil::filepath_join(dir_, "lock"));
        if (lockf.error()) {
            err_.set(error::wrap(lockf.error(), "lock DB directory"));
            return;
        }
    }

    compactor = std::unique_ptr<compact::CompactorInterface>(
        new compact::LeveledCompactor(opts.block_ranges, compact_cancel));
    if (compactor->error()) {
        err_.set(error::wrap(compactor->error(), "create LeveledCompactor"));
        return;
    }

    // TODO(Alec), thread number optimization.
    pool_->start(8);

    std::unique_ptr<wal::WAL> wal;
    // Wal is enabled.
    if (opts.wal_segment_size >= 0) {
        if (opts.wal_segment_size > 0)
            wal = std::unique_ptr<wal::WAL>(
                new wal::WAL(tsdbutil::filepath_join(dir_, "wal"), pool_,
                             opts.wal_segment_size));
        else
            wal = std::unique_ptr<wal::WAL>(
                new wal::WAL(tsdbutil::filepath_join(dir_, "wal"), pool_));
        if (wal->error()) {
            err_.set(error::wrap(wal->error(), "create WAL"));
            return;
        }
    }

    head_ = std::shared_ptr<head::Head>(
        new head::Head(opts.block_ranges[0], std::move(wal), pool_));
    if (head_->error()) {
        err_.set(error::wrap(head_->error(), "create Head"));
        return;
    }

    error::Error err = reload();
    if (err) {
        err_.set(error::wrap(err, "error DB::reload"));
        return;
    }
    // Set the min valid time for the ingested samples
    // to be no lower than the maxt of the last block.
    auto b = blocks();
    int64_t min_valid_time = std::numeric_limits<int64_t>::min();
    if (!b.empty()) min_valid_time = b.back()->meta().max_time;
    err = head_->init(min_valid_time);
    if (err) err_.set(error::wrap(err, "error head::init"));

    pool_->run(boost::bind(&DB::run, this));
    // run();
}

std::unique_ptr<db::AppenderInterface> DB::appender()
{
    return std::unique_ptr<db::AppenderInterface>(
        new DBAppender(std::move(head_->appender()), this));
}

std::pair<std::unique_ptr<querier::QuerierInterface>, error::Error>
DB::querier(int64_t mint, int64_t maxt)
{
    std::vector<std::shared_ptr<block::BlockInterface>>
        bs; // block::BlockInterface for constructing querier::BlockQuerier.
    block::BlockMetas bms; // For calling overlapping_blocks().

    base::RWLockGuard lock(mutex_, 0);
    for (auto const& b : blocks_) {
        if (b->overlap_closed(mint, maxt)) {
            bs.push_back(b);
            bms.push_back(b->meta());
        }
    }
    if (maxt >= head_->MinTime())
        bs.push_back(std::shared_ptr<block::BlockInterface>(
            new head::RangeHead(head_, mint, maxt)));

    std::vector<std::shared_ptr<querier::QuerierInterface>> queriers;
    for (auto const& b : bs) {
        std::shared_ptr<querier::QuerierInterface> q(
            new querier::BlockQuerier(b, mint, maxt));
        if (!q->error()) {
            queriers.push_back(q);
            continue;
        }
        // If we fail, all previously opened queriers must be closed.
        // clear() will call the destructor of indexr, chunkr, tombstones. Thus,
        // close the BlockQuerier and BlockInterface.
        queriers.clear();
        return {nullptr,
                error::wrap(q->error(), "open querier for block " + b->dir())};
    }

    if (!overlapping_blocks(bms).empty()) {
        // TODO(Alec), add vertical querier.
        return {nullptr,
                error::Error("cannot get querier on overlapping blocks")};
    }

    return {std::unique_ptr<querier::QuerierInterface>(
                new querier::Querier(queriers)),
            error::Error()};
}

std::deque<std::shared_ptr<block::BlockInterface>> DB::blocks()
{
    base::RWLockGuard lock(mutex_, 0);
    std::deque<std::shared_ptr<block::BlockInterface>> r(blocks_.begin(),
                                                         blocks_.end());
    return r;
}

std::shared_ptr<block::BlockInterface> DB::get_block(const ulid::ULID& ulid)
{
    base::RWLockGuard lock(mutex_, 0);
    for (auto& b : blocks_) {
        if (ulid::CompareULIDs(b->meta().ulid_, ulid) == 0) return b;
    }
    return nullptr;
}

std::unordered_map<ulid::ULID, error::Error>
DB::open_blocks(std::deque<std::shared_ptr<block::BlockInterface>>& blocks)
{
    std::deque<std::string> dirs = block_dirs(dir_);

    std::unordered_map<ulid::ULID, error::Error> corrupted;
    for (auto const& dir : dirs) {
        std::pair<block::BlockMeta, bool> meta_pair =
            block::read_block_meta(dir);
        if (!meta_pair.second) {
            LOG_ERROR << "msg=\"cannot read block meta\" dir=" << dir;
            continue;
        }

        // See if we already have the block in memory or open it otherwise.
        std::shared_ptr<block::BlockInterface> b =
            get_block(meta_pair.first.ulid_);
        if (b == nullptr) {
            b = std::shared_ptr<block::BlockInterface>(new block::Block(dir));
            if (b->error()) {
                corrupted[meta_pair.first.ulid_] = b->error();
                continue;
            }
        }
        blocks.push_back(b);
    }
    return corrupted;
}

std::unordered_map<ulid::ULID, std::shared_ptr<block::BlockInterface>>
DB::deletable_blocks(std::deque<std::shared_ptr<block::BlockInterface>>& blocks)
{
    std::unordered_map<ulid::ULID, std::shared_ptr<block::BlockInterface>>
        deletable;

    // Sort the blocks by time - newest to oldest (largest to smallest
    // timestamp). This ensures that the retentions will remove the oldest
    // blocks.
    std::sort(blocks.begin(), blocks.end(),
              [](const std::shared_ptr<block::BlockInterface>& lhs,
                 const std::shared_ptr<block::BlockInterface>& rhs) {
                  return lhs->meta().max_time > rhs->meta().max_time;
              });

    for (auto const& b : blocks) {
        if (b->meta().compaction.deletable) deletable[b->meta().ulid_] = b;
    }

    for (auto const& p : beyond_time_retention(blocks))
        deletable[p.first] = p.second;
    for (auto const& p : beyond_size_retention(blocks))
        deletable[p.first] = p.second;

    return deletable;
}

// The blocks passed in are sorted by time - newest to oldest (largest to
// smallest timestamp).
std::unordered_map<ulid::ULID, std::shared_ptr<block::BlockInterface>>
DB::beyond_time_retention(
    std::deque<std::shared_ptr<block::BlockInterface>>& blocks)
{
    std::unordered_map<ulid::ULID, std::shared_ptr<block::BlockInterface>>
        deletable;

    // Time retention is disabled or no blocks to work with.
    if (blocks.empty() || opts.retention_duration == 0) return deletable;

    for (int i = 0; i < blocks.size(); ++i) {
        // The difference between the first block and this block is larger than
        // the retention period so any blocks after that are added as
        // deleteable.
        if (i > 0 && blocks[0]->meta().max_time - blocks[i]->meta().max_time >
                         static_cast<int64_t>(opts.retention_duration)) {
            for (int j = i; j < blocks.size(); ++j)
                deletable[blocks[j]->meta().ulid_] = blocks[j];
            break;
        }
    }
    return deletable;
}

// The blocks passed in are sorted by time - newest to oldest (largest to
// smallest timestamp).
std::unordered_map<ulid::ULID, std::shared_ptr<block::BlockInterface>>
DB::beyond_size_retention(
    std::deque<std::shared_ptr<block::BlockInterface>>& blocks)
{
    std::unordered_map<ulid::ULID, std::shared_ptr<block::BlockInterface>>
        deletable;

    // Size retention is disabled or no blocks to work with.
    if (blocks.empty() || opts.max_bytes <= 0) return deletable;

    int64_t block_size = 0;
    for (int i = 0; i < blocks.size(); ++i) {
        block_size += blocks[i]->size();
        if (block_size > opts.max_bytes) {
            // Add this and all following blocks for deletion.
            for (int j = i; j < blocks.size(); ++j)
                deletable[blocks[j]->meta().ulid_] = blocks[j];
            break;
        }
    }
    return deletable;
}

// delete_blocks closes and deletes blocks from the disk.
// When the map contains a non nil block object it means it is loaded in memory
// so needs to be closed first as it might need to wait for pending readers to
// complete.
void DB::delete_blocks(
    const std::unordered_map<ulid::ULID,
                             std::shared_ptr<block::BlockInterface>>& deletable)
{
    for (auto const& p : deletable) {
        if (p.second) {
            // This is a blocking function.
            p.second->close();
        }
        boost::filesystem::remove_all(
            tsdbutil::filepath_join(dir_, ulid::Marshal(p.first)));
    }
}

// validate_block_sequence returns error if given block meta files indicate that
// some blocks overlaps within sequence. blocks are sorted by min_time.
error::Error DB::validate_block_sequence(
    const std::deque<std::shared_ptr<block::BlockInterface>>& blocks)
{
    if (blocks.size() <= 1) return error::Error();

    block::BlockMetas bms;
    for (auto const& b : blocks)
        bms.push_back(b->meta());

    TimeRangeOverlaps overlaps = overlapping_blocks(bms);
    if (!overlaps.empty())
        return error::Error("blocks time ranges overlap: " + overlaps.str());

    return error::Error();
}

// reload blocks and trigger head truncation if new blocks appeared.
// Blocks that are obsolete due to replacement or retention will be deleted.
error::Error DB::reload()
{
    std::deque<std::shared_ptr<block::BlockInterface>> loadable;
    std::unordered_map<ulid::ULID, error::Error> corrupted =
        open_blocks(loadable);

    std::unordered_map<ulid::ULID, std::shared_ptr<block::BlockInterface>>
        deletable = deletable_blocks(loadable);

    // Corrupted blocks that have been replaced by parents can be safely ignored
    // and deleted. This makes it resilient against the process crashing towards
    // the end of a compaction. Creation of a new block and deletion of its
    // parents cannot happen atomically. By creating blocks with their parents,
    // we can pick up the deletion where it left off during a crash.
    for (auto const& b : loadable) {
        LOG_DEBUG << ulid::Marshal(b->meta().ulid_);
        for (auto const& p : b->meta().compaction.parents) {
            std::unordered_map<ulid::ULID, error::Error>::iterator it;
            if ((it = corrupted.find(p.ulid_)) != corrupted.end())
                corrupted.erase(it);
            deletable[p.ulid_] = nullptr;
        }
    }
    if (!corrupted.empty()) {
        // Close all new blocks to release the lock for windows.
        for (auto const& b : loadable) {
            std::shared_ptr<block::BlockInterface> block =
                get_block(b->meta().ulid_);
            if (block) block->close();
        }
        std::string s;
        for (auto const& c : corrupted)
            s += ulid::Marshal(c.first) + ", ";
        return error::Error("unexpected corrupted blocks: " +
                            s.substr(0, s.length() - 2));
    }

    // All deletable blocks should not be loaded.
    auto it = loadable.begin();
    while (it != loadable.end()) {
        if (deletable.find((*it)->meta().ulid_) != deletable.end()) {
            deletable[(*it)->meta().ulid_] = *it;
            it = loadable.erase(it);
        } else
            ++it;
    }
    // Sort loadable blocks by the min_time.
    std::sort(loadable.begin(), loadable.end(),
              [](const std::shared_ptr<block::BlockInterface>& lhs,
                 const std::shared_ptr<block::BlockInterface>& rhs) {
                  return lhs->meta().min_time < rhs->meta().min_time;
              });
    if (!opts.allow_overlapping_blocks) {
        error::Error err = validate_block_sequence(loadable);
        if (err)
            return error::wrap(err,
                               "invalid block sequence(overlapping blocks)");
    }

    // Swap new blocks first for subsequently created readers to be seen.
    {
        base::RWLockGuard lock(mutex_, 1);
        for (auto const& b : this->blocks_) {
            if (deletable.find(b->meta().ulid_) != deletable.end())
                deletable[b->meta().ulid_] = b;
        }
        this->blocks_.assign(loadable.begin(), loadable.end());
    }
    block::BlockMetas bms;
    for (auto const& b : loadable)
        bms.push_back(b->meta());
    TimeRangeOverlaps overlaps = overlapping_blocks(bms);
    if (!overlaps.empty())
        LOG_WARN << "msg=\"overlapping blocks found during reload\" detail="
                 << overlaps.str();

    delete_blocks(deletable);

    // Garbage collect data in the head if the most recent persisted block
    // covers data of its current time range.
    if (loadable.empty()) return error::Error();

    error::Error err = head_->truncate(loadable.back()->meta().max_time);
    if (err)
        return error::wrap(err, "head truncate failed");
    else
        return error::Error();
}

// Compact data if possible. After successful compaction blocks are reloaded
// which will also trigger blocks to be deleted that fall out of the retention
// window. If no blocks are compacted, the retention window state doesn't
// change. Thus, this is sufficient to reliably delete old data. Old blocks are
// only deleted on reload based on the new block's parent information. See
// DB.reload documentation for further information.
error::Error DB::compact()
{
    base::MutexLockGuard lock(cmutex_);
    // Check whether we have pending head blocks that are ready to be persisted.
    // They have the highest priority.
    while (true) {
        // Return when receiving stop signal.
        if (!stopc->empty()) return error::Error();
        // The head has a compactable range if 1.5 level 0 ranges are between
        // the oldest and newest timestamp. The 0.5 acts as a buffer of the
        // appendable window.
        if (head_->MaxTime() - head_->MinTime() <= opts.block_ranges[0] / 2 * 3)
            break;

        // Compute the range to be written to disk.
        int64_t mint = head_->MinTime();
        int64_t maxt = range_for_timestamp(mint, opts.block_ranges[0]);

        // Wrap head into a range that bounds all reads to it.
        std::shared_ptr<block::BlockInterface> h(new head::RangeHead(
            head_, mint,
            // We remove 1 millisecond from maxt because block
            // intervals are half-open: [b.MinTime, b.MaxTime). But
            // chunk intervals are closed: [c.MinTime, c.MaxTime];
            // so in order to make sure that overlaps are evaluated
            // consistently, we explicitly remove the last value
            // from the block interval here.
            maxt - 1));
        LOG_DEBUG << "before persist head block " << mint << " " << maxt;
        std::pair<ulid::ULID, error::Error> ulid_pair =
            compactor->write(dir_, h, mint, maxt, nullptr);
        if (ulid_pair.second)
            return error::wrap(ulid_pair.second, "persist head block");
        LOG_DEBUG << "after persist head block " << mint << " " << maxt;
        error::Error err = reload();
        if (err) {
            try {
                boost::filesystem::remove_all(tsdbutil::filepath_join(
                    dir_, ulid::Marshal(ulid_pair.first)));
            } catch (const boost::filesystem::filesystem_error& e) {
                return error::wrap(
                    err,
                    "delete persisted head block after failed db reload: " +
                        ulid::Marshal(ulid_pair.first));
            }
            return error::wrap(err, "reload blocks");
        }
        if (ulid::CompareULIDs(ulid_pair.first, ulid::ULID()) == 0) {
            // Compaction resulted in an empty block.
            // Head truncating during db.reload() depends on the persisted
            // blocks and in this case no new block will be persisted so
            // manually truncate the head.
            err = head_->truncate(maxt);
            if (err)
                return error::wrap(err, "head truncate failed (in compact)");
        }
    }

    // Check for compactions of multiple blocks.
    while (true) {
        std::pair<std::deque<std::string>, error::Error> plan =
            compactor->plan(dir_);
        if (plan.second) return error::wrap(plan.second, "plan compaction");
        if (plan.first.empty()) break;

        // Return when receiving stop signal.
        if (!stopc->empty()) return error::Error();

        std::pair<ulid::ULID, error::Error> ulid_pair = compactor->compact(
            dir_, plan.first,
            std::shared_ptr<block::Blocks>(new block::Blocks(blocks_)));
        if (ulid_pair.second) {
            std::string plan_s("compact [");
            for (const std::string& s : plan.first)
                plan_s += s + ", ";
            plan_s[plan_s.length() - 2] = ']';
            return error::wrap(ulid_pair.second, plan_s);
        }

        error::Error err = reload();
        if (err) {
            try {
                boost::filesystem::remove_all(tsdbutil::filepath_join(
                    dir_, ulid::Marshal(ulid_pair.first)));
            } catch (const boost::filesystem::filesystem_error& e) {
                return error::wrap(
                    err,
                    "delete persisted head block after failed db reload: " +
                        ulid::Marshal(ulid_pair.first));
            }
            return error::wrap(err, "reload blocks");
        }
    }
    return error::Error();
}

void backoff_timing(std::shared_ptr<base::Channel<char>> chan, int sleep_secs)
{
    chan->wait_for_seconds(sleep_secs);
    chan->send(0);
}

void DB::run()
{
    int backoff = 0;
    std::shared_ptr<base::Channel<char>> backoff_chan(
        new base::Channel<char>());
    while (true) {
        int select = -1;
        pool_->run(boost::bind(&backoff_timing, backoff_chan, backoff));
        while ((select = base::channel_select<char>({stopc, backoff_chan})) ==
               -1) {
        }
        if (select == 0) {
            backoff_chan->notify();
            break;
        } else
            backoff_chan->flush();

        select = -1;
        pool_->run(boost::bind(&backoff_timing, compactc, 60));
        while ((select = base::channel_select<char>({stopc, compactc})) == -1) {
        }
        if (select == 0) {
            compactc->notify();
            break;
        } else {
            base::MutexLockGuard lock(auto_compact_mutex_);

            // NOTE(Alec), the main thread can already created a compact signal
            // before backoff_timing() ends, so need to call notify() here to
            // wake up asleep backoff_timing thread.
            compactc->notify();
            if (auto_compact) {
                error::Error err = compact();
                if (err) {
                    LOG_ERROR << "msg=\"compaction failed\" err="
                              << err.error();
                    backoff = exponential(
                        backoff, 1, 60); // backoff time between 1s and 1m.
                    if (!compact_cancel->empty()) break;
                } else
                    backoff = 0;
            }
            compactc->flush();
        }
    }
    donec->send(0);
    // LOG_DEBUG << "DB::run quit";
}

void DB::close()
{
    stopc->send(0);
    compact_cancel->send(0);

    // Wait until DB::run() exits.
    while (donec->empty()) {
    }

    base::RWLockGuard lock(mutex_, 1);
    for (auto b : blocks_)
        b->close();
    // lockf.release();
    // head_->close();
    // LOG_DEBUG << "DB::close";
}

void del_helper(
    std::shared_ptr<block::BlockInterface> b, int64_t mint, int64_t maxt,
    const std::deque<std::shared_ptr<label::MatcherInterface>>* matchers,
    base::WaitGroup* wg, error::MultiError* multi_err)
{
    // LOG_DEBUG << ulid::Marshal(b->meta().ulid_) << " mint:" << mint << "
    // maxt:" << maxt << " matcher:" << (*matchers)[0]->name() << " " <<
    // (*matchers)[0]->value();
    error::Error err = b->del(mint, maxt, *matchers);
    if (err) multi_err->add(err);
    wg->done();
}

error::Error
DB::del(int64_t mint, int64_t maxt,
        const std::deque<std::shared_ptr<label::MatcherInterface>>& matchers)
{
    base::MutexLockGuard lock1(cmutex_);
    base::RWLockGuard lock2(mutex_, 0);

    base::WaitGroup wg;
    error::MultiError multi_err;

    // On disk blocks.
    for (auto& b : blocks_) {
        // LOG_DEBUG << ulid::Marshal(b->meta().ulid_) << " mint:" <<
        // b->meta().min_time << " maxt:" << b->meta().max_time;
        if (b->overlap_closed(mint, maxt)) {
            wg.add(1);
            pool_->run(boost::bind(&del_helper, b, mint, maxt, &matchers, &wg,
                                   &multi_err));
        }
    }

    // Head.
    if (head_->overlap_closed(mint, maxt)) {
        wg.add(1);
        // std::shared_ptr<head::Head> can be converted to
        // std::shared_ptr<block::BlockInterface> automatically.
        pool_->run(boost::bind(&del_helper, head_, mint, maxt, &matchers, &wg,
                               &multi_err));
    }
    wg.wait();
    return error::Error(multi_err.error());
}

error::Error DB::clean_tombstones()
{
    error::Error err;
    std::deque<ulid::ULID> new_ulids;
    base::MutexLockGuard lock1(cmutex_);
    // base::TimeStamp now = base::TimeStamp::now();
    std::deque<std::shared_ptr<block::BlockInterface>> bs = blocks();
    for (auto& b : bs) {
        std::pair<ulid::ULID, error::Error> ulid_pair =
            b->clean_tombstones(dir_, compactor.get());
        if (ulid_pair.second) {
            err = error::wrap(ulid_pair.second, "clean tombstones " + b->dir());
            break;
        } else if (ulid_pair.first != ulid::ULID())
            new_ulids.push_back(ulid_pair.first);
    }
    if (err) {
        for (ulid::ULID& uid : new_ulids) {
            try {
                boost::filesystem::remove(
                    tsdbutil::filepath_join(dir_, ulid::Marshal(uid)));
            } catch (const boost::filesystem::filesystem_error& e) {
                LOG_ERROR << "msg=\"failed to delete block after failed "
                             "`clean_tombstones()`\" dir="
                          << tsdbutil::filepath_join(dir_, ulid::Marshal(uid))
                          << " err=" << err.error();
            }
        }
    }
    err = reload();
    if (err) return error::wrap(err, "reload blocks in 'clean_tombstones()'");
    return error::Error();
}

} // namespace db
} // namespace tsdb
