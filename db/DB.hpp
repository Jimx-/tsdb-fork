#ifndef TSDB_DB_H
#define TSDB_DB_H

#include <unordered_map>

#include "base/Channel.hpp"
#include "base/Error.hpp"
#include "base/FLock.hpp"
#include "base/Logging.hpp"
#include "base/Mutex.hpp"
#include "base/ThreadPool.hpp"
#include "block/BlockInterface.hpp"
#include "compact/CompactorInterface.hpp"
#include "db/AppenderInterface.hpp"
#include "db/DBUtils.hpp"
#include "external/ulid.hpp"
#include "head/Head.hpp"
#include "querier/QuerierInterface.hpp"

namespace tsdb {
namespace db {

class DB {
private:
    std::string dir_;
    base::FLock lockf;
    Options opts;

    std::unique_ptr<compact::CompactorInterface> compactor;

    // Mutex for that must be held when modifying the general block layout.
    base::RWMutexLock mutex_;
    std::deque<std::shared_ptr<block::BlockInterface>> blocks_;

    std::shared_ptr<head::Head> head_;

    std::shared_ptr<base::Channel<char>> compactc;
    std::shared_ptr<base::Channel<char>> donec;
    std::shared_ptr<base::Channel<char>> stopc;
    std::shared_ptr<base::Channel<char>> compact_cancel;

    // cmtx ensures that compactions and deletions don't run simultaneously.
    base::MutexLock cmutex_;

    // auto_compact_mutex_ ensures that no compaction gets triggered while
    // changing the auto_compact var.
    base::MutexLock auto_compact_mutex_;
    bool auto_compact;

    std::shared_ptr<base::ThreadPool> pool_;
    error::Error err_;

public:
    DB(const std::string& dir_, const Options& options = DefaultOptions);

    std::string dir() { return dir_; }

    std::shared_ptr<head::Head> head() { return head_; }

    error::Error error() { return err_; }

    std::deque<std::shared_ptr<block::BlockInterface>> blocks();

    std::shared_ptr<block::BlockInterface> get_block(const ulid::ULID& ulid);

    // return corrupted blocks.
    std::unordered_map<ulid::ULID, error::Error>
    open_blocks(std::deque<std::shared_ptr<block::BlockInterface>>& blocks_);

    // deletable_blocks sorts the loadable blocks and
    // returns all blocks past retention policy.
    std::unordered_map<ulid::ULID, std::shared_ptr<block::BlockInterface>>
    deletable_blocks(
        std::deque<std::shared_ptr<block::BlockInterface>>& blocks_);
    std::unordered_map<ulid::ULID, std::shared_ptr<block::BlockInterface>>
    beyond_time_retention(
        std::deque<std::shared_ptr<block::BlockInterface>>& blocks_);
    std::unordered_map<ulid::ULID, std::shared_ptr<block::BlockInterface>>
    beyond_size_retention(
        std::deque<std::shared_ptr<block::BlockInterface>>& blocks_);

    // delete_blocks closes and deletes blocks from the disk.
    // When the map contains a non nil block object it means it is loaded in
    // memory so needs to be closed first as it might need to wait for pending
    // readers to complete.
    void delete_blocks(
        const std::unordered_map<
            ulid::ULID, std::shared_ptr<block::BlockInterface>>& deletable);

    // validate_block_sequence returns error if given block meta files indicate
    // that some blocks overlaps within sequence. blocks are sorted by min_time.
    error::Error validate_block_sequence(
        const std::deque<std::shared_ptr<block::BlockInterface>>& blocks_);

    // reload blocks and trigger head truncation if new blocks appeared.
    // Blocks that are obsolete due to replacement or retention will be deleted.
    error::Error reload();

    // Compact data if possible. After successful compaction blocks are reloaded
    // which will also trigger blocks to be deleted that fall out of the
    // retention window. If no blocks are compacted, the retention window state
    // doesn't change. Thus, this is sufficient to reliably delete old data. Old
    // blocks are only deleted on reload based on the new block's parent
    // information. See DB.reload documentation for further information.
    error::Error compact();

    void run();

    void close();

    void disable_auto_compaction()
    {
        base::MutexLockGuard lock(auto_compact_mutex_);
        auto_compact = false;
        LOG_INFO << "msg=\"auto compaction disabled\"";
    }
    void enable_auto_compaction()
    {
        base::MutexLockGuard lock(auto_compact_mutex_);
        auto_compact = true;
        LOG_INFO << "msg=\"auto compaction enabled\"";
    }

    std::shared_ptr<base::Channel<char>> compact_channel() { return compactc; }

    std::unique_ptr<db::AppenderInterface> appender();

    std::pair<std::unique_ptr<querier::QuerierInterface>, error::Error>
    querier(int64_t mint, int64_t maxt);

    error::Error
    del(int64_t mint, int64_t maxt,
        const std::deque<std::shared_ptr<label::MatcherInterface>>& matchers);

    // clean_tombstones re-writes any blocks with tombstones.
    error::Error clean_tombstones();

    ~DB() { close(); }
};

} // namespace db
} // namespace tsdb

#endif
