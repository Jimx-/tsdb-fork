#ifndef BLOCKQUERIER_H
#define BLOCKQUERIER_H

#include "block/BlockInterface.hpp"
#include "block/ChunkReaderInterface.hpp"
#include "block/IndexReaderInterface.hpp"
#include "querier/QuerierInterface.hpp"
#include "tombstone/TombstoneReaderInterface.hpp"

namespace tsdb {
namespace querier {

class BlockQuerier : public QuerierInterface {
private:
    std::shared_ptr<block::IndexReaderInterface> indexr;
    std::shared_ptr<block::ChunkReaderInterface> chunkr;
    std::shared_ptr<tombstone::TombstoneReaderInterface> tombstones;
    int64_t min_time;
    int64_t max_time;
    mutable error::Error err_;

public:
    BlockQuerier(const std::shared_ptr<block::BlockInterface>& block,
                 int64_t min_time, int64_t max_time);

    std::shared_ptr<SeriesSetInterface>
    select(const std::unordered_set<tagtree::TSID>& l) const;

    std::deque<std::string> label_values(const std::string& s) const;

    std::deque<std::string> label_names() const;

    error::Error error() const { return err_; }
};

} // namespace querier
} // namespace tsdb

#endif
