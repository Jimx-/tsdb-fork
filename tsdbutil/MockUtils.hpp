#ifndef MOCKUTIlS_H
#define MOCKUTIlS_H
/*
    This header file defines some mocking classes which are useful
    in unit tests. The mocking class will store the results in memory.
*/

#include <deque>
#include <limits>
#include <set>
#include <stdint.h>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "block/BlockInterface.hpp"
#include "block/ChunkReaderInterface.hpp"
#include "block/ChunkWriterInterface.hpp"
#include "block/IndexReaderInterface.hpp"
#include "block/IndexWriterInterface.hpp"
#include "chunk/ChunkMeta.hpp"
#include "common/tsid.h"
#include "index/IndexUtils.hpp"
#include "index/PostingSet.hpp"
#include "label/Label.hpp"
#include "tombstone/MemTombstones.hpp"
#include "tsdbutil/ListStringTuples.hpp"

namespace tsdb {
namespace tsdbutil {

class Sample {
public:
    int64_t t;
    double v;
    Sample() : t(0), v(0) {}
    Sample(int64_t t, double v) : t(t), v(v) {}
    Sample(const std::pair<int64_t, double>& p) : t(p.first), v(p.second) {}
    bool operator==(const Sample& s) const { return t == s.t && v == s.v; }
    bool operator!=(const Sample& s) const { return !(*this == s); }
};

class Series {
public:
    common::TSID tsid;
    std::vector<std::shared_ptr<chunk::ChunkMeta>> chunks;
};

class SeriesSamples {
    // This just represent one time series.
public:
    common::TSID tsid;
    std::vector<std::vector<Sample>> chunks;

    SeriesSamples() = default;
    SeriesSamples(const common::TSID& tsid,
                  const std::vector<std::vector<Sample>>& chunks)
        : tsid(tsid), chunks(chunks)
    {}
    SeriesSamples(const common::TSID& tsid) : tsid(tsid) {}

    bool operator==(const SeriesSamples& s) const
    {
        if (tsid != s.tsid) return false;
        if (chunks.size() != s.chunks.size()) return false;
        for (int i = 0; i < chunks.size(); ++i) {
            if (chunks[i].size() != s.chunks[i].size()) return false;
            for (int j = 0; j < chunks[i].size(); ++j) {
                if (chunks[i][j] != s.chunks[i][j]) return false;
            }
        }
        return true;
    }
};

void print_seriessamples(const SeriesSamples& s);

class MockIndexReader : public block::IndexReaderInterface {
public:
    std::unordered_map<common::TSID, Series> series_;

    std::pair<uint64_t, bool> get_posting(const common::TSID& tsid)
    {
        return {0, false};
    }

    std::pair<std::unique_ptr<index::PostingsInterface>, bool>
    get_all_postings()
    {
        std::unordered_set<common::TSID> s;
        for (auto&& p : series_) {
            s.insert(p.first);
        }
        return {std::make_unique<index::PostingSet>(s), true};
    }

    bool series(const common::TSID& tsid,
                std::vector<std::shared_ptr<chunk::ChunkMeta>>& chunks)
    {
        if (series_.find(tsid) == series_.end()) return false;
        for (auto const& c : series_[tsid].chunks)
            chunks.push_back(c);
        return true;
    }

    bool error() { return false; }
    uint64_t size() { return 0; }
};

class MockChunkReader : public block::ChunkReaderInterface {
public:
    std::unordered_map<uint64_t, std::shared_ptr<chunk::ChunkInterface>> chunks;

    std::pair<std::shared_ptr<chunk::ChunkInterface>, bool> chunk(uint64_t ref)
    {
        if (chunks.find(ref) == chunks.end()) return {nullptr, false};
        return {chunks[ref], true};
    }

    std::pair<std::shared_ptr<chunk::ChunkInterface>, bool>
    chunk(const common::TSID& tsid, uint64_t ref)
    {
        return {nullptr, false};
    }

    bool error() { return false; }
    uint64_t size() { return 0; }
};

class MockIndexWriter : public block::IndexWriterInterface {
public:
    std::vector<SeriesSamples> series;

    int add_series(const common::TSID& tsid,
                   const std::vector<std::shared_ptr<chunk::ChunkMeta>>& chunks)
    {
        int i = -1;
        for (int j = 0; j < series.size(); ++j) {
            if ((series[j].tsid, tsid) == 0) {
                i = j;
                break;
            }
        }
        if (i == -1) {
            series.push_back(SeriesSamples(tsid));
            i = series.size() - 1;
        }

        for (auto const& cm : chunks) {
            std::vector<Sample> samples;

            auto it = cm->chunk->iterator();
            while (it->next()) {
                samples.emplace_back(it->at().first, it->at().second);
            }
            if (it->error()) return -1;
            series[i].chunks.push_back(samples);
        }
        return 0;
    }
};

class NopChunkWriter : public block::ChunkWriterInterface {
public:
    void
    write_chunks(const std::vector<std::shared_ptr<chunk::ChunkMeta>>& chunks)
    {}
    void close() {}
};

class MockBlockReader : public block::BlockInterface {
public:
    std::shared_ptr<block::IndexReaderInterface> ir;
    std::shared_ptr<block::ChunkReaderInterface> cr;
    int64_t min_time;
    int64_t max_time;

    MockBlockReader()
        : min_time(std::numeric_limits<int64_t>::max()),
          max_time(std::numeric_limits<int64_t>::min())
    {}
    MockBlockReader(const std::shared_ptr<block::IndexReaderInterface>& ir,
                    const std::shared_ptr<block::ChunkReaderInterface>& cr,
                    int64_t min_time, int64_t max_time)
        : ir(ir), cr(cr), min_time(min_time), max_time(max_time)
    {}

    // index returns an IndexReader over the block's data, succeed or not.
    std::pair<std::shared_ptr<block::IndexReaderInterface>, bool> index() const
    {
        return {ir, true};
    }

    // chunks returns a ChunkReader over the block's data, succeed or not.
    std::pair<std::shared_ptr<block::ChunkReaderInterface>, bool> chunks() const
    {
        return {cr, true};
    }

    // tombstones returns a TombstoneReader over the block's deleted data,
    // succeed or not.
    std::pair<std::shared_ptr<tombstone::TombstoneReaderInterface>, bool>
    tombstones() const
    {
        return {std::shared_ptr<tombstone::TombstoneReaderInterface>(
                    new tombstone::MemTombstones()),
                true};
    }

    int64_t MaxTime() const { return max_time; }

    int64_t MinTime() const { return min_time; }

    error::Error error() const { return error::Error(); }
};

std::tuple<std::shared_ptr<block::IndexReaderInterface>,
           std::shared_ptr<block::ChunkReaderInterface>, int64_t, int64_t>
create_idx_chk_readers(std::deque<SeriesSamples>& tc);

} // namespace tsdbutil
} // namespace tsdb

#endif
