#include "tsdbutil/MockUtils.hpp"
#include "base/Logging.hpp"
#include "chunk/XORChunk.hpp"
#include "index/MemPostings.hpp"

namespace tsdb {
namespace tsdbutil {

void print_seriessamples(const SeriesSamples& s)
{
    std::string str = "";
    for (int i = 0; i < s.chunks.size(); ++i) {
        str += "[";
        for (int j = 0; j < s.chunks[i].size(); ++j)
            str += "<" + std::to_string(s.chunks[i][j].t) + "," +
                   std::to_string(s.chunks[i][j].v) + ">,";
        str += "],";
    }
    LOG_DEBUG << str;
}

std::tuple<std::shared_ptr<block::IndexReaderInterface>,
           std::shared_ptr<block::ChunkReaderInterface>, int64_t, int64_t>
create_idx_chk_readers(std::deque<SeriesSamples>& tc)
{
    index::MemPostings postings(true);
    std::unordered_map<std::string, std::set<std::string>> ld;
    auto ir = new MockIndexReader();
    auto cr = new MockChunkReader();
    int64_t block_mint = std::numeric_limits<int64_t>::max();
    int64_t block_maxt = std::numeric_limits<int64_t>::min();

    uint64_t ref = 1; // ref for locating chunk.
    for (int i = 0; i < tc.size(); ++i) {
        Series s;
        for (auto const& chk : tc[i].chunks) {
            if (chk.front().t < block_mint) block_mint = chk.front().t;
            if (chk.back().t > block_maxt) block_maxt = chk.back().t;

            s.chunks.push_back(std::shared_ptr<chunk::ChunkMeta>(
                new chunk::ChunkMeta(ref, chk.front().t, chk.back().t)));
            std::shared_ptr<chunk::ChunkInterface> chunk(new chunk::XORChunk());
            auto app = chunk->appender();
            for (auto const& sample : chk)
                app->append(sample.t, sample.v);
            cr->chunks[ref++] = chunk;
            s.chunks.back()->chunk = chunk;
        }
        ir->series_[common::TSID()] = s;
    }

    return std::make_tuple(std::shared_ptr<block::IndexReaderInterface>(ir),
                           std::shared_ptr<block::ChunkReaderInterface>(cr),
                           block_mint, block_maxt);
}

} // namespace tsdbutil
} // namespace tsdb
