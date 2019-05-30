#include <boost/bind.hpp>
#include <boost/filesystem.hpp>
#include <fstream>
#include <iostream>
#include <vector>
#include <google/profiler.h>

#include "base/Atomic.hpp"
#include "base/TimeStamp.hpp"
#include "base/WaitGroup.hpp"
#include "chunk/ChunkWriter.hpp"
#include "chunk/XORChunk.hpp"
#include "db/DB.hpp"
#include "external/rapidjson/document.h"
#include "external/rapidjson/writer.h"
#include "external/rapidjson/stringbuffer.h"
#include "external/rapidjson/rapidjson.h"
#include "head/RangeHead.hpp"
#include "index/IndexWriter.hpp"
#include "label/EqualMatcher.hpp"
#include "querier/BlockQuerier.hpp"
#include "querier/Querier.hpp"
#include "querier/QuerierUtils.hpp"
#include "test/TestUtils.hpp"
#include "tombstone/MemTombstones.hpp"
#include "tombstone/TombstoneUtils.hpp"
#include "wal/WAL.hpp"

using namespace std;
using namespace tsdb;

ulid::ULID create_block(const std::string & dir, 
    const deque<tsdbutil::RefSeries> & series, 
    const deque<deque<tsdbutil::RefSample>> & samples, 
    const deque<tsdbutil::Stone> & stones)
{
    if(series.size() != samples.size())
        return ulid::ULID();
    ulid::ULID ulid = ulid::CreateNowRand();
    // TEST_COUT << ulid::Marshal(ulid) << endl;
    boost::filesystem::create_directories(tsdbutil::filepath_join(tsdbutil::filepath_join(dir, ulid::Marshal(ulid)), "chunks"));

    deque<deque<shared_ptr<chunk::ChunkMeta>>> metas;
    block::BlockMeta bm;
    for(const deque<tsdbutil::RefSample> & ss: samples){
        // Write chunk.
        bm.stats.num_samples += ss.size();
        std::shared_ptr<chunk::ChunkInterface> c(dynamic_cast<chunk::ChunkInterface*>(new chunk::XORChunk()));
        unique_ptr<chunk::ChunkAppenderInterface> app = c->appender();
        int64_t mint=numeric_limits<int64_t>::max(), maxt=numeric_limits<int64_t>::min();
        for(const auto & s: ss){
            if(s.t < mint)
                mint = s.t;
            if(s.t > maxt)
                maxt = s.t;
            app->append(s.t, s.v);
        }
        if(mint < bm.min_time)
            bm.min_time = mint;
        if(maxt > bm.max_time);
            bm.max_time = maxt;
        shared_ptr<chunk::ChunkMeta> m = shared_ptr<chunk::ChunkMeta>(new chunk::ChunkMeta(c, mint, maxt));
        deque<shared_ptr<chunk::ChunkMeta>> v;
        v.push_back(m);
        chunk::ChunkWriter cw(tsdbutil::filepath_join(tsdbutil::filepath_join(dir, ulid::Marshal(ulid)), "chunks"));
        cw.write_chunks(v);
        metas.push_back(v);
    }

    // Write index.
    index::IndexWriter iw(tsdbutil::filepath_join(tsdbutil::filepath_join(dir, ulid::Marshal(ulid)), "index"));
    unordered_set<string> symbols;
    map<string, set<string>> label_indices;
    map<string, map<string, deque<uint64_t>>> postings;
    for(const auto & s: series){
        for(const auto & l: s.lset){
            symbols.insert(l.label);
            symbols.insert(l.value);
            label_indices[l.label].insert(l.value);
            postings[l.label][l.value].push_back(s.ref);
        }
    }
    EXPECT_EQ(iw.add_symbols(symbols), 0);
    for(int i = 0; i < series.size(); ++ i)
        EXPECT_EQ(iw.add_series(series[i].ref, series[i].lset, metas[i]), 0);
    for(auto & pair: label_indices)
        EXPECT_EQ(iw.write_label_index({pair.first}, deque<string>(pair.second.begin(), pair.second.end())), 0);
    for(auto & p1: postings){
        for(auto & p2: p1.second){
            index::ListPostings l(p2.second);
            EXPECT_EQ(iw.write_postings(p1.first, p2.first, &l), 0);
        }
    }

    // Write tombstones.
    if(!stones.empty()){
        shared_ptr<tombstone::TombstoneReaderInterface> tr(new tombstone::MemTombstones());
        for(auto & s: stones){
            for(auto & iv: s.itvls){
                tr->add_interval(s.ref, iv);
            }
        }
        tombstone::write_tombstones(tsdbutil::filepath_join(dir, ulid::Marshal(ulid)), tr.get());
    }

    bm.ulid_ = ulid;
    bm.stats.num_chunks = samples.size();
    bm.stats.num_series = series.size();
    bm.stats.num_tombstones = stones.size();
    EXPECT_TRUE(write_block_meta(tsdbutil::filepath_join(dir, ulid::Marshal(ulid)), bm));
    return ulid;
}

deque<pair<int64_t, double>> generate_data(int64_t min, int64_t max, int64_t step){
    deque<pair<int64_t, double>> d;
    for(int64_t i = min; i < max; i += step)
        d.emplace_back(i, static_cast <float> (rand()) / static_cast <float> (RAND_MAX) * 1000);
    return d;
}

TEST(DBTest, All){
    boost::filesystem::remove_all("db_test");
    
    {
        auto data = generate_data(1, 4 * 3600 * 1000, 1000);
        { 
            db::DB db("db_test/test");
            ASSERT_FALSE(db.error());
            deque<label::Labels> lsets({
                {{"a", "1"}, {"b", "1"}},
                {{"a", "2"}, {"c", "1"}},
                {{"a", "3"}, {"d", "1"}},
                {{"a", "4"}, {"e", "1"}},
                {{"a", "5"}, {"f", "1"}},
                {{"a", "6"}, {"g", "1"}},
                {{"a", "7"}, {"h", "1"}},
                {{"a", "8"}, {"i", "1"}},
                {{"a", "9"}, {"j", "1"}},
                {{"a", "10"}, {"k", "1"}},
            });
            {
                auto app = db.appender();
                for(auto & d: data){
                    // TEST_COUT << d.first << endl;
                    for(auto & lset: lsets){
                        auto p = app->add(lset, d.first, d.second);
                        ASSERT_FALSE(p.second);
                    }
                    // TEST_COUT << d.first << endl;
                    auto e = app->commit();
                    ASSERT_FALSE(e);
                }
            }
            TEST_COUT << "append finished" << endl;
            {
                auto q = db.querier(1, 4 * 3600 * 1000);
                ASSERT_FALSE(q.second);
                auto s = q.first->select({shared_ptr<label::MatcherInterface>(new label::EqualMatcher("a", "1"))});
                ASSERT_TRUE(s);
                while(s->next()){
                    auto si = s->at();
                    auto it = si->iterator();
                    int i = 0;
                    while(it->next()){
                        ASSERT_EQ(it->at(), data[i]);
                        ++ i;
                    }
                }
            }
        }
        { 
            db::DB db("db_test/test");
            ASSERT_FALSE(db.error());
            int64_t mint = data[data.size() / 4].first;
            {
                // test DB::del().
                auto e = (db.del(mint, 4 * 3600 * 1000, {shared_ptr<label::MatcherInterface>(new label::EqualMatcher("a", "1"))}));
                TEST_COUT << e.error() << endl;
                ASSERT_FALSE(e);

                e = (db.del(1, mint, {shared_ptr<label::MatcherInterface>(new label::EqualMatcher("k", "1"))}));
                TEST_COUT << e.error() << endl;
                ASSERT_FALSE(e);
            }
            ASSERT_FALSE(db.reload());
            {
                auto q = db.querier(1, 4 * 3600 * 1000);
                ASSERT_FALSE(q.second);
                auto s = q.first->select({shared_ptr<label::MatcherInterface>(new label::EqualMatcher("a", "1"))});
                ASSERT_TRUE(s);
                while(s->next()){
                    auto si = s->at();
                    auto it = si->iterator();
                    int i = 0;
                    while(it->next()){
                        ASSERT_EQ(it->at(), data[i]);
                        ++ i;
                    }
                    ASSERT_EQ(i, data.size() / 4);
                }
            }
            {
                auto q = db.querier(1, 4 * 3600 * 1000);
                ASSERT_FALSE(q.second);
                auto s = q.first->select({shared_ptr<label::MatcherInterface>(new label::EqualMatcher("k", "1"))});
                ASSERT_TRUE(s);
                while(s->next()){
                    auto si = s->at();
                    auto it = si->iterator();
                    int i = data.size() / 4 + 1;
                    while(it->next()){
                        ASSERT_EQ(it->at(), data[i]);
                        ++ i;
                    }
                    ASSERT_EQ(i, 4 * 3600);
                }
            }
        }
    }
}