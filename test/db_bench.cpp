#include <boost/bind.hpp>
#include <boost/filesystem.hpp>
#include <fstream>
#include <iostream>
#include <stdlib.h>
#include <unordered_set>
#include <vector>

#include "base/Atomic.hpp"
#include "base/TimeStamp.hpp"
#include "base/WaitGroup.hpp"
#include "db/DB.hpp"
#include "external/rapidjson/document.h"
#include "external/rapidjson/writer.h"
#include "external/rapidjson/stringbuffer.h"
#include "external/rapidjson/rapidjson.h"
#include "head/RangeHead.hpp"
#include "label/EqualMatcher.hpp"
#include "querier/BlockQuerier.hpp"
#include "querier/Querier.hpp"
#include "querier/QuerierUtils.hpp"
#include "test/TestUtils.hpp"
#include "wal/WAL.hpp"

using namespace std;
using namespace tsdb;

ulid::ULID create_block(const std::string & dir, 
    const deque<tsdbutil::RefSeries> & series, 
    const deque<deque<tsdbutil::RefSample>> & samples, 
    const deque<tsdbutil::Stone> & stones);

deque<pair<int64_t, double>> generate_data(int64_t min, int64_t max, int64_t step);

int64_t time_delta = 30000;

void ingest_shard(db::DB * db, deque<label::Labels> * lsets, int start, int end, int64_t base_t, base::AtomicInt * total, base::WaitGroup * wg){
    vector<pair<int64_t, double>> v;
    v.reserve(end - start);
    for(int i = start; i < end; ++ i)
        v.emplace_back(-1, 123456789);
    int count = 0;
    double cd = 0;
    auto s = base::TimeStamp::now();
    for(int i = 0; i < 100; ++ i){
        auto app = db->appender();
        base_t += time_delta;
        for(int j = 0; j < v.size(); ++ j){
            v[j].second += 1000;
            if(v[j].first == -1){
                auto p = app->add((*lsets)[start + j], base_t, v[j].second);
                ASSERT_FALSE(p.second);
                v[j].first = p.first;
            }
            else{
                auto e = app->add_fast(v[j].first, base_t, v[j].second);
                ASSERT_FALSE(e);
            }
            ++ count;
        }
        auto c = base::TimeStamp::now();
        auto e = app->commit();
        cd += base::timeDifference(base::TimeStamp::now(), c);
        ASSERT_FALSE(e);
    }
    TEST_COUT << "duration=" << base::timeDifference(base::TimeStamp::now(), s) << " commit duration=" << cd << endl;
    total->add(count);
    wg->done();
}

void db_bench(){
    boost::filesystem::remove_all("db_test");
    
    db::DB db("db_test/bench_write");
    ASSERT_FALSE(db.error());
    deque<label::Labels> lsets;
    {
        // Benchmark write.
        {
            auto start = base::TimeStamp::now();
            ifstream file("../20kseries.json");
            string line;
            unordered_set<uint64_t> hashes;
            while(getline(file, line) && lsets.size() < 1000){
                rapidjson::Document d;
                d.Parse(line.c_str());
                label::Labels lset;
                for(auto & m : d.GetObject())
                    lset.emplace_back(m.name.GetString(), m.value.GetString());
                sort(lset.begin(), lset.end());
                bool duplicate = false;
                uint64_t hash = label::lbs_hash(lset);
                if(hashes.find(hash) == hashes.end()){
                    lsets.push_back(lset);
                    hashes.insert(hash);
                }
            }
            TEST_COUT << "> complete stage=load labels duration=" << base::timeDifference(base::TimeStamp::now(), start) << endl;
        }
        {
            base::ThreadPool pool;
            pool.start(16);
            base::WaitGroup wg;
            base::AtomicInt total;
            int scrape_count = 3000;
            auto start = base::TimeStamp::now();
            for(int i = 0; i < scrape_count; i += 100){
                int si = 0;
                while(si < lsets.size()){
                    wg.add(1);
                    pool.run(boost::bind(&ingest_shard, &db, &lsets, si, si + 1000, time_delta * i, &total, &wg));
                    // ingest_shard(&db, &lsets, si, si + 1000, time_delta * i, &total, &wg);
                    si += 1000;
                }
                wg.wait();
                // TEST_COUT << "Round " << i << " Total " << total.get() << endl;
            }
            double duration = base::timeDifference(base::TimeStamp::now(), start);
            TEST_COUT << "> complete stage=ingest duration=" << duration << endl;
            TEST_COUT << "  > total samples=" << total.get() << endl;
            TEST_COUT << "  > samples/sec=" << (double)(total.get()) / duration << endl;
        }
    }
    sleep(5);
    {
        // Benchmark query.
        auto start = base::TimeStamp::now();
        deque<pair<int64_t, int64_t>> qranges;
        for(int i = 0; i < 1000; i += 10)
            qranges.emplace_back(i * time_delta, (i + 1000) * time_delta);
        int total = 0;
        vector<pair<int64_t, double>> samples;
        samples.reserve(1000);
        deque<shared_ptr<label::MatcherInterface>> matchers;
        for(int i = 0; i < 10; ++ i)
            matchers.push_back(shared_ptr<label::MatcherInterface>(new label::EqualMatcher(lsets[i][0].label, lsets[i][0].value)));
        for(auto & r: qranges){
            auto q = db.querier(r.first, r.second);
            if(q.second){
                TEST_COUT << "Error get Querier" << endl;
                break;
            }
            for(int i = 0; i < 10; ++ i){
                samples.clear();
                auto ss = q.first->select({matchers[i]});
                if(!ss){
                    TEST_COUT << "Error select" << endl;
                    continue;
                }
                while(ss->next()){
                    auto it = ss->at()->iterator();
                    while(it->next())
                        samples.push_back(it->at());
                }
                total += samples.size();
            }
        }
        TEST_COUT << "> complete stage=query duration=" << base::timeDifference(base::TimeStamp::now(), start) << endl;
        TEST_COUT << "  > total samples=" << total << endl;
    }
}