# TSDB

A research-used c++11 version derived from Prometheus tsdb.  
We have implemented
* The original version of Prometheus tsdb
* The group chunk version based on our own idea.

## Introduction
### Time series
`identifier -> (t0, v0), (t1, v1), (t2, v2), (t3, v3), ....`
### identifier(labels)
Outside the braces defines the measurement.  
`requests_total{path="/status", method="GET", instance=”10.0.0.1:80”}`
The measurement can be converted to label(used in tsdb internally).  
`{__name__="requests_total", path="/status", method="GET", instance=”10.0.0.1:80”}`


## HelloWorld
```c++
#include <deque>
#include <iostream>
#include "db/DB.hpp"                // Include the DB api.
#include "label/EqualMatcher.hpp"   // EqualMatcher matches <label name, label value> exactly.

int main(){
    db::DB db("db_test");   // Create DB under db_test folder.

    // Generate 10 Labels to represent 10 time series.
    std::deque<label::Labels> lsets({
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

    // Append some data points for each time series.
    std::deque<pair<int64_t, double>> data({{1,1.1},{2,2.2},{3,3.3},{4,4.4},{5,5.5},{100,100.1},{200,200.2}});
    auto app = db.appender();                           // Create DB appender.
    for(auto & d: data){                                // For each data point.
        for(auto & lset: lsets){                        // For each time series
            auto p = app->add(lset, d.first, d.second); // Append data point.
        }
        auto e = app->commit();                         // Commit the appended data and they will be flushed to disk.
    }

    // Query data in time range [1, 1000].
    auto q = db.querier(1, 1000);                       // Create DB querier for time range [1, 1000].
                                                        // Select time series which contains Label <"a", "1">.
    auto s = q.first->select({shared_ptr<label::MatcherInterface>(new label::EqualMatcher("a", "1"))});
    while(s->next()){                                   // For each time series contains <"a", "1">.
        auto si = s->at();                              // Get current time series.
        auto it = si->iterator();                       // Get chunk iterator of current time series.
        int i = 0;
        while(it->next()){                              // Iterate chunk.
            std::cout << it->at() << std::endl;
            ++ i;
        }
    }

    // Delete data inside time range [1, 100] of time series containing Label <"k", "1">.
    db.del(1, 100, {shared_ptr<label::MatcherInterface>(new label::EqualMatcher("k", "1"))})
    return 0;
}
```

## Dependency
Boost 1.67 (Older version may be okay).

## Build & Test
To build the TSDB, run
```
$ mkdir build && cd build
$ cmake .. && make
$ ln -s ../promql/static
$ ln -s ../promql/templates
$ ./tsdb
```
and navigate to <http://localhost:9090/graph> for the web UI.

NOTE: some test files not contained in test/CMakeLists.txt may be outdated.

## Benchmark
### Write Benchmark
1k time series from test/20kseries.json, 3k points for each time series.  
Test on my Macbook Pro.
```
[          ] > complete stage=ingest duration=1.26669
[          ]   > total samples=3000000
[          ]   > samples/sec=2.36837e+06
```

Compare to the same test of Prometheus/tsdb/cmd/tsdb/main.go.
```
>> completed stage=ingestScrapes duration=1.269653028s
 > total samples: 3000000
 > samples/sec: 2.362834142494024e+06
 > head MaxTime:  90000000
```
### Query Benchmark
Use the data generated in the above write benchmark.  
I use the first label pair of the first 10 time series. Thus, totally 10 EqualMatcher.
For each EqaulMatcher, I create queriers for 1k ranges, each range cover 1k points.  
  
The test code I add to Prometheus/tsdb/cmd/tsdb/main.go is as follows.
```
func (b *writeBenchmark) query_bench(lsets []labels.Labels) (uint64) {
    var ranges []qrange
    for i := 0; i < 1000; i += 10 {
        ranges = append(ranges, qrange{start: (int64)(i * timeDelta), end: (int64)((i + 1000) * timeDelta)})
    }
    var samples []pair
    var total uint64
    for _, r := range ranges {
        querier, err := b.storage.Querier(r.start, r.end)
        if err != nil {
            panic("cannot get querier")
        }
        for i := 0; i < 10; i ++ {
            samples = samples[:0]
            ss, err := querier.Select(labels.NewEqualMatcher(lsets[i][0].Name, lsets[i][0].Value))
            if err != nil {
                panic("select error")
            }
            for ss.Next() {
                series := ss.At()

                it := series.Iterator()
                for it.Next() {
                    t, v := it.At()
                    samples = append(samples, pair{t: t, v: v})
                }
            }
            total += (uint64)(len(samples))
        }
    }
    return total
}
```
  
Result of my cpp version  
```
[          ] > complete stage=query duration=1.43306
[          ]   > total samples=24824552
```
Result of Prometheus/tsdb
```
>> completed stage=queryBench duration=1.8959129s
 > total samples: 24824552
```
### Index file component portion
In test/index_size_test.cpp.  
(1) For 10000 time series, each series has 200 points(2 chunks in test case). Print out the TOC and calculate portions.
```
DEBUG close symbols:5 series:107672 label_indices:873046 postings:1371188 label_indices_table:1943676 postings_table:1944696 end:2093398 - IndexWriter.cpp:426
DEBUG close symbols:0.05 series:0.37 label_indices:0.24 postings:0.27 label_indices_table:0.00 postings_table:0.07 - IndexWriter.cpp:427
```
(2) For 10000 time series, each series has 1000 points(10 chunks in test case). Print out the TOC and calculate portions.
```
DEBUG close symbols:5 series:107672 label_indices:1244478 postings:1742620 label_indices_table:2315108 postings_table:2316128 end:2465650 - IndexWriter.cpp:426
DEBUG close symbols:0.04 series:0.46 label_indices:0.20 postings:0.23 label_indices_table:0.00 postings_table:0.06 - IndexWriter.cpp:427
```

## Reference
[[1]](https://github.com/prometheus/tsdb) Prometheus tsdb github.  
[[2]](https://fabxc.org/tsdb/) Writing a Time Series Database from Scratch | Fabian Reinartz.  
[[3]](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf) Gorilla: A Fast, Scalable, In-Memory Time Series Database.  
[[4]](http://db.csail.mit.edu/projects/cstore/vldb.pdf) C-Store: A Column-oriented DBMS.  
[[5]](http://vldb.org/pvldb/vol5/p1790_andrewlamb_vldb2012.pdf) The Vertica Analytic Database: C-Store 7 Years Later.  
[[6]](https://github.com/chenshuo/muduo) Muduo: A C++ non-blocking network library for multi-threaded server in Linux.  
[[7]](https://github.com/suyash/ulid) A C++ ulid library.  
[[8]](https://github.com/tylertreat/chan) Pure C implementation of Go channels.  
[[9]](https://github.com/Tencent/rapidjson/) A fast JSON parser/generator for C++ with both SAX/DOM style API http://rapidjson.org/  
[[10]](https://stackoverflow.com/a/28311607) Detect if a rune is valid utf8.  
[[11]](https://github.com/timescale/tsbs) Time Series Benchmark Suite, a tool for comparing and evaluating databases for time series data.  
