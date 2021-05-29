#include "db/DB.hpp"
#include "db/DBAdapter.hpp"

#include "insert_data_loader.h"
#include "mixed_data_loader.h"
#include "null_storage.h"
#include "promql/web/http_server.h"
#include "tagtree/adapters/prom/indexed_storage.h"
#include "tagtree/series/series_file_manager.h"

#include "cxxopts.hpp"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <random>
typedef std::chrono::high_resolution_clock Clock;
namespace fs = std::filesystem;

using cxxopts::OptionException;

cxxopts::ParseResult parse(int argc, char* argv[])
{
    try {
        cxxopts::Options options(argv[0], " - TSBS benchmark for tagtree");

        options.add_options()("r,root", "Database root directory",
                              cxxopts::value<std::string>())(
            "d,data", "Path to the data file to load",
            cxxopts::value<std::string>())(
            "w,workload", "Workload type (insert, query, mixed)",
            cxxopts::value<std::string>())("q,query", "Query type (1-3)",
                                           cxxopts::value<int>())(
            "a,alpha", "Alpha for Zipfian distribution",
            cxxopts::value<double>())(
            "s,cache-size", "Series cache size (default 10000000)",
            cxxopts::value<size_t>()->default_value("10000000"))(
            "t,rw-ratio", "Read-write ratio",
            cxxopts::value<double>()->default_value("0.0"))(
            "n,ncpu", "Number of workers",
            cxxopts::value<size_t>()->default_value("8"))(
            "b,bitmap", "Use bitmap-only mode")("f,full", "Use full DB mode")(
            "p,partial", "Use partial cache mode")(
            "g,range", "Time range of query",
            cxxopts::value<int>()->default_value("1"))("m,randomize",
                                                       "Randomize time range")(
            "c,checkpoint", "Checkpoint policy",
            cxxopts::value<std::string>()->default_value("normal"))(
            "k,runs", "Repeated runs of query",
            cxxopts::value<int>()->default_value("100"))("h,help",
                                                         "Print help");

        auto result = options.parse(argc, argv);

        if (result.count("help")) {
            std::cerr << options.help({""}) << std::endl;
            exit(0);
        }

        return result;
    } catch (const OptionException& e) {
        exit(1);
    }
}

template <typename T>
T get_arg(const cxxopts::ParseResult& result, const std::string& arg_name)
{
    if (!result.count(arg_name)) {
        throw OptionException("option '" + arg_name +
                              "' is required but not given");
    }

    return result[arg_name].as<T>();
}

std::tuple<uint64_t, uint64_t> get_time_range(int time_range, bool randomize)
{
    uint64_t start, end;
    start = 1569888000000;

    if (time_range == 5) {
        end = 1569924000000;
    } else {
        end = 1569974400000;
    }

    if (!randomize) return std::tie(start, end);

    const uint64_t hour = 3600000;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> start_distrib(start, end - hour);
    auto random_start = start_distrib(gen);
    std::uniform_int_distribution<uint64_t> end_distrib(
        std::min(random_start + hour, end - 1), end);
    auto random_end = end_distrib(gen);

    return std::tie(random_start, random_end);
}

void bench_query(tagtree::prom::IndexedStorage& storage, int query_type,
                 int runs, int time_range, bool randomize)
{
    // const std::vector<promql::LabelMatcher> queries[]{
    //     {{promql::MatchOp::EQL, "__name__", "cpu"},
    //      {promql::MatchOp::NEQ, "__metric__", "usage_user"},
    //      {promql::MatchOp::EQL, "hostname", "host_1"}},
    //     {{promql::MatchOp::EQL, "__name__", "cpu"},
    //      {promql::MatchOp::NEQ, "__metric__", "usage_user"}},
    //     {{promql::MatchOp::EQL, "__name__", "cpu"},
    //      {promql::MatchOp::NEQ, "__metric__", "usage_user"},
    //      {promql::MatchOp::EQL_REGEX, "hostname", "host_1\\d{3}$"}},
    //     {{promql::MatchOp::EQL, "__name__", "cpu"},
    //      {promql::MatchOp::EQL, "__metric__", "usage_user"},
    //      {promql::MatchOp::EQL, "hostname", "host_1"}},
    //     {{promql::MatchOp::EQL, "__name__", "cpu"},
    //      {promql::MatchOp::EQL, "__metric__", "usage_user"},
    //      {promql::MatchOp::EQL_REGEX, "hostname", "host_[1-8]$"}},
    //     {{promql::MatchOp::EQL, "__name__", "cpu"},
    //      {promql::MatchOp::NEQ, "__metric__", "usage_user"},
    //      {promql::MatchOp::NEQ_REGEX, "hostname", "host_1\\d{3}$"}},
    //     {{promql::MatchOp::EQL, "__name__", "cpu"},
    //      {promql::MatchOp::EQL_REGEX, "__metric__",
    //       "usage_(user|system|idle|nice|iowait)"},
    //      {promql::MatchOp::EQL, "hostname", "host_1"}},
    //     {{promql::MatchOp::EQL, "__name__", "cpu"},
    //      {promql::MatchOp::EQL_REGEX, "__metric__",
    //       "usage_(user|system|idle|nice|iowait)"},
    //      {promql::MatchOp::EQL_REGEX, "hostname", "host_[1-8]$"}},
    // };

    const std::vector<promql::LabelMatcher> queries[]{
        // single-groupby-1-1-1
        {{promql::MatchOp::EQL, "__name__", "cpu"},
         {promql::MatchOp::EQL, "__metric__", "usage_user"},
         {promql::MatchOp::EQL, "hostname", "host_1"}},
        // cpu-max-all-1
        {{promql::MatchOp::EQL, "__name__", "cpu"},
         {promql::MatchOp::EQL, "hostname", "host_1"}},
        // single-groupby-1-8-1
        {{promql::MatchOp::EQL, "__name__", "cpu"},
         {promql::MatchOp::EQL, "__metric__", "usage_user"},
         {promql::MatchOp::EQL_REGEX, "hostname", "host_[1-8]$"}},
        // cpu-max-all-8
        {{promql::MatchOp::EQL, "__name__", "cpu"},
         {promql::MatchOp::EQL_REGEX, "hostname", "host_[1-8]$"}},
        // single-groubpy-5-8-1
        {{promql::MatchOp::EQL, "__name__", "cpu"},
         {promql::MatchOp::EQL_REGEX, "__metric__",
          "usage_(user|system|idle|nice|iowait)"},
         {promql::MatchOp::EQL_REGEX, "hostname", "host_[1-8]$"}},
        // large inequality
        {{promql::MatchOp::EQL, "__name__", "cpu"},
         {promql::MatchOp::NEQ, "__metric__", "usage_user"}},
        // low-selectivity regexp
        {{promql::MatchOp::EQL, "__name__", "cpu"},
         {promql::MatchOp::NEQ, "__metric__", "usage_user"},
         {promql::MatchOp::EQL_REGEX, "hostname", "host_1\\d{3}$"}},
        // high-selectivity regexp
        {{promql::MatchOp::EQL, "__name__", "cpu"},
         {promql::MatchOp::NEQ, "__metric__", "usage_user"},
         {promql::MatchOp::NEQ_REGEX, "hostname", "host_1\\d{3}$"}},
    };

    std::cout << "time,count" << std::endl;

    for (int i = 0; i < runs; i++) {
        auto t1 = Clock::now();
        auto [start, end] = get_time_range(time_range, randomize);
        auto q = storage.querier(start, end);
        auto ss = q->select(queries[query_type - 1]);

        int count = 0;
        while (ss->next()) {
            std::vector<promql::Label> labels;
            ss->at()->labels(labels);
            count++;
        }
        auto t2 = Clock::now();

        std::cout << std::chrono::duration_cast<std::chrono::microseconds>(t2 -
                                                                           t1)
                         .count()
                  << "," << count << std::endl;
    }
}

int main(int argc, char* argv[])
{
    auto result = parse(argc, argv);

    std::string dir;
    std::string workload, checkpoint_option;
    size_t cache_size;
    size_t ncpu;
    int time_range, query_runs;
    bool bitmap_only, full_db, partial_cache, randomize;
    try {
        dir = get_arg<std::string>(result, "root");
        workload = get_arg<std::string>(result, "workload");
        cache_size = result["cache-size"].as<size_t>();
        ncpu = result["ncpu"].as<size_t>();
        bitmap_only = result["bitmap"].as<bool>();
        full_db = result["full"].as<bool>();
        partial_cache = result["partial"].as<bool>();

        time_range = result["range"].as<int>();
        randomize = result["randomize"].as<bool>();
        query_runs = result["runs"].as<int>();

        checkpoint_option = result["checkpoint"].as<std::string>();
    } catch (const OptionException& e) {
        std::cerr << "Failed to parse options: " << e.what() << std::endl;
        exit(1);
    }

    tagtree::CheckpointPolicy checkpoint_policy =
        tagtree::CheckpointPolicy::NORMAL;

    if (checkpoint_option == "disabled")
        checkpoint_policy = tagtree::CheckpointPolicy::DISABLED;
    else if (checkpoint_option == "print")
        checkpoint_policy = tagtree::CheckpointPolicy::PRINT;

    fs::path root_path(dir);
    fs::path index_path;

    tagtree::Storage* storage = nullptr;

    if (full_db) {
        tsdb::base::Logger::setLogLevel(tsdb::base::Logger::NUM_LOG_LEVELS);

        tsdb::db::DB* db = new tsdb::db::DB(root_path.string());
        storage = new tsdb::db::DBAdapter(db);

        index_path = root_path / "index";

        fs::create_directory(index_path);
    } else {
        storage = new NullStorage();

        index_path = root_path;
    }

    fs::path series_path = index_path / "series";

    tagtree::SeriesFileManager sfm(cache_size, series_path.string(), 50000);
    tagtree::prom::IndexedStorage indexed_storage(
        index_path.string(), 4096, storage, &sfm, bitmap_only, !partial_cache,
        checkpoint_policy);

    if (workload == "insert") {
        std::string data_dir;

        try {
            data_dir = get_arg<std::string>(result, "data");
        } catch (const OptionException& e) {
            std::cerr << "Failed to parse options: " << e.what() << std::endl;
            exit(1);
        }

        InsertDataLoader data_loader(data_dir);
        data_loader.load(indexed_storage, ncpu);

        indexed_storage.get_index()->manual_compact();
    } else if (workload == "query") {
        int query_type;
        try {
            query_type = get_arg<int>(result, "query");

            if (query_type <= 0 || query_type > 8) {
                throw OptionException("invalid query type");
            }
        } catch (const OptionException& e) {
            std::cerr << "Failed to parse options: " << e.what() << std::endl;
            exit(1);
        }

        bench_query(indexed_storage, query_type, query_runs, time_range,
                    randomize);
    } else if (workload == "mixed") {
        std::string data_dir;
        double alpha;
        double rw_ratio;

        try {
            data_dir = get_arg<std::string>(result, "data");
            alpha = get_arg<double>(result, "alpha");
            rw_ratio = result["rw-ratio"].as<double>();
        } catch (const OptionException& e) {
            std::cerr << "Failed to parse options: " << e.what() << std::endl;
            exit(1);
        }

        MixedDataLoader data_loader(data_dir, alpha, rw_ratio);
        data_loader.load(indexed_storage, 8);
    } else {
        std::cerr << "Unknown workload type" << std::endl;
        exit(1);
    }
    // DataLoader data_loader(
    //     "/home/jimx/prometheus-data-cpu-only-s10m-i1m-100000");
    // DataLoader
    // data_loader("/home/jimx/prometheus-data-cpu-only-s1d-i1h-20000");
    // DataLoader data_loader(
    //     "/home/jimx/prometheus-data-cpu-only-s1d-i10m-10000");

    return 0;
}
