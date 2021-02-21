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
            "b,bitmap", "Use bitmap-only mode")("h,help", "Print help");

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

void bench_query(tagtree::prom::IndexedStorage& storage, int query_type)
{
    const std::vector<promql::LabelMatcher> queries[]{
        {{promql::MatchOp::EQL, "__name__", "cpu"},
         {promql::MatchOp::NEQ, "__metric__", "usage_user"},
         {promql::MatchOp::EQL, "hostname", "host_1"}},
        {{promql::MatchOp::EQL, "__name__", "cpu"},
         {promql::MatchOp::NEQ, "__metric__", "usage_user"}},
        {{promql::MatchOp::EQL, "__name__", "cpu"},
         {promql::MatchOp::NEQ, "__metric__", "usage_user"},
         {promql::MatchOp::EQL_REGEX, "hostname", "host_1\\d{3}$"}},
    };

    std::cout << "time,count" << std::endl;

    for (int i = 0; i < 100; i++) {
        auto t1 = Clock::now();
        auto q = storage.querier(0, UINT64_MAX);
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
    std::string workload;
    size_t cache_size;
    size_t ncpu;
    bool bitmap_only;
    try {
        dir = get_arg<std::string>(result, "root");
        workload = get_arg<std::string>(result, "workload");
        cache_size = result["cache-size"].as<size_t>();
        ncpu = result["ncpu"].as<size_t>();
        bitmap_only = result["bitmap"].as<bool>();
    } catch (const OptionException& e) {
        std::cerr << "Failed to parse options: " << e.what() << std::endl;
        exit(1);
    }

    // std::string dir = "./s10m-i1m-100000-3";
    // std::string dir = "./s1d-i1h-20000-2";
    // std::string dir = "./s1d-i10m-10000-2";

    fs::path root_path(dir);

    tagtree::Storage* storage = nullptr;

    tsdb::db::DB db(root_path.string());
    tsdb::db::DBAdapter adapter(&db);
    fs::path index_path = root_path / "index";
    fs::path series_path = index_path / "series";

    fs::create_directory(index_path);

    NullStorage null_storage;

    storage = &adapter;

    tagtree::SeriesFileManager sfm(cache_size, series_path.string(), 50000);
    tagtree::prom::IndexedStorage indexed_storage(index_path.string(), 4096,
                                                  storage, &sfm, bitmap_only);

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

            if (query_type <= 0 || query_type > 3) {
                throw OptionException("invalid query type");
            }
        } catch (const OptionException& e) {
            std::cerr << "Failed to parse options: " << e.what() << std::endl;
            exit(1);
        }

        bench_query(indexed_storage, query_type);
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
