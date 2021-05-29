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
#include <iomanip>
#include <iostream>
typedef std::chrono::high_resolution_clock Clock;
namespace fs = std::filesystem;

using cxxopts::OptionException;

#define METRIC_MAX 584

cxxopts::ParseResult parse(int argc, char* argv[])
{
    try {
        cxxopts::Options options(argv[0],
                                 " - Node exporter benchmark for tagtree");

        options.add_options()("r,root", "Database root directory",
                              cxxopts::value<std::string>())(
            "d,data", "Path to the node exporter data directory",
            cxxopts::value<std::string>())(
            "l,labels", "Name of the label JSON file",
            cxxopts::value<std::string>()->default_value("timeseries0.json"))(
            "o,output", "Name of the output file",
            cxxopts::value<std::string>()->default_value("result.csv"))(
            "t,targets", "Number of targets",
            cxxopts::value<int>()->default_value("70"))("h,help", "Print help");

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

static void split_string(const std::string& s, std::vector<std::string>& sv,
                         const char delim = ' ')
{
    sv.clear();

    if (s.empty()) return;

    auto p = s.c_str();
    auto lim = p + s.length();
    const char* c;

    do {
        c = (const char*)::memchr(p, delim, lim - p);
        if (!c) c = lim;
        sv.emplace_back(p, c - p);
        p = c + 1;
    } while (c != lim);
}

struct TimeSeries {
    std::vector<promql::Label> labels;
    std::vector<float> data;
};
using NodeData = std::vector<TimeSeries>;

void read_labels(const fs::path& labels_file,
                 std::vector<std::vector<promql::Label>>& labels)
{
    std::ifstream ifs(labels_file);
    if (!ifs.is_open()) throw std::runtime_error("Error reading label file");

    std::string line;
    while (std::getline(ifs, line) && labels.size() < METRIC_MAX) {
        if (!line.empty() && line[line.length() - 1] == '\n') {
            line.erase(line.length() - 1);
        }
        if (line.empty()) continue;
        std::vector<std::string> sv;
        std::vector<promql::Label> lbls;
        split_string(line.substr(1, line.length() - 2), sv, ',');
        for (auto&& s : sv) {
            auto pos = s.find(':');
            auto name = s.substr(1, pos - 2);
            auto value = s.substr(pos + 2, s.length() - pos - 3);
            if (name == "instance") continue;
            lbls.push_back({name, value});
        }
        labels.push_back(lbls);
    }
}

void read_node_exporter_data(
    int targets, const fs::path& data_path,
    const std::vector<std::vector<promql::Label>>& labels,
    std::vector<NodeData>& nodes)
{
    for (int i = 0; i < targets; i++) {
        NodeData node_data;
        for (auto&& lbls : labels) {
            std::stringstream ss;
            auto& ts = node_data.emplace_back();
            ts.labels = lbls;
            ss << "pc9" << std::setfill('0') << std::setw(4) << i << ":9100";
            ts.labels.emplace_back("instance", ss.str());
        }

        std::ifstream ifs(data_path / ("data" + std::to_string(i)));
        if (!ifs.is_open()) throw std::runtime_error("Error reading data file");

        std::string line;
        size_t count = 0;
        while (std::getline(ifs, line)) {
            if (!line.empty() && line[line.length() - 1] == '\n') {
                line.erase(line.length() - 1);
            }
            if (line.empty()) continue;
            std::vector<std::string> sv;
            split_string(line, sv, ',');
            count = std::max(count, sv.size());

            for (int i = 0; i < sv.size() && i < node_data.size(); i++) {
                double value = std::stod(sv[i]);
                node_data[i].data.push_back(value);
            }
        }

        nodes.emplace_back(std::move(node_data));
    }
}

void insert_thread(tagtree::prom::IndexedStorage& storage,
                   const std::vector<NodeData>& nodes, bool& stopped)
{
    int i = 0;
    auto prev = Clock::now();
    auto last_compaction = prev;
    const auto interval = std::chrono::seconds(5);
    const auto compact_interval = std::chrono::hours(1);
    auto total = 60 * 60 * 3 / 5;

    while (i < total) {
        auto app = storage.appender();
        auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                             prev.time_since_epoch())
                             .count();

        for (auto&& node : nodes) {
            for (auto&& ts : node) {
                // if (ts.data.empty()) {
                //     for (auto&& lbl : ts.labels) {
                //         std::cout << lbl.name << ":" << lbl.value <<
                //         std::endl;
                //     }
                // }
                app->add(ts.labels, timestamp, ts.data[i]);
            }
        }

        app->commit();

        if (i == 0 || prev - last_compaction > compact_interval)
            storage.get_index()->manual_compact();

        std::cout << i << "/" << total << std::endl;

        auto cur = Clock::now();
        auto delta = cur - prev;

        if (delta < interval) {
            std::this_thread::sleep_for(interval - delta);
        }

        i++;

        prev = Clock::now();
    }

    stopped = true;
}

void query_thread(tagtree::prom::IndexedStorage& storage,
                  const fs::path& output, bool& stopped)
{
    std::ofstream ofs(output);

    const auto interval = std::chrono::seconds(10);

    const std::vector<promql::LabelMatcher> queries[] = {
        {{promql::MatchOp::EQL, "__name__", "node_cpu_seconds_total"}},
        {{promql::MatchOp::EQL, "__name__", "node_memory_MemAvailable_bytes"}},
        {{promql::MatchOp::EQL, "__name__",
          "node_network_transmit_bytes_total"}},
    };

    ofs << "timestamp,cpu,cpu_count,mem,mem_count,transmit,transmit_count"
        << std::endl;

    auto prev = Clock::now();

    while (!stopped) {
        std::stringstream sstream;

        auto end = std::chrono::duration_cast<std::chrono::milliseconds>(
                       prev.time_since_epoch())
                       .count();
        auto start = std::chrono::duration_cast<std::chrono::milliseconds>(
                         (prev - std::chrono::minutes(5)).time_since_epoch())
                         .count();
        auto q = storage.querier(start, end);

        sstream << end;
        for (int i = 0; i < 3; i++) {
            auto t1 = Clock::now();
            auto ss = q->select(queries[i]);

            int count = 0;

            while (ss->next()) {
                std::vector<promql::Label> labels;
                ss->at()->labels(labels);
                count++;
            }
            auto t2 = Clock::now();

            auto elapsed =
                std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1)
                    .count();

            sstream << "," << elapsed << "," << count;
        }

        auto cur = Clock::now();
        auto delta = cur - prev;

        ofs << sstream.str() << std::endl;

        if (delta < interval) {
            std::this_thread::sleep_for(interval - delta);
        }

        prev = Clock::now();
    }
}

int main(int argc, char* argv[])
{
    auto result = parse(argc, argv);

    std::string dir, data_dir, labels_file, output_file;
    int targets;
    try {
        dir = get_arg<std::string>(result, "root");
        data_dir = get_arg<std::string>(result, "data");
        targets = result["targets"].as<int>();
        labels_file = result["labels"].as<std::string>();
        output_file = result["output"].as<std::string>();
    } catch (const OptionException& e) {
        std::cerr << "Failed to parse options: " << e.what() << std::endl;
        exit(1);
    }

    fs::path root_path(dir);
    fs::path data_path(data_dir);
    fs::path index_path;

    tagtree::Storage* storage = nullptr;

    tsdb::base::Logger::setLogLevel(tsdb::base::Logger::NUM_LOG_LEVELS);

    tsdb::db::DB* db = new tsdb::db::DB(root_path.string());
    storage = new tsdb::db::DBAdapter(db);

    index_path = root_path / "index";

    fs::create_directory(index_path);

    fs::path series_path = index_path / "series";

    tagtree::SeriesFileManager sfm(1000000, series_path.string(), 50000);
    tagtree::prom::IndexedStorage indexed_storage(index_path.string(), 4096,
                                                  storage, &sfm, true);

    std::vector<std::vector<promql::Label>> labels;
    read_labels(data_path / labels_file, labels);

    std::vector<NodeData> nodes;
    read_node_exporter_data(targets, data_path, labels, nodes);

    std::vector<std::thread> threads;

    bool stopped = false;

    threads.emplace_back([&indexed_storage, &nodes, &stopped] {
        insert_thread(indexed_storage, nodes, stopped);
    });
    threads.emplace_back([&indexed_storage, output_file, &stopped] {
        query_thread(indexed_storage, output_file, stopped);
    });

    for (auto&& t : threads)
        t.join();

    return 0;
}
