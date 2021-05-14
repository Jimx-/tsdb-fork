#include "mixed_data_loader.h"

#include <cstdlib>
#include <iostream>
#include <thread>
#include <vector>

extern "C"
{
    extern int zipf(double alpha, int n);
}

static void split(const std::string& s, std::vector<std::string>& sv,
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

MixedDataLoader::MixedDataLoader(std::string_view filename, double alpha,
                                 double rw_ratio)
    : BaseDataLoader(filename), stopped(false), alpha(alpha), rw_ratio(rw_ratio)
{}

void MixedDataLoader::load(tagtree::prom::IndexedStorage& storage,
                           int num_workers)
{
    load_file();

    int N = dataset.size();

    stopped = false;
    inserted_rows.store(0);
    queried_rows.store(0);
    std::vector<std::thread> threads;
    for (int i = 0; i < num_workers; i++) {
        threads.emplace_back([this, &storage, i, N, num_workers]() {
            auto zipf_N = std::min(N, 100000);

            for (int k = 0; k < N / num_workers; k++) {
                int r = zipf(this->alpha, zipf_N);
                auto row = this->dataset[r - 1];

                std::vector<std::string> args, tags, metrics;
                std::vector<promql::Label> labs;
                split(row, args);

                split("__name__=" + args[0], tags, ',');
                for (auto&& tag : tags) {
                    auto pos = tag.find('=');
                    labs.emplace_back(tag.substr(0, pos), tag.substr(pos + 1));
                }

                auto timestamp = std::stoull(std::string(args[2]));
                timestamp /= 1000000;

                split(args[1], metrics, ',');

                double eps = (double)rand() / RAND_MAX;

                if (eps > rw_ratio) {
                    auto app = storage.appender();

                    for (auto&& metric : metrics) {
                        auto pos = metric.find('=');
                        auto metric_name = metric.substr(0, pos);
                        auto metric_value =
                            metric.substr(pos + 1, metric.length() - 1);
                        labs.emplace_back("__metric__", metric_name);

                        auto value = std::stoi(metric_value);
                        app->add(labs, timestamp, value);
                        labs.pop_back();
                    }

                    this->inserted_rows.fetch_add(1);

                    app->commit();
                } else {
                    std::vector<promql::LabelMatcher> matchers;
                    auto q = storage.querier(1569888000000, 1569974400000);

                    for (auto&& lab : labs) {
                        matchers.push_back(
                            {promql::MatchOp::EQL, lab.name, lab.value});
                    }
                    for (auto&& metric : metrics) {
                        auto pos = metric.find('=');
                        auto metric_name = metric.substr(0, pos);
                        labs.emplace_back("__metric__", metric_name);

                        matchers.push_back(
                            {promql::MatchOp::EQL, "__metric__", metric_name});

                        auto ss = q->select(matchers);
                        matchers.pop_back();
                    }

                    this->queried_rows.fetch_add(1);
                }
            }

            stopped = true;
        });
    }

    threads.emplace_back([this, &storage, num_workers]() {
        size_t prev = 0;
        double secs = 0;
        double interval = 0.05;

        std::cout << "time,per. row/s,row total,overall row/s" << std::endl;
        while (!stopped) {
            usleep(interval * 1000000ULL);
            secs += interval;

            size_t current =
                this->inserted_rows.load() + this->queried_rows.load();
            size_t delta = current - prev;
            std::cout << time(nullptr) << "," << delta / interval << ","
                      << current << "," << current / secs << std::endl;

            if (stopped) break;

            prev = current;
        }

        // std::cout << std::endl;
        // std::cout << "Summary:" << std::endl;
        // std::cout << "loaded " << this->inserted_row * 10 << " metrics in "
        //           << secs << " secs with " << num_workers
        //           << " workers(mean rate " << this->inserted_row * 10 / secs
        //           << " / sec)" << std::endl;
    });

    for (auto&& t : threads) {
        t.join();
    }
}
