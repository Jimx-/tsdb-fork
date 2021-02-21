#include "insert_data_loader.h"

#include <chrono>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iostream>
#include <thread>
#include <unistd.h>
#include <vector>

typedef std::chrono::high_resolution_clock Clock;

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

InsertDataLoader::InsertDataLoader(std::string_view filename)
    : BaseDataLoader(filename)
{}

void InsertDataLoader::load(tagtree::prom::IndexedStorage& storage,
                            int num_workers)
{
    load_file();
    rowidx.store(0);
    stopped.store(0);

    std::vector<std::thread> threads;
    rowidx.store(0);
    inserted_row.store(0);
    for (int i = 0; i < num_workers; i++) {
        threads.emplace_back([this, &storage, i]() {
            while (true) {
                size_t ridx = this->rowidx.fetch_add(1000);
                if (ridx >= this->dataset.size()) {
                    break;
                }

                auto app = storage.appender();

                for (size_t r = ridx;
                     r < this->dataset.size() && r < ridx + 1000; r++) {
                    auto row = this->dataset[r];

                    std::vector<std::string> args, tags, metrics;
                    std::vector<promql::Label> labs;
                    split(row, args);

                    split("__name__=" + args[0], tags, ',');
                    for (auto&& tag : tags) {
                        auto pos = tag.find('=');
                        labs.emplace_back(tag.substr(0, pos),
                                          tag.substr(pos + 1));
                    }

                    auto timestamp = std::stoull(std::string(args[2]));
                    timestamp /= 1000000;

                    split(args[1], metrics, ',');
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

                    this->inserted_row.fetch_add(1);
                }

                app->commit();
            }

            stopped.fetch_add(1, std::memory_order_release);
        });
    }

    threads.emplace_back([this, &storage, num_workers]() {
        size_t prev = 0;
        double secs = 0;
        double interval = 0.05;
        auto last_time = Clock::now();

        std::cout << "time,per. row/s,row total,overall row/s" << std::endl;
        while (stopped.load() != num_workers) {
            double elapsed;

            usleep(interval * 1000000ULL);

            size_t current = this->inserted_row.load();
            size_t delta = current - prev;
            auto current_time = Clock::now();

            elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                          current_time - last_time)
                          .count() /
                      1000000.0;

            secs += elapsed;

            if (stopped.load() == num_workers) break;

            std::cout << Clock::to_time_t(current_time) << ","
                      << delta / elapsed << "," << current << ","
                      << current / secs << std::endl;

            prev = current;
            last_time = current_time;
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
