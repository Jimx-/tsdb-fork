#include "bptree/heap_page_cache.h"
#include "bptree/mem_page_cache.h"
#include "null_storage.h"
#include "tagtree/adapters/prom/indexed_storage.h"
#include "tagtree/series/series_file_manager.h"
#include "tagtree/tree/cow_tree_node.h"

#include <cassert>
#include <filesystem>
#include <signal.h>

#define SCALE 10

class TempDir {
public:
    TempDir()
    {
        char* tempname = strdup("/tmp/tagtree-XXXXXX");
        dirname = mkdtemp(tempname);
    }

    ~TempDir()
    {
        std::filesystem::remove_all(dirname);
        free(dirname);
        dirname = nullptr;
    }

    std::filesystem::path get_path() const { return dirname; }

private:
    char* dirname;
};

int count_series_set(promql::SeriesSet& ss)
{
    int count = 0;
    while (ss.next())
        count++;
    return count;
}

int main()
{
    TempDir temp_dir;
    auto temp_path = temp_dir.get_path();
    auto series_path = temp_path / "series";

    NullStorage null_storage;
    tagtree::SeriesFileManager sfm(200000, series_path.c_str(), 50000);
    tagtree::prom::IndexedStorage indexed_storage(temp_path.c_str(), 4096,
                                                  &null_storage, &sfm);

    sfm.add(1, {{"a", "1"}, {"b", "2"}, {"c", "3"}});
    sfm.add(2, {{"a", "2"}, {"b", "3"}, {"c", "4"}});
    std::cout << sfm.get_by_label_set({{"a", "1"}, {"b", "2"}, {"c", "3"}})
              << std::endl;
    return 0;
    {
        auto appender = indexed_storage.appender();
        uint64_t t = 1000;

        for (int i = 0; i < SCALE; i++) {
            for (int j = 0; j < SCALE; j++) {
                for (int k = 0; k < SCALE; k++) {
                    appender->add({{"i", std::to_string(i)},
                                   {"j", std::to_string(j)},
                                   {"k", std::to_string(k)}},
                                  t, 0);
                    t++;
                }
            }
        }
        appender->commit();
    }

    auto querier = indexed_storage.querier(1000, 2000);

    {
        auto ss = querier->select({{promql::MatchOp::EQL, "i", "5"}});
        assert(count_series_set(*ss) == 100 && "i == 5 -> 100");
    }

    {
        auto ss = querier->select({{promql::MatchOp::NEQ, "i", "5"}});
        assert(count_series_set(*ss) == 900 && "i != 5 -> 900");
    }

    indexed_storage.get_index()->manual_compact();

    {
        auto ss = querier->select({{promql::MatchOp::EQL, "i", "5"}});
        assert(count_series_set(*ss) == 100 && "i == 5 -> 100");
    }

    {
        auto ss = querier->select({{promql::MatchOp::NEQ, "i", "5"}});
        assert(count_series_set(*ss) == 900 && "i != 5 -> 900");
    }

    //     using COWTreeType = tagtree::COWTree<100, uint32_t,
    //     uint32_t>; std::unique_ptr<bptree::AbstractPageCache>
    //     page_cache;

    //     page_cache =
    //     std::make_unique<bptree::HeapPageCache>("test-pc", true,
    //     4096); COWTreeType cow_tree(page_cache.get());

    //     int N = 1000000;

    //     // #define INSERT

    // #ifdef INSERT
    //     COWTreeType::Transaction txn;
    //     cow_tree.get_write_tree(txn);
    //     for (uint32_t i = 0; i < N; i++) {
    //         cow_tree.insert(i, i, txn);
    //     }
    //     cow_tree.commit(txn);

    //     COWTreeType::Transaction txn2;
    //     cow_tree.get_write_tree(txn2);
    //     for (uint32_t i = 0; i < N; i++) {
    //         if (!cow_tree.update(i, i + 100, txn2)) {
    //             printf("%d not updated\n", i);
    //         }
    //     }
    //     cow_tree.commit(txn2);
    // #else
    //     for (uint32_t i = 0; i < N; i++) {
    //         std::vector<uint32_t> v;
    //         cow_tree.get_value(i, v);

    //         if (v.size() != 1) printf("no value %d %d\n", i,
    //         v.size()); if (*v.begin() != i + 100) printf("wrong
    //         value %d %d\n", i, *v.begin());
    //     }

    //     auto it = cow_tree.begin(0);
    //     int count = 0;
    //     while (it != cow_tree.end()) {
    //         if (it->second != it->first + 100)
    //             printf("iter wrong value %d %d\n", it->first,
    //             it->second);
    //         it++;
    //         count++;
    //     }

    //     if (count != N) printf("wrong elements %d\n", count);
    // #endif

    return 0;
}
