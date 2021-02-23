#ifndef MEMPOSTINGS_H
#define MEMPOSTINGS_H

#include "base/Mutex.hpp"
#include "base/ThreadPool.hpp"
#include "base/WaitGroup.hpp"
#include "index/PostingSet.hpp"
#include "label/Label.hpp"
#include "tagtree/tsid.h"

#include <cstdint>
#include <deque>
#include <functional>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace tsdb {
namespace index {

class MemPostings {
private:
    base::RWMutexLock mutex_;
    std::set<tagtree::TSID> m;
    bool ordered;
    base::WaitGroup wg;

public:
    MemPostings(bool ordered = false);

    std::unique_ptr<PostingsInterface> all();

    // Used under lock.
    void add(tagtree::TSID tsid);

    void del(const std::unordered_set<tagtree::TSID>& deleted);

    void iter(const std::function<void(tagtree::TSID)>& f);

    // Used under lock.
    int size();

    // This ThreadPool can be shared at the same time.
    void ensure_order(base::ThreadPool* pool);

    // This ThreadPool can be shared at the same time.
    void ensure_order(const std::shared_ptr<base::ThreadPool>& pool);
};

} // namespace index
} // namespace tsdb

#endif
