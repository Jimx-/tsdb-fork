#include "index/MemPostings.hpp"

#include <functional>

namespace tsdb {
namespace index {

MemPostings::MemPostings(bool ordered) : mutex_(), m(), ordered(ordered) {}

std::unique_ptr<PostingsInterface> MemPostings::all()
{
    return std::make_unique<PostingSet>(m);
}

// Used under lock.
void MemPostings::add(tagtree::TSID tsid) { m.insert(tsid); }

void MemPostings::del(const std::unordered_set<tagtree::TSID>& deleted)
{
    base::RWLockGuard mutex(mutex_, 1);

    for (auto&& d : deleted) {
        m.erase(d);
    }
}

void MemPostings::iter(const std::function<void(tagtree::TSID)>& f)
{
    base::RWLockGuard mutex(mutex_, 0);
    for (auto const& v_set : m) {
        f(v_set);
    }
}

// Used under lock.
int MemPostings::size() { return m.size(); }

// This ThreadPool can be shared at the same time.
// Use base::WaitGroup to ensure all sortings are finished.
void MemPostings::ensure_order(base::ThreadPool* pool) {}

// This ThreadPool can be shared at the same time.
// Use base::WaitGroup to ensure all sortings are finished.
void MemPostings::ensure_order(const std::shared_ptr<base::ThreadPool>& pool) {}

} // namespace index
} // namespace tsdb
