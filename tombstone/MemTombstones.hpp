#ifndef MEMTONBSTONES_H
#define MEMTONBSTONES_H

#include <unordered_map>

#include "base/Mutex.hpp"
#include "tombstone/Interval.hpp"
#include "tombstone/TombstoneReaderInterface.hpp"

namespace tsdb {

namespace tombstone {

class MemTombstones : public TombstoneReaderInterface {
public:
    std::unordered_map<tagtree::TSID, Intervals> interval_groups;
    mutable base::RWMutexLock mutex_;

    // NOTICE, may throw std::out_of_range.
    const Intervals& get(tagtree::TSID tsid) const
    {
        base::RWLockGuard mutex(mutex_, false);
        return interval_groups.at(tsid);
    }

    void iter(const IterFunc& f) const
    {
        base::RWLockGuard mutex(mutex_, false);
        for (auto const& pair : interval_groups)
            f(pair.first, pair.second);
    }
    error::Error iter(const ErrIterFunc& f) const
    {
        base::RWLockGuard mutex(mutex_, false);
        for (auto const& pair : interval_groups) {
            error::Error err = f(pair.first, pair.second);
            if (err) return error::wrap(err, "MemTombstones::iter");
        }
        return error::Error();
    }

    uint64_t total() const
    {
        base::RWLockGuard mutex(mutex_, false);
        uint64_t r = 0;
        for (auto const& pair : interval_groups)
            r += pair.second.size();
        return r;
    }

    void add_interval(tagtree::TSID tsid, const Interval& itvl)
    {
        base::RWLockGuard mutex(mutex_, true);
        itvls_add(interval_groups[tsid], itvl);
    }
};

} // namespace tombstone

} // namespace tsdb

#endif
