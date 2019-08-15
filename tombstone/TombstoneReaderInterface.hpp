#ifndef TOMBSTONEREADERINTERFACE_H
#define TOMBSTONEREADERINTERFACE_H

#include "base/Error.hpp"
#include "tagtree/tsid.h"
#include "tombstone/Interval.hpp"

#include <functional>

namespace tsdb {
namespace tombstone {

class TombstoneReaderInterface {
public:
    typedef std::function<void(tagtree::TSID, const Intervals&)> IterFunc;
    typedef std::function<error::Error(tagtree::TSID, const Intervals&)>
        ErrIterFunc;

    // NOTICE, may throw std::out_of_range.
    virtual const Intervals& get(tagtree::TSID tsid) const = 0;
    virtual void iter(const IterFunc& f) const = 0;
    virtual error::Error iter(const ErrIterFunc& f) const = 0;

    // Number of Interval
    virtual uint64_t total() const = 0;

    virtual void add_interval(tagtree::TSID, const Interval& itvl){};

    virtual ~TombstoneReaderInterface() = default;
};

} // namespace tombstone
} // namespace tsdb

#endif
