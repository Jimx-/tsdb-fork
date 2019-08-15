#ifndef STRIPESERIES_H
#define STRIPESERIES_H

#include <unordered_set>
#include <vector>

#include "base/Mutex.hpp"
#include "head/MemSeries.hpp"

namespace tsdb {
namespace head {

// stripeSeries locks modulo ranges of IDs and hashes to reduce lock contention.
// The locks are padded to not be on the same cache line. Filling the padded
// space with the maps was profiled to be slower – likely due to the additional
// pointer dereferences.
class StripeSeries {
public:
    std::vector<std::unordered_map<tagtree::TSID, std::shared_ptr<MemSeries>>>
        series; // Index by TSID.
    std::vector<base::PadRWMutexLock>
        locks; // To align cache line (multiples of 64 bytes)

    StripeSeries();

    // gc garbage collects old chunks that are strictly before mint and removes
    // series entirely that have no chunks left. return <set of removed series,
    // number of removed chunks>
    std::pair<std::unordered_set<tagtree::TSID>, int> gc(int64_t min_time);

    std::shared_ptr<MemSeries> get_by_id(tagtree::TSID tsid);

    // Return <MemSeries, if the series being set>.
    std::pair<std::shared_ptr<MemSeries>, bool>
    get_or_set(tagtree::TSID tsid, const std::shared_ptr<MemSeries>& s);
};

} // namespace head
} // namespace tsdb

#endif
