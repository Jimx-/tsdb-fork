#include "head/StripeSeries.hpp"
#include "base/Logging.hpp"
#include "head/HeadUtils.hpp"

#include <iostream>

namespace tsdb {
namespace head {

StripeSeries::StripeSeries() : series(STRIPE_SIZE), locks(STRIPE_SIZE) {}

// gc garbage collects old chunks that are strictly before mint and removes
// series entirely that have no chunks left. return <set of removed series,
// number of removed chunks>
std::pair<std::unordered_set<common::TSID>, int>
StripeSeries::gc(int64_t min_time)
{
    // Run through all series and truncate old chunks. Mark those with no
    // chunks left as deleted and store their ID.
    std::unordered_set<common::TSID> rm_series;
    int rm_chunks = 0;
    for (int i = 0; i < STRIPE_SIZE; ++i) {
        base::PadRWLockGuard lock_i(locks[i], 1);

        // Iterate over all series sharing the same hash mod.
        std::shared_ptr<MemSeries> temp;
        std::unordered_map<common::TSID, std::shared_ptr<MemSeries>>::iterator
            series_it = series[i].begin();
        while (series_it != series[i].end()) {
            auto sp = series_it->second;
            base::MutexLockGuard series_lock(sp->mutex_);
            rm_chunks += sp->truncate_chunk_before(min_time);

            if (!sp->chunks.empty() || sp->pending_commit) {
                ++series_it;
                continue;
            }
            // Since serieslist_it will be removed, we increment it here.
            auto temp_series = series_it;
            ++series_it;

            // This makes sure the series_ will not be deconstructed when
            // its lock being held. And provide ref for deleting series.
            temp = temp_series->second;

            // The series is gone entirely. We need to keep the series lock
            // and make sure we have acquired the stripe locks for hash and
            // ID of the series alike. If we don't hold them all, there's a
            // very small chance that a series receives samples again while
            // we are half-way into deleting it.
            int j = static_cast<int>(
                std::hash<common::TSID>()(temp_series->second->tsid) &
                STRIPE_MASK);
            if (i != j) {
                base::PadRWLockGuard lock_j(locks[j], 1);
                rm_series.insert(temp_series->second->tsid);
                series[j].erase(series[j].find(temp->tsid));
            } else {
                rm_series.insert(temp_series->second->tsid);
                series[j].erase(series[j].find(temp->tsid));
            }
        }
    }

    return {rm_series, rm_chunks};
}

std::shared_ptr<MemSeries> StripeSeries::get_by_id(const common::TSID& tsid)
{
    uint64_t i = std::hash<common::TSID>()(tsid) & STRIPE_MASK;
    std::unordered_map<common::TSID, std::shared_ptr<MemSeries>>::iterator r;
    base::PadRWLockGuard lock_i(locks[i], 0);
    if ((r = series[i].find(tsid)) == series[i].end()) {
        return nullptr;
    }
    return r->second;
}

// Return <MemSeries, if the series being set>.
std::pair<std::shared_ptr<MemSeries>, bool>
StripeSeries::get_or_set(const common::TSID& tsid,
                         const std::shared_ptr<MemSeries>& s)
{
    uint64_t i = std::hash<common::TSID>()(tsid) & STRIPE_MASK;
    {
        base::PadRWLockGuard lock_i(locks[i], 1);
        series[i][tsid] = s;
    }
    return {s, true};
}

} // namespace head
} // namespace tsdb
