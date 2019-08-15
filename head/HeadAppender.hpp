#ifndef HEADAPPENDER_H
#define HEADAPPENDER_H

#include <deque>
#include <iostream>
#include <vector>

#include "base/TimeStamp.hpp"
#include "db/AppenderInterface.hpp"
#include "head/Head.hpp"
#include "head/HeadUtils.hpp"
#include "tsdbutil/RecordEncoder.hpp"
#include "tsdbutil/tsdbutils.hpp"

namespace tsdb {
namespace head {

// TODO(Alec), determine if lock is needed.
class HeadAppender : public db::AppenderInterface {
private:
    Head* head;
    int64_t min_valid_time; // No samples below this timestamp are allowed.
    int64_t min_time;
    int64_t max_time;

    std::vector<tsdbutil::RefSeries> series;
    std::vector<tsdbutil::RefSample> samples;

public:
    HeadAppender(Head* head, int64_t min_valid_time, int64_t min_time,
                 int64_t max_time)
        : head(head), min_valid_time(min_valid_time), min_time(min_time),
          max_time(max_time)
    {}

    error::Error add(tagtree::TSID tsid, int64_t t, double v)
    {
        if (t < min_valid_time) return ErrOutOfBounds;

        std::pair<std::shared_ptr<MemSeries>, bool> s =
            head->get_or_create(tsid);
        if (s.second) series.emplace_back(tsid);

        if (t < min_valid_time) return ErrOutOfBounds;

        {
            // TODO(Alec), figure out if lock here.
            // It's meaningless to have multiple appenders on the same Memseries
            // in current use case because it can trigger unneccessary
            // ErrOutOfOrderSample easily.
            //
            // base::MutexLockGuard lock(s->mutex_);
            // error::Error err = s->appendable(t, v);
            // if(err)
            //     return err;
            s.first->pending_commit = true;
        }

        // if(t < min_time)
        //     min_time = t;
        // if(t > max_time)
        //     max_time = t;

        samples.emplace_back(tsid, t, v, s.first);
        return error::Error();
    }

    error::Error commit()
    {
        // auto start = base::TimeStamp::now();
        error::Error err = log();
        if (err) return error::wrap(err, "write to WAL");
        // LOG_DEBUG << "log duration=" <<
        // base::timeDifference(base::TimeStamp::now(), start); LOG_DEBUG <<
        // "before append"; auto start = base::TimeStamp::now();
        for (tsdbutil::RefSample& s : samples) {
            base::MutexLockGuard lock(s.series->mutex_);
            if (s.series->append(s.t, s.v).first) {
                if (s.t < min_time) min_time = s.t;
                if (s.t > max_time) max_time = s.t;
            }
            s.series->pending_commit = false;
        }
        // LOG_DEBUG << "append duration=" <<
        // base::timeDifference(base::TimeStamp::now(), start); LOG_DEBUG <<
        // "before clean";
        series.clear();
        samples.clear();
        head->update_min_max_time(min_time, max_time);
        return error::Error();
    }

    error::Error rollback()
    {
        for (tsdbutil::RefSample& s : samples) {
            base::MutexLockGuard lock(s.series->mutex_);
            s.series->pending_commit = false;
        }

        // Series are created in the head memory regardless of rollback. Thus we
        // have to log them to the WAL in any case.
        samples.clear();
        return log();
    }

    error::Error log()
    {
        if (!head->wal) return error::Error();

        std::vector<uint8_t> rec;
        if (!series.empty()) {
            tsdbutil::RecordEncoder::series(series, rec);
            error::Error err = head->wal->log(rec);
            if (err) return error::wrap(err, "log series");
        }
        if (!samples.empty()) {
            rec.clear();
            tsdbutil::RecordEncoder::samples(samples, rec);
            // LOG_DEBUG << "after RecordEncoder::samples()";
            error::Error err = head->wal->log(rec);
            // LOG_DEBUG << "after wal->log()";
            if (err) return error::wrap(err, "log series");
        }
        return error::Error();
    }
};

} // namespace head
} // namespace tsdb

#endif
