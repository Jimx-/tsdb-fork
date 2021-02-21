#ifndef _NULL_SERIES_STORAGE_H_
#define _NULL_SERIES_STORAGE_H_

#include "tagtree/storage.h"

#include <cstdint>

class NullSeriesIterator : public tagtree::SeriesIterator {
public:
    NullSeriesIterator() : first(true) {}
    virtual bool seek(uint64_t t)
    {
        bool ret = first;
        first = false;
        return ret;
    }
    virtual std::pair<uint64_t, double> at() { return {0, 0}; }
    virtual bool next()
    {
        bool ret = first;
        first = false;
        return ret;
    }

private:
    bool first;
};

class NullSeries : public tagtree::Series {
public:
    NullSeries(tagtree::TSID tsid) : _tsid(tsid) {}
    virtual tagtree::TSID tsid() { return _tsid; }
    virtual std::unique_ptr<tagtree::SeriesIterator> iterator()
    {
        return std::make_unique<NullSeriesIterator>();
    }

private:
    tagtree::TSID _tsid;
};

class NullSeriesSet : public tagtree::SeriesSet {
public:
    NullSeriesSet(const tagtree::MemPostingList& tsids)
        : tsids(tsids), first(true), it(this->tsids.begin())
    {}

    virtual bool next()
    {
        if (first) {
            first = false;
            return it != this->tsids.end();
        }

        it++;
        return it != this->tsids.end();
    }

    virtual std::shared_ptr<tagtree::Series> at()
    {
        return std::make_unique<NullSeries>(*it);
    }

private:
    tagtree::MemPostingList tsids;
    tagtree::MemPostingList::const_iterator it;
    bool first;
};

class NullQuerier : public tagtree::Querier {
public:
    virtual std::shared_ptr<tagtree::SeriesSet>
    select(const tagtree::MemPostingList& tsids)
    {
        return std::make_shared<NullSeriesSet>(tsids);
    }
};

class NullAppender : public tagtree::Appender {
public:
    virtual void add(tagtree::TSID tsid, uint64_t t, double v) {}
};

class NullStorage : public tagtree::Storage {
public:
    virtual std::shared_ptr<tagtree::Querier> querier(uint64_t mint,
                                                      uint64_t maxt)
    {
        return std::make_shared<NullQuerier>();
    }

    virtual std::shared_ptr<tagtree::Appender> appender()
    {
        return std::make_shared<NullAppender>();
    }
    virtual void close() {}
};

#endif
