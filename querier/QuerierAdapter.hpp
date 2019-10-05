#ifndef QUERIERADAPTER_H
#define QUERIERADAPTER_H

#include "querier/QuerierInterface.hpp"
#include "querier/SeriesInterface.hpp"
#include "querier/SeriesIteratorInterface.hpp"
#include "querier/SeriesSetInterface.hpp"
#include "tagtree/storage.h"

namespace tsdb {
namespace querier {

class SeriesIteratorAdapter : public tagtree::SeriesIterator {
public:
    SeriesIteratorAdapter(std::unique_ptr<SeriesIteratorInterface>&& si)
        : si(std::move(si))
    {}
    virtual bool seek(uint64_t t) { return si->seek(t); }
    virtual std::pair<uint64_t, double> at() { return si->at(); }
    virtual bool next() { return si->next(); }

private:
    std::unique_ptr<SeriesIteratorInterface> si;
};

class SeriesAdapter : public tagtree::Series {
public:
    SeriesAdapter(std::shared_ptr<SeriesInterface> series) : series(series) {}
    virtual tagtree::TSID tsid() { return series->tsid(); }
    virtual std::unique_ptr<tagtree::SeriesIterator> iterator()
    {
        return std::make_unique<SeriesIteratorAdapter>(series->iterator());
    }

private:
    std::shared_ptr<SeriesInterface> series;
};

class SeriesSetAdapter : public tagtree::SeriesSet {
public:
    SeriesSetAdapter(std::shared_ptr<SeriesSetInterface> ss) : ss(ss) {}
    virtual bool next() { return ss->next(); }
    virtual std::shared_ptr<tagtree::Series> at()
    {
        return std::make_shared<SeriesAdapter>(ss->at());
    }

private:
    std::shared_ptr<SeriesSetInterface> ss;
};

class QuerierAdapter : public tagtree::Querier {
public:
    QuerierAdapter(std::unique_ptr<QuerierInterface>&& q) : q(std::move(q)) {}

    virtual std::shared_ptr<tagtree::SeriesSet>
    select(const tagtree::MemPostingList& tsids)
    {
        std::unordered_set<tagtree::TSID> tsid_set;
        for (auto it = tsids.begin(); it != tsids.end(); it++) {
            tsid_set.insert(*it);
        }

        return std::make_shared<SeriesSetAdapter>(q->select(tsid_set));
    }

private:
    std::unique_ptr<QuerierInterface> q;
};

} // namespace querier
} // namespace tsdb

#endif
