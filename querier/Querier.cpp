#include <set>

#include "base/Logging.hpp"
#include "querier/EmptySeriesSet.hpp"
#include "querier/MergedSeriesSet.hpp"
#include "querier/Querier.hpp"

namespace tsdb {
namespace querier {

Querier::Querier(
    const std::initializer_list<std::shared_ptr<QuerierInterface>>& list)
    : queriers(list.begin(), list.end())
{}
Querier::Querier(const std::vector<std::shared_ptr<QuerierInterface>>& queriers)
    : queriers(queriers)
{}

std::shared_ptr<SeriesSetInterface>
Querier::select(const std::initializer_list<common::TSID>& l) const
{
    std::shared_ptr<SeriesSets> ss(new SeriesSets());
    for (auto const& querier : queriers) {
        auto i = querier->select(l);
        if (i) ss->push_back(i);
    }
    if (!ss->empty()) {
        // LOG_INFO << "Create MergedSeriesSet, num of SeriesSetInterface: " <<
        // ss->size();
        return std::shared_ptr<SeriesSetInterface>(new MergedSeriesSet(ss));
    } else
        return nullptr;
}

error::Error Querier::error() const
{
    std::string err;
    for (auto const& q : queriers)
        err += q->error().error();
    return error::Error(err);
}

} // namespace querier
} // namespace tsdb
