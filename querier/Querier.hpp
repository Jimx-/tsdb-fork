#ifndef QUERIER_H
#define QUERIER_H

#include <unordered_set>

#include "querier/QuerierInterface.hpp"

namespace tsdb {
namespace querier {

class Querier : public QuerierInterface {
private:
    std::vector<std::shared_ptr<QuerierInterface>> queriers;

public:
    Querier() = default;
    Querier(
        const std::initializer_list<std::shared_ptr<QuerierInterface>>& list);
    Querier(const std::vector<std::shared_ptr<QuerierInterface>>& queriers);

    std::shared_ptr<SeriesSetInterface>
    select(const std::unordered_set<common::TSID>& l) const;

    error::Error error() const;
};

} // namespace querier
} // namespace tsdb

#endif
