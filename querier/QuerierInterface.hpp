#ifndef QUERIERINTERFACE_H
#define QUERIERINTERFACE_H

#include <deque>
#include <unordered_set>

#include "base/Error.hpp"
#include "label/Label.hpp"
#include "label/MatcherInterface.hpp"
#include "querier/SeriesSetInterface.hpp"
#include "tagtree/tsid.h"

namespace tsdb {
namespace querier {

class QuerierInterface {
public:
    // Return nullptr when no series match.
    virtual std::shared_ptr<SeriesSetInterface>
    select(const std::set<tagtree::TSID>& l) const = 0;

    virtual error::Error error() const = 0;
    virtual ~QuerierInterface() = default;
};

} // namespace querier
} // namespace tsdb

#endif
