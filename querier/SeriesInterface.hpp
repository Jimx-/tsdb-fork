#ifndef SERIESINTERFACE_H
#define SERIESINTERFACE_H

#include "tagtree/tsid.h"
#include "label/Label.hpp"
#include "querier/SeriesIteratorInterface.hpp"

namespace tsdb {
namespace querier {

class SeriesInterface {
public:
    virtual tagtree::TSID tsid() = 0;
    virtual std::unique_ptr<SeriesIteratorInterface> iterator() = 0;

    virtual ~SeriesInterface() = default;
};

} // namespace querier
} // namespace tsdb

#endif
