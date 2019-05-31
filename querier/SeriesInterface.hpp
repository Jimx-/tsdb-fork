#ifndef SERIESINTERFACE_H
#define SERIESINTERFACE_H

#include "label/Label.hpp"
#include "querier/SeriesIteratorInterface.hpp"

namespace tsdb {
namespace querier {

class SeriesInterface {
public:
    virtual std::unique_ptr<SeriesIteratorInterface> iterator() = 0;

    virtual ~SeriesInterface() = default;
};

} // namespace querier
} // namespace tsdb

#endif
