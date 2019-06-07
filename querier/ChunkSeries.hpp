#ifndef CHUNKSERIES_H
#define CHUNKSERIES_H

#include "querier/ChunkSeriesMeta.hpp"
#include "querier/SeriesInterface.hpp"

namespace tsdb {
namespace querier {

// ChunkSeries is a series that is backed by a sequence of chunks holding
// time series data.
class ChunkSeries : public SeriesInterface {
private:
    std::shared_ptr<ChunkSeriesMeta> cm;
    int64_t min_time;
    int64_t max_time;

public:
    ChunkSeries(const std::shared_ptr<ChunkSeriesMeta>& cm, int64_t min_time,
                int64_t max_time);

    common::TSID tsid();
    std::unique_ptr<SeriesIteratorInterface> iterator();
};

} // namespace querier
} // namespace tsdb

#endif
