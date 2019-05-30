#ifndef SERIESITERATORINTERFACE_H
#define SERIESITERATORINTERFACE_H

#include <stdint.h>
#include <utility>

namespace tsdb{
namespace querier{

class SeriesIteratorInterface{
    public:
        virtual bool seek(int64_t t) const=0;
        virtual std::pair<int64_t, double> at() const=0;
        virtual bool next() const=0;
        virtual bool error() const=0;
        virtual ~SeriesIteratorInterface()=default;
};

}
}

#endif