#ifndef SERIESSETINTERFACE_H
#define SERIESSETINTERFACE_H

#include "base/Error.hpp"
#include "querier/SeriesInterface.hpp"

namespace tsdb{
namespace querier{

// Expose series object
class SeriesSetInterface{
    public:
        virtual bool next() const=0;
        virtual std::shared_ptr<SeriesInterface> at()=0;
        virtual bool error() const=0;
        virtual error::Error error_detail() const{ return error::Error(); }
        virtual ~SeriesSetInterface()=default;
};

}
}

#endif