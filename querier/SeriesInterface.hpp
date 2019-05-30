#ifndef SERIESINTERFACE_H
#define SERIESINTERFACE_H

#include "querier/SeriesIteratorInterface.hpp"
#include "label/Label.hpp"

namespace tsdb{
namespace querier{

class SeriesInterface{
    public:
        virtual const label::Labels & labels() const=0;

        virtual std::unique_ptr<SeriesIteratorInterface> iterator()=0;

        virtual ~SeriesInterface()=default;
};

}
}

#endif