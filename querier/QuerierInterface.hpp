#ifndef QUERIERINTERFACE_H
#define QUERIERINTERFACE_H

#include <deque>
#include <initializer_list>

#include "base/Error.hpp"
#include "label/MatcherInterface.hpp"
#include "label/Label.hpp"
#include "querier/SeriesSetInterface.hpp"

namespace tsdb{
namespace querier{

class QuerierInterface{
    public:
        // Return nullptr when no series match.
        virtual std::shared_ptr<SeriesSetInterface> select(const std::initializer_list<std::shared_ptr<label::MatcherInterface> > & l) const=0;

        // LabelValues returns all SORTED values for a label name.
        virtual std::deque<std::string> label_values(const std::string & s) const=0;
        
        // label_values_for returns all potential values for a label name.
        // under the constraint of another label.
        // virtual std::deque<boost::string_ref> label_values_for(const std::string & s, const label::Label & label) const=0;
        
        // label_names returns all the unique label names present in the block in sorted order.
        virtual std::deque<std::string> label_names() const=0;

        virtual error::Error error() const=0;
        virtual ~QuerierInterface()=default;
};

}
}

#endif