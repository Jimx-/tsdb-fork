#include <set>

#include "base/Logging.hpp"
#include "querier/EmptySeriesSet.hpp"
#include "querier/MergedSeriesSet.hpp"
#include "querier/Querier.hpp"

namespace tsdb{
namespace querier{

Querier::Querier(const std::initializer_list<std::shared_ptr<QuerierInterface> > & list): queriers(list.begin(), list.end()){}
Querier::Querier(const std::deque<std::shared_ptr<QuerierInterface> > & queriers): queriers(queriers){}

std::shared_ptr<SeriesSetInterface> Querier::select(const std::initializer_list<std::shared_ptr<label::MatcherInterface> > & l) const{
    std::shared_ptr<SeriesSets> ss(new SeriesSets());
    for(auto const& querier: queriers){
        auto i = querier->select(l);
        if(i)
            ss->push_back(i);
    }
    if(!ss->empty()){
        // LOG_INFO << "Create MergedSeriesSet, num of SeriesSetInterface: " << ss->size();
        return std::shared_ptr<SeriesSetInterface>(new MergedSeriesSet(ss));
    }
    else
        return nullptr;
}

// LabelValues returns all potential values for a label name.
std::deque<std::string> Querier::label_values(const std::string & label) const{
    std::set<std::string> s;
    for(auto const& querier: queriers){
        std::deque<std::string> temp = querier->label_values(label);
        s.insert(temp.begin(), temp.end());
    }
    return std::deque<std::string>(s.begin(), s.end());
}
        
// label_names returns all the unique label names present in the block in sorted order.
std::deque<std::string> Querier::label_names() const{
    std::set<std::string> s;
    for(auto const& querier: queriers){
        std::deque<std::string> temp = querier->label_names();
        s.insert(temp.begin(), temp.end());
    }
    return std::deque<std::string>(s.begin(), s.end());
}

error::Error Querier::error() const{
    std::string err;
    for(auto const& q: queriers)
        err += q->error().error();
    return error::Error(err); 
}

}}