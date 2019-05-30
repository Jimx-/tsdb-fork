#ifndef INTERSECTPOSTINGS_H
#define INTERSECTPOSTINGS_H

#include <deque>
#include "index/PostingsInterface.hpp"

namespace tsdb{
namespace index{

class IntersectPostings: public PostingsInterface{
    private:
        std::deque<std::shared_ptr<PostingsInterface> > p_s;
        std::deque<std::unique_ptr<PostingsInterface> > p_u;
        bool mode;

    public:
        IntersectPostings(std::deque<std::shared_ptr<PostingsInterface> > & p_s);
        IntersectPostings(std::deque<std::unique_ptr<PostingsInterface> > & p_u);

        bool recursive_next_u(uint64_t max) const;

        bool recursive_next_s(uint64_t max) const;

        bool next() const;

        bool seek(uint64_t v) const;

        uint64_t at() const;
        // ~IntersectPostings(){std::cout << "intersect" << std::endl;}
};

// Pass r-value reference
std::shared_ptr<PostingsInterface> intersect_s(std::deque<std::shared_ptr<PostingsInterface> > & list);

std::shared_ptr<PostingsInterface> intersect_s(std::deque<std::unique_ptr<PostingsInterface> > & list);

// std::unique_ptr<PostingsInterface> intersect_u(std::deque<std::shared_ptr<PostingsInterface> > && list){
//     if(list.size() == 0)
//         return std::unique_ptr<PostingsInterface>(dynamic_cast<PostingsInterface*>(new EmptyPostings()));
//     else if(list.size() == 1)
//         return std::unique_ptr<PostingsInterface>(std::move(*(list.begin())));
//     return std::unique_ptr<PostingsInterface>(dynamic_cast<PostingsInterface*>(new IntersectPostings(std::move(list))));
// }

std::unique_ptr<PostingsInterface> intersect_u(std::deque<std::unique_ptr<PostingsInterface> > & list);

}}

#endif