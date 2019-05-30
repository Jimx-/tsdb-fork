#ifndef UINT32BEPOSTINGS_H
#define UINT32BEPOSTINGS_H

#include "index/PostingsInterface.hpp"

namespace tsdb{
namespace index{

class Uint32BEPostings: public PostingsInterface{
    private:
        const uint8_t * p;
        uint32_t size;
        mutable uint32_t index;
        mutable uint64_t cur;

    public:
        // Need to check if size = 4x before using
        Uint32BEPostings(const uint8_t * p, uint32_t size);

        bool next() const;

        bool seek(uint64_t v) const;

        uint64_t at() const;
};

}}

#endif