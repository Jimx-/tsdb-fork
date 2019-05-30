#include "base/Endian.hpp"
#include "index/Uint32BEPostings.hpp"

namespace tsdb{
namespace index{

// Need to check if size = 4x before using
Uint32BEPostings::Uint32BEPostings(const uint8_t * p, uint32_t size): p(p), size(size), index(-4){}

bool Uint32BEPostings::next() const{
    if(index + 8 > size || size == 0)
        return false;
    
    index += 4;
    cur = static_cast<uint64_t>(base::get_uint32_big_endian(p + index));
    return true;
}

bool Uint32BEPostings::seek(uint64_t v) const{
    if(size == 0)
        return false;
    if(index < 0){
        if(!next()){
            return false;
        }
    }
    if(cur >= v)
        return true;

    uint32_t i = index + 4;
    for(; i < size; i += 4){
        cur = static_cast<uint64_t>(base::get_uint32_big_endian(p + i));
        if(cur >= v){
            break;
        }
    }
    index = i;
    if(i >= size)
        return false;
    return true;
}

uint64_t Uint32BEPostings::at() const{
    return cur;
}

}}