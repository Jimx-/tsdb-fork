#include "index/PostingSet.hpp"

namespace tsdb {
namespace index {

// NOTICE!!!
// Must be called based on existed deque/vector, cannot pass temporary
// deque/vector into it
PostingSet::PostingSet(const std::unordered_set<tagtree::TSID>& set)
    : elements(set.begin(), set.end()), index(-1)
{
    begin = elements.cbegin();
    size = elements.size();
}

bool PostingSet::next()
{
    ++index;
    return index < size;
}

bool PostingSet::seek(const tagtree::TSID& v)
{
    auto it = std::find(begin, begin + size, v);
    if (it == elements.end()) {
        return false;
    }
    index = it - begin;
    return true;
}

tagtree::TSID PostingSet::at() const { return *(begin + index); }

} // namespace index
} // namespace tsdb
