#ifndef LISTPOSTINGS_H
#define LISTPOSTINGS_H

#include "index/PostingsInterface.hpp"

#include <unordered_set>

namespace tsdb {
namespace index {

class PostingSet : public PostingsInterface {
private:
    std::vector<tagtree::TSID> elements;
    std::vector
    <tagtree::TSID>::const_iterator begin;
    size_t index;
    size_t size;

public:
    PostingSet(const std::unordered_set<tagtree::TSID>& set);

    bool next();

    bool seek(const tagtree::TSID& v);

    tagtree::TSID at() const;
};

} // namespace index
} // namespace tsdb

#endif
