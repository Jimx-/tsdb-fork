#ifndef LISTPOSTINGS_H
#define LISTPOSTINGS_H

#include "index/PostingsInterface.hpp"

#include <set>

namespace tsdb {
namespace index {

class PostingSet : public PostingsInterface {
private:
    std::vector<tagtree::TSID> elements;
    std::vector<tagtree::TSID>::const_iterator begin;
    size_t index;
    size_t size;

public:
    PostingSet(const std::set<tagtree::TSID>& set);

    bool next();

    bool seek(tagtree::TSID v);

    tagtree::TSID at() const;
};

} // namespace index
} // namespace tsdb

#endif
