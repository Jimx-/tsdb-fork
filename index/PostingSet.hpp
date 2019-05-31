#ifndef LISTPOSTINGS_H
#define LISTPOSTINGS_H

#include "index/PostingsInterface.hpp"

#include <unordered_set>

namespace tsdb {
namespace index {

class PostingSet : public PostingsInterface {
private:
    std::vector<common::TSID> elements;
    std::vector
    <common::TSID>::const_iterator begin;
    size_t index;
    size_t size;

public:
    PostingSet(const std::unordered_set<common::TSID>& set);

    bool next();

    bool seek(const common::TSID& v);

    common::TSID at() const;
};

} // namespace index
} // namespace tsdb

#endif
