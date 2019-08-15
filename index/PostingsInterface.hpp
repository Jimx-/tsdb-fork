#ifndef POSTINGSINTERFACE_H
#define POSTINGSINTERFACE_H

#include "tagtree/tsid.h"

#include <algorithm>
#include <deque>
#include <memory>
#include <stdint.h>
#include <vector>
// #include <iostream>

namespace tsdb {
namespace index {

template <typename T, typename V> int binary_search(const T& d, V v)
{
    int left = 0;
    int right = d.size();
    int middle = (left + right) / 2;
    while (left < right) {
        if (d[middle] == v)
            return middle;
        else if (d[middle] < v)
            left = middle;
        else
            right = middle;
        middle = (left + right) / 2;
    }
    if (d[middle] == v)
        return middle;
    else
        return d.size();
}

template <typename T> bool binary_cut(std::deque<T>* d, T v)
{
    auto it = std::lower_bound(d->begin(), d->end(), v);
    if (it == d->end()) {
        d->clear();
        return false;
    }
    d->erase(d->begin(), it);
    return true;
}

template <typename T> void binary_insert(std::deque<T>* d, T v)
{
    auto it = std::lower_bound(d->begin(), d->end(), v);
    if (it == d->end())
        d->emplace_back(v);
    else if (v < *it)
        d->emplace_front(v);
    else if (v != *it)
        d->insert(it + 1, v);
}

// Sorted postings
class PostingsInterface {
public:
    virtual bool next() = 0;
    virtual bool seek(tagtree::TSID v) = 0;
    virtual tagtree::TSID at() const = 0;
    virtual ~PostingsInterface() {}
};

inline std::vector<tagtree::TSID>
expand_postings(const std::shared_ptr<PostingsInterface>& p)
{
    std::vector<tagtree::TSID> d;
    while (p->next())
        d.push_back(p->at());
    return d;
}

} // namespace index
} // namespace tsdb

#endif
