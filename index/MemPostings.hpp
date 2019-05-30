#ifndef MEMPOSTINGS_H
#define MEMPOSTINGS_H

#include "base/Mutex.hpp"
#include "base/ThreadPool.hpp"
#include "base/WaitGroup.hpp"
#include "label/Label.hpp"
#include "index/ListPostings.hpp"

#include <boost/function.hpp>
#include <deque>
#include <set>
#include <stdint.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace tsdb{
namespace index{

void sort_slice(std::deque<uint64_t> * d, base::WaitGroup * wg);

class MemPostings{
    private:
        base::RWMutexLock mutex_;
        std::unordered_map<std::string, std::unordered_map<std::string, std::deque<uint64_t> > > m;
        bool ordered;
        base::WaitGroup wg;

    public:
        MemPostings(bool ordered=false);

        // Will get a const reference of deque list.
        // Like slice in Go.
        std::unique_ptr<PostingsInterface> get(const std::string & label, const std::string & value);

        std::unique_ptr<PostingsInterface> all();

        label::Labels sorted_keys();

        // Used under lock.
        void add(uint64_t id, const label::Label & l);

        void add(uint64_t id, const label::Labels & ls);

        void del(const std::set<uint64_t> & deleted);

        void del(const std::unordered_set<uint64_t> & deleted);

        void iter(const boost::function<void (const label::Label &, const ListPostings &)> & f);

        // Used under lock.
        int size();

        // This ThreadPool can be shared at the same time.
        void ensure_order(base::ThreadPool * pool);

        // This ThreadPool can be shared at the same time.
        void ensure_order(const std::shared_ptr<base::ThreadPool> & pool);
};  

}}

#endif