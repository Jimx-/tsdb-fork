#include <set>
#include <vector>
#include <iostream>

#include "base/Logging.hpp"
#include "head/Head.hpp"
#include "head/HeadIndexReader.hpp"
#include "head/HeadUtils.hpp"
#include "index/VectorPostings.hpp"
#include "tsdbutil/ListStringTuples.hpp"

namespace tsdb{
namespace head{

HeadIndexReader::HeadIndexReader(Head * head, int64_t min_time, int64_t max_time): head(head), min_time(min_time), max_time(max_time){}

std::set<std::string> HeadIndexReader::symbols(){
    base::RWLockGuard lock(head->mutex_, 0);
    return std::set<std::string>(head->symbols.begin(), head->symbols.end());
}

const std::deque<std::string> & HeadIndexReader::symbols_deque()const{
    base::RWLockGuard lock(head->mutex_, 0);
    symbols_.assign(head->symbols.cbegin(), head->symbols.cend());
    return symbols_;
}

// Return empty SerializedTuples when error
std::unique_ptr<tsdbutil::StringTuplesInterface> HeadIndexReader::label_values(const std::initializer_list<std::string> & names){
    if(names.size() != 1)
        return nullptr;
    tsdbutil::ListStringTuples * l;
    {
        base::RWLockGuard lock(head->mutex_, 0);
        if(head->label_values.find(*names.begin()) == head->label_values.end())
            return nullptr;
        l = new tsdbutil::ListStringTuples(head->label_values.size());
        for(auto const& s: head->label_values[*names.begin()])
            l->push_back(s);
    }
    l->sort();

    return std::unique_ptr<tsdbutil::StringTuplesInterface>(l);
}

// postings returns the postings list iterator for the label pair.
std::pair<std::unique_ptr<index::PostingsInterface>, bool> HeadIndexReader::postings(const std::string & name, const std::string & value){
    auto p = head->posting_list->get(name, value);
    if(p) 
        return {std::move(p), true};
    else
        return {nullptr, false};
}

bool HeadIndexReader::series(uint64_t ref, label::Labels & lset, std::deque<std::shared_ptr<chunk::ChunkMeta> > & chunks){
    std::shared_ptr<MemSeries> s = head->series->get_by_id(ref);
    if(!s){
        LOG_ERROR << "not existed, series id: " << ref;
        return false;
    }
    lset.insert(lset.end(), s->labels.begin(), s->labels.end());

    base::MutexLockGuard series_lock(s->mutex_);
    int i = 0;
    for(auto const& chk: s->chunks){
        // Do not expose chunks that are outside of the specified range.
        if(!chk->overlap_closed(min_time, max_time))
            continue;
        chunks.push_back(std::shared_ptr<chunk::ChunkMeta>(new chunk::ChunkMeta(pack_chunk_id(chk->ref, s->chunk_id(i)), chk->min_time, chk->max_time)));
        ++ i;
    }

    return true;
}

std::deque<std::string> HeadIndexReader::label_names(){
    base::RWLockGuard lock(head->mutex_, 0);
    std::deque<std::string> names;
    for(auto const& p: head->label_values){
        if(p.first != label::ALL_POSTINGS_KEYS.label)
            names.push_back(p.first);
    }
    std::sort(names.begin(), names.end());

    return names;
}

std::unique_ptr<index::PostingsInterface> HeadIndexReader::sorted_postings(std::unique_ptr<index::PostingsInterface> && p){
    // TODO(Alec), choice between vector and deque depends on later benchmark.
    // Current concern relies on the size of passed in Postings.

    std::vector<std::shared_ptr<MemSeries>> series;

    while(p->next()){
        std::shared_ptr<MemSeries> s = head->series->get_by_id(p->at());
        if(!s){
            LOG_DEBUG << "msg=\"looked up series not found\"";
        }
        else{
            series.push_back(s);
        }
    }
    std::sort(series.begin(), series.end(), [](const std::shared_ptr<MemSeries> & lhs, const std::shared_ptr<MemSeries> & rhs){
        return label::lbs_compare(lhs->labels, rhs->labels) < 0;
    });
    
    // Avoid copying twice.
    index::VectorPostings * vp = new index::VectorPostings(series.size());
    for(auto const& s: series)
        vp->push_back(s->ref);
    // std::cerr << "vp size: " << vp->size() << std::endl;
    // std::unique_ptr<index::PostingsInterface> pp(vp);
    // while(vp->next())
    //     std::cerr << "vp: " << vp->at() << std::endl;
    return std::unique_ptr<index::PostingsInterface>(vp);
}

// TODO(Alec).
bool HeadIndexReader::error(){
    return false;
}

// TODO(Alec).
uint64_t HeadIndexReader::size(){
    return 0;
}

}
}