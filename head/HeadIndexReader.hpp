#ifndef HEADINDEXREADER_H
#define HEADINDEXREADER_H

#include "block/IndexReaderInterface.hpp"
#include "tsdbutil/StringTuplesInterface.hpp"

namespace tsdb{
namespace head{

class Head;

class HeadIndexReader: public block::IndexReaderInterface{
    private:
        Head * head;
        int64_t min_time;
        int64_t max_time;
        mutable std::deque<std::string> symbols_;

    public:
        HeadIndexReader(Head * head, int64_t min_time, int64_t max_time);

        std::set<std::string> symbols();
        const std::deque<std::string> & symbols_deque()const;

        std::unique_ptr<tsdbutil::StringTuplesInterface> label_values(const std::initializer_list<std::string> & names);
        std::pair<std::unique_ptr<index::PostingsInterface>, bool> postings(const std::string & name, const std::string & value);

        bool series(uint64_t ref, label::Labels & lset, std::deque<std::shared_ptr<chunk::ChunkMeta> > & chunks);
        std::deque<std::string> label_names();

        std::unique_ptr<index::PostingsInterface> sorted_postings(std::unique_ptr<index::PostingsInterface> && p);

        bool error();
        uint64_t size();
};

}
}

#endif