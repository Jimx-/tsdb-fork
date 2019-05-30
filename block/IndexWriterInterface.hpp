#ifndef INDEXWRITERINTERFACE
#define INDEXWRITERINTERFACE

#include <unordered_set>
#include <deque>
#include <stdint.h>

#include "index/PostingsInterface.hpp"
#include "label/Label.hpp"
#include "chunk/ChunkMeta.hpp"

namespace tsdb{

namespace block{

class IndexWriterInterface{
    public:
        // 0 succeed, -1 error
        virtual int add_symbols(const std::unordered_set<std::string> & sym)=0;
        virtual int add_series(uint64_t ref, const label::Labels & l, const std::deque<std::shared_ptr<chunk::ChunkMeta> > & chunks)=0;
        virtual int add_series(uint64_t ref, const label::Labels & l, const std::shared_ptr<chunk::ChunkMeta> & chunk){ return 0; }
        virtual int write_label_index(const std::deque<std::string> & names, const std::deque<std::string> & values)=0;
        virtual int write_label_index(const std::string & name, const std::deque<std::string> & values){ return -10; }
        virtual int write_postings(const std::string & name, const std::string & value, const index::PostingsInterface * values)=0;
        virtual int write_group_postings(const index::PostingsInterface * values){ return -1; }
        virtual ~IndexWriterInterface(){}
};

}

}

#endif