#ifndef INDEXREADER_H
#define INDEXREADER_H

#include "block/BlockUtils.hpp"
#include "block/IndexReaderInterface.hpp"
#include "index/IndexUtils.hpp"
#include "index/TOC.hpp"
#include "tsdbutil/ByteSlice.hpp"
#include "tsdbutil/SerializedStringTuples.hpp"

namespace tsdb {
namespace index {

class IndexReader : public block::IndexReaderInterface {
private:
    std::shared_ptr<tsdbutil::ByteSlice> b;

    bool err_;

    // offset table
    std::unordered_map<tagtree::TSID, uint64_t> offset_table;

public:
    IndexReader(std::shared_ptr<tsdbutil::ByteSlice> b);
    IndexReader(const std::string& filename);

    void init();

    std::pair<std::unique_ptr<PostingsInterface>, bool> get_all_postings();

    bool validate(const std::shared_ptr<tsdbutil::ByteSlice>& b);

    bool read_offset_table(uint64_t offset);

    // Reference is the offset of Series entry / 16
    // lset and chunks supposed to be empty
    bool series(const tagtree::TSID& tsid,
                std::vector<std::shared_ptr<chunk::ChunkMeta>>& chunks);

    bool error();
    uint64_t size();
};

} // namespace index
} // namespace tsdb

#endif
