#ifndef INDEXWRITER_H
#define INDEXWRITER_H

#include <deque>
#include <initializer_list>
#include <unordered_map>

#include "block/IndexWriterInterface.hpp"
#include "index/IndexUtils.hpp"
#include "index/TOC.hpp"
#include "tsdbutil/CacheVector.hpp"
#include "tsdbutil/EncBuf.hpp"

namespace tsdb {
namespace index {

class IndexWriter : public block::IndexWriterInterface {
private:
    FILE* f;
    uint64_t pos;

    IndexWriterStage stage;
    TOC toc;

    tsdbutil::EncBuf buf1;
    tsdbutil::EncBuf buf2;

    tsdbutil::CacheVector<uint32_t>
        uint32_cache; // For sorting when writing postings list

    std::unordered_map<tagtree::TSID, uint64_t> series; // Series offsets

    int version;

public:
    // All the dirs inside filename should be existed.
    IndexWriter(const std::string& filename);

    void write_meta();

    void write(std::initializer_list<std::pair<const uint8_t*, int>> l);

    void add_padding(uint64_t padding);

    // 0 succeed, -1 error
    int ensure_stage(IndexWriterStage s);

    // clang-format off
    // ┌──────────────────────────────────────────────────────────────────────────┐
    // │ len <uvarint>                                                            │
    // ├──────────────────────────────────────────────────────────────────────────┤
    // │ ┌──────────────────────────────────────────────────────────────────────┐ │
    // │ │                     labels count <uvarint64>                         │ │
    // │ ├──────────────────────────────────────────────────────────────────────┤ │
    // │ │              ┌────────────────────────────────────────────┐          │ │
    // │ │              │ ref(l_i.name) <uvarint32>                  │          │ │
    // │ │              ├────────────────────────────────────────────┤          │ │
    // │ │              │ ref(l_i.value) <uvarint32>                 │          │ │
    // │ │              └────────────────────────────────────────────┘          │ │
    // │ │                             ...                                      │ │
    // │ ├──────────────────────────────────────────────────────────────────────┤ │
    // │ │                     chunks count <uvarint64>                         │ │
    // │ ├──────────────────────────────────────────────────────────────────────┤ │
    // │ │              ┌────────────────────────────────────────────┐          │ │
    // │ │              │ c_0.mint <varint64>                        │          │ │
    // │ │              ├────────────────────────────────────────────┤          │ │
    // │ │              │ c_0.maxt - c_0.mint <uvarint64>            │          │ │
    // │ │              ├────────────────────────────────────────────┤          │ │
    // │ │              │ ref(c_0.data) <uvarint64>                  │          │ │
    // │ │              └────────────────────────────────────────────┘          │ │
    // │ │              ┌────────────────────────────────────────────┐          │ │
    // │ │              │ c_i.mint - c_i-1.maxt <uvarint64>          │          │ │
    // │ │              ├────────────────────────────────────────────┤          │ │
    // │ │              │ c_i.maxt - c_i.mint <uvarint64>            │          │ │
    // │ │              ├────────────────────────────────────────────┤          │ │
    // │ │              │ ref(c_i.data) - ref(c_i-1.data) <varint64> │          │ │
    // │ │              └────────────────────────────────────────────┘          │ │
    // │ │                             ...                                      │ │
    // │ └──────────────────────────────────────────────────────────────────────┘ │
    // ├──────────────────────────────────────────────────────────────────────────┤
    // │ CRC32 <4b>                                                               │
    // └──────────────────────────────────────────────────────────────────────────┘
    // clang-format on
    //
    // Adrress series entry by 4 bytes reference
    // Align to 16 bytes
    // NOTICE: The ref here is just a temporary ref assigned as monotonically
    // increasing id in memory.
    //
    // chunks here better to be sorted by time.
    int
    add_series(const tagtree::TSID& tsid,
               const std::vector<std::shared_ptr<chunk::ChunkMeta>>& chunks);

    void write_offset_table();
    void write_TOC();

    void close();
    ~IndexWriter();
};

} // namespace index
} // namespace tsdb

#endif
