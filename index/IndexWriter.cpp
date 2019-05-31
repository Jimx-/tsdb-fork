#include <boost/filesystem.hpp>
#include <stdio.h>
#include <unordered_map>
#include <vector>

#include "base/Checksum.hpp"
#include "base/Logging.hpp"
#include "index/IndexWriter.hpp"
#include "label/Label.hpp"
#include "tsdbutil/StringTuples.hpp"

namespace tsdb {
namespace index {

// All the dirs inside filename should be existed.
IndexWriter::IndexWriter(const std::string& filename)
    : pos(0), stage(IDX_STAGE_NONE), buf1(1 << 22), buf2(1 << 22),
      uint32_cache(1 << 15)
{
    boost::filesystem::path p(filename);
    if (boost::filesystem::exists(p)) boost::filesystem::remove_all(p);

    f = fopen(filename.c_str(), "wb+");

    series.clear();
    series.reserve(1 << 16);

    write_meta();
}

void IndexWriter::write_meta()
{
    buf1.reset();
    buf1.put_BE_uint32(MAGIC_INDEX);
    buf1.put_byte(INDEX_VERSION_V1);
    write({buf1.get()});
}

void IndexWriter::write(std::initializer_list<std::pair<const uint8_t*, int>> l)
{
    for (auto& p : l) {
        fwrite(p.first, 1, p.second, f);
        pos += static_cast<uint64_t>(p.second);
    }
}

void IndexWriter::add_padding(uint64_t padding)
{
    uint64_t p = pos % padding;
    if (p != 0) {
        uint8_t s[padding - p];
        memset(s, 0, padding - p);
        write({std::pair<const uint8_t*, int>(s, padding - p)});
    }
}

// 0 succeed, -1 error
int IndexWriter::ensure_stage(IndexWriterStage s)
{
    if (s == stage) {
        return 0;
    }
    if (s < stage) {
        LOG_ERROR << "Invalid stage: " << stage_to_string(s) << ", current at "
                  << stage_to_string(stage);
        return -1;
    }

    if (s == IDX_STAGE_SERIES) {
        toc.series = pos;
    } else if (s == IDX_STAGE_DONE) {
        toc.label_indices_table = pos;
        write_offset_table();

        write_TOC();
    }

    stage = s;
    return 0;
}

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
//
// Adrress series entry by 4 bytes reference
// Align to 16 bytes
// NOTICE: The ref here is just a temporary ref assigned as monotonically
// increasing id in memory.
int IndexWriter::add_series(
    const common::TSID& tsid,
    const std::vector<std::shared_ptr<chunk::ChunkMeta>>& chunks)
{
    if (ensure_stage(IDX_STAGE_SERIES) == -1) return INVALID_STAGE;

    if (series.find(tsid) != series.end()) {
        LOG_ERROR << "Labels already added";
        return EXISTED;
    }

    // Align each entry to 16 bytes
    add_padding(16);
    series[tsid] = pos / 16;
    buf2.reset();
    // LOG_INFO << "stage1";
    buf2.put_unsigned_variant(chunks.size());
    if (chunks.size() > 0) {
        int64_t last_t = chunks[0]->max_time;
        uint64_t last_ref = chunks[0]->ref;
        buf2.put_signed_variant(chunks[0]->min_time);
        buf2.put_unsigned_variant(
            static_cast<uint64_t>(chunks[0]->max_time - chunks[0]->min_time));
        buf2.put_unsigned_variant(chunks[0]->ref);

        for (int i = 1; i < chunks.size(); i++) {
            // LOG_INFO << chunks[i]->min_time - last_t;
            buf2.put_unsigned_variant(
                static_cast<uint64_t>(chunks[i]->min_time - last_t));
            // LOG_INFO << chunks[i]->max_time - chunks[i]->min_time;
            buf2.put_unsigned_variant(static_cast<uint64_t>(
                chunks[i]->max_time - chunks[i]->min_time));
            buf2.put_signed_variant(
                static_cast<int64_t>(chunks[i]->ref - last_ref));
            last_t = chunks[i]->max_time;
            last_ref = chunks[i]->ref;
        }
    }
    // LOG_INFO << "stage2";
    buf1.reset();
    buf1.put_unsigned_variant(buf2.len());          // Len in the beginning
    buf2.put_BE_uint32(base::GetCrc32(buf2.get())); // Crc32 in the end

    write({buf1.get(), buf2.get()});

    return 0;
}

// ┌─────────────────────┬────────────────────┐
// │ len <4b>            │ #entries <4b>      │
// ├─────────────────────┴────────────────────┤
// │ ┌──────────────────────────────────────┐ │
// │ │  n = #strs <uvarint>                 │ │
// │ ├──────────────────────┬───────────────┤ │
// │ │ len(str_1) <uvarint> │ str_1 <bytes> │ │
// │ ├──────────────────────┴───────────────┤ │
// │ │  ...                                 │ │
// │ ├──────────────────────┬───────────────┤ │
// │ │ len(str_n) <uvarint> │ str_n <bytes> │ │
// │ ├──────────────────────┴───────────────┤ │
// │ │  offset <uvarint>                    │ │
// │ └──────────────────────────────────────┘ │
// │                  . . .                   │
// ├──────────────────────────────────────────┤
// │  CRC32 <4b>                              │
// └──────────────────────────────────────────┘
// Need to record the starting offset in the TOC first
void IndexWriter::write_offset_table()
{
    buf2.reset();
    buf2.put_BE_uint32(series.size());

    for (auto&& s : series) {
        buf2.put_tsid(s.first);
        buf2.put_unsigned_variant(s.second);
    }

    buf1.reset();
    buf1.put_BE_uint32(buf2.len());
    buf2.put_BE_uint32(base::GetCrc32(buf2.get())); // Crc32 in the end

    write({buf1.get(), buf2.get()});
}

// ┌─────────────────────────────────────────┐
// │ ref(series) <8b>                        │
// ├─────────────────────────────────────────┤
// │ ref(offset table) <8b>                  │
// ├─────────────────────────────────────────┤
// │ CRC32 <4b>                              │
// └─────────────────────────────────────────┘
void IndexWriter::write_TOC()
{
    buf1.reset();
    buf1.put_BE_uint64(toc.series);
    buf1.put_BE_uint64(toc.label_indices_table);
    buf1.put_BE_uint32(base::GetCrc32(buf1.get())); // Crc32 in the end

    write({buf1.get()});
}

void IndexWriter::close()
{
    ensure_stage(IDX_STAGE_DONE);
    // #ifdef INDEX_DEBUG
    // LOG_DEBUG << toc_string(toc) << " end:" << pos;
    // LOG_DEBUG << toc_portion_string(toc, pos);
    // #endif
    fflush(f);
    fclose(f);
}

IndexWriter::~IndexWriter() { close(); }

} // namespace index
} // namespace tsdb
