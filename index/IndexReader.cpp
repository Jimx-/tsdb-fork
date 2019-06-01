#include <cstring>
// #include <iostream>

#include "base/Endian.hpp"
#include "base/Logging.hpp"
#include "index/IndexReader.hpp"
#include "index/PostingSet.hpp"
#include "label/Label.hpp"
#include "tsdbutil/DecBuf.hpp"
#include "tsdbutil/MMapSlice.hpp"

namespace tsdb {
namespace index {

IndexReader::IndexReader(std::shared_ptr<tsdbutil::ByteSlice> b) : err_(false)
{
    if (!validate(b)) {
        LOG_ERROR << "Fail to create IndexReader, invalid ByteSlice";
        err_ = true;
        return;
    }
    this->b = b;
    init();
}

IndexReader::IndexReader(const std::string& filename) : err_(false)
{
    std::shared_ptr<tsdbutil::ByteSlice> temp =
        std::shared_ptr<tsdbutil::ByteSlice>(new tsdbutil::MMapSlice(filename));
    if (!validate(temp)) {
        LOG_ERROR << "Fail to create IndexReader, invalid ByteSlice";
        err_ = true;
        return;
    }
    this->b = temp;
    init();
    get_all_postings();
}

void IndexReader::init()
{
    std::pair<TOC, bool> toc_pair = toc_from_ByteSlice(b.get());
    if (!toc_pair.second) {
        LOG_ERROR << "Fail to create IndexReader, error reading TOC";
        b.reset();
        err_ = true;
        return;
    }

    // Read label indices table
    if (!read_offset_table(toc_pair.first.label_indices_table)) {
        LOG_ERROR
            << "Fail to create IndexReader, error reading label indices table";
        b.reset();
        err_ = true;
        return;
    }
}

std::pair<std::unique_ptr<PostingsInterface>, bool>
IndexReader::get_all_postings()
{
    std::unordered_set<common::TSID> all;
    for (auto&& p : offset_table) {
        all.insert(p.first);
    }

    return {std::make_unique<PostingSet>(all), true};
}

bool IndexReader::validate(const std::shared_ptr<tsdbutil::ByteSlice>& b)
{
    if (b->len() < 4) {
        LOG_ERROR << "Length of ByteSlice < 4";
        return false;
    }
    if (base::get_uint32_big_endian((b->range(0, 4)).first) != MAGIC_INDEX) {
        LOG_ERROR << "Not beginning with MAGIC_INDEX";
        return false;
    }
    if (*((b->range(4, 5)).first) != INDEX_VERSION_V1) {
        LOG_ERROR << "Invalid Index Version";
        return false;
    }
    return true;
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
bool IndexReader::read_offset_table(uint64_t offset)
{
    std::pair<const uint8_t*, int> table_begin = b->range(offset, offset + 4);
    if (table_begin.second != 4) return false;
    uint32_t len = base::get_uint32_big_endian(table_begin.first);
    uint32_t num_entries = base::get_uint32_big_endian(table_begin.first + 4);
    tsdbutil::DecBuf dec_buf(table_begin.first + 8, len - 4);

    // Read label name to offset of label value
    for (int i = 0; i < num_entries; i++) {
        common::TSID tsid = dec_buf.get_tsid();
        offset_table[tsid] = dec_buf.get_unsigned_variant();
    }
    if (dec_buf.err != tsdbutil::NO_ERR)
        return false;
    else
        return true;
}

// ┌─────────────────────────────────────────────────────────────────────────┐
// │ len <uvarint>                                                           │
// ├─────────────────────────────────────────────────────────────────────────┤
// │ ┌──────────────────┬──────────────────────────────────────────────────┐ │
// │ │                  │ ┌──────────────────────────────────────────┐     │ │
// │ │                  │ │ c_0.mint <varint>                        │     │ │
// │ │                  │ ├──────────────────────────────────────────┤     │ │
// │ │                  │ │ c_0.maxt - c_0.mint <uvarint>            │     │ │
// │ │                  │ ├──────────────────────────────────────────┤     │ │
// │ │                  │ │ ref(c_0.data) <uvarint>                  │     │ │
// │ │      #chunks     │ └──────────────────────────────────────────┘     │ │
// │ │     <uvarint>    │ ┌──────────────────────────────────────────┐     │ │
// │ │                  │ │ c_i.mint - c_i-1.maxt <uvarint>          │     │ │
// │ │                  │ ├──────────────────────────────────────────┤     │ │
// │ │                  │ │ c_i.maxt - c_i.mint <uvarint>            │     │ │
// │ │                  │ ├──────────────────────────────────────────┤ ... │ │
// │ │                  │ │ ref(c_i.data) - ref(c_i-1.data) <varint> │     │ │
// │ │                  │ └──────────────────────────────────────────┘     │ │
// │ └──────────────────┴──────────────────────────────────────────────────┘ │
// ├─────────────────────────────────────────────────────────────────────────┤
// │ CRC32 <4b>                                                              │
// └─────────────────────────────────────────────────────────────────────────┘
//
// Reference is the offset of Series entry / 16
// lset and chunks supposed to be empty
bool IndexReader::series(
    const common::TSID& tsid,
    std ::vector<std::shared_ptr<chunk::ChunkMeta>>& chunks)
{
    if (!b) return false;

    auto it = offset_table.find(tsid);
    if (it == offset_table.end()) return false;
    auto ref = it->second;
    ref *= 16;

    const uint8_t* start = (b->range(ref, ref + base::MAX_VARINT_LEN_64)).first;
    int decoded;
    uint64_t len =
        base::decode_unsigned_varint(start, decoded, base::MAX_VARINT_LEN_64);
    tsdbutil::DecBuf dec_buf(start + decoded, len);
    // Decode the Chunks
    uint64_t num_chunks = dec_buf.get_unsigned_variant();
    if (num_chunks == 0) return true;

    // First chunk meta
    int64_t last_t = dec_buf.get_signed_variant();
    uint64_t delta_t = dec_buf.get_unsigned_variant();
    int64_t last_ref = static_cast<int64_t>(dec_buf.get_unsigned_variant());
    if (dec_buf.err != tsdbutil::NO_ERR) {
        LOG_ERROR << "Fail to read series, fail to read chunk meta 0";
        return false;
    }
    // LOG_INFO << last_t << " " << delta_t;
    chunks.push_back(std::shared_ptr<chunk::ChunkMeta>(
        new chunk::ChunkMeta(static_cast<uint64_t>(last_ref), last_t,
                             static_cast<int64_t>(delta_t) + last_t)));

    for (int i = 1; i < num_chunks; i++) {
        last_t +=
            static_cast<int64_t>(dec_buf.get_unsigned_variant() + delta_t);
        delta_t = dec_buf.get_unsigned_variant();
        last_ref += dec_buf.get_signed_variant();
        if (dec_buf.err != tsdbutil::NO_ERR) {
            LOG_ERROR << "Fail to read series, fail to read chunk meta " << i;
            return false;
        }
        // LOG_INFO << last_t << " " << delta_t;
        chunks.push_back(std::shared_ptr<chunk::ChunkMeta>(
            new chunk::ChunkMeta(static_cast<uint64_t>(last_ref), last_t,
                                 static_cast<int64_t>(delta_t) + last_t)));
    }
    return true;
}

bool IndexReader::error() { return err_; }

uint64_t IndexReader::size()
{
    if (!b) return 0;
    return b->len();
}

} // namespace index
} // namespace tsdb
