#include "chunk/ChunkReader.hpp"
#include "base/Endian.hpp"
#include "base/Logging.hpp"
#include "chunk/ChunkUtils.hpp"
#include "chunk/EmptyChunk.hpp"
#include "chunk/XORChunk.hpp"
#include "tsdbutil/MMapSlice.hpp"

namespace tsdb {
namespace chunk {

// Implicit construct from const char *
ChunkReader::ChunkReader(const std::string& dir) : err_(false), size_(0)
{
    std::deque<std::string> files = sequence_files(dir);
    for (std::string& s : files) {
        bs.push_back(
            std::shared_ptr<tsdbutil::ByteSlice>(new tsdbutil::MMapSlice(s)));
        if (!validate()) {
            LOG_ERROR << "Invalid chunk: " << s;
            bs.clear();
            err_ = true;
            return;
        }
        size_ += bs.back()->len();
    }
}

// Validate the back of bs after each push_back
bool ChunkReader::validate()
{
    bool valid = true;
    if (bs.back()->len() < 4)
        valid = false;
    else if (static_cast<uint32_t>(base::get_uint32_big_endian(
                 bs.back()->range(0, 4).first)) != MAGIC_CHUNK)
        valid = false;
    return valid;
}

// Will return EmptyChunk when error
std::pair<std::shared_ptr<ChunkInterface>, bool>
ChunkReader::chunk(tagtree::TSID, uint64_t ref)
{
    int seq = static_cast<int>(ref >> 32);
    int offset = static_cast<int>((ref << 32) >> 32);
    if (seq >= bs.size() || offset >= bs[seq]->len()) {
        LOG_ERROR << "Ref: " << ref
                  << " chunk is invalid ---- bs.size(): " << bs.size();
        return {std::shared_ptr<ChunkInterface>(new EmptyChunk()), false};
    }

    // Get the length of the chunk
    std::pair<const uint8_t*, int> stream =
        bs[seq]->range(offset, offset + base::MAX_VARINT_LEN_32);
    int decoded = 0;
    uint64_t l = base::decode_unsigned_varint(stream.first, decoded,
                                              base::MAX_VARINT_LEN_32);
    // LOG_INFO << l << " " << decoded;
    if (l > std::numeric_limits<uint32_t>::max() ||
        offset + l + decoded + 1 > bs[seq]->len()) {
        LOG_ERROR << "Ref: " << ref << " chunk length exceed uint32_t maximum";
        return {std::shared_ptr<ChunkInterface>(new EmptyChunk()), false};
    }

    stream = bs[seq]->range(offset + decoded + 1,
                            offset + decoded + 1 + static_cast<int>(l));

    // TODO determine different encoding
    // A read mode XORChunk
    return {std::shared_ptr<ChunkInterface>(
                new XORChunk(stream.first, static_cast<int>(l))),
            true};
}

bool ChunkReader::error() { return err_; }

uint64_t ChunkReader::size() { return size_; }

} // namespace chunk
} // namespace tsdb
