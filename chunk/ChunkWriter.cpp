#include <boost/filesystem.hpp>

#include "base/Logging.hpp"
#include "chunk/ChunkUtils.hpp"
#include "chunk/ChunkWriter.hpp"

namespace tsdb {
namespace chunk {

// Implicit construct from const char *
ChunkWriter::ChunkWriter(const std::string& dir)
    : dir(dir), pos(0), chunk_size(DEFAULT_CHUNK_SIZE)
{
    boost::filesystem::path block_dir = boost::filesystem::path(dir);
    if (!boost::filesystem::create_directories(block_dir)) {
        LOG_INFO << "Block Directory existed: " << block_dir.string();
        // throw AlecException("Directory Already Created: " +
        // wal_path.string());
    } else {
        LOG_INFO << "Create Block Directory: " << block_dir.string();
    }
}

FILE* ChunkWriter::tail()
{
    if (files.empty()) return NULL;
    return files.back();
}

// finalize_tail writes all pending data to the current tail file and close it
void ChunkWriter::finalize_tail()
{
    if (!files.empty()) {
        fflush(files.back());
        fclose(files.back());
    }
}

void ChunkWriter::cut()
{
    // Sync current tail to disk and close.
    finalize_tail();
    auto p = next_sequence_file(dir);
    FILE* f = fopen(p.second.c_str(), "wb+");

    // Write header metadata for new file.
    uint8_t temp[8];
    base::put_uint32_big_endian(temp, MAGIC_CHUNK);
    base::put_uint32_big_endian(temp + 4, CHUNK_FORMAT_V1);
    fwrite(temp, 1, 8, f);
    files.push_back(f);
    seqs.push_back(p.first);
    pos = 8;
}

void ChunkWriter::write(const uint8_t* bytes, int size)
{
    int written =
        fwrite(reinterpret_cast<const void*>(bytes), 1, size, files.back());
    pos += written;
}

void ChunkWriter::write_chunks(
    const std::vector<std::shared_ptr<ChunkMeta>>& chunks)
{
    uint64_t max_len = 0;
    for (auto& ptr : chunks) {
        max_len += 5 + base::MAX_VARINT_LEN_32;
        max_len += static_cast<uint64_t>(ptr->chunk->size());
    }

    if (files.empty() || pos > chunk_size ||
        (pos + max_len > chunk_size && max_len <= chunk_size))
        cut();

    uint8_t b[base::MAX_VARINT_LEN_32];
    uint64_t sequence = (seq() << 32);
    for (auto& chk : chunks) {
        chk->ref = sequence | static_cast<uint64_t>(pos);

        // Write len of chk->chunk->bytes()
        int encoded = base::encode_unsigned_varint(
            b, static_cast<uint64_t>(chk->chunk->size()));
        write(b, encoded);

        // Write encoding
        b[0] = static_cast<uint8_t>(chk->chunk->encoding());
        write(b, 1);

        // Write data
        write(chk->chunk->bytes(), chk->chunk->size());

        // Write crc32
        base::put_uint32_big_endian(
            b, base::GetCrc32(chk->chunk->bytes(), chk->chunk->size()));
        write(b, 4);
    }
}

uint64_t ChunkWriter::seq()
{
    // return static_cast<uint64_t>(files.size() - 1);
    return static_cast<uint64_t>(seqs.back());
}

void ChunkWriter::close() { finalize_tail(); }

ChunkWriter::~ChunkWriter() { close(); }

} // namespace chunk
} // namespace tsdb
