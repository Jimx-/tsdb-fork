#ifndef BLOCK_H
#define BLOCK_H

#include "base/Error.hpp"
#include "base/WaitGroup.hpp"
#include "block/BlockInterface.hpp"
#include "block/ChunkReaderInterface.hpp"
#include "block/IndexReaderInterface.hpp"
#include "tombstone/TombstoneReaderInterface.hpp"
#include "tsdbutil/StringTuplesInterface.hpp"

namespace tsdb{
namespace block{

enum BlockType {OriginalBlock, GroupBlock};

class Block;

class BlockChunkReader: public ChunkReaderInterface{
    private:
        std::shared_ptr<ChunkReaderInterface> chunkr;
        const Block * b;

    public:
        BlockChunkReader(const std::shared_ptr<ChunkReaderInterface> & chunkr, const Block * b);

        std::pair<std::shared_ptr<chunk::ChunkInterface>, bool> chunk(uint64_t ref);

        bool error();

        uint64_t size();

        ~BlockChunkReader();
};

class BlockIndexReader: public IndexReaderInterface{
    private:
        std::shared_ptr<IndexReaderInterface> indexr;
        const Block * b;

    public:
        BlockIndexReader(const std::shared_ptr<IndexReaderInterface> & indexr, const Block * b);

        std::set<std::string> symbols();

        const std::deque<std::string> & symbols_deque() const;

        // Return empty SerializedTuples when error
        std::unique_ptr<tsdbutil::StringTuplesInterface> label_values(const std::initializer_list<std::string> & names);
        std::unique_ptr<tsdbutil::StringTuplesInterface> label_values(const std::string & name);

        // 1. Get the corresponding group postings entry.
        // 2. Pass ALL_GROUP_POSTINGS, get offsets of all group postings entries.
        std::pair<std::unique_ptr<index::PostingsInterface>, bool> group_postings(uint64_t group_ref);
        std::pair<std::unique_ptr<index::PostingsInterface>, bool> postings(const std::string & name, const std::string & value);

        bool series(uint64_t ref, label::Labels & lset, std::deque<std::shared_ptr<chunk::ChunkMeta> > & chunks);
        bool series(uint64_t ref, label::Labels & lset, const std::shared_ptr<chunk::ChunkMeta> & chunk);

        std::deque<std::string> label_names();

        bool error();

        uint64_t size();

        std::unique_ptr<index::PostingsInterface> sorted_postings(std::unique_ptr<index::PostingsInterface> && p);
        std::unique_ptr<index::PostingsInterface> sorted_group_postings(std::unique_ptr<index::PostingsInterface> && p);

        ~BlockIndexReader();
};

class BlockTombstoneReader: public tombstone::TombstoneReaderInterface{
    private:
        std::shared_ptr<tombstone::TombstoneReaderInterface> tombstones;
        const Block * b;

    public:
        BlockTombstoneReader(const std::shared_ptr<tombstone::TombstoneReaderInterface> & tombstones, const Block * b);

        // NOTICE, may throw std::out_of_range.
        const tombstone::Intervals & get(uint64_t ref) const;

        void iter(const IterFunc & f) const;
        error::Error iter(const ErrIterFunc & f) const;

        uint64_t total() const;

        void add_interval(uint64_t ref, const tombstone::Interval & itvl);

        ~BlockTombstoneReader();
};

class Block: public BlockInterface{
    private:
        mutable base::RWMutexLock mutex_;
        mutable base::WaitGroup pending_readers;
        mutable bool closing;
        std::string dir_;
        BlockMeta meta_;

        uint64_t symbol_table_size_;

        std::shared_ptr<ChunkReaderInterface> chunkr;
        std::shared_ptr<IndexReaderInterface> indexr;
        std::shared_ptr<tombstone::TombstoneReaderInterface> tr;

        error::Error err_;

        uint8_t type_;

        Block( const Block& ) = delete; // non construction-copyable
        Block& operator=( const Block& ) = delete; // non copyable

    public:
        Block(uint8_t type_=static_cast<uint8_t>(OriginalBlock));
        Block(const std::string & dir, uint8_t type_=static_cast<uint8_t>(OriginalBlock));
        Block(bool closing, const std::string & dir_, const BlockMeta & meta_, uint64_t symbol_table_size_,
            const std::shared_ptr<ChunkReaderInterface> & chunkr,
            const std::shared_ptr<IndexReaderInterface> & indexr,
            std::shared_ptr<tombstone::TombstoneReaderInterface> & tr,
            const error::Error & err_,
            uint8_t type_=static_cast<uint8_t>(OriginalBlock));

        bool is_closing(){ return closing; }
        uint8_t type(){ return type_; }
        // dir returns the directory of the block.
        std::string dir();

        bool overlap_closed(int64_t mint, int64_t maxt) const;

        // meta returns meta information about the block.
        BlockMeta meta() const;

        int64_t MaxTime() const;

        int64_t MinTime() const;

        // size returns the number of bytes that the block takes up.
        uint64_t size() const;

        // get_symbol_table_size returns the Symbol Table Size in the index of this block.
        uint64_t get_symbol_table_size();

        error::Error error() const;

        bool start_read() const;

        // Wrapper for add() of pending_readers.
        void p_add(int i) const;

        // Wrapper for done() of pending_readers.
        void p_done() const;

        // Wrapper for wait() of pending_readers.
        void p_wait() const;

        bool set_compaction_failed();

        bool set_deletable();

        // label_names returns all the unique label names present in the Block in sorted order.
        std::deque<std::string> label_names();

        std::pair<std::shared_ptr<IndexReaderInterface>, bool> index() const;

        std::pair<std::shared_ptr<ChunkReaderInterface>, bool> chunks() const;

        std::pair<std::shared_ptr<tombstone::TombstoneReaderInterface>, bool> tombstones() const;

        error::Error del(int64_t mint, int64_t maxt, const std::deque<std::shared_ptr<label::MatcherInterface>> & matchers);

        // clean_tombstones will remove the tombstones and rewrite the block (only if there are any tombstones).
        // If there was a rewrite, then it returns the ULID of the new block written, else nil.
        //
        // NOTE(Alec), use it carefully.
        std::pair<ulid::ULID, error::Error> clean_tombstones(const std::string & dest, void * compactor);

        void close() const;

        ~Block();
};

}}

#endif