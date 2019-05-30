#ifndef XORCHUNK_H
#define XORCHUNK_H

#include "chunk/BitStream.hpp"
#include "chunk/ChunkInterface.hpp"
#include "chunk/ChunkAppenderInterface.hpp"
#include "chunk/ChunkIteratorInterface.hpp"
#include "chunk/XORIterator.hpp"

namespace tsdb{
namespace chunk{

class XORChunk: public ChunkInterface{
    private:
        BitStream bstream;
        bool read_mode;
        uint64_t size_;

    public:
        // The first two bytes store the num of samples using big endian
        XORChunk();

        XORChunk(const uint8_t * stream_ptr, uint64_t size);

        const uint8_t * bytes();

        uint8_t encoding();

        std::unique_ptr<ChunkAppenderInterface> appender();

        std::unique_ptr<ChunkIteratorInterface> iterator();

        std::unique_ptr<XORIterator> xor_iterator();

        int num_samples();

        uint64_t size();
};

}}

#endif