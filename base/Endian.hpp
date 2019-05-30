#ifndef ENDIAN_H
#define ENDIAN_H

#include <deque>
#include <vector>
#include <stdint.h>
#include <string>
#include "base/TSDBException.hpp"

namespace tsdb{
namespace base{

extern const int MAX_VARINT_LEN_64;
extern const int MAX_VARINT_LEN_32;

// Decoding for char array
template<typename T>
uint16_t big_endian_uint16(T arr){
    return static_cast<uint16_t>((arr[0] << 8) + arr[1]);
}

void put_uint16_big_endian(std::deque<uint8_t> & bytes, int num);

int get_uint16_big_endian(const std::deque<uint8_t> & bytes);

void put_uint16_big_endian(std::vector<uint8_t> & bytes, int num);

int get_uint16_big_endian(const std::vector<uint8_t> & bytes);

void put_uint16_big_endian(uint8_t * bytes, int num);

int get_uint16_big_endian(const uint8_t * bytes);

uint32_t get_uint32_big_endian(const uint8_t * bytes);

void put_uint32_big_endian(uint8_t * bytes, uint32_t num);

uint32_t get_uint32_big_endian(const std::deque<uint8_t> & bytes);

void put_uint32_big_endian(std::deque<uint8_t> & bytes, uint32_t num);

uint32_t get_uint32_big_endian(const std::vector<uint8_t> & bytes);

void put_uint32_big_endian(std::vector<uint8_t> & bytes, uint32_t num);

uint64_t get_uint64_big_endian(const std::vector<uint8_t> & bytes);

void put_uint64_big_endian(std::vector<uint8_t> & bytes, uint64_t num);

uint64_t get_uint64_big_endian(const uint8_t * bytes);

void put_uint64_big_endian(uint8_t * bytes, uint64_t num);

uint64_t encode_double(double value);

double decode_double(uint64_t value);

uint64_t decode_unsigned_varint( const std::deque<uint8_t> & data, int &decoded_bytes );

int64_t decode_signed_varint( const std::deque<uint8_t> & data, int &decoded_bytes );

uint64_t decode_unsigned_varint(const uint8_t * data, int &decoded_bytes, int size);

int64_t decode_signed_varint(const uint8_t * data, int &decoded_bytes, int size);

// Encode an unsigned 64-bit varint.  Returns number of encoded bytes.
// Buffer's size is at least 10
int encode_unsigned_varint(uint8_t *const buffer, uint64_t value);

// Encode a signed 64-bit varint.  Works by first zig-zag transforming
// signed value into an unsigned value, and then reusing the unsigned
// encoder. 
// Buffer's size is at least 10 
int encode_signed_varint(uint8_t *const buffer, int64_t value);

// Encode an unsigned 64-bit varint.  Returns number of encoded bytes.
// Buffer's size is at least 10
int encode_unsigned_varint(std::vector<uint8_t> & buffer, uint64_t value);

// Encode a signed 64-bit varint.  Works by first zig-zag transforming
// signed value into an unsigned value, and then reusing the unsigned
// encoder. 
// Buffer's size is at least 10 
int encode_signed_varint(std::vector<uint8_t> & buffer, int64_t value);

template <typename I> std::string n2hexstr(I w, size_t hex_len = sizeof(I)<<1){
    static const char* digits = "0123456789ABCDEF";
    std::string rc(hex_len,'0');
    for (size_t i=0, j=(hex_len-1)*4 ; i<hex_len; ++i,j-=4)
        rc[i] = digits[(w>>j) & 0x0f];
    return rc;
}

uint8_t max_bits(const std::deque<int64_t> & d);
uint8_t max_bits(const std::vector<int64_t> & v);
int64_t get_origin(uint64_t data, int bits);
}}

#endif