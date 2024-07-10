#pragma once
#include <vector>
#include "utils.h"
#define MAGIC 0xff

class vLogEntry
{
public:
    uint8_t Magic = MAGIC;
    uint16_t CheckSum;
    uint64_t Key;
    uint32_t vlen;
    std::string Value;
    vLogEntry(uint64_t key, const std::string &value) : Key(key), Value(value), vlen(value.length())
    {
        std::vector<unsigned char> data( 12 + vlen);
        // 拷贝 key 到 data
        std::memcpy(data.data(), &Key, 8);

        // 拷贝 vlen 到 data
        std::memcpy(data.data() + 8, &vlen, 4);

        // 拷贝 value 到 data
        std::memcpy(data.data() + 12, value.data(), vlen);

        CheckSum = utils::crc16(data);
    }
    ~vLogEntry() = default;

    // Copy constructor
    vLogEntry(const vLogEntry &other) = default;

    // Move constructor
    vLogEntry(vLogEntry &&other) = default;

    // Copy assignment operator
    vLogEntry &operator=(const vLogEntry &other) = default;

    // Move assignment operator
    vLogEntry &operator=(vLogEntry &&other) = default;
};