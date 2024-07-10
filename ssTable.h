#pragma once
#include "MurmurHash3.h"
#include <string>
#include <vector>
#include <fstream>

/* Bloom Filter 大小为8kB = 8*1024bytes 65536bits */
#define BFSIZE 65536

class SSTable
{
public:
    struct Header
    {
        uint64_t time;    // 时间戳
        uint64_t kv_nums; // 键值对的数量
        uint64_t minK;    // 键最小值
        uint64_t maxK;    // 键最大值
    };

    struct KOVPari
    {
        uint64_t key;
        uint64_t offset; // value在vlog文件中的offset
        uint32_t vlen;   // value的长度
        KOVPari(uint64_t _key, uint64_t _offset, uint32_t len)
            : key(_key), offset(_offset), vlen(len) {}
    };

private:
    // 标记层号
    int level;
    // 标记在本层的编号
    int id;
    Header header;
    uint64_t currentTime = 0;

    /*返回key对应在SSTable里面的索引 没找到则返回 UINT64_MAX*/
    uint64_t binarySearch(uint64_t key) const
    {
        if (this->size() == 0)
        {
            return UINT64_MAX;
        }

        int low = 0, high = idx.size() - 1, mid = 0;
        while (low <= high)
        {
            mid = (low + high) / 2;
            if (idx[mid].key == key)
            {
                return mid;
            }
            else if (idx[mid].key > key)
            {
                high = mid - 1;
            }
            else
            {
                low = mid + 1;
            }
        }
        return UINT64_MAX;
    }

public:
    std::vector<KOVPari> idx;
    const static uint32_t BASE = sizeof(Header) + BFSIZE / 8;

    // 布隆过滤器
    std::vector<bool> bloomFilter;
    // 创建表 assignedTime是时间戳
    SSTable(const std::vector<KOVPari> data,
            int _level, int _id, std::vector<bool> bf, const uint64_t assignedTime)
        : level(_level), id(_id), bloomFilter(bf), idx(data)
    {
        header.time = assignedTime;
        header.kv_nums = data.size();
        if (!data.empty())
        {
            header.minK = data.front().key;
            header.maxK = data.back().key;
        }
        else
        {
            std::cerr << "SSTable: data is empty" << std::endl;
            header.minK = 0;
            header.maxK = 0;
        }
    }

    ~SSTable() {}

    /* 查找key，如果没找到会返回false
     * 找到会为offset和vlen写入对应的值，并返回true
     */
    bool get(uint64_t key, uint64_t &offset, uint32_t &vlen)
    {
        if (key < header.minK || key > header.maxK)
        {
            return false;
        }

        if (!findBloom(key))
        {
            return false;
        }

        uint64_t target = binarySearch(key);
        if (target == UINT64_MAX)
        {
            return false;
        }

        offset = idx[target].offset;
        vlen = idx[target].vlen;
        return true;
    }

    bool findBloom(uint64_t &key)
    {
        uint32_t hash[4] = {0};
        MurmurHash3_x64_128(&key, sizeof(key), 1, hash);

        if (bloomFilter[hash[0] % (BFSIZE)] == false)
            return false;
        if (bloomFilter[hash[1] % (BFSIZE)] == false)
            return false;
        if (bloomFilter[hash[2] % (BFSIZE)] == false)
            return false;
        if (bloomFilter[hash[3] % (BFSIZE)] == false)
            return false;

        return true;
    }

    uint64_t getTime() const
    {
        return header.time;
    }
    uint64_t minK() const
    {
        return header.minK;
    }
    uint64_t maxK() const
    {
        return header.maxK;
    }
    uint64_t size() const
    {
        return header.kv_nums;
    }
    int getLevel() const
    {
        return level;
    }
    int getId() const
    {
        return id;
    }
    void changeId(int newId)
    {
        id = newId;
    }
    void changeTime(uint64_t newTime)
    {
        header.time = newTime;
    }
};
