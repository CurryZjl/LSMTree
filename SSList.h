#pragma once

#include <vector>
#include <fstream>
#include <iostream>
#include <cstdint>
#include "ssTable.h"
// 管理所有在磁盘的SSTable

const size_t kovSize = 20;

class SSList
{
public:
    // tables[l][i] 表示第l层的第i个SStable
    std::vector<std::vector<SSTable *>> tables;
    std::vector<uint64_t> MAXSET;
    std::vector<uint64_t> MINSET;
    std::vector<uint64_t> TIMESTAMPSET;
    SSList(){};
    ~SSList()
    {
        clear();
    };
    // 通过层号，层的索引来读取SSTable
    SSTable *readSSTable(int _level, int _id, std::fstream *in)
    {
        SSTable::Header header;
        // 读取头部
        in->read((char *)&header, sizeof(header));
        // 读取过滤器
        std::vector<bool> bf(BFSIZE, 0);
        char b;
        std::vector<char> bfBuffer(BFSIZE / 8); // 由于每个bool占用一个bit，因此每8个bool占用一个字节
        in->read(bfBuffer.data(), BFSIZE / 8);
        for (size_t i = 0; i < BFSIZE; i += 8)
        {
            char b = bfBuffer[i / 8];
            (b & (1 << 7)) && (bf[i] = 1);
            (b & (1 << 6)) && (bf[i + 1] = 1);
            (b & (1 << 5)) && (bf[i + 2] = 1);
            (b & (1 << 4)) && (bf[i + 3] = 1);
            (b & (1 << 3)) && (bf[i + 4] = 1);
            (b & (1 << 2)) && (bf[i + 5] = 1);
            (b & (1 << 1)) && (bf[i + 6] = 1);
            (b & (1)) && (bf[i + 7] = 1);
        }
        std::vector<SSTable::KOVPari> data;
        std::vector<char> dataBuffer(header.kv_nums * kovSize);
        in->read(dataBuffer.data(), header.kv_nums * kovSize);
        uint64_t key;
        uint64_t offset;
        uint32_t vlen;
        for (uint64_t i = 0; i < header.kv_nums; i++)
        {
            size_t Offset = i * kovSize;
            std::memcpy(&key, (dataBuffer.data() + Offset), 8);
            std::memcpy(&offset, (dataBuffer.data() + Offset + 8), 8);
            std::memcpy(&vlen, (dataBuffer.data() + Offset + 16), sizeof(vlen));
            data.emplace_back(key, offset, vlen);
        }
        return addToList(_level, _id, header.time, bf, data);
    }

    // 添加SSTable
    SSTable *addToList(int level, int id, uint64_t time, std::vector<bool> BF, std::vector<SSTable::KOVPari> &data)
    {
        SSTable *s = new SSTable(data, level, id, BF, time);
        // 如果需要创建新层
        while ((int)(tables.size() - 1) < level)
        {
            tables.push_back(std::vector<SSTable *>());
        }
        bool flag = true;
        for (std::vector<SSTable *>::iterator it = tables[level].begin(); it != tables[level].end(); it++)
        {
            if (id < (*it)->getId())
            {
                tables[level].insert(it, s);
                flag = false;
                break;
            }
        }

        if (flag == true)
        {
            tables[level].push_back(s);
        }
        return s;
    }
    // 由key返回对应SSTable的指针并设置offset 、vlen的参数
    SSTable *search(uint64_t key, uint64_t &offset, uint32_t &vlen)
    {
        SSTable *s = nullptr;
        bool flag = false;

        uint64_t tmpOffset = 0;
        uint32_t tmpVlen = 0;

        size_t tSize = tables.size();
        for (size_t i = 0; i < tSize; i++)
        {
            size_t tiSize = tables[i].size();
            for (size_t j = 0; j < tiSize; j++)
            {
                if (tables[i][j]->get(key, tmpOffset, tmpVlen))
                {
                    // 如果为空就直接更改s，如果非空需要对时间戳进行比较，用较大的时间戳替换较小的时间戳，保证数据是最新的
                    if ((s == nullptr) || ((s != nullptr) && (tables[i][j]->getTime() > s->getTime())))
                    {
                        s = tables[i][j];
                        offset = tmpOffset;
                        vlen = tmpVlen;
                        flag = true;
                    }
                }
            }
            // 找到了直接返回
            if (flag)
            {
                break;
            }
        }
        return s;
    }

    void scan(uint64_t key1, uint64_t key2, std::map<uint64_t, std::string> &RMap)
    {
        SSTable *s = nullptr;
        bool flag = false;

        uint64_t tmpOffset = 0;
        uint32_t tmpVlen = 0;

        size_t tSize = tables.size();
        for (size_t i = 0; i < tSize; i++)
        {
            size_t tiSize = tables[i].size();
            for (size_t j = 0; j < tiSize; j++)
            {
                SSTable *sstable = tables[i][j];
                if (sstable->minK() <= key2 && sstable->maxK() >= key1)
                {
                    // size_t ssSize = sstable->size();
                    // for (size_t k = 0; k < ssSize; k++)
                    // {
                    //     uint64_t key = sstable->idx[k].key;
                    //     if (key >= key1 && key <= key2)
                    //     {
                    //         uint64_t offset;
                    //         uint32_t vlen;
                    //         if (sstable->get(key, offset, vlen))
                    //         {
                    //             RMap[key] = value;
                    //         }
                    //     }
                    // }
                }
            }
        }
    }

    // 删除缓存文件
    void deleteTable(int level, int id)
    {
        for (std::vector<SSTable *>::iterator it = tables[level].begin(); it != tables[level].end(); it++)
        {
            if ((*it)->getId() == id)
            {
                tables[level].erase(it);
                break;
            }
        }
    }
    // 返回level层键值与minK到maxK有交集的所有SSTable的id，并在tables里删除这些索引，重排id
    std::vector<int> Intersection(int level, uint64_t minK, uint64_t maxK)
    {
        std::vector<int> a;
        MAXSET.clear();
        MINSET.clear();
        this->TIMESTAMPSET.clear();

        for (std::vector<SSTable *>::iterator it = tables[level].begin(); it != tables[level].end(); it++)
        {
            if (!(((*it)->minK() > maxK) || ((*it)->maxK() < minK)))
            {
                a.push_back((*it)->getId());
                MAXSET.push_back((*it)->maxK());
                MINSET.push_back((*it)->minK());
                TIMESTAMPSET.push_back((*it)->getTime());
                tables[level].erase(it);
                it--;
            }
        }
        int num = tables[level].size();
        for (int i = 0; i < num; i++)
        {
            tables[level][i]->changeId(i);
        }
        return a;
    }

    // 将level层中为oldid的文件更新为newid
    void changeId(int level, int newId, int oldId)
    {
        for (std::vector<SSTable *>::iterator it = tables[level].begin(); it != tables[level].end(); it++)
        {
            if ((*it)->getId() == oldId)
            {
                (*it)->changeId(newId);
                break;
            }
        }
    }

    void clear()
    {
        for (size_t i = 0; i < tables.size(); i++)
        {
            for (size_t j = 0; j < tables[i].size(); j++)
            {
                delete tables[i][j];
            }
            tables[i].clear();
        }
        tables.clear();
    }
};
