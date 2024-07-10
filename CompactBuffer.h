#pragma once
#include <cstdint>
#include <string>
#include <fstream>
#include <vector>
#include "ssTable.h"

#define MAXSIZE (16 * 1024)
#define NODESIZE 20

// 存放一个SSTable的数据和时间戳
struct DataTable
{
    uint64_t timeStamp;
    std::vector<SSTable::KOVPari> data;
    DataTable(std::vector<SSTable::KOVPari> nodes, uint64_t time) : data(nodes), timeStamp(time) {}
};

class CompactBuffer
{
    std::vector<DataTable> dataTables;      // 所有要被合并的SSTable
    std::vector<SSTable::KOVPari> tmpNodes; // 完成归并排序后的所有KOVPair
public:
    uint64_t timeStamp; // 合并后的新时间
    CompactBuffer()
    {
        clear();
    }
    ~CompactBuffer() {}

    // 将需要的合并的一个SSTable读入
    void read(std::fstream *in)
    {
        uint64_t time;
        in->read((char *)&time, sizeof(time));
        uint64_t num = 0;
        in->read((char *)&num, sizeof(num));

        // 定位到数据区的起始位置
        in->seekg(SSTable::BASE, std::ios::beg);
        std::vector<SSTable::KOVPari> Index;
        uint64_t key;
        uint64_t offset;
        uint32_t vlen;
        for (uint64_t i = 0; i < num; i++)
        {
            in->read((char *)&key, sizeof(key));
            in->read((char *)&offset, sizeof(offset));
            in->read((char *)&vlen, sizeof(vlen));
            Index.emplace_back(key, offset, vlen);
        }
        // Index里面装满了一个SSTable的所有KOVPair
        dataTables.emplace_back(Index, time);
    }

    // isempty为true代表下一层为空，否则为false
    void compact(bool isempty)
    {
        tmpNodes.clear();
        int location = 0;

        uint64_t min = UINT64_MAX, max = 0;
        // 一轮排序中的最大时间戳
        uint64_t maxTime = 0;

        this->timeStamp = 0;
        // 选取最大的时间戳
        for (size_t i = 0; i < dataTables.size(); i++)
        {
            if (dataTables[i].timeStamp > this->timeStamp)
            {
                this->timeStamp = dataTables[i].timeStamp;
            }
        }
        // 如果待合并的Tables不是空的，就删除所有的重复键值，选择其中最大时间戳的键值作为真正的键值
        while (dataTables.empty() == false)
        {
            size_t ss_num = dataTables.size();
            min = UINT64_MAX;
            maxTime = 0;
            location = 0;
            for (size_t i = 0; i < ss_num; i++)
            {
                SSTable::KOVPari data = dataTables[i].data.front();
                if (data.key < min)
                {
                    maxTime = dataTables[i].timeStamp;
                    location = i;
                    min = data.key;
                }
                else if (data.key == min)
                {
                    if (dataTables[i].timeStamp > maxTime)
                    {
                        dataTables[location].data.erase(dataTables[location].data.begin());
                        maxTime = dataTables[i].timeStamp;
                        location = i;
                    }
                    else
                    {
                        dataTables[i].data.erase(dataTables[i].data.begin());
                    }
                }
            }

            // 现在拿到了最小的键
            SSTable::KOVPari node = dataTables[location].data.front();
            // 如果 不是 一个被删除的node且下一层为空，则将数据存在tmpNodes中
            if (!(isempty && node.vlen == 0))
            {
                tmpNodes.push_back(node);
            }
            dataTables[location].data.erase(dataTables[location].data.begin());
            ss_num = dataTables.size();
            // 如果有已经读完的table，就删除
            for (int i = 0; i < ss_num; i++)
            {
                if (dataTables[i].data.empty())
                {
                    dataTables.erase(dataTables.begin() + i);
                    i--;
                    ss_num--;
                }
            }
        }
    }

    // 以SSTable格式输出
    void write(std::fstream *out)
    {
        if (this->tmpNodes.empty())
        {
            return;
        }
        std::vector<bool> bf;
        std::vector<SSTable::KOVPari> dataSet = getData(bf);
        out->write((char *)&this->timeStamp, sizeof(timeStamp));
        size_t Size = dataSet.size();
        out->write((char *)&Size, sizeof(Size));
        uint64_t Min = dataSet.front().key;
        uint64_t Max = dataSet.back().key;
        out->write((char *)&Min, sizeof(Min));
        out->write((char *)&Max, sizeof(Max));
        // 写入过滤器
        for (size_t i = 0; i < BFSIZE; i += 8)
        {
            char b = 0;
            for (size_t j = 0; j < 8; j++)
            {
                if (bf[i + j])
                {
                    b = (1 << (7 - j)) | b;
                }
            }
            out->write(&b, sizeof(b));
        }
        for (SSTable::KOVPari kovP : dataSet)
        {
            out->write((char *)&kovP.key, sizeof(kovP.key));
            out->write((char *)&kovP.offset, sizeof(kovP.offset));
            out->write((char *)&kovP.vlen, sizeof(kovP.vlen));
        }
    }

    uint64_t maxKey()
    {
        uint64_t max = 0;
        size_t size = dataTables.size();
        for (size_t i = 0; i < size; i++)
        {
            if (max < dataTables[i].data.back().key)
            {
                max = dataTables[i].data.back().key;
            }
        }
        return max;
    }

    uint64_t minKey()
    {
        uint64_t min = UINT64_MAX;
        size_t size = dataTables.size();
        for (size_t i = 0; i < size; i++)
        {
            if (min > dataTables[i].data.front().key)
            {
                min = dataTables[i].data.front().key;
            }
        }
        return min;
    }

    // 从nodes中获取SSTable不超过16kB的数据，
    std::vector<SSTable::KOVPari> getData(std::vector<bool> &BF)
    {
        BF.resize(BFSIZE, 0);
        std::vector<SSTable::KOVPari> data;
        size_t init_size = SSTable::BASE;
        while (1)
        {
            if (this->tmpNodes.empty())
                break;
            init_size += NODESIZE;
            if (init_size > MAXSIZE)
                break;
            data.push_back(this->tmpNodes.front());
            this->tmpNodes.erase(this->tmpNodes.begin());
            uint32_t hash[4] = {0};
            MurmurHash3_x64_128(&data.back().key, sizeof(data.back().key), 1, hash);
            for (int i = 0; i < 4; i++)
            {
                BF[hash[i] % BFSIZE] = 1;
            }
        }
        return data;
    }

    // 清空数据，用于实现初始化
    void clear()
    {
        dataTables.clear();
        tmpNodes.clear();
        timeStamp = 0;
    }

    bool isEmpty(){
        return this->tmpNodes.empty();
    }

     std::vector<SSTable::KOVPari> getData_Tmp()
    {
        std::vector<SSTable::KOVPari> data;
        size_t init_size = SSTable::BASE;
        std::vector<SSTable::KOVPari> tmp = tmpNodes;
        while (1)
        {
            if (tmp.empty())
                break;
            init_size += NODESIZE;
            if (init_size > MAXSIZE)
                break;
            data.push_back(tmp.front());
            tmp.erase(tmp.begin());
        }
        return data;
    }
};