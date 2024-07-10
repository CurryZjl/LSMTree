#pragma once
#include "mem_table.h"
#include "MurmurHash3.h"
#include <string>
#include <vector>
#include <fstream>
#include <utility>
#include "utils.h"
#include "ssTable.h"
#include "vLogEntry.h"

#define MAGIC 0xff
#define ENTRYOFFSET (15)

class vLog
{
private:
    // 输入的文件名，就为“./data/vLog”
    std::string fileName;
    /* head就是当前文件的大小 */
    uint32_t head = 0;
    /* tail是从头找到第一个magic，之后进行crc校验，校验通过则这个magic的位置就是tail */
    uint32_t tail = 0;

    // 初始化，扫描文件
    void init(std::string &_filename)
    {
        std::fstream fs(_filename, std::ios::in | std::ios::binary);
        if (!fs.is_open())
        {
           // std::cerr << "Error: Failed to open vLog file." << std::endl;
            return;
        }
        // Get the size of the file (head)
        fs.seekg(0, std::ios::end);
        head = fs.tellg();
        if (head == 0)
        {
            return;
        }

        // Start scanning from the beginning to find the tail
        uint64_t current_pos = utils::seek_data_block(fileName);
        fs.seekg(current_pos);
        bool found_magic = false;
        while (current_pos < this->head)
        {
            // Read the magic byte
            uint8_t byte;
            fs.read(reinterpret_cast<char *>(&byte), sizeof(byte));

            if (byte == MAGIC)
            {
                uint16_t checksum;
                fs.read(reinterpret_cast<char *>(&checksum), sizeof(checksum)); // Read checksum

                if (checkCrc(current_pos))
                {
                    tail = current_pos; // Set the tail position
                    found_magic = true;
                    break;
                }
                else
                {
                    fs.seekg(-2, std::ios::cur);
                }
            }

            current_pos++;
        }

        // if (!found_magic)
        // {
        //     // 文件有大小却没有合适的value
        //     tail = head = 0;
        //     utils::rmfile(fileName);
        // }

        fs.close();
    }

    /* Magic和checksum在读取数据时检查数据是否被完整写入 */
    bool checkCrc(uint64_t pos)
    {
        // 设置读取的位置
        std::ifstream file(fileName, std::ios::binary);
        file.seekg(pos);
        uint8_t Magic;
        uint16_t CheckSum;
        uint64_t Key;
        uint32_t vlen;
        std::string Value;
        file.read((char *)&Magic, 1);
        file.read((char *)&CheckSum, 2);
        file.read((char *)&Key, 8);
        file.read((char *)&vlen, 4);
        // 读取 value 到临时缓冲区
        std::vector<char> temp(vlen);
        file.read(temp.data(), vlen);

        // 将临时缓冲区中的数据转换为 std::string
        Value.assign(temp.data(), temp.size());

        uint16_t checksum = generateCheckSum(Key, vlen, Value);
        file.close();
        return CheckSum == checksum;
    }

public:
    // 构造函数，如果已经有曾经的文件，则读取这个文件，如果还没有文件就创建一个新文件
    vLog(const std::string &_fileName) : fileName(_fileName)
    {
        init(fileName);
    }
    ~vLog() {}

    uint32_t getHead()
    {
        return this->head;
    }

    uint32_t getTail()
    {
        return this->tail;
    }

    uint32_t updateTail()
    {
        init(this->fileName);
    }

    /* 在LSMTree get操作中，如果找到了，则通过this->get函数拿取value */
    bool get(std::string &value, uint64_t offset, uint32_t vlen)
    {
        std::ifstream fs(fileName, std::ios::binary);
        if (!fs.is_open())
        {
            std::cerr << "Error: Failed to open vLog file to get data." << std::endl;
            return false;
        }

        if(offset < tail)
        {
            return false;
        }

        fs.seekg(offset + ENTRYOFFSET);
        value.resize(vlen);       // Resize the string to accommodate vlen
        fs.read(&value[0], vlen); // Read directly into the string buffer
        fs.close();
        return true;
    }

    /* 将内存中的KV储存到vLog，然后返回对应的一系列KOVpari，之后就可以生成SSTable保存在Level0 */
    void put(MemTable &memTable, std::vector<SSTable::KOVPari> &kovPairs)
    {
        kovPairs.clear();
        // 打开vLog文件以进行写入
        std::ofstream file(fileName, std::ios::binary | std::ios::app);
        if (!file.is_open())
        {
            std::cerr << "Error: Failed to open vLog file for writing." << std::endl;
            return;
        }

        uint64_t currentOffset = head;

        // 将MemTable中的每个条目写入到vLog文件中
        std::vector<vLogEntry> entrys;
        memTable.getAllNodes(entrys);
        // 使用流缓冲区来提高写入性能
        std::ostringstream buffer;

        // 注意这里的vLogEntry里面的value有可能是DELETED
        for (const vLogEntry &entry : entrys)
        {
            if (entry.Value == "~DELETED~")
            {
                // 不写入文件，但是要搞成vlen = 0 的KOVPair
                kovPairs.emplace_back(entry.Key, currentOffset, 0);
                continue;
            }

            size_t entryL = ENTRYOFFSET + entry.vlen + 1;

            buffer.write((char *)(&entry.Magic), sizeof(entry.Magic));
            buffer.write((char *)(&entry.CheckSum), sizeof(entry.CheckSum));
            buffer.write((char *)(&entry.Key), sizeof(entry.Key));
            buffer.write((char *)(&entry.vlen), sizeof(entry.vlen));
            buffer.write((char *)(entry.Value.c_str()), entry.vlen + 1); // 需要注意Value有自带的\0

            // 构造KOVPair并添加到返回的向量中
            kovPairs.emplace_back(entry.Key, currentOffset, entry.vlen);

            // 更新当前偏移量
            currentOffset += entryL;
        }
        file << buffer.str();

        // 关闭文件
        file.close();

        // 更新头部指针
        head = currentOffset;

        return;
    }

    void reset()
    {
        head = tail = 0;
        utils::rmfile(fileName);
    }

    uint16_t generateCheckSum(uint64_t &key, uint32_t &vlen, const std::string &value)
    {
        std::vector<unsigned char> data(sizeof(key) + sizeof(vlen) + vlen);

        // 拷贝 key 到 data
        std::memcpy(data.data(), &key, sizeof(key));

        // 拷贝 vlen 到 data
        std::memcpy(data.data() + sizeof(key), &vlen, sizeof(vlen));

        // 拷贝 value 到 data
        std::memcpy(data.data() + sizeof(key) + sizeof(vlen), value.data(), vlen);

        // 计算CRC16校验和
        return utils::crc16(data);
    }
};
