#pragma once
#include "utils.h"
#include "kvstore_api.h"
#include "mem_table.h"
#include "SSList.h"
#include "vLog.h"
#include "CompactBuffer.h"
#include <string>
#include <map>


class KVStore : public KVStoreAPI
{
private:
	//内存
	MemTable memTable;
	//根目录
	std::string sstDir;

	//vlog文件名
	std::string vlogFileName;

	//每层文件的数量
	std::vector<int> level_file_num;
	//管辖所有SSTable
	SSList *ssList;
	//vLog文件
	vLog *vlog;
	//用于合并的缓冲区
	CompactBuffer *buffer;
	
	uint64_t maxTime; //记录最大的时间戳

	size_t memSize;

	//存储到磁盘
	void saveMem();
	//合并函数
	void compact(int level);

	std::string searchInDisk(uint64_t key);
	std::string createDirByLevel(int level);
	std::string generateLevelName(int level);
	std::string SSTableName(int idx, uint64_t min, uint64_t max, uint64_t time);
public:
	KVStore(const std::string &dir, const std::string &vlog);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	/* 将所有层的SSTable文件和目录、vLog文件删除，还要清除内存中的MemTable和缓存，将teil和head置为0 */
	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;

	void gc(uint64_t chunk_size) override;

};
