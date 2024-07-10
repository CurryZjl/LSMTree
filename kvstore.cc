#include "kvstore.h"
#include <string>

/* 最大内存尺寸为16kB */
#define MAXMEMSIZE 16384
#define INITSIZE 8224
/* size of Key(8) & Offset(8) & Vlen(4) */
#define KOVSIZE 20
#define DELETEFLAG "~DELETED~"

/* 启动时，检查现有目录的各层SSTable文件，在内存中构建相应缓存，同时恢复tail和head的值。即启动时需要读取以前的SSTable数据和vLog文件 */
KVStore::KVStore(const std::string &dir, const std::string &vlogN) : KVStoreAPI(dir, vlogN)
{
	this->sstDir = dir;
	this->vlogFileName = vlogN;
	this->memSize = 0;
	ssList = new SSList();
	vlog = new vLog(vlogFileName);
	buffer = new CompactBuffer();
	maxTime = 1;
	int level;
	std::string pathname;
	// 读取磁盘中已经有了的SSTable
	for (level = 0, pathname = generateLevelName(level); utils::dirExists(pathname); pathname = generateLevelName(++level))
	{
		level_file_num.push_back(0); // 当前层SSTable初始为0
		std::vector<std::string> ret;
		ret.clear();
		int k = 0;
		utils::scanDir(pathname, ret);

		for (std::vector<std::string>::iterator it = ret.begin(); it != ret.end(); it++)
		{
			std::fstream *input = new std::fstream((pathname + *it).c_str(), std::ios::binary | std::ios::in);
			if (!input->is_open())
			{
				input->close();
				delete input;
				break;
			}
			level_file_num[level]++; // 打开成功，这一层的文件数量增加
			// 读取对应位置的SSTable
			SSTable *t = ssList->readSSTable(level, k, input);
			if (t->getTime() >= maxTime)
			{
				maxTime = t->getTime() + 1; // 更新最大时间戳
			}
			k++;
			input->close();
			delete input;
		}
	}
}

KVStore::~KVStore()
{
	// 系统正常关闭，应该将MemTable的数据写入SSTable和vLog
	saveMem();
	delete ssList;
	delete vlog;
	delete buffer;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
	uint64_t tmpSize = memSize * KOVSIZE + INITSIZE;
	if (tmpSize >= MAXMEMSIZE)
	{
		saveMem(); // 内存中如果即将添加后满了，就要保存到磁盘 SSTable第0层
		if (level_file_num[0] > 2)
		{
			compact(0);
		}
	}
	memTable.put(key, s) ? ++memSize : memSize; // 放入新的key，注意如果成功保存到磁盘了，这是的内存就是新的
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	std::string tmpV = memTable.get(key);
	if (tmpV != "" && tmpV != DELETEFLAG)
	{
		return tmpV; // 在内存中找到，直接返回
	}
	if (tmpV == DELETEFLAG)
	{
		return ""; // 发现被删除了，返回“”
	}
	tmpV = searchInDisk(key);
	return tmpV;
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
	std::string res = get(key);
	if (res == "")
	{
		return false;
	}
	else
	{
		put(key, DELETEFLAG);
		return true;
	}
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
	memTable.clear();
	this->vlog->reset();
	ssList->clear();
	level_file_num.clear();
	maxTime = 1;
	memSize = 0;
	int Level;
	std::string directPath;
	for (Level = 0, directPath = generateLevelName(Level); utils::dirExists(directPath); directPath = generateLevelName(++Level))
	{
		std::vector<std::string> Lists;
		utils::scanDir(directPath, Lists);
		for (std::vector<std::string>::iterator it = Lists.begin(); it != Lists.end(); ++it)
		{
			std::string path = directPath + *it;
			utils::rmfile(path.c_str());
		}
		utils::rmdir(directPath.c_str());
	}
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list)
{
	for (uint64_t i = key1; i <= key2; i++)
	{
		std::string str = get(i);
		if (str != "" && str != DELETEFLAG)
		{
			list.push_back({i, str});
		}
	}
	// std::map<uint64_t, std::string> mergeMap;
	// //先去SSList扫描

	// memTable.scan(key1, key2, mergeMap);

	// for(auto iter = mergeMap.begin(); iter != mergeMap.end(); iter++)
	// {
	// 	list.push_back({iter->first, iter->second});
	// }

}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size)
{
	uint64_t currentSize = 0; // 记录已经搜索过的size
	uint32_t head = this->vlog->getHead();
	uint32_t tail = this->vlog->getTail();
	std::fstream file(this->vlogFileName.c_str(), std::ios::binary | std::ios::in);
	file.seekg(tail);
	// 先把chunk_size字节数据缓存到内存中
	std::vector<char> buffer(3*chunk_size);
	file.read(buffer.data(), 3*chunk_size);
	file.close();
	// 当搜索过的区域小时就继续循环
	const char *dataPtr = buffer.data();
	while (currentSize < chunk_size)
	{
		uint8_t Magic;
        uint16_t CheckSum;
        uint64_t Key;
        uint32_t vlen;
        std::string Value;
		std::memcpy(&Magic, dataPtr, 1);
		dataPtr += 1;
		std::memcpy(&CheckSum, dataPtr, 2);
		dataPtr += 2;
		std::memcpy(&Key, dataPtr, 8);
		dataPtr += 8;
		std::memcpy(&vlen, dataPtr, 4);
		dataPtr += 4;
		// 读取 value 到临时缓冲区
		std::vector<char> temp(vlen);
		std::memcpy(temp.data(), dataPtr, vlen);
		dataPtr += (vlen + 1);

		// 将临时缓冲区中的数据转换为 std::string
		Value.assign(temp.data(), temp.size());
		// 读取vLogEntry结束

		uint64_t tmpOffset = UINT64_MAX;
		uint32_t tmpVlen = UINT32_MAX;
		if (this->ssList->search(Key,tmpOffset,tmpVlen))
		{
			if ((tmpOffset == tail + currentSize) && (tmpVlen != 0))
			{
				if(memTable.get(Key) != "")
				{
					currentSize += (ENTRYOFFSET + vlen + 1);
					continue;
				}
				// 找到了而且确定是最新的有效数据
				this->put(Key, Value);
			}
			// 否则不做处理
		}
		// 没找到（不应该），不做处理，读下一个就行
		currentSize += (ENTRYOFFSET + vlen + 1);
	}
	// 扫描完毕
	saveMem();
	utils::de_alloc_file(this->vlogFileName, tail, currentSize);
	this->vlog->updateTail();
}

void KVStore::compact(int level)
{
	buffer->clear();
	std::string SSTablePath;
	int order = 0;
	// 第0层直接从第0个SSTable进行合并
	if (level == 0)
	{
		order = 0;
	}
	// 其他层跳出超出本层的那部分进行合并
	else
	{
		order = 1 << (level + 1);
	}
	int old_file_num = level_file_num[level];
	//删除本层多余的sst文件，并且放到合并buffer里面去
	for (int i = order; i < old_file_num; i++)
	{
		SSTable *s = ssList->tables[level][order];
		SSTablePath = SSTableName(level, s->minK(), s->maxK(), s->getTime());
		std::fstream *input = new std::fstream(SSTablePath.c_str(), std::ios::binary | std::ios::in);
		buffer->read(input);
		delete input;
		utils::rmfile(SSTablePath.c_str());
		ssList->deleteTable(level, i);
		level_file_num[level]--;
	}

	int nextL = level + 1;
	std::vector<int> tableNums; // 有相同键值的SSTable的编号
	if (level_file_num.size() == nextL)
	{
		//下一层为空的合并
		buffer->compact(true);
		createDirByLevel(nextL);
	}
	else
	{
		//下一层不为空
		old_file_num = level_file_num[nextL];
		int min = buffer->minKey();
		int max = buffer->maxKey();
		tableNums = ssList->Intersection(nextL, min, max);
		size_t tableNumsSize = tableNums.size();
		for (size_t i = 0; i < tableNumsSize; i++)
		{
			SSTablePath = SSTableName(nextL, ssList->MINSET[i], ssList->MAXSET[i], ssList->TIMESTAMPSET[i]);
			std::fstream *input = new std::fstream(SSTablePath.c_str(), std::ios::binary | std::ios::in);
			buffer->read(input);
			input->close();
			delete input;
			utils::rmfile(SSTablePath.c_str());
			level_file_num[nextL]--;
		}
		buffer->compact(false);
	}

	// 若非空，就继续写文件
	while (!buffer->isEmpty())
	{
		SSTablePath = SSTableName(nextL, buffer->getData_Tmp().front().key, buffer->getData_Tmp().back().key, buffer->timeStamp);
		level_file_num[nextL]++;
		std::fstream *output = new std::fstream(SSTablePath.c_str(), std::ios::out | std::ios::binary);
		buffer->write(output);
		output->close();
		delete output;
		std::fstream *input = new std::fstream(SSTablePath.c_str(), std::ios::in | std::ios::binary);
		ssList->readSSTable(nextL, level_file_num[nextL] - 1, input);
		input->close();
		delete input;
	}
	// 如果该层数量还是太多继续递归
	if (level_file_num[nextL] > (1 << (1 + nextL)))
	{
		compact(nextL);
	}
}

std::string KVStore::searchInDisk(uint64_t key)
{
	// 先在缓存中查询
	std::string res;
	uint64_t offset = 0;
	uint32_t vlen = 0;
	SSTable *tmp = ssList->search(key, offset, vlen);
	if (!tmp || vlen == 0)
	{
		return "";
	}
	// 不为空，去vLog读出相应字符串
	if (!this->vlog->get(res, offset, vlen))
	{
		// 拿取错误
		return "";
	}
	return res;
}

std::string KVStore::createDirByLevel(int le)
{
	std::string pathName = generateLevelName(le);
	if (!utils::dirExists(pathName))
	{
		utils::mkdir(pathName.c_str());
		level_file_num.push_back(0);
	}
	return pathName;
}

std::string KVStore::generateLevelName(int level)
{
	return std::string(this->sstDir + "/level-" + std::to_string(level) + "/");
}

std::string KVStore::SSTableName(int idx, uint64_t min, uint64_t max, uint64_t time)
{
	std::string pathName = generateLevelName(idx);
	pathName += "SSTable" + std::to_string(min) + "-" + std::to_string(max) + "-time:" + std::to_string(time) + ".sst";
	return pathName;
}

void KVStore::saveMem()
{
	// 首先将memTable的KV写入vLog，然后返回需要写入sstable的KOVPairs
	uint64_t size = memTable.size();
	if(size == 0)
		return;
	std::vector<SSTable::KOVPari> kovPairs;
	this->vlog->put(this->memTable, kovPairs);
	const uint64_t min = memTable.minKey();
	const uint64_t max = memTable.maxKey();
	std::string Level_0 = createDirByLevel(0);
	// 这里返回的kovPairs里面可能含有vlen = 0的，表示这key是被删除的
	std::string ssTableName = SSTableName(0, min, max, maxTime);

	level_file_num[0] += 1;
	std::fstream output(ssTableName.c_str(), std::ios::binary | std::ios::out);
	// 时间戳
	output.write((char *)&maxTime, sizeof(maxTime));
	maxTime++;
	// 键值对数量
	output.write((char *)&size, sizeof(size));
	// 最小键
	output.write((char *)&min, sizeof(min));
	// 最大键
	output.write((char *)&max, sizeof(max));
	// 写入bf
	std::vector<bool> bf;
	memTable.getBF(bf);
	size_t bfSize = bf.size();
	std::vector<char> bfBuffer((bfSize + 7) / 8); // Buffer to hold BF data
	for (size_t i = 0; i < bfSize; ++i)
	{
		if (bf[i])
		{
			bfBuffer[i / 8] |= (1 << (7 - (i % 8))); // Set corresponding bit in buffer
		}
	}
	output.write(bfBuffer.data(), bfBuffer.size());
	std::vector<char> buffer;
	buffer.reserve(kovPairs.size() * KOVSIZE);
	for (SSTable::KOVPari kovP : kovPairs)
	{
		buffer.insert(buffer.end(), reinterpret_cast<char *>(&kovP.key), reinterpret_cast<char *>(&kovP.key) + sizeof(kovP.key));
		buffer.insert(buffer.end(), reinterpret_cast<char *>(&kovP.offset), reinterpret_cast<char *>(&kovP.offset) + sizeof(kovP.offset));
		buffer.insert(buffer.end(), reinterpret_cast<char *>(&kovP.vlen), reinterpret_cast<char *>(&kovP.vlen) + sizeof(kovP.vlen));
	}
	output.write(buffer.data(), buffer.size());
	output.close();

	// 将新的SSTable加入SSList监管
	std::fstream input(ssTableName.c_str(), std::ios::binary | std::ios::in);
	ssList->readSSTable(0, level_file_num[0] - 1, &input);
	input.close();
	memTable.clear();
	memSize = 0;
}
