#include <iostream>
#include <cstdint>
#include <string>
#include <assert.h>
#include <chrono>

#include "test.h"

/* 进行不同测试时只需要打开define标识即可 */
#define REGULAR_T
// #define CACHE_T
// #define COMPACTION_T
// #define BFSIZE_T

class MyTest : public Test
{
private:
    const uint64_t TEST_MAX = 1024 * 48;
    const size_t BF_SIZE = 8192*8;
    const int CACHE_MODE = 2;
    const bool COM = true;

    void regular_test(uint64_t max)
    {
        std::cout << "Data nums: " << max << std::endl;
        uint64_t i;
        // 执行操作 max次PUT
        auto t1 = std::chrono::system_clock::now();
        for (i = 0; i < max; ++i)
        {
            store.put(i, std::string(i + 1, 's'));
        }
        auto t2 = std::chrono::system_clock::now();
        auto duration = t2 - t1;
        auto duration_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();
        std::cout << "PUT: ";
        report_throughput(duration_seconds, max / duration_seconds);

        // 执行操作 max次GET 有效GET和无效GET各占一半
        t1 = std::chrono::system_clock::now();
        for (i = 0; i < max; ++i)
        {
            store.get(2 * i);
        }
        t2 = std::chrono::system_clock::now();
        duration = t2 - t1;
        duration_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();
        std::cout << "GET: ";
        report_throughput(duration_seconds, max / duration_seconds);

        // 执行操作 1 次scan
        std::list<std::pair<uint64_t, std::string>> list_stu;
        t1 = std::chrono::system_clock::now();
        store.scan(0, max / 2 - 1, list_stu);
        t2 = std::chrono::system_clock::now();
        duration = t2 - t1;
        duration_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();
        std::cout << "SCAN: ";
        report_throughput(duration_seconds, 10 / duration_seconds);

        // 执行操作 max 次删除, 有效删除和无效删除各占一半
        t1 = std::chrono::system_clock::now();
        for (i = 0; i < max; ++i)
        {
            store.del(2 * i);
        }
        t2 = std::chrono::system_clock::now();
        duration = t2 - t1;
        duration_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();
        std::cout << "DEL: ";
        report_throughput(duration_seconds, max / duration_seconds);
    }

    void cache_and_bf_test(uint64_t max, int mode)
    {
        switch (mode)
        {
        case 0:
            std::cout << "without any cache: " << std::endl;
            break;
        case 1:
            std::cout << "with index cache: " << std::endl;
            break;
        case 2:
            std::cout << "with index cache and BloomFilter: " << std::endl;
            break;
        }

        uint64_t i;
        // 执行操作 max次PUT 预备工作
        for (i = 0; i < max; ++i)
        {
            store.put(i, std::string(i + 1, 's'));
        }

        // 执行操作 max次GET 有效GET和无效GET各占一半
        auto t1 = std::chrono::system_clock::now();
        for (i = 0; i < max; ++i)
        {
            store.get(2 * i);
        }
        auto t2 = std::chrono::system_clock::now();
        auto duration = t2 - t1;
        auto duration_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();
        std::cout << "GET: ";
        report_throughput(duration_seconds, max / duration_seconds);
    }

    void compaction_test(uint64_t max)
    {
        std::cout << "with compaction:" << std::endl;
        uint64_t i;
        uint64_t count = 0;
        // 执行操作 max次PUT
        auto t1 = std::chrono::system_clock::now();
        for (i = 0; i < max; ++i)
        {
            store.put(i, std::string(i + 1, 's'));
            count++;
            auto t2 = std::chrono::system_clock::now();
            auto duration = t2 - t1;
            auto duration_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();
            if (duration_seconds >= 3.0)
            {
                std::cout << "PUT count after " << duration_seconds << " seconds: " << count << std::endl;
                count = 0;
                t1 = std::chrono::system_clock::now();
            }
        }
    }

    void bfsize_test(uint64_t max, size_t size)
    {
        std::cout << "BFSize: " << size / 8 << " bytes" << std::endl;
        uint64_t i;
        // 执行操作 max次PUT
        auto t1 = std::chrono::system_clock::now();
        for (i = 0; i < max; ++i)
        {
            store.put(i, std::string(i + 1, 's'));
        }
        auto t2 = std::chrono::system_clock::now();
        auto duration = t2 - t1;
        auto duration_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();
        std::cout << "PUT: ";
        report_throughput(duration_seconds, max / duration_seconds);

        // 执行操作 max次GET 有效GET和无效GET各占一半
        t1 = std::chrono::system_clock::now();
        for (i = 0; i < max; ++i)
        {
            store.get(2 * i);
        }
        t2 = std::chrono::system_clock::now();
        duration = t2 - t1;
        duration_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();
        std::cout << "GET: ";
        report_throughput(duration_seconds, max / duration_seconds);
    }

    void report_throughput(double seconds, long ops_per_second)
    {
        std::cout << "time: " << seconds << "s\t"
                                            "throughput: "
                  << ops_per_second << " ops/s\n";
    }

public:
    MyTest(const std::string &dir, const std::string &vlog, bool v = true) : Test(dir, vlog, v)
    {
    }

    void start_test(void *args = NULL)
    {
        std::cout << "KVStore performance Test" << std::endl;

#ifdef REGULAR_T
        store.reset();

        std::cout << "[Regular Test]" << std::endl;
        regular_test(TEST_MAX);
#endif

#ifdef CACHE_T
        store.reset();
        std::cout << std::endl
                  << "[Cache Test]" << std::endl;
        cache_and_bf_test(TEST_MAX, CACHE_MODE);
#endif

#ifdef COMPACTION_T
        store.reset();

        std::cout << std::endl
                  << "[Compaction Test]" << std::endl;
        compaction_test(TEST_MAX);
#endif
#ifdef BFSIZE_T
        store.reset();

        std::cout << std::endl
                  << "[BFSize Test]" << std::endl;
        bfsize_test(TEST_MAX, BF_SIZE);
#endif
    }
};

int main(int argc, char *argv[])
{
    // bool verbose = (argc == 2 && std::string(argv[1]) == "-v");

    std::cout << "Usage: " << argv[0] << std::endl
              << std::endl;
    std::cout.flush();

    MyTest test("./data", "./data/vlog");

    test.start_test();

    return 0;
}
