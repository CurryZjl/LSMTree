#pragma once
#include <cstdint>
#include <stack>
#include <string>
#include <ctime>
#include <cstdlib>
#include <iostream>
#include <vector>
#include <map>
#include "vLogEntry.h"
#include "ssTable.h"
#include "MurmurHash3.h"

class MemTable
{
private:
    struct Node
    {
        uint64_t key;
        std::string val;
        Node *right, *down;
        Node(uint64_t _key, std::string &_val) : right(nullptr), down(nullptr), key(_key), val(_val) {}
        Node() : right(nullptr), down(nullptr), key(0), val("") {}
        Node(Node *r, Node *d, uint64_t _key, std::string _val) : right(r), down(d), key(_key), val(_val) {}
    };

    Node *head;
    void clear(Node *n)
    {
        while (n)
        {
            Node *next = n->down;
            while (n)
            {
                Node *tmp = n->right;
                delete n;
                n = tmp;
            }
            n = next;
        }
    }

public:
    MemTable()
    {
        head = new Node();
    }
    ~MemTable()
    {
        clear(head);
    }

    void clear()
    {
        clear(head);
        head = new Node();
    }

    bool empty()
    {
        Node *tmp = head;
        while (tmp->down)
        {
            tmp = tmp->down; // go down
        }
        return !tmp->right;
    }

    size_t size()
    {
        size_t n = 0;
        Node *tmp = head;
        while (tmp->down)
        {
            tmp = tmp->down; // go down
        }

        while (tmp->right)
        {
            tmp = tmp->right;
            ++n;
        }
        return n;
    }

    std::string get(uint64_t &_key) const
    {
        Node *p = head;
        while (true)
        {
            while (p->right && p->right->key < _key)
            {
                p = p->right;
            }
            if (p->right && p->right->key == _key)
                return p->right->val;
            if (!p->down)
                return "";
            p = p->down;
        }
    }

    bool put(uint64_t &key, const std::string &val)
    {
        Node *p = head;
        std::vector<Node *> pathList; // 只记录从上到下的搜索路径
        bool exist = false;
        while (p)
        {
            // 如果右边的节点存在并且该节点的键值小于要插入的键值，向右移动
            while (p->right && p->right->key < key)
                p = p->right;
            if (p->right && p->right->key == key)
            {
                p->right->val = val; // 覆盖val
                exist = true;
            }
            // 找到对应的从上到下的路径
            pathList.push_back(p);
            // 该节点的键值小于key，但右节点的键的值大于key
            p = p->down;
        }
        if (exist)
            return false; // 已经覆盖完毕，return
        // 不存在这样的一个键值
        Node *downNode = nullptr;
        bool Up = true; // 代表是否需要往上插入
        // 从下至上搜索路径回溯，50%概率
        while (Up && pathList.size() > 0)
        {
            // 取出末尾的节点，当前节点的键值小于key，但是该节点右节点的键值又大于key
            Node *newNode = pathList.back();
            pathList.pop_back();
            newNode->right = new Node(newNode->right, downNode, key, val);
            downNode = newNode->right;
            Up = (rand() & 1);
        }
        // 可能会有超出原来跳表高度的情况，这个时候需要插入新的头结点
        if (Up)
        { // 插入新的头结点，加层
            Node *oldHead = head;
            head = new Node();
            head->right = new Node(nullptr, downNode, key, val);
            head->down = oldHead;
        }
        return true;
    }

    bool search(Node *&list, Node *&p, const uint64_t &k)
    {

        while (true)
        {
            while (p->right && p->right->key < k)
            {
                p = p->right;
            }
            if (p->right && p->right->key == k)
            {
                p = p->right;
                return true;
            }
            list = list->down;
            if (!list)
                return false;
            p = list;
        }
    }

    bool remove(const uint64_t &key)
    {
        Node *p = head;
        while (true)
        {
            while (p->right && p->right->key < key)
                p = p->right;
            if (p->right && p->right->key == key)
                break;
            if (!p->down)
                return false;
            p = p->down;
        }

        Node *tmp = p->right;
        while (true)
        {
            p->right = tmp->right;
            Node *next = tmp->down;
            delete tmp;

            if (!next)
                break;
            p = p->down;
            while (p->right != next)
                p = p->right;
            tmp = next;
        }

        while (head->down && !head->right)
        {
            tmp = head;
            head = tmp->down;
            delete tmp;
        }

        return true;
    }
    void show()
    {
        Node *p = head;
        while (p)
        {
            Node *node = p->right;
            std::cout << "head";
            while (node)
            {
                std::cout << "-->" << node->val;
                node = node->right;
            }
            std::cout << "-->NULL" << std::endl;
            p = p->down;
            // std::cout << p->down->val;
        }
    };

    void getAllNodes(std::vector<vLogEntry> &entrys) const
    {
        entrys.clear();

        size_t n = 0;
        Node *tmp = head;
        while (tmp->down)
        {
            tmp = tmp->down;
        }

        Node *tmp2 = tmp;
        while (tmp2->right)
        {
            tmp2 = tmp2->right;
            ++n;
        }
        entrys.reserve(n);

        while (tmp->right)
        {
            entrys.emplace_back(tmp->right->key, tmp->right->val);
            tmp = tmp->right;
        }
        return;
    }

    uint64_t maxKey()
    {
        Node *tmp = head;
        while (tmp->down)
        {
            tmp = tmp->down;
        }

        while (tmp->right)
            tmp = tmp->right;
        uint64_t maxKey = tmp->key > 0 ? tmp->key : 0;
        return maxKey;
    }

    uint64_t minKey()
    {
        Node *tmp = head;
        while (tmp->down)
        {
            tmp = tmp->down;
        }

        if (tmp->right)
            tmp = tmp->right;
        uint64_t minKey = tmp->key > 0 ? tmp->key : 0;
        return minKey;
    }

    void getBF(std::vector<bool> &bf)
    {
        bf.resize(BFSIZE, 0);
        Node *tmp = head;
        while (tmp->down)
        {
            tmp = tmp->down;
        }
        while (tmp->right)
        {
            uint32_t hash[4] = {0};
            MurmurHash3_x64_128(&(tmp->right->key), sizeof(tmp->right->key), 1, hash);
            bf[hash[0] % BFSIZE] = 1;
            bf[hash[1] % BFSIZE] = 1;
            bf[hash[2] % BFSIZE] = 1;
            bf[hash[3] % BFSIZE] = 1;
            tmp = tmp->right;
        }
        return;
    }

    void scan(uint64_t key1, uint64_t key2, std::map<uint64_t, std::string> &RMap)
    {
        Node *tmp = head;
        while (tmp->down)
        {
            tmp = tmp->down; //到达最底层
        }
        while (tmp->right)
        {
            if(tmp->right->key < key1)
                tmp = tmp->right;
            else
                break;
        }
        //现在tmp->right到达了key >= key1的第一个位置 或到结束了
        while(tmp->right)
        {
            if(tmp->right->key > key2){
                break;
            }
            //确认区间，而且要抛弃删除标记
            if(tmp->right->key <= key2 && tmp->right->val != "~DELETED~")
                RMap[tmp->right->key] =  tmp->right->val;
            tmp = tmp->right;
        }
        return;
    }
};
