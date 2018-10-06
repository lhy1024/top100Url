#pragma once

#include <algorithm>
#include <cstdlib>
#include <unistd.h>
#include <fcntl.h>
#include <malloc.h>
#include <cerrno>
#include <cassert>
#include <cstring>
#include <sstream>
#include <string>
#include <cmath>
#include <vector>
#include <thread>
#include <sys/time.h>
#include <iostream>

#include "Counter.h"
#include "Queue.h"
#include "Signal.h"
#include "Util.h"
#include "Solve.h"
#include "Node.h"
#include "Constants.h"
#include <map>
#include <bits/unordered_set.h>
#include <climits>
namespace TopUrl {
    static const unsigned long long IOSIZE = 1024 * 1024 * 1024;//1G

    struct HeapNode {
        char *key;
        int num;

        HeapNode(const char *key, const int &num, const int &len) {
            this->key = new char[len];
            strcpy(this->key, key);
            this->num = num;
        }

        ~HeapNode() {
            delete[] key;
        }
    };

    class Solve {
        const int URLSIZE = 32;
        const int PAGESIZE = 4096;
        const int MASK = 65535;
        const int parallelism = std::thread::hardware_concurrency();
        int partitions = 1024;//todo
        unsigned long long totalBytes;;
        char **rawAns;
        unsigned long long fileSize;
        Counter urlNum;
        int topNum;
        int Threshold = URLSIZE * 16;
        const int grid_buffer_size = URLSIZE * 64;
        std::vector<std::string> fileNames;

        int getId(char *str, int length);

    public:


        Solve(const unsigned long long &fileSize, const int &topNum);

        ~Solve();

        void initUrlFile(const std::string &fileName, bool toReInit = true);

        void streamHash(const std::string &input, const std::string &temp);

        void streamHeap();

        void showRawAns();

        void match();
    };
}
