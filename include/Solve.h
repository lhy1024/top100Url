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
        const int parallelism =  std::thread::hardware_concurrency();
        int partitions = 32;//todo
        unsigned long long totalBytes = 0;
        char **rawAns = nullptr;
        unsigned long long fileSize = 0;
        Counter urlNum;
        int topNum = 0;
        int Threshold = URLSIZE * 16;
        const int grid_buffer_size = URLSIZE * 64;

        int getId(char *str, int length);

    public:


        Solve(const unsigned long long &fileSize, const int &topNum);

        ~Solve();

        void initUrlFile(const std::string &fileName, bool toReInit = true);

        void streamHash(const std::string &input, const std::string &temp);

        void streamHeap(const std::string &temp, const std::string &output);

    private:
        std::tuple<int, unsigned long long> getTempFile(const std::string &temp, int i, char mode);

        void streamUrl(const char *buffer, unsigned long long &pos, int &urlLength, const unsigned long long &bytes);

        void showRawAns();

        void match();
    };
}
