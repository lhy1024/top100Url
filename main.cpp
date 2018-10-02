#pragma once

#include <algorithm>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <malloc.h>
#include <errno.h>
#include <assert.h>
#include <string.h>
#include <sstream>
#include <string>
#include <cmath>
#include <vector>
#include <thread>
#include <sys/time.h>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>

namespace TopUrl {
    const int URLSIZE = 32;
    const int PAGESIZE = 4096;
    const unsigned long long IOSIZE = 1024 * 1024 * 1024;//1G
    const int parallelism = 1;// std::thread::hardware_concurrency();

    inline double get_time() {
        struct timeval tv{};
        gettimeofday(&tv, nullptr);
        return tv.tv_sec + (tv.tv_usec / 1e6);
    }

    inline bool file_exists(std::string filename) {
        struct stat st;
        return stat(filename.c_str(), &st) == 0;
    }

    inline long file_size(std::string filename) {
        struct stat st;
        assert(stat(filename.c_str(), &st) == 0);
        return st.st_size;
    }

    inline void create_directory(std::string path) {
        assert(mkdir(path.c_str(), 0764) == 0 || errno == EEXIST);
    }

    inline void remove_directory(std::string path) {
        char command[1024];
        sprintf(command, "rm -rf %s", path.c_str());
        system(command);
    }

    class Solve {
        char **ans;
        unsigned long long fileSize;
        int topNum;
    public:
        Solve(const unsigned long long &fileSize, const int &topNum) : fileSize(fileSize), topNum(topNum) {
            ans = new char *[topNum];
            for (int i = 0; i < topNum; ++i) {
                ans[i] = new char[URLSIZE];
            }
        }

        ~Solve() {
            for (int i = 0; i < topNum; ++i) {
                delete[](ans[i]);
            }
            delete[](ans);
        }

        void initUrlFile(const std::string &fileName) {
            //create ans
            for (int i = 0; i < topNum; ++i) {
                do {
                    sprintf(ans[i], "%lf%s\n\0", get_time(), "ans");
                } while (i && strcmp(ans[i - 1], ans[i]) == 0);
            }

            //create fileBuffer
            unsigned long long fileBufferSize = IOSIZE * 0.8;
            char *fileBuffer = (char *) memalign(PAGESIZE, fileBufferSize);

            unsigned long long localSize = 0;
            char *url = new char[URLSIZE];
            char *cur = nullptr;
            int fout = open(fileName.c_str(), O_WRONLY | O_APPEND | O_CREAT, 0644);
            while (localSize < fileSize) {
                unsigned long long size = 0;
                while (size + localSize < fileSize && size < fileBufferSize) {
                    //get current url
                    double time = get_time();
                    int ptr = (int((time - int(time)) * 1e6)) % (topNum * 5);
                    if (ptr >= topNum) {
                        sprintf(url, "%lf%s\n\0", time, "normal");
                        cur = url;
                    } else {
                        cur = ans[ptr];
                    }
                    //write buffer
                    memcpy(fileBuffer + size, cur, URLSIZE);
                    memset(fileBuffer + size + strlen(cur), 0, URLSIZE - strlen(cur));
                    size += URLSIZE;
                }
                //write file
                assert(write(fout, fileBuffer, size) != -1);
                localSize += size;
            }
            close(fout);
        }

        void streamHash(){

        }

        void showRawAns() {
            for (int i = 0; i < topNum; ++i) {
                std::cout << ans[i];
            }
        }
    };
}

int main() {
    auto start = TopUrl::get_time();
    TopUrl::Solve solve(TopUrl::IOSIZE * 1, 100);
    solve.initUrlFile("text");
    std::cout << TopUrl::get_time() - start << std::endl;
    return 0;
}