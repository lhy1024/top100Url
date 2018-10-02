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
#include <condition_variable>
#include <functional>
#include "mutex"

class Signal {
    int count;
    std::mutex m_mutex;
    std::condition_variable condition;


    bool isZero() {
        return 0 == count;
    }

public:
    Signal() {
        count = 0;
    }

    void notify() {
        std::unique_lock<std::mutex> lck(m_mutex);
        count--;
        if (count == 0) {
            lck.unlock();
            condition.notify_all();
        }
    }

    void wait() {
        std::unique_lock<std::mutex> lck(m_mutex);
        condition.wait(lck, std::bind(&Signal::isZero, this));
    }

    void inc() {
        std::unique_lock<std::mutex> lck(m_mutex);
        count++;
    }
};

namespace TopUrl {
    const int URLSIZE = 32;
    const int PAGESIZE = 4096;
    const unsigned long long IOSIZE = 1024 * 1024 * 1024;//1G
    const int parallelism = std::thread::hardware_concurrency();

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

        void initUrlFile(const std::string &fileName, const std::string &tempDir) {
            //create ans
            char post[] = "ans";
            for (int i = 0; i < topNum; ++i) {
                do {
                    sprintf(ans[i], "%lf%s\n\0", get_time(), post);
                } while (i && strcmp(ans[i - 1], ans[i]) == 0);

            }

            //clear temp dir
            if (file_exists(tempDir)) {
                remove_directory(tempDir);
            }
            create_directory(tempDir);

            //create buffer
            auto perFileSize = fileSize / parallelism;
            auto fileBufferSize = static_cast<unsigned long long>(
                    fileSize > IOSIZE * 0.8 / parallelism ? IOSIZE * 0.8 / parallelism : fileSize);
            auto count = fileSize / fileBufferSize;
            if (fileSize % fileBufferSize != 0)
                count++;
            char *fileBuffer = (char *) memalign(PAGESIZE, fileBufferSize);

            //pre write file
            char command[4096];
            sprintf(command, "dd if=/dev/zero of=%s bs=%lld count=%lld", fileName.c_str(), fileBufferSize, count);
            std::cout << command << std::endl;
            system(command);


            //create fout
            int fout[parallelism];
            for (int ti = 0; ti < parallelism; ti++) {
                fout[ti] = open(fileName.c_str(), O_WRONLY | O_APPEND | O_CREAT, 0644);
            }

            //write
            std::vector<std::thread> threads;
            Signal signal;
            for (int ti = 0; ti < parallelism; ti++) {
                threads.emplace_back([&](int ti) {
                    lseek(fout[ti], perFileSize * ti, SEEK_SET);//todo align
                    unsigned long long localSize = 0;
                    char *url = new char[URLSIZE];
                    char *cur = nullptr;
                    while (localSize < perFileSize) {
                        unsigned long long size = 0;
                        while (size + localSize < perFileSize && size < fileBufferSize) {
                            //get url
                            double time = get_time();
                            int ptr = (int((time - int(time)) * 1e6)) % (topNum * 5);
                            if (ptr >= topNum) {
                                sprintf(url, "%lfnormal\n\0", time);
                                cur = url;
                            } else {
                                cur = ans[ptr];
                            }
                            //write ram
                            memcpy(fileBuffer + size, cur, URLSIZE);
                            memset(fileBuffer + size + strlen(cur), 0, URLSIZE - strlen(cur));
                            size += URLSIZE;
                        }

                        assert(write(fout[ti], fileBuffer, size) != -1);
                        localSize += size;
                    }
                    close(fout[ti]);
                    signal.notify();
                }, ti);
                threads[ti].detach();
                signal.inc();
            }
            signal.wait();
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
    TopUrl::Solve solve(TopUrl::IOSIZE * 10, 100);
    solve.initUrlFile("text", "temp");
    std::cout << TopUrl::get_time() - start << std::endl;
    return 0;
}