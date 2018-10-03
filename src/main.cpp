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
#include <sys/stat.h>
#include <sys/types.h>
#include <condition_variable>
#include <functional>
#include <queue>
#include "mutex"
#include "murmur3/murmur3.h"

template<typename T>
class Queue {
    const size_t capacity;
    std::queue<T> queue;
    std::mutex mutex;
    std::condition_variable cond_full;
    std::condition_variable cond_empty;
public:
    explicit Queue(const size_t capacity) : capacity(capacity) {}

    void push(const T &item) {
        std::unique_lock<std::mutex> lock(mutex);
        cond_full.wait(lock, [&] { return !is_full(); });
        queue.push(item);
        lock.unlock();
        cond_empty.notify_one();
    }

    T pop() {
        std::unique_lock<std::mutex> lock(mutex);
        cond_empty.wait(lock, [&] { return !is_empty(); });
        auto item = queue.front();
        queue.pop();
        lock.unlock();
        cond_full.notify_one();
        return item;
    }

    bool is_full() {
        return queue.size() == capacity;
    }

    bool is_empty() {
        return queue.empty();
    }
};

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

class Counter {
    unsigned long long cnt = 0;
    std::mutex m_mutex;
public:
    void inc() {
        std::unique_lock<std::mutex> lck(m_mutex);
        cnt++;
    }

    unsigned long long count() {
        std::unique_lock<std::mutex> lck(m_mutex);
        return cnt;
    }

};

namespace TopUrl {
    const int URLSIZE = 32;
    const int PAGESIZE = 4096;
    const unsigned long long IOSIZE = 1024 * 1024 * 1024;//1G
    const int parallelism = std::thread::hardware_concurrency();
    int partitions = 128;//todo
    const int mod[] = {29, 31, 37, 41};

    inline double get_time() {
        struct timeval tv{};
        gettimeofday(&tv, nullptr);
        return tv.tv_sec + (tv.tv_usec / 1e6);
    }

    inline bool file_exists(const std::string &filename) {
        struct stat st{};
        return stat(filename.c_str(), &st) == 0;
    }

    inline long file_size(const std::string &filename) {
        struct stat st{};
        assert(stat(filename.c_str(), &st) == 0);
        return st.st_size;
    }

    inline void create_directory(const std::string &path) {
        assert(mkdir(path.c_str(), 0764) == 0 || errno == EEXIST);
    }

    inline void remove_directory(const std::string &path) {
        char command[1024];
        sprintf(command, "rm -rf %s", path.c_str());
        system(command);
    }

    inline bool isDelimiter(const char &c) {
        return !c || c == '\n' || c == ' ' || c == '\t';
    }

    int getId(char *str, int length) {
        uint32_t hash[4];                /* Output for the hash */
        uint32_t seed = 42;              /* Seed value for hash */
        MurmurHash3_x64_128(str, length, seed, hash);
        return (hash[0] % mod[0] + hash[1] % mod[1] + hash[2] % mod[2] + hash[3] % mod[3]) % partitions;
    }

    class Node {
    public:
        char *content;
        int size = 0;

        Node(const int &length) {//todo explicit
            size = 0;
            content = new char[length];
        }

        ~Node() {
            //delete[]content;//by vector
        }

        //Node &operator=(Node const &) = delete;

        //Node(Node const &) = delete;

        Node() = delete;
    };

    class Solve {
        char **rawAns;
        unsigned long long fileSize;
        Counter urlNum;
        int topNum;
        int Threshold = URLSIZE;
        const int grid_buffer_size = URLSIZE * 64;
        std::vector<std::string> fileNames;
    public:
        Solve(const unsigned long long &fileSize, const int &topNum) : fileSize(fileSize), topNum(topNum) {
            rawAns = new char *[topNum];
            for (int i = 0; i < topNum; ++i) {
                rawAns[i] = new char[URLSIZE];
            }
        }

        ~Solve() {
            for (int i = 0; i < topNum; ++i) {
                delete[]rawAns[i];
            }
            delete[]rawAns;
        }

        void initUrlFile(const std::string &fileName, bool toReInit = true) {
            //create ans
            char post[] = "ans";
            for (int i = 0; i < topNum; ++i) {
                do {
                    sprintf(rawAns[i], "%lf%s\n\0", get_time(), post);
                } while (i && strcmp(rawAns[i - 1], rawAns[i]) == 0);
            }

            //clear temp dir
            if (toReInit && file_exists(fileName)) {
                remove_directory(fileName);
            }

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
                assert(fout[ti] != -1);
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
                                cur = rawAns[ptr];
                            }
                            //write ram
                            memcpy(fileBuffer + size, cur, URLSIZE);
                            memset(fileBuffer + size + strlen(cur), 0, URLSIZE - strlen(cur));
                            size += URLSIZE;
                            urlNum.inc();
                        }

                        assert(write(fout[ti], fileBuffer, size) != -1);
                        localSize += size;
                    }
                    close(fout[ti]);
                    delete[]url;
                    signal.notify();
                }, ti);
                threads[ti].detach();
                signal.inc();
            }
            signal.wait();
            //free(fileBuffer);
            std::cout << "Generate url: " << urlNum.count() << std::endl;
        }


        void streamHash(const std::string &input, const std::string &temp) {
            auto perThreadSize = IOSIZE / parallelism;
            auto readBufferSize = perThreadSize / 4;
            auto writeBufferSize = perThreadSize / 4;

            //init
            char **buffers = new char *[parallelism * 2];
            bool *occupied = new bool[parallelism * 2];
            for (int i = 0; i < parallelism * 2; i++) {
                buffers[i] = (char *) memalign(PAGESIZE, static_cast<size_t>(readBufferSize));//todo align
                occupied[i] = false;
            }
            Queue<std::tuple<int, unsigned long long, int>> tasks(static_cast<const size_t>(parallelism));


            int *fout = new int[partitions];
            auto *mutexes = new std::mutex[partitions];

            if (file_exists(temp)) {
                remove_directory(temp);
            }
            create_directory(temp);

            for (int i = 0; i < partitions; i++) {
                std::ostringstream oss;
                oss << temp.c_str() << "/" << i;
                fileNames.push_back(oss.str());
                fout[i] = open(fileNames[i].c_str(), O_WRONLY | O_APPEND | O_CREAT, 0644);
                assert(fout[i] != -1);
            }


            char *global_grid_buffer = (char *) memalign(PAGESIZE, grid_buffer_size * partitions);
            char **grid_buffer = new char *[partitions];
            int *grid_buffer_offset = new int[partitions];
            for (int i = 0; i < partitions; i++) {
                grid_buffer[i] = global_grid_buffer + i * grid_buffer_size;
                grid_buffer_offset[i] = 0;
            }


            int tag = 0;
            const auto count = 65536;//((fileSize / readBufferSize) << 1) + 16;
            std::vector<Node> head(count, URLSIZE);
            std::vector<Node> tail(count, URLSIZE);

            //init threads
            Counter hashUrlNum;
            std::vector<std::thread> threads;
            for (int ti = 0; ti < parallelism; ti++) {
                threads.emplace_back([&]() {
                    char *local_buffer = (char *) memalign(PAGESIZE, writeBufferSize);//todo
                    int *local_grid_offset = new int[partitions];//todo
                    int *local_grid_cursor = new int[partitions];

                    while (true) {
                        int cursor;
                        unsigned long long bytes, pos;
                        int tag;
                        std::tie(cursor, bytes, tag) = tasks.pop();
                        if (cursor == -1)
                            break;

                        memset(local_grid_offset, 0, sizeof(int) * partitions);
                        memset(local_grid_cursor, 0, sizeof(int) * partitions);
                        char *buffer = buffers[cursor];
                        int urlLength;
                        bool isTail;


                        auto streamUrl = [&]() {
                            isTail = false;
                            urlLength = 0;
                            //filter

                            while (isDelimiter(buffer[pos])) {
                                pos++;
                                if (pos == bytes) {
                                    isTail = true;
                                    return;
                                }
                            }

                            while (!isDelimiter(buffer[urlLength + pos])) {
                                ++urlLength;
                                if (urlLength + pos == bytes) {
                                    isTail = true;
                                    return;
                                }
                            }
                        };

                        // read buffer
                        for (pos = 0; pos < bytes; pos += urlLength) {
                            streamUrl();
                            if (urlLength) {
                                int id = getId(buffer + pos, urlLength);
                                if (pos != 0 && !isTail)//not head or tail
                                    local_grid_offset[id] += urlLength;

                                hashUrlNum.inc();
                                //std::cout << hashUrlNum.count() << " : " << buffer + pos << std::endl;
                            }
                        }
                        // get offset
                        local_grid_cursor[0] = 0;
                        for (int i = 1; i < partitions; i++) {
                            local_grid_cursor[i] = local_grid_offset[i - 1];
                            local_grid_offset[i] += local_grid_cursor[i];
                        }

                        // write ram by offset
                        bool isHead = true;
                        for (pos = 0; pos < bytes; pos += urlLength) {
                            streamUrl();
                            if (isHead) {
                                //std::cout << tag << "/" << count << std::endl;
                                memcpy(head[tag].content, buffer + pos, urlLength);
                                head[tag].size = urlLength;
                                isHead = false;
                            }


                            if (isTail) {
                                memcpy(tail[tag].content, buffer + pos, urlLength);
                                tail[tag].size = urlLength;
                            } else if (urlLength) {
                                int id = getId(buffer + pos, urlLength);
                                memcpy(local_buffer + local_grid_cursor[id], buffer + pos, urlLength);
                                local_grid_cursor[id] += urlLength;
                            }
                        }
                        //write file
                        int start = 0;
                        for (int i = 0; i < partitions; i++) {
                            std::unique_lock<std::mutex> lock(mutexes[i]);
                            int length = local_grid_offset[i] - start;
                            if (length > Threshold) {
                                write(fout[i], local_buffer + start, length);
                            } else if (length == 0) {
                                continue;
                            } else {
                                memcpy(grid_buffer[i] + grid_buffer_offset[i], local_buffer + start, length);
                                grid_buffer_offset[i] += length;
                                if (grid_buffer_offset[i] == grid_buffer_size) {
                                    write(fout[i], grid_buffer[i], grid_buffer_size);
                                    grid_buffer_offset[i] = 0;
                                }
                                //std::cout << "Use grid Buffer: " << i << std::endl;
                            }
                            start = local_grid_offset[i];
                        }
                        occupied[cursor] = false;
                    }
                    free(local_buffer);
                    free(local_grid_cursor);
                    free(local_grid_offset);
                });
            }

            std::cout << "Open thread num: " << threads.size() << std::endl;
            int fin = open(input.c_str(), O_RDONLY);
            if (fin == -1) printf("%s\n", strerror(errno));
            assert(fin != -1);
            int cursor = 0;
            long total_bytes = file_size(input);
            long read_bytes = 0;

            while (true) {
                long bytes = read(fin, buffers[cursor], readBufferSize);
                assert(bytes != -1);
                if (bytes == 0) break;
                occupied[cursor] = true;
                tasks.push(std::make_tuple(cursor, bytes, tag));
                read_bytes += bytes;
                printf("progress: %.2f%%\r", 100. * read_bytes / total_bytes);
                fflush(stdout);
                while (occupied[cursor]) {
                    cursor = (cursor + 1) % (parallelism * 2);
                }
                tag++;
            }

            close(fin);
            std::cout << "Load done" << std::endl;
            for (int ti = 0; ti < parallelism; ti++) {
                tasks.push(std::make_tuple(-1, 0, -1));
            }

            for (int ti = 0; ti < parallelism; ti++) {
                threads[ti].join();
            }
            std::cout << "Hash grid done" << std::endl;
            char htBuffer[URLSIZE];
            int tailLength, headLength;

            for (int i = 0; i < count - 1; ++i) {
                tailLength = tail[i].size;
                headLength = head[i + 1].size;
                if (tailLength && headLength) {
                    memcpy(htBuffer, tail[i].content, tailLength);
                    memcpy(htBuffer + tail[i].size, head[i + 1].content, headLength);
                    int id = getId(htBuffer, headLength + tailLength);
                    write(fout[id], htBuffer, headLength + tailLength);
                }
            }

            std::cout << "To hash url : " << hashUrlNum.count() << std::endl;
//            if(urlNum.count())
//                assert(hashUrlNum.count() == urlNum.count());

            std::cout << "done" << std::endl;
            //destory
            delete[]grid_buffer;
            free(grid_buffer_offset);

            delete[]mutexes;
            for (int i = 0; i < partitions; ++i) {
                close(fout[i]);
            }

            for (int i = 0; i < parallelism * 2; i++) {
                delete[]buffers[i];
            }
            delete[]buffers;
            delete[]occupied;
        }

        void streamHeap() {

        }

        void showRawAns() {
            for (int i = 0; i < topNum; ++i) {
                std::cout << rawAns[i];
            }
        }
    };
}

int main() {
    auto start = TopUrl::get_time();
    TopUrl::Solve solve(TopUrl::IOSIZE * 2, 100);
    solve.initUrlFile("text");
    std::cout << TopUrl::get_time() - start << std::endl;
    start = TopUrl::get_time();
    solve.streamHash("text", "temp");
    std::cout << TopUrl::get_time() - start << std::endl;
    return 0;
}