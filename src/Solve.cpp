#include "Solve.h"

namespace TopUrl {


    Solve::Solve(const unsigned long long &fileSize, const int &topNum) : fileSize(fileSize), topNum(topNum) {
        rawAns = new char *[topNum];
        for (int i = 0; i < topNum; ++i) {
            rawAns[i] = new char[URLSIZE];
        }
    }

    Solve::~Solve() {
        for (int i = 0; i < topNum; ++i) {
            delete[]rawAns[i];
        }
        delete[]rawAns;
    }

    void Solve::initUrlFile(const std::string &fileName, bool toReInit) {
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
                        int ptr = (int((time - int(time)) * 1e6)) % (topNum);
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


    void Solve::streamHash(const std::string &input, const std::string &temp) {
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
                                local_grid_offset[id] += URLSIZE;//align

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
                        if (isHead && buffer) {//no zero
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
                            memcpy(local_buffer + local_grid_cursor[id], buffer + pos, URLSIZE);
                            memset(local_buffer + local_grid_cursor[id] + urlLength, 0, URLSIZE - urlLength);//align
                            local_grid_cursor[id] += URLSIZE;
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
        //std::cout << "Load done" << std::endl;
        for (int ti = 0; ti < parallelism; ti++) {
            tasks.push(std::make_tuple(-1, 0, -1));
        }

        for (int ti = 0; ti < parallelism; ti++) {
            threads[ti].join();
        }
        std::cout << "Hash grid done" << std::endl;
        //write grid
        for (int i = 0; i < partitions; i++) {
            if (grid_buffer_offset[i] > 0) {
                write(fout[i], grid_buffer[i], grid_buffer_offset[i]);
            }
        }
        //write head and tail
        char htBuffer[URLSIZE];
        int tailLength, headLength;
        for (int i = 0; i < count - 1; ++i) {

            tailLength = tail[i].size;
            headLength = head[i + 1].size;

            if (tailLength || headLength) {
                if (tailLength + headLength > URLSIZE) {
                    memcpy(htBuffer, tail[i].content, tailLength);
                    memset(htBuffer + tailLength, 0, URLSIZE - tailLength);
                    int id = getId(htBuffer, tailLength);
                    write(fout[id], htBuffer, URLSIZE);

                    memcpy(htBuffer, head[i + 1].content, headLength);
                    memset(htBuffer + headLength, 0, URLSIZE - headLength);
                    id = getId(htBuffer, headLength);
                    write(fout[id], htBuffer, URLSIZE);
                } else {
                    memcpy(htBuffer, tail[i].content, tailLength);
                    memcpy(htBuffer + tail[i].size, head[i + 1].content, headLength);
                    memset(htBuffer + headLength + tailLength, 0, URLSIZE - headLength - tailLength);
                    int id = getId(htBuffer, headLength + tailLength);
                    write(fout[id], htBuffer, URLSIZE);
                }
            }
        }

        //std::cout << "To hash url : " << hashUrlNum.count() << std::endl;
//            if(urlNum.count())
//                assert(hashUrlNum.count() == urlNum.count());

        //std::cout << "done" << std::endl;
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

    void Solve::streamHeap() {
        auto bufferLength = IOSIZE / 5 * 2 / URLSIZE * URLSIZE;
        char *buffer = new char[bufferLength];
        auto cmpMap = [](char const *a, char const *b) {
            return std::strcmp(a, b) < 0;
        };
        auto cmpHeap = [](const HeapNode &a, const HeapNode &b) {
            return a.num > b.num;
        };
        std::priority_queue<HeapNode, std::vector<HeapNode>, decltype(cmpHeap)> heap(cmpHeap);
        std::map<const char *, int, decltype(cmpMap)> map(cmpMap);//stack safe?

        for (const auto &fileName:fileNames) {


            int fin = open(fileName.c_str(), O_RDONLY);
            if (fin == -1) printf("%s\n", strerror(errno));
            assert(fin != -1);

            while (true) {
                auto fileSize = file_size(fileName.c_str());
                assert(fileSize < bufferLength);
                long bytes = read(fin, buffer, fileSize);
                assert(bytes != -1);
                if (bytes == 0) break;
                unsigned long long readBytes = 0;
                while (readBytes < fileSize) {
                    const char *key = buffer + readBytes;
                    map[key]++;
                    readBytes += URLSIZE;
                }
            }
            for (const auto &item : map) {
                heap.push(HeapNode(item.first, item.second, URLSIZE));
                while (heap.size() > topNum) {
                    heap.pop();
                }
            }
            map.clear();
        }
        int fans = open("ans.txt", O_WRONLY | O_CREAT, 0644);
        std::unordered_set<std::string> set;
        while (!heap.empty()) {
            auto node = heap.top();
            write(fans, node.key, strlen(node.key));
            set.emplace(node.key);
            write(fans, "\n", 1);
            heap.pop();
        }
        close(fans);
        std::cout << "Get ans in ans.txt" << std::endl;
        for (int i = 0; i < topNum; ++i) {
            std::string string(rawAns[i]);
            string.pop_back();
            set.erase(string);
        }
        if (set.size() == 0) {
            std::cout << "Accept" << std::endl;
        } else {
            std::cout << "Wrong Answer" << std::endl;
        }
    }

    void Solve::showRawAns() {
        for (int i = 0; i < topNum; ++i) {
            std::cout << rawAns[i];
        }
    }


    int Solve::getId(char *str, int length) {
        uint32_t hash[4];                /* Output for the hash */
        uint32_t seed = 42;              /* Seed value for hash */
        MurmurHash3_x64_128(str, length, seed, hash);
        return (hash[0] % mod[0] + hash[1] % mod[1] + hash[2] % mod[2] + hash[3] % mod[3]) % partitions;
    }
}
