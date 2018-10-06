#include "Solve.h"
#include "google/sparsehash/dense_hash_map"
#include "google/sparsehash/sparse_hash_map"


struct cmp {
    bool operator()(const char *s1, const char *s2) const {
        return (s1 == s2) || (s1 && s2 && strcmp(s1, s2) == 0);
    }
};

struct hash_func {
    uint32_t seed = 131;              /* Seed value for hash */
    size_t operator()(const char *str) const {
        uint32_t hash[4];
        MurmurHash3_x64_128(str, strlen(str), seed, hash);
        return (hash[0] + hash[1] + hash[2] + hash[3]) / 4;
    }
};


namespace TopUrl {

    Solve::Solve(const unsigned long long &fileSize, const int &topNum) : fileSize(fileSize), topNum(topNum) {
        assert(topNum < MASK && MASK < 1e6);
        assert(((MASK + 1) & MASK) == 0 && MASK != -1);

        rawAns = new char *[topNum];
        for (int i = 0; i < topNum; ++i) {
            rawAns[i] = new char[URLSIZE];
            memset(rawAns[i], 0, URLSIZE);
        }
    }

    Solve::~Solve() {
        for (int i = 0; i < topNum; ++i) {
            delete[]rawAns[i];
        }
        delete[]rawAns;
    }

    std::tuple<int, unsigned long long> Solve::getTempFile(const std::string &temp, int i, char mode) {
        //mode 0:read 1:write

        std::ostringstream oss;
        oss << temp.c_str() << "/" << i;
        const char *fileName = oss.str().c_str();

        int file = -1;
        if (mode == 'w') {
            file = open(fileName, O_WRONLY | O_APPEND | O_CREAT, 0644);
        } else if (mode == 'r') {
            file = open(fileName, O_RDONLY);
        }
        assert(file != -1);

        auto fileSize = file_size(fileName);
        return std::make_tuple(file, fileSize);
    }

    void
    Solve::streamUrl(const char *buffer, unsigned long long &pos, int &urlLength, const unsigned long long &bytes) {
        urlLength = 0;
        //filter
        while (TopUrl::isDelimiter(buffer[pos])) {
            pos++;
            if (pos == bytes) {
                return;
            }
        }

        while (urlLength + pos < bytes && !TopUrl::isDelimiter(buffer[urlLength + pos])) {
            ++urlLength;
        }
        if (urlLength + pos < bytes)
            urlLength += 1;// keep delimiter
    };

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

        char *fileBuffer = (char *) memalign(PAGESIZE, fileBufferSize);

        //pre write file
//        auto count = fileSize / fileBufferSize;
//        if (fileSize % fileBufferSize != 0)
//            count++;
//        char command[4096];
//        sprintf(command, "dd if=/dev/zero of=%s bs=%lld count=%lld", fileName.c_str(), fileBufferSize, count);
//        std::cout << command << std::endl;
//        system(command);


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
                        int ptr = (int((time - int(time)) * 1e6)) & MASK;
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
        free(fileBuffer);
        std::cout << "Generate url: " << urlNum.count() << std::endl;
    }


    void Solve::streamHash(const std::string &input, const std::string &temp) {
        auto perThreadSize = IOSIZE / parallelism;
        auto readBufferSize = perThreadSize * 5 / 16;
        auto writeBufferSize = perThreadSize * 5 / 16;

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

        unsigned long long foutSize;
        for (int i = 0; i < partitions; i++) {
            std::tie(fout[i], foutSize) = getTempFile(temp, i, 'w');
            assert(foutSize == 0);
        }

        char *global_grid_buffer = (char *) memalign(PAGESIZE, grid_buffer_size * partitions);
        char **grid_buffer = new char *[partitions];
        int *grid_buffer_offset = new int[partitions];
        for (int i = 0; i < partitions; i++) {
            grid_buffer[i] = global_grid_buffer + i * grid_buffer_size;
            grid_buffer_offset[i] = 0;
        }


        int tag = 0;
        const auto count = fileSize / readBufferSize + 16;
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

                    // read buffer
                    int urlLength;
                    for (pos = 0; pos < bytes; pos += urlLength) {
                        if (tag != 0 && pos == 0) {//isHead
                            streamUrl(buffer, pos, urlLength, bytes);
                        } else {
                            streamUrl(buffer, pos, urlLength, bytes);
                            if (pos + urlLength == bytes || urlLength == 0) {//isTail
                                continue;
                            } else {
                                int id = getId(buffer + pos, urlLength);
                                local_grid_offset[id] += URLSIZE;//align
                                hashUrlNum.inc();
                            }
                        }
                    }
                    // get offset
                    local_grid_cursor[0] = 0;
                    for (int i = 1; i < partitions; i++) {
                        local_grid_cursor[i] = local_grid_offset[i - 1];
                        local_grid_offset[i] += local_grid_cursor[i];
                    }

                    // write ram by offset
                    for (pos = 0; pos < bytes; pos += urlLength) {
                        if (tag != 0 && pos == 0) {//isHead
                            streamUrl(buffer, pos, urlLength, bytes);
                            head[tag].init(buffer + pos, urlLength);
                        } else {
                            streamUrl(buffer, pos, urlLength, bytes);
                            if (pos + urlLength == bytes || urlLength == 0) {//isTail
                                tail[tag].init(buffer + pos, urlLength);
                            } else {
                                int id = getId(buffer + pos, urlLength);
                                memcpy(local_buffer + local_grid_cursor[id], buffer + pos, urlLength);
                                memset(local_buffer + local_grid_cursor[id] + urlLength, 0, URLSIZE - urlLength);//align
                                local_grid_cursor[id] += URLSIZE;
                            }
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

        //start write fin
        int fin = open(input.c_str(), O_RDONLY);
        if (fin == -1) printf("%s\n", strerror(errno));
        assert(fin != -1);
        int cursor = 0;
        totalBytes = file_size(input);

        unsigned long long readBytes = 0;
        while (true) {
            long bytes = read(fin, buffers[cursor], readBufferSize);
            assert(bytes != -1);
            if (bytes == 0) break;
            occupied[cursor] = true;
            tasks.push(std::make_tuple(cursor, bytes, tag));
            readBytes += bytes;
            log::log(readBytes, totalBytes);
            while (occupied[cursor]) {
                cursor = (cursor + 1) % (parallelism * 2);
            }
            tag++;
        }
        close(fin);
        std::cout << "Load done" << std::endl;

        //wait threads
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
        std::cout << "Write grid done" << std::endl;

        //write head and tail
        char htBuffer[URLSIZE];
        int tailLength, headLength, length;

        auto writeHeadOrTail = [&](const Node &node) {
            memcpy(htBuffer, node.content, node.size);
            memset(htBuffer + node.size, 0, URLSIZE - node.size);
            int id = getId(htBuffer, node.size);
            write(fout[id], htBuffer, URLSIZE);
            hashUrlNum.inc();
        };

        for (int i = 0; i < count - 1; ++i) {
            tailLength = tail[i].size;
            headLength = head[i + 1].size;
            length = headLength + tailLength;
            if (length > URLSIZE || (tailLength && isDelimiter(tail[i].content[tailLength - 1]))) {
                writeHeadOrTail(tail[i]);
                writeHeadOrTail(head[i + 1]);
            }else {
                if (tailLength)
                    memcpy(htBuffer, tail[i].content, tailLength);
                if (headLength)
                    memcpy(htBuffer + tailLength, head[i + 1].content, headLength);
                if (length) {
                    memset(htBuffer + length, 0, URLSIZE - length);
                    int id = getId(htBuffer, length);
                    write(fout[id], htBuffer, URLSIZE);
                    hashUrlNum.inc();
                }
            }
        }

        std::cout << "To hash url : " << hashUrlNum.count() << std::endl;
        if(urlNum.count())
            assert(hashUrlNum.count() == urlNum.count());

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

    void Solve::streamHeap(const std::string &temp, const std::string &output) {
        auto bufferLength = IOSIZE / 5 * 2 / URLSIZE * URLSIZE;
        char *buffer = (char *) memalign(PAGESIZE, bufferLength);
        auto cmpHeap = [](const std::shared_ptr<HeapNode> a, const std::shared_ptr<HeapNode> b) {
            return a->num > b->num;
        };
        std::priority_queue<std::shared_ptr<HeapNode>, std::vector<std::shared_ptr<HeapNode>>, decltype(cmpHeap)> heap(
                cmpHeap);

        google::dense_hash_map<const char *, int, hash_func, cmp> map;
        map.set_empty_key(nullptr);

        int fin;
        unsigned long long readBytes = 0;
        unsigned long long finSize = 0;
        if (totalBytes == 0)
            totalBytes = fileSize;
        for (int i = 0; i < partitions; i++) {
            std::tie(fin, finSize) = getTempFile(temp, i, 'r');
            long bytes = read(fin, buffer, finSize);
            assert(bytes != -1);//no need bytes != 0
            if (!bytes)
                continue;
            unsigned long long num = finSize / URLSIZE;//has aligned
            for (unsigned long long i = 0; i < num; ++i) {
                const char *key = buffer + i * URLSIZE;
                map[key]++;
            }

            readBytes += finSize;
            log::log(readBytes, totalBytes);

            for (const auto &item : map) {
                if (heap.size() < topNum) {
                    heap.push(std::make_shared<HeapNode>(item.first, item.second, URLSIZE));
                } else {
                    if (item.second > heap.top()->num) {
                        heap.push(std::make_shared<HeapNode>(item.first, item.second, URLSIZE));
                    }
                    while (heap.size() > topNum) {
                        heap.pop();
                    }
                }
            }
            map.clear();
        }

        //save ans
        int fans = open(output.c_str(), O_WRONLY | O_CREAT, 0644);
        std::unordered_set<std::string> set;
        for (auto node = heap.top(); !heap.empty(); heap.pop()) {
            write(fans, node->key, strlen(node->key));
            set.emplace(node->key);
        }
        close(fans);
        std::cout << "Get ans in " << output << std::endl;

        //match ans
        if (rawAns[0][0]) {
            for (int i = 0; i < topNum; ++i) {
                std::string string(rawAns[i]);
                set.erase(string);
            }
            if (set.size() == 0) {
                std::cout << "Accept" << std::endl;
            } else {
                std::cout << "Wrong Answer" << std::endl;
            }
        } else {
            for (const auto &str:set) {
                if (str.find("ans") == str.npos) {
                    std::cout << "Wrong Answer" << std::endl;
                    return;
                }
            }
            std::cout << "Accept" << std::endl;
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
        return (hash[0] + hash[1] + hash[2] + hash[3]) % partitions;
    }
}
