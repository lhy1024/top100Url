#pragma once

#include "Solve.h"

int main(int argc, char **argv) {
    int opt, num = -1, partitions = -1;
    int mode = 7;//default all
    std::string input{};
    std::string temp{};
    std::string output{};

    while ((opt = getopt(argc, argv, "i:o:t:n:m:p:")) != -1) {
        switch (opt) {
            case 'i':
                input = optarg;
                break;
            case 'o':
                output = optarg;
                break;
            case 'n':
                num = atoi(optarg);
                break;
            case 't':
                temp = optarg;
                break;
            case 'm':
                mode = atoi(optarg);
                break;
            case 'p':
                partitions = atoi(optarg);
                break;
        }
    }
    if (input.empty() || output.empty() || temp.empty()
        || num > 100 || num < 0
        || mode > 7 || mode < 0
        || partitions == -1) {
        std::cout
                << "usage: -i [input path] -o [output path] -n <int>[url file size/G](0~100)  -t [temp path] -a [ans path] -m <int>[mode](0-7) -p <int>[partitions]"
                << std::endl;
        std::cout << " -i text -o ans.txt -t temp -n 10 -m 4 -p 256" << std::endl;
        exit(-1);
    }

    TopUrl::Solve solve(TopUrl::IOSIZE * num, 100, partitions);

    if (mode & 1) {
        auto start = TopUrl::get_time();
        std::cout << "Start init url file" << std::endl;
        solve.initUrlFile(input);
        std::cout << "Cost: " << TopUrl::get_time() - start << std::endl << std::endl;
    }

    if (mode & 2) {
        auto start = TopUrl::get_time();
        std::cout << "Start stream to hash" << std::endl;
        solve.streamHash(input, temp);
        std::cout << "Cost: " << TopUrl::get_time() - start << std::endl << std::endl;
    }

    if (mode & 4) {
        auto start = TopUrl::get_time();
        std::cout << "Start stream to heap" << std::endl;
        solve.streamHeap(temp, output);
        std::cout << "Cost: " << TopUrl::get_time() - start << std::endl << std::endl;
    }

    TopUrl::log::logMaxMemory();
    return 0;
}