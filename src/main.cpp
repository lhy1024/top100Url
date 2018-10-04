#pragma once

#include "Solve.h"
#include "folly/AtomicHashMap.h"//todo

int main() {

    TopUrl::Solve solve(TopUrl::IOSIZE * 20, 100);

    auto start = TopUrl::get_time();
    solve.initUrlFile("text");
    std::cout <<"Cost: "<<TopUrl::get_time() - start << std::endl<< std::endl;

    start = TopUrl::get_time();
    solve.streamHash("text", "temp");
    std::cout << "Cost: "<<TopUrl::get_time() - start << std::endl<< std::endl;


    start = TopUrl::get_time();
    solve.streamHeap();
    std::cout <<"Cost: "<<TopUrl::get_time() - start << std::endl<< std::endl;

    return 0;
}