#pragma once

#include "Solve.h"
#include "folly/AtomicHashMap.h"//todo

int main() {

    TopUrl::Solve solve(TopUrl::IOSIZE , 100);

    auto start = TopUrl::get_time();
    solve.initUrlFile("text");
    std::cout << TopUrl::get_time() - start << std::endl;

    start = TopUrl::get_time();
    solve.streamHash("text", "temp");
    std::cout << TopUrl::get_time() - start << std::endl;


    start = TopUrl::get_time();
    solve.streamHeap();
    std::cout << TopUrl::get_time() - start << std::endl;

    return 0;
}