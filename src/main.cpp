#pragma once

#include "Solve.h"


int main() {
    auto start = TopUrl::get_time();
    TopUrl::Solve solve(TopUrl::Solve::IOSIZE * 2, 100);
    solve.initUrlFile("text");
    std::cout << TopUrl::get_time() - start << std::endl;
    start = TopUrl::get_time();
    solve.streamHash("text", "temp");
    std::cout << TopUrl::get_time() - start << std::endl;
    return 0;
}