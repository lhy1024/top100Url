#pragma once

#include "Constants.h"
#include "thread"

namespace TopUrl {
    const int URLSIZE = 32;
    const int PAGESIZE = 4096;
    const int MASK = 65535;
    int Threshold = URLSIZE * 16;
    const int grid_buffer_size = URLSIZE * 64;
    const int parallelism = std::thread::hardware_concurrency();
}
