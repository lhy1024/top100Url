#pragma once

#include "Constants.h"
#include "sys/time.h"
#include "cstdlib"
#include <sys/stat.h>
#include <sys/types.h>
#include "murmur3/murmur3.h"
#include "cassert"

namespace TopUrl {
    double get_time();

    bool file_exists(const std::string &filename);

    long file_size(const std::string &filename);

    void create_directory(const std::string &path);

    void remove_directory(const std::string &path);

    bool isDelimiter(const char &c);

    namespace log {
        void log(unsigned long long cur, unsigned long long total);

        void logMaxMemory();
    }
}
