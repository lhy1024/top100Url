#include "Constants.h"
#include "sys/time.h"
#include "cstdlib"
#include <sys/stat.h>
#include <sys/types.h>
#include "murmur3/murmur3.h"
#include "cassert"

namespace TopUrl {


    double get_time() {
        struct timeval tv{};
        gettimeofday(&tv, nullptr);
        return tv.tv_sec + (tv.tv_usec / 1e6);
    }

    bool file_exists(const std::string &filename) {
        struct stat st{};
        return stat(filename.c_str(), &st) == 0;
    }

    long file_size(const std::string &filename) {
        struct stat st{};
        assert(stat(filename.c_str(), &st) == 0);
        return st.st_size;
    }

    void create_directory(const std::string &path) {
        assert(mkdir(path.c_str(), 0764) == 0 || errno == EEXIST);
    }

    void remove_directory(const std::string &path) {
        char command[1024];
        sprintf(command, "rm -rf %s", path.c_str());
        system(command);
    }

    bool isDelimiter(const char &c) {
        return !c || c == '\n' || c == ' ' || c == '\t';
    }


}
