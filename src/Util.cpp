#include "Constants.h"
#include "sys/time.h"
#include "cstdlib"
#include <sys/stat.h>
#include <sys/types.h>
#include "murmur3/murmur3.h"
#include "cassert"
#include "unistd.h"

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
        return !c || c == '\n' || c == ' ' || c == '\t' || c == '\r';
    }

    namespace log {
        int _LINE_LENGTH = 32;
        float cpu = 0;
        size_t mem = 0;
        int pid = getpid();
        int tid = -1;
        //http://blog.51cto.com/ziloeng/984723
        bool GetCpuMem(float &cpu, size_t &mem, int pid, int tid = -1) {
            bool ret = false;
            char cmdline[100];
            sprintf(cmdline, "ps -o %%cpu,rss,%%mem,pid,tid -mp %d", pid);
            FILE *file;
            file = popen(cmdline, "r");
            if (file == NULL) {
                printf("file == NULL\n");
                return false;
            }

            char line[_LINE_LENGTH];
            float l_cpuPrec = 0;
            int l_mem = 0;
            float l_memPrec = 0;
            int l_pid = 0;
            int l_tid = 0;
            if (fgets(line, _LINE_LENGTH, file) != NULL) {
                //  printf("1st line:%s",line);
                if (fgets(line, _LINE_LENGTH, file) != NULL) {
                    //      printf("2nd line:%s",line);
                    sscanf(line, "%f %d %f %d -", &l_cpuPrec, &l_mem, &l_memPrec, &l_pid);
                    cpu = l_cpuPrec;
                    mem = l_mem / 1024;
                    if (tid == -1)
                        ret = true;
                    else {
                        while (fgets(line, _LINE_LENGTH, file) != NULL) {
                            sscanf(line, "%f - - - %d", &l_cpuPrec, &l_tid);
                            //              printf("other line:%s",line);
                            //              cout<<l_cpuPrec<<'\t'<<l_tid<<endl;
                            if (l_tid == tid) {
                                printf("cpuVal is tid:%d\n", tid);
                                cpu = l_cpuPrec;
                                ret = true;
                                break;
                            }
                        }
                        if (l_tid != tid)
                            printf("TID not exist\n");
                    }
                } else
                    printf("PID not exist\n");
            } else
                printf("Command or Parameter wrong\n");
            pclose(file);
            return ret;
        }

        void log(unsigned long long cur, unsigned long long total){
            GetCpuMem(cpu, mem, pid, tid);
            printf("progress: %.2f%% CPU:%.1f\tMEM:%dMB\r", 100. * cur / total, cpu, mem);
            fflush(stdout);
        }

    }
}
