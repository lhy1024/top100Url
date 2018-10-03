#pragma once
#include<mutex>
class Counter {
    unsigned long long cnt = 0;
    std::mutex m_mutex;
public:
    void inc() {
        std::unique_lock<std::mutex> lck(m_mutex);
        cnt++;
    }

    unsigned long long count() {
        std::unique_lock<std::mutex> lck(m_mutex);
        return cnt;
    }

};