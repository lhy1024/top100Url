#pragma once
#include "mutex"
#include <condition_variable>
#include <functional>

    class Signal {
        int count;
        std::mutex m_mutex;
        std::condition_variable condition;


        bool isZero() {
            return 0 == count;
        }

    public:
        Signal() {
            count = 0;
        }

        void notify() {
            std::unique_lock<std::mutex> lck(m_mutex);
            count--;
            if (count == 0) {
                lck.unlock();
                condition.notify_all();
            }
        }

        void wait() {
            std::unique_lock<std::mutex> lck(m_mutex);
            condition.wait(lck, std::bind(&Signal::isZero, this));
        }

        void inc() {
            std::unique_lock<std::mutex> lck(m_mutex);
            count++;
        }
    };
