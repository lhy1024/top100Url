#pragma once

#include <cstring>
#include <iostream>
#include <cassert>

namespace TopUrl {

    class Node {
        int length = 0;
    public:
        char content[32];
        size_t size = 0;
        bool isSet = false;

        Node(const int &length) {
            size = 0;
            this->length = length;
        }

        void init(const char *content, const size_t &size) {
            if (!isSet) {
                assert(size <= length);
                this->size = size;
                memcpy(this->content, content, size);
                memset(this->content + size, 0, length - size);
                isSet = true;
            }
        }

        ~Node() {
            //delete[]content;//by vector
        }

        //Node &operator=(Node const &) = delete;

        //Node(Node const &) = delete;

        Node() = delete;
    };
}