#pragma once
namespace TopUrl {

    class Node {
    public:
        char *content;
        int size = 0;

        Node(const int &length) {//todo explicit
            size = 0;
            content = new char[length];
        }

        ~Node() {
            //delete[]content;//by vector
        }

        //Node &operator=(Node const &) = delete;

        //Node(Node const &) = delete;

        Node() = delete;
    };
}