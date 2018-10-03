cmake_minimum_required(VERSION 3.5)

add_executable(topUrl
        src/main.cpp
        src/murmur3/murmur3.c)

target_include_directories(
        topUrl
        PUBLIC
        /usr/local/include/
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(
        topUrl
        /usr/lib/libpthread.so
)

target_compile_options(
        topUrl
        PUBLIC
        -w -O3 -std=c++17
)
