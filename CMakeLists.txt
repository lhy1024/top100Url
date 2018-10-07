cmake_minimum_required(VERSION 3.5)

add_executable(topUrl
        src/main.cpp
        src/murmur3/murmur3.c src/Util.cpp src/Solve.cpp src/Constants.cpp)

target_include_directories(
        topUrl
        PUBLIC
        /usr/local/include/
        /usr/local/include/google/
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(
        topUrl
        /usr/lib/libpthread.so
        #/usr/lib/libglog.so
        #/usr/lib/libdl.so
        #/usr/local/lib/libfolly.a
        #/usr/local/lib/libdouble-conversion.a
        # /usr/lib/libgflag.so
        # /usr/lib/libtcmalloc.so
)

target_compile_options(
        topUrl
        PUBLIC
        -w -O3 -std=c++17
)
