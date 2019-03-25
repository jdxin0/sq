cmake_minimum_required(VERSION 3.13)
project(sq C)

set(CMAKE_C_STANDARD 11)

include_directories(include /usr/local/include/)

add_executable(sq main.c)
target_link_libraries(sq /usr/local/lib/libzmq.dylib /usr/local/lib/libmsgpackc.dylib /usr/local/lib/libleveldb.dylib)