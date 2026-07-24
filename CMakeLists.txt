cmake_minimum_required(VERSION 3.10)   # minimum CMake version required
project(mini_load_balancer CXX)         # names the project, declares it's C++

set(CMAKE_CXX_STANDARD 17)              # equivalent to your -std=c++17 flag
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_compile_options(-Wall -Wextra)      # applies these warnings to ALL targets below

add_executable(lb src/main.cpp)               # target 1: the load balancer
add_executable(dummy_backend src/dummy_backend.cpp)  # target 2: the test backend
add_executable(slow_backend src/slow_backend.cpp)
