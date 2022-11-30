cmake_minimum_required(VERSION 3.23)
project(restart)

set(CMAKE_CXX_STANDARD 17)

add_executable(restart main.cpp FileProcessorBase.hpp MapperBase.hpp ShufflerBase.hpp ReducerBase.hpp)
target_link_libraries(restart ${CMAKE_DL_LIBS})