cmake_minimum_required(VERSION 3.5.0)
project(SearchEngine VERSION 0.1.0 LANGUAGES C CXX)

# Add these lines to set up include directories
include_directories(${SearchEngine_SOURCE_DIR}/include 
	${SearchEngine_SOURCE_DIR}/submodules/cppjieba/include
	${SearchEngine_SOURCE_DIR}/submodules/cppjieba/deps/limonp/include)

if(NOT DEFINED CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 11)
endif()

add_executable(SearchEngine src/main.cpp)

ADD_EXECUTABLE(demo src/demo.cpp)


