cmake_minimum_required(VERSION 3.5.0)
project(SearchEngine VERSION 0.1.0 LANGUAGES C CXX)

if(NOT DEFINED CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 11)
endif()

set(CMAKE_CXX_FLAGS -Wunused-variable)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${SearchEngine_SOURCE_DIR}/bin)

# Add these lines to set up include directories
include_directories(${SearchEngine_SOURCE_DIR}/include 
	${SearchEngine_SOURCE_DIR}/submodules/cppjieba/include
	${SearchEngine_SOURCE_DIR}/submodules/cppjieba/deps/limonp/include)

find_library(LIBRT rt)
find_package(OpenSSL REQUIRED)

find_package(workflow REQUIRED CONFIG HINTS ..)
include_directories(${OPENSSL_INCLUDE_DIR} ${WORKFLOW_INCLUDE_DIR})
link_directories(${WORKFLOW_LIB_DIR})


find_path(SNAPPY_INCLUDE_PATH NAMES snappy.h)
include_directories(${SNAPPY_INCLUDE_PATH})
set(WORKFLOW_LIB workflow pthread OpenSSL::SSL OpenSSL::Crypto ${LIBRT})

add_executable(SearchEngine src/main.cpp)

add_executable(WebServer src/WebServer.cc)
target_link_libraries(WebServer wfrest ${LIBEVENT_LIBRARIES} pthread dl wfkafka z snappy lz4 zstd ${WORKFLOW_LIB}) 

add_executable(RedisCli src/redis_cli.cc)
target_link_libraries(RedisCli ${WORKFLOW_LIB})

ADD_EXECUTABLE(simhash src/simhash.cpp)

add_executable(jieba src/jieba.cpp)
