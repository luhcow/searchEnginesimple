cmake_minimum_required(VERSION 3.5.0)
project(SearchEngine VERSION 0.1.0 LANGUAGES C CXX)

if(NOT DEFINED CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 17)
endif()

set(CMAKE_CXX_FLAGS -Wunused-variable)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${SearchEngine_SOURCE_DIR}/bin)



include_directories(${SearchEngine_SOURCE_DIR}/include 
	${SearchEngine_SOURCE_DIR}/include/helper 
	${SearchEngine_SOURCE_DIR}/submodules/cppjieba/include
	${SearchEngine_SOURCE_DIR}/submodules/cppjieba/deps/limonp/include
	${SearchEngine_SOURCE_DIR}/submodules/tinyxml2)

find_library(LIBRT rt)
find_package(OpenSSL REQUIRED)

find_package(workflow REQUIRED CONFIG HINTS ..)
include_directories(${OPENSSL_INCLUDE_DIR} ${WORKFLOW_INCLUDE_DIR})
link_directories(${WORKFLOW_LIB_DIR})


find_path(SNAPPY_INCLUDE_PATH NAMES snappy.h)
include_directories(${SNAPPY_INCLUDE_PATH})
set(WORKFLOW_LIB workflow pthread OpenSSL::SSL OpenSSL::Crypto ${LIBRT})

add_executable(WebServer src/Server/WebServer.cc)
target_link_libraries(WebServer wfrest ${LIBEVENT_LIBRARIES} pthread dl wfkafka z snappy lz4 zstd ${WORKFLOW_LIB}) 

add_executable(RedisCli src/test/redis_cli.cc)
target_link_libraries(RedisCli ${WORKFLOW_LIB})

ADD_EXECUTABLE(simhash src/test/simhash.cpp)

add_executable(jieba src/test/jieba.cpp)

add_executable(keyWord src/keyWord.cc)
target_link_libraries(keyWord fmt )

add_executable(keyServer src/Server/KeyServer.cc)
target_link_libraries(keyServer wfrest fmt ${WORKFLOW_LIB})

add_executable(xml src/test/xmlParseForRSS.cc submodules/tinyxml2/tinyxml2.cpp)

add_executable(page src/page.cc submodules/tinyxml2/tinyxml2.cpp)
target_link_libraries(page fmt)

add_executable(pageq src/test/pageq.cc)
target_link_libraries(pageq fmt)

add_executable(PageServer src/Server/PageServer.cc)
target_link_libraries(PageServer wfrest fmt)

add_executable(clsm src/clm.cc)