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
	${SearchEngine_SOURCE_DIR}/submodules/tinyxml2
	${OPENSSL_INCLUDE_DIR}
	${Protobuf_INCLUDE_DIR}
    ${WORKFLOW_INCLUDE_DIR} 
	${SRPC_INCLUDE_DIR})

	link_directories(${OPENSSL_LINK_DIR} 
		${Protobuf_LIB_DIR}
    	${WORKFLOW_LIB_DIR} 
		${SRPC_LIB_DIR})

find_library(LIBRT rt)
find_package(OpenSSL REQUIRED)

find_package(workflow REQUIRED CONFIG HINTS ..)
include_directories(${OPENSSL_INCLUDE_DIR} ${WORKFLOW_INCLUDE_DIR})
link_directories(${WORKFLOW_LIB_DIR})


find_path(SNAPPY_INCLUDE_PATH NAMES snappy.h)
include_directories(${SNAPPY_INCLUDE_PATH})
set(WORKFLOW_LIB workflow pthread OpenSSL::SSL OpenSSL::Crypto ${LIBRT})



add_executable(RedisCli src/test/redis_cli.cc)
target_link_libraries(RedisCli ${WORKFLOW_LIB})

ADD_EXECUTABLE(simhash src/test/simhash.cpp)

add_executable(jieba src/test/jieba.cpp)

add_executable(keyWord src/keyWord.cc)
target_link_libraries(keyWord fmt leveldb)

add_executable(keyServer src/Server/KeyServer.cc)
target_link_libraries(keyServer wfrest fmt ${WORKFLOW_LIB} leveldb)

add_executable(xml src/test/xmlParseForRSS.cc submodules/tinyxml2/tinyxml2.cpp)

add_executable(page src/page.cc submodules/tinyxml2/tinyxml2.cpp)
target_link_libraries(page fmt)

add_executable(pageq src/test/pageq.cc)
target_link_libraries(pageq fmt xapian)

add_executable(PageServer src/Server/PageServer.cc)
target_link_libraries(PageServer wfrest fmt xapian)

add_executable(bptest src/test/bpteat.cc )
target_link_libraries(bptest leveldb)

add_executable(xiapian src/test/xiapian.cc )
target_link_libraries(xiapian xapian)

add_executable(xiapiansea src/test/xapiansea.cc )
target_link_libraries(xiapiansea xapian)

# for srpc
find_package(srpc REQUIRED CONFIG HINTS)

find_package(Protobuf)
if ("x${Protobuf_DIR}" STREQUAL "xProtobuf_DIR-NOTFOUND")
    if (APPLE)
        message (FATAL_ERROR ${Protobuf_ERROR_MSG_MACOS})
    else ()
        message (FATAL_ERROR ${Protobuf_ERROR_MSG_LINUX})
    endif ()
endif ()

set(LZ4_LIB lz4)

set(SNAPPY_LIB snappy)

find_package(ZLIB REQUIRED)

# Generate idl code: xx.srpc.h xx.pb.h xx.pb.cc xx.thrift.h
set(IDL_FILE /home/rings/searchEngine/src/stronly/stronly.proto)
set(SRPC_GEN_PROGRAM ${SRPC_BIN_DIR}/srpc_generator) 
find_program(PROTOC "protoc")
if(${PROTOC} STREQUAL "PROTOC-NOTFOUND")
    message(FATAL_ERROR "Protobuf compiler is missing!")
endif ()
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${IDL_FILE})

add_custom_target(SRPC_GEN ALL
    COMMAND ${SRPC_GEN_PROGRAM} ${PROJECT_SOURCE_DIR}/${IDL_FILE} ${PROJECT_SOURCE_DIR} -s
    COMMENT "srpc generator..."
)

# Prefer to static link first
set(CMAKE_FIND_LIBRARY_SUFFIXES ".a" ${CMAKE_FIND_LIBRARY_SUFFIXES})
find_library(Workflow_LIB workflow HINTS)
find_library(Srpc_LIB srpc HINTS)

# Set all the libraries here
set(LIB ${Srpc_LIB} ${Workflow_LIB} pthread OpenSSL::SSL OpenSSL::Crypto
    protobuf z ${SNAPPY_LIB} ${LZ4_LIB})

# Add all the common code here
set(COMMON_CODE ${SearchEngine_SOURCE_DIR}/src/stronly/config/config.cc ${SearchEngine_SOURCE_DIR}/src/stronly/config/Json.cc ${PROTO_SRCS})

# Build executable outputs
set(PROJECT_OUTPUT server client pageserver)
foreach(output ${PROJECT_OUTPUT})
    add_executable(${output} /home/rings/searchEngine/src/stronly/${output}_main.cc ${COMMON_CODE})
    target_link_libraries(${output} ${LIB})
endforeach()

add_executable(WebServer src/stronly/WebServer.cc ${COMMON_CODE})
target_link_libraries(WebServer wfrest ${LIBEVENT_LIBRARIES} pthread dl wfkafka z snappy lz4 zstd ${WORKFLOW_LIB}) 
target_link_libraries(WebServer ${LIB})

target_link_libraries(server leveldb)
target_link_libraries(pageserver xapian)
