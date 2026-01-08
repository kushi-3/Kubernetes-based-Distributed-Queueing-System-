cmake_minimum_required(VERSION 3.25)
project(simplequeue LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(BUILD_SHARED_LIBS OFF)

set(ENABLE_PUSH OFF CACHE BOOL "Disable prometheus push library" FORCE)
set(ENABLE_COMPRESSION OFF CACHE BOOL "Disable prometheus compression" FORCE)

add_subdirectory(third_party/prometheus-cpp)

find_package(Threads REQUIRED)
find_package(ZLIB REQUIRED)
find_package(absl REQUIRED)
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_package(spdlog REQUIRED)

set(PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
set(PROTO_FILE ${PROTO_DIR}/queue.proto)

set(GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GEN_DIR})

set(GEN_CPP
    ${GEN_DIR}/queue.pb.cc
    ${GEN_DIR}/queue.grpc.pb.cc
)
set(GEN_HDR
    ${GEN_DIR}/queue.pb.h
    ${GEN_DIR}/queue.grpc.pb.h
)

get_target_property(GRPC_CPP_PLUGIN_EXECUTABLE gRPC::grpc_cpp_plugin LOCATION)

add_custom_command(
    OUTPUT ${GEN_CPP} ${GEN_HDR}
    COMMAND protobuf::protoc
    ARGS
        --grpc_out=${GEN_DIR}
        --cpp_out=${GEN_DIR}
        -I ${PROTO_DIR}
        --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
        ${PROTO_FILE}
    DEPENDS ${PROTO_FILE}
    COMMENT "Generating gRPC / Protobuf code from ${PROTO_FILE}"
)

add_library(protolib ${GEN_CPP})
target_include_directories(protolib PUBLIC ${GEN_DIR})
target_link_libraries(protolib
    PUBLIC
        gRPC::grpc++
        protobuf::libprotobuf
        Threads::Threads
)

add_executable(queue src/queue.cc)
add_executable(distributor src/distributor.cc)
add_executable(stateless-server src/stateless-server.cc)
add_executable(stateful-server src/stateful-server.cc)
target_link_libraries(stateless-server PRIVATE protolib spdlog::spdlog)
target_link_libraries(stateful-server PRIVATE protolib spdlog::spdlog)
target_link_libraries(queue
  PRIVATE
    protolib
    spdlog::spdlog
    prometheus-cpp::core
    prometheus-cpp::pull
    ZLIB::ZLIB
    Threads::Threads
)
target_link_libraries(distributor
  PRIVATE
    protolib
    spdlog::spdlog
    prometheus-cpp::core
    prometheus-cpp::pull
    ZLIB::ZLIB
    Threads::Threads
)

