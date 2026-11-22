cmake_minimum_required(VERSION 4.0)
project(bitcoin-ipc CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED YES)

cmake_minimum_required(VERSION 4.0)

find_package(Threads REQUIRED)
find_package(CapnProto REQUIRED)
find_package(nlohmann_json REQUIRED)

# Find all capmp schemas and generate
file(GLOB_RECURSE CAPNP_SCHEMAS CONFIGURE_DEPENDS src/schemas/capnp/*.capnp)
set(CAPNPC_SRC_PREFIX ${CMAKE_CURRENT_SOURCE_DIR}/src/schemas/capnp)
set(CAPNPC_IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/src/schemas/capnp)
capnp_generate_cpp(CAPNP_SRCS CAPNP_HDRS ${CAPNP_SCHEMAS})

add_library(schemas STATIC ${CAPNP_SRCS})
target_link_libraries(schemas PUBLIC Threads::Threads CapnProto::capnp-rpc)
target_include_directories(schemas PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

add_executable(client src/client.cpp)
target_link_libraries(client PRIVATE schemas)

add_executable(mempool-mirror src/mempool-mirror.cpp)
target_link_libraries(mempool-mirror PRIVATE schemas nlohmann_json::nlohmann_json)
