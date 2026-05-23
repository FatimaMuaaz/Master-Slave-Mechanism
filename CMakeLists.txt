cmake_minimum_required(VERSION 3.10)
project(MasterSlaveReplicationEngine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(kvdb
    kvdb.cpp
    KVStore.cpp
    WALWriter.cpp
    WALReplayer.cpp
    ClientHandler.cpp
    ReplicationServer.cpp
    ReplicationClient.cpp
)

add_executable(kvdb-cli
    kvdb-cli.cpp
)

if(WIN32)
    target_link_libraries(kvdb ws2_32)
    target_link_libraries(kvdb-cli ws2_32)
else()
    target_link_libraries(kvdb pthread)
    target_link_libraries(kvdb-cli pthread)
endif()
