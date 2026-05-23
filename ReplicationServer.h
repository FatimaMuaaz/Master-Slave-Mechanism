#pragma once
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include "KVStore.h"

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
typedef SOCKET socket_t;
#else
typedef int socket_t;
#define INVALID_SOCKET -1
#endif

class ReplicationServer {
public:
    ReplicationServer(int port, KVStore* store, int node_id);
    ~ReplicationServer();

    void start();

private:
    void listen_loop();
    void handle_follower(socket_t sock);
    void recv_loop(socket_t sock, int follower_id);

    int port_;
    KVStore* store_;
    int node_id_;
    std::atomic<bool> running_;
    std::thread listen_thread_;
};
