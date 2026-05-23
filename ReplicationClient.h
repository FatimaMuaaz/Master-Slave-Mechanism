#pragma once
#include <string>
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

class ReplicationClient {
public:
    ReplicationClient(const std::string& master_host, int master_port, KVStore* store, int node_id);
    ~ReplicationClient();

    void start();

private:
    void run_loop();
    bool connect_and_handshake(socket_t& sock);
    void receive_stream(socket_t sock);

    std::string master_host_;
    int master_port_;
    KVStore* store_;
    int node_id_;
    std::atomic<bool> running_;
    std::thread client_thread_;
};
