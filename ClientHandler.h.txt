#pragma once
#include <string>
#include <memory>

class KVStore;

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
typedef SOCKET socket_t;
#else
typedef int socket_t;
#endif

class ClientHandler {
public:
    static void handle_client(socket_t client_sock, KVStore* store);
};
