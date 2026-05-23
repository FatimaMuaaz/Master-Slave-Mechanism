#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include "KVStore.h"
#include "WALReplayer.h"
#include "ClientHandler.h"
#include "ReplicationServer.h"
#include "ReplicationClient.h"
#include <memory>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#define INVALID_SOCKET -1
#define SOCKET_ERROR -1
#endif

int main(int argc, char* argv[]) {
    int node_id = 1;
    int port = 6001;
    int repl_port = 0;
    std::string master_addr = "";
    std::string data_dir = "./data";

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--node-id" && i + 1 < argc) {
            node_id = std::stoi(argv[++i]);
        } else if (arg == "--port" && i + 1 < argc) {
            port = std::stoi(argv[++i]);
        } else if (arg == "--repl-port" && i + 1 < argc) {
            repl_port = std::stoi(argv[++i]);
        } else if (arg == "--master" && i + 1 < argc) {
            master_addr = argv[++i];
        } else if (arg == "--data" && i + 1 < argc) {
            data_dir = argv[++i];
        }
    }

#ifdef _WIN32
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        std::cerr << "WSAStartup failed.\n";
        return 1;
    }
#endif

    std::cout << "[node " << node_id << "] starting up...\n";

    KVStore store;
    
    // Replay WAL
    WALReplayer replayer(data_dir);
    size_t replayed = replayer.replay(&store);
    
    std::cout << "[node " << node_id << "] WAL replay: " << replayed << " records\n";

    store.init(data_dir);
    store.set_client_port(port);
    
    store.set_node_id(node_id);
    if (!master_addr.empty()) {
        store.set_role("FOLLOWER");
        size_t colon = master_addr.find(':');
        if (colon != std::string::npos) {
            std::string host = master_addr.substr(0, colon);
            int m_port = std::stoi(master_addr.substr(colon + 1));
            store.set_master_addr(host, m_port);
        } else {
            store.set_master_addr(master_addr, 0);
        }
    } else {
        store.set_role("MASTER");
    }

    std::unique_ptr<ReplicationServer> repl_server;
    if (repl_port > 0) {
        repl_server = std::make_unique<ReplicationServer>(repl_port, &store, node_id);
        repl_server->start();
    }

    std::unique_ptr<ReplicationClient> repl_client;
    if (!master_addr.empty()) {
        size_t colon = master_addr.find(':');
        if (colon != std::string::npos) {
            std::string host = master_addr.substr(0, colon);
            int m_port = std::stoi(master_addr.substr(colon + 1));
            repl_client = std::make_unique<ReplicationClient>(host, m_port, &store, node_id);
            repl_client->start();
        }
    }

    socket_t listen_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_sock == INVALID_SOCKET) {
        std::cerr << "Failed to create socket.\n";
        return 1;
    }

    int opt = 1;
    setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));

    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(listen_sock, (struct sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) {
        std::cerr << "Failed to bind to port " << port << ".\n";
        return 1;
    }

    if (listen(listen_sock, SOMAXCONN) == SOCKET_ERROR) {
        std::cerr << "Failed to listen.\n";
        return 1;
    }

    std::cout << "[node " << node_id << "] accepting client writes on port " << port << "\n";

    while (true) {
        sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        socket_t client_sock = accept(listen_sock, (struct sockaddr*)&client_addr, &client_len);

        if (client_sock == INVALID_SOCKET) {
            continue;
        }

        std::thread client_thread(ClientHandler::handle_client, client_sock, &store);
        client_thread.detach();
    }

#ifdef _WIN32
    WSACleanup();
#endif
    return 0;
}
