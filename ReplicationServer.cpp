#include "ReplicationServer.h"
#include <iostream>
#include <sstream>
#include <vector>
#include <fcntl.h>
#include <sys/stat.h>

#ifdef _WIN32
#include <io.h>
#include <ws2tcpip.h>
#define open _open
#define close _close
#ifndef O_BINARY
#define O_BINARY _O_BINARY
#endif
#else
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#define closesocket close
#define INVALID_SOCKET -1
#define SOCKET_ERROR -1
#ifndef O_BINARY
#define O_BINARY 0
#endif
#endif

ReplicationServer::ReplicationServer(int port, KVStore* store, int node_id)
    : port_(port), store_(store), node_id_(node_id), running_(false) {
}

ReplicationServer::~ReplicationServer() {
    running_ = false;
    // In a real system, we'd shut down the socket to wake up the listen thread
    if (listen_thread_.joinable()) listen_thread_.detach();
}

void ReplicationServer::start() {
    running_ = true;
    listen_thread_ = std::thread(&ReplicationServer::listen_loop, this);
}

void ReplicationServer::listen_loop() {
    socket_t listen_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_sock == INVALID_SOCKET) return;

    int opt = 1;
    setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));

    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);

    if (bind(listen_sock, (struct sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) {
        std::cerr << "[repl] Failed to bind replication port " << port_ << "\n";
        return;
    }

    if (listen(listen_sock, SOMAXCONN) == SOCKET_ERROR) return;

    std::cout << "[repl] Listening for followers on port " << port_ << "\n";

    while (running_) {
        sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        socket_t client_sock = accept(listen_sock, (struct sockaddr*)&client_addr, &client_len);

        if (client_sock != INVALID_SOCKET) {
            std::thread t(&ReplicationServer::handle_follower, this, client_sock);
            t.detach();
        }
    }
    closesocket(listen_sock);
}

void ReplicationServer::handle_follower(socket_t sock) {
    std::thread recv_thread;
    int f_node_id = 0;

    char buf[1024];
    int r = recv(sock, buf, sizeof(buf) - 1, 0);
    if (r <= 0) {
        closesocket(sock);
        return;
    }
    buf[r] = '\0';

    std::string handshake(buf);
    std::istringstream iss(handshake);
    std::string cmd;
    uint64_t f_term, f_lsn;

    if (!(iss >> cmd >> f_node_id >> f_term >> f_lsn) || cmd != "HELLO") {
        closesocket(sock);
        return;
    }

    std::cout << "[repl] Follower " << f_node_id << " connected (term=" << f_term << ", lsn=" << f_lsn << ")\n";

    // Validation
    uint64_t m_term = store_->get_term();
    uint64_t m_lsn = store_->get_lsn();

    if (f_term > m_term) {
        std::cerr << "[repl] Rejecting follower: term " << f_term << " > master term " << m_term << "\n";
        closesocket(sock);
        return;
    }

    if (f_lsn > m_lsn) {
        std::cerr << "[repl] Rejecting follower: LSN " << f_lsn << " > master LSN " << m_lsn << "\n";
        closesocket(sock);
        return;
    }

    // Reply
    std::string reply = "HELLO " + std::to_string(node_id_) + " " + std::to_string(m_term) + " " + std::to_string(store_->get_client_port()) + "\n";
    send(sock, reply.c_str(), reply.size(), 0);

    // Register the peer
    store_->register_peer(f_node_id);

    // Spin up background thread to receive ACKs
    recv_thread = std::thread(&ReplicationServer::recv_loop, this, sock, f_node_id);

    // Streaming loop
    std::string wal_path = store_->get_data_dir() + "/wal.log";
    std::ifstream wal_file(wal_path, std::ios::binary);
    
    uint64_t last_sent_lsn = f_lsn;

    while (running_) {
        // Try to read records from WAL file
        while (true) {
            if (last_sent_lsn >= store_->get_lsn()) {
                break; // Wait for new data
            }

            uint8_t header[25];
            if (!wal_file.read((char*)header, 25)) {
                if (wal_file.eof()) {
                    wal_file.clear();
                    break; // Wait for new data
                }
                goto cleanup;
            }

            // Extract LSN from header (bytes 4-11)
            uint64_t rec_lsn = 0;
            for (int i = 0; i < 8; ++i) rec_lsn |= (static_cast<uint64_t>(header[4 + i]) << (i * 8));

            // Extract lengths
            uint16_t key_len = 0;
            for (int i = 0; i < 2; ++i) key_len |= (static_cast<uint16_t>(header[21 + i]) << (i * 8));
            uint16_t val_len = 0;
            for (int i = 0; i < 2; ++i) val_len |= (static_cast<uint16_t>(header[23 + i]) << (i * 8));

            size_t body_size = key_len + val_len;
            std::vector<uint8_t> body(body_size);
            if (body_size > 0 && !wal_file.read((char*)body.data(), body_size)) goto cleanup;

            if (rec_lsn > last_sent_lsn) {
                // Send record
                if (send(sock, (const char*)header, 25, 0) != 25) goto cleanup;
                if (body_size > 0 && send(sock, (const char*)body.data(), body_size, 0) != (int)body_size) goto cleanup;
                last_sent_lsn = rec_lsn;
            }
        }

        // Wait for more data in store
        store_->wait_for_lsn(last_sent_lsn);
    }

cleanup:
    std::cout << "[repl] Follower " << f_node_id << " disconnected\n";
    closesocket(sock);
    if (recv_thread.joinable()) {
        recv_thread.join();
    }
    if (f_node_id > 0) {
        store_->remove_peer(f_node_id);
    }
}

void ReplicationServer::recv_loop(socket_t sock, int follower_id) {
    char buf[4096];
    std::string internal_buf;
    while (running_) {
        int bytes = recv(sock, buf, sizeof(buf), 0);
        if (bytes <= 0) {
            break;
        }
        internal_buf.append(buf, bytes);
        size_t pos;
        while ((pos = internal_buf.find('\n')) != std::string::npos) {
            std::string line = internal_buf.substr(0, pos);
            internal_buf.erase(0, pos + 1);
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            if (line.empty()) continue;

            std::istringstream iss(line);
            std::string type;
            uint64_t lsn = 0;
            if (iss >> type >> lsn && type == "ACK") {
                store_->update_peer_ack(follower_id, lsn);
            }
        }
    }
    closesocket(sock);
}
