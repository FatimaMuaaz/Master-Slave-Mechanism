#define NOMINMAX
#include "ReplicationClient.h"
#include "Crc32.h"
#include <iostream>
#include <sstream>
#include <vector>
#include <chrono>

#ifdef _WIN32
#include <ws2tcpip.h>
#define closesocket closesocket
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#define closesocket close
#define INVALID_SOCKET -1
#define SOCKET_ERROR -1
#endif

ReplicationClient::ReplicationClient(const std::string& master_host, int master_port, KVStore* store, int node_id)
    : master_host_(master_host), master_port_(master_port), store_(store), node_id_(node_id), running_(false) {
}

ReplicationClient::~ReplicationClient() {
    running_ = false;
    if (client_thread_.joinable()) client_thread_.join();
}

void ReplicationClient::start() {
    running_ = true;
    client_thread_ = std::thread(&ReplicationClient::run_loop, this);
}

void ReplicationClient::run_loop() {
    int backoff = 1;

    while (running_) {
        socket_t sock = INVALID_SOCKET;
        if (connect_and_handshake(sock)) {
            backoff = 1; // Reset backoff on success
            receive_stream(sock);
        }

        if (sock != INVALID_SOCKET) closesocket(sock);

        if (running_) {
            std::cout << "[repl] Reconnecting in " << backoff << "s...\n";
            std::this_thread::sleep_for(std::chrono::seconds(backoff));
            backoff = std::min(backoff * 2, 10);
        }
    }
}

bool ReplicationClient::connect_and_handshake(socket_t& sock) {
    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == INVALID_SOCKET) return false;

    struct addrinfo hints = {0}, *res;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(master_host_.c_str(), std::to_string(master_port_).c_str(), &hints, &res) != 0) {
        return false;
    }

    if (connect(sock, res->ai_addr, res->ai_addrlen) == SOCKET_ERROR) {
        freeaddrinfo(res);
        return false;
    }
    freeaddrinfo(res);

    // Send HELLO
    std::string hello = "HELLO " + std::to_string(node_id_) + " " + 
                        std::to_string(store_->get_term()) + " " + 
                        std::to_string(store_->get_lsn()) + "\n";
    if (send(sock, hello.c_str(), hello.size(), 0) <= 0) return false;

    // Receive HELLO
    char buf[1024];
    int r = recv(sock, buf, sizeof(buf) - 1, 0);
    if (r <= 0) return false;
    buf[r] = '\0';

    std::string reply(buf);
    std::istringstream iss(reply);
    std::string cmd;
    int m_node_id;
    uint64_t m_term;
    int m_client_port = 0;

    if (!(iss >> cmd >> m_node_id >> m_term >> m_client_port) || cmd != "HELLO") {
        return false;
    }

    store_->set_master_addr(master_host_, m_client_port);

    std::cout << "[repl] Connected to master node " << m_node_id << " (term=" << m_term << ", client_port=" << m_client_port << ")\n";
    return true;
}

void ReplicationClient::receive_stream(socket_t sock) {
    auto recv_exact = [&](char* buf, size_t len) -> bool {
        size_t total = 0;
        while (total < len) {
            int r = recv(sock, buf + total, len - total, 0);
            if (r <= 0) return false;
            total += r;
        }
        return true;
    };

    while (running_) {
        uint8_t header[25];
        if (!recv_exact((char*)header, 25)) break;

        uint32_t expected_crc = 0;
        for (int i = 0; i < 4; ++i) expected_crc |= (static_cast<uint32_t>(header[i]) << (i * 8));

        uint64_t lsn = 0;
        for (int i = 0; i < 8; ++i) lsn |= (static_cast<uint64_t>(header[4 + i]) << (i * 8));

        uint64_t term = 0;
        for (int i = 0; i < 8; ++i) term |= (static_cast<uint64_t>(header[12 + i]) << (i * 8));

        uint8_t op_type = header[20];

        uint16_t key_len = 0;
        for (int i = 0; i < 2; ++i) key_len |= (static_cast<uint16_t>(header[21 + i]) << (i * 8));

        uint16_t value_len = 0;
        for (int i = 0; i < 2; ++i) value_len |= (static_cast<uint16_t>(header[23 + i]) << (i * 8));

        std::vector<uint8_t> key_bytes(key_len);
        if (key_len > 0 && !recv_exact((char*)key_bytes.data(), key_len)) break;

        std::vector<uint8_t> value_bytes(value_len);
        if (value_len > 0 && !recv_exact((char*)value_bytes.data(), value_len)) break;

        // Verify CRC
        std::vector<uint8_t> payload;
        payload.reserve(21 + key_len + value_len);
        payload.insert(payload.end(), header + 4, header + 25);
        payload.insert(payload.end(), key_bytes.begin(), key_bytes.end());
        payload.insert(payload.end(), value_bytes.begin(), value_bytes.end());

        uint32_t calculated_crc = Crc32::calculate(payload.data(), payload.size());
        if (calculated_crc != expected_crc) {
            std::cerr << "[repl] CRC mismatch on replication stream. Disconnecting.\n";
            break;
        }

        std::string key(key_bytes.begin(), key_bytes.end());
        std::string value(value_bytes.begin(), value_bytes.end());

        if (store_->apply_replicated_record(lsn, term, op_type, key, value)) {
            // Send ACK
            std::string ack = "ACK " + std::to_string(lsn) + "\n";
            send(sock, ack.c_str(), ack.size(), 0);
        } else {
            // Protocol violation (gap)
            break;
        }
    }
}
