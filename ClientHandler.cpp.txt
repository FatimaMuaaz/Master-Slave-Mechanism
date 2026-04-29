#include "ClientHandler.h"
#include "KVStore.h"
#include <iostream>
#include <sstream>
#include <vector>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#define closesocket closesocket
#else
#include <sys/socket.h>
#include <unistd.h>
#define closesocket close
#endif

void ClientHandler::handle_client(socket_t client_sock, KVStore* store) {
    char buf[4096];
    std::string internal_buffer;

    auto send_resp = [client_sock](const std::string& msg) {
        std::string out = msg + "\n";
        send(client_sock, out.c_str(), out.size(), 0);
    };

    while (true) {
        int bytes_read = recv(client_sock, buf, sizeof(buf), 0);
        if (bytes_read <= 0) {
            break;
        }

        internal_buffer.append(buf, bytes_read);

        size_t pos;
        while ((pos = internal_buffer.find('\n')) != std::string::npos) {
            std::string line = internal_buffer.substr(0, pos);
            internal_buffer.erase(0, pos + 1);

            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }

            if (line.empty()) continue;

            std::istringstream iss(line);
            std::string cmd;
            iss >> cmd;

            if (cmd == "PUT") {
                std::string key, val;
                iss >> key >> val;
                uint64_t lsn;
                if (store->put(key, val, lsn)) {
                    send_resp("OK (lsn=" + std::to_string(lsn) + ")");
                } else {
                    send_resp("ERROR writing to WAL");
                }
            } else if (cmd == "GET") {
                std::string key;
                iss >> key;
                std::string val;
                if (store->get(key, val)) {
                    send_resp(val);
                } else {
                    send_resp("NULL");
                }
            } else if (cmd == "DELETE") {
                std::string key;
                iss >> key;
                uint64_t lsn;
                store->del(key, lsn); // We can proceed even if not found, it increases LSN and logs DELETE
                send_resp("OK (lsn=" + std::to_string(lsn) + ")");
            } else if (cmd == "\\info") {
                std::ostringstream info;
                info << "node_id: 1\n"
                     << "role: MASTER\n"
                     << "term: " << store->get_term() << "\n"
                     << "lsn: " << store->get_lsn() << "\n";
                // Optionally adding sync_mode or followers if requested later
                send_resp(info.str());
            } else if (cmd == "\\sync") {
                send_resp("OK");
            } else if (cmd == "QUIT") {
                closesocket(client_sock);
                return;
            } else {
                send_resp("UNKNOWN COMMAND");
            }
        }
    }
    closesocket(client_sock);
}
