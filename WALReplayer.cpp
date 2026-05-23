#include "WALReplayer.h"
#include "KVStore.h"
#include "Crc32.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <iostream>
#include <vector>
#include <filesystem>

#ifdef _WIN32
#include <io.h>
#define open _open
#define read _read
#define close _close
#define lseek _lseek
#ifndef O_BINARY
#define O_BINARY _O_BINARY
#endif
#else
#include <unistd.h>
#ifndef O_BINARY
#define O_BINARY 0
#endif
#endif
using namespace std;
WALReplayer::WALReplayer(const string& directory) {
    filepath_ = directory + "/wal.log";
}

WALReplayer::~WALReplayer() {
}

size_t WALReplayer::replay(KVStore* store) {
    int fd = open(filepath_.c_str(), O_RDONLY | O_BINARY);
    if (fd < 0) {
        // It's normal if the file doesn't exist yet
        return 0;
    }

    size_t count = 0;
    uint64_t max_lsn = 0;
    uint64_t max_term = 0;
    uint64_t last_valid_offset = 0;

    auto read_exact = [&](int fd, uint8_t* buf, size_t len) -> bool {
        size_t total = 0;
        while (total < len) {
            int r = read(fd, buf + total, len - total);
            if (r <= 0) return false;
            total += r;
        }
        return true;
    };

    while (true) {
        uint8_t header[25];
        if (!read_exact(fd, header, 25)) {
            break; // EOF or truncation
        }

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

        vector<uint8_t> key_bytes(key_len);
        if (key_len > 0 && !read_exact(fd, key_bytes.data(), key_len)) break;

        vector<uint8_t> value_bytes(value_len);
        if (value_len > 0 && !read_exact(fd, value_bytes.data(), value_len)) break;

        // Verify CRC
        vector<uint8_t> payload;
        payload.reserve(21 + key_len + value_len);
        payload.insert(payload.end(), header + 4, header + 25);
        payload.insert(payload.end(), key_bytes.begin(), key_bytes.end());
        payload.insert(payload.end(), value_bytes.begin(), value_bytes.end());

        uint32_t calculated_crc = Crc32::calculate(payload.data(), payload.size());
        if (calculated_crc != expected_crc) {
            std::cerr << "WAL Replay: CRC mismatch at record " << (count + 1) << ". Stopping replay.\n";
            break;
        }

        string key(key_bytes.begin(), key_bytes.end());
        string value(value_bytes.begin(), value_bytes.end());

        store->apply_raw(op_type, key, value);

        if (lsn > max_lsn) max_lsn = lsn;
        if (term > max_term) max_term = term;
        count++;

        last_valid_offset = lseek(fd, 0, SEEK_CUR);
    }

    close(fd);

    try {
        if (std::filesystem::exists(filepath_)) {
            uint64_t file_size = std::filesystem::file_size(filepath_);
            if (file_size > last_valid_offset) {
                std::cerr << "WAL Replay: Truncating corrupt/truncated tail of " << (file_size - last_valid_offset)
                          << " bytes from WAL file: " << filepath_ << "\n";
                std::filesystem::resize_file(filepath_, last_valid_offset);
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "WAL Replay: Failed to truncate WAL file: " << e.what() << "\n";
    }

    store->set_lsn_and_term(max_lsn, max_term);
    return count;
}
