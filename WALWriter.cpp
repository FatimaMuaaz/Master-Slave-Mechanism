#include "WALWriter.h"
#include "Crc32.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <iostream>

#ifdef _WIN32
#include <io.h>
#define open _open
#define write _write
#define close _close
#define fsync _commit
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

WALWriter::WALWriter(const string& directory) {
    filepath_ = directory + "/wal.log";
    fd_ = open(filepath_.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_BINARY, 0644);
    if (fd_ < 0) {
        std::cerr << "Failed to open WAL file for writing: " << filepath_ << "\n";
    }
}

WALWriter::~WALWriter() {
    if (fd_ >= 0) {
        close(fd_);
    }
}

bool WALWriter::append(uint64_t lsn, uint64_t term, uint8_t op_type, const string& key, const string& value) {
    if (fd_ < 0) return false;

    uint16_t key_len = static_cast<uint16_t>(key.size());
    uint16_t value_len = static_cast<uint16_t>(value.size());

    // Serialize body and header part (everything after crc32)
    vector<uint8_t> payload;
    payload.reserve(21 + key_len + value_len); // Header without CRC (25-4 = 21) + strings

    // LSN
    for (int i = 0; i < 8; ++i) payload.push_back((lsn >> (i * 8)) & 0xFF);
    // Term
    for (int i = 0; i < 8; ++i) payload.push_back((term >> (i * 8)) & 0xFF);
    // OpType
    payload.push_back(op_type);
    // Key Length
    for (int i = 0; i < 2; ++i) payload.push_back((key_len >> (i * 8)) & 0xFF);
    // Value Length
    for (int i = 0; i < 2; ++i) payload.push_back((value_len >> (i * 8)) & 0xFF);
    
    // Key Bytes
    for (char c : key) payload.push_back(static_cast<uint8_t>(c));
    // Value Bytes
    for (char c : value) payload.push_back(static_cast<uint8_t>(c));

    uint32_t crc = Crc32::calculate(payload.data(), payload.size());

    vector<uint8_t> final_record;
    final_record.reserve(4 + payload.size());
    for (int i = 0; i < 4; ++i) final_record.push_back((crc >> (i * 8)) & 0xFF);
    final_record.insert(final_record.end(), payload.begin(), payload.end());

    size_t total_written = 0;
    while (total_written < final_record.size()) {
        int r = write(fd_, final_record.data() + total_written, final_record.size() - total_written);
        if (r < 0) return false;
        total_written += r;
    }

    fsync(fd_);
    return true;
}
