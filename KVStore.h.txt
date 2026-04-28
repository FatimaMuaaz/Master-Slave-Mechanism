#pragma once
#include <string>
#include <unordered_map>
#include <mutex>
#include <memory>
#include "WALWriter.h"

class KVStore {
public:
    KVStore();
    ~KVStore();

    void init(const std::string& data_dir);

    // Replay methods
    void set_lsn_and_term(uint64_t lsn, uint64_t term);
    void apply_raw(uint8_t op_type, const std::string& key, const std::string& value);

    // Client methods
    bool put(const std::string& key, const std::string& value, uint64_t& out_lsn);
    bool del(const std::string& key, uint64_t& out_lsn);
    bool get(const std::string& key, std::string& out_val);
    
    // Server info
    uint64_t get_lsn();
    uint64_t get_term();

private:
    std::mutex mu_;
    std::unordered_map<std::string, std::string> map_;
    std::unique_ptr<WALWriter> wal_;
    uint64_t current_lsn_;
    uint64_t current_term_;
};
