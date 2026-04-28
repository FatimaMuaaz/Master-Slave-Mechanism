#include "KVStore.h"

KVStore::KVStore() : current_lsn_(0), current_term_(0) {
}

KVStore::~KVStore() {
}

void KVStore::init(const std::string& data_dir) {
    wal_ = std::make_unique<WALWriter>(data_dir);
}

void KVStore::set_lsn_and_term(uint64_t lsn, uint64_t term) {
    std::lock_guard<std::mutex> lock(mu_);
    current_lsn_ = lsn;
    current_term_ = term;
}

void KVStore::apply_raw(uint8_t op_type, const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mu_);
    if (op_type == 1) { // PUT
        map_[key] = value;
    } else if (op_type == 2) { // DELETE
        map_.erase(key);
    }
}

bool KVStore::put(const std::string& key, const std::string& value, uint64_t& out_lsn) {
    std::lock_guard<std::mutex> lock(mu_);
    current_lsn_++;
    out_lsn = current_lsn_;
    
    if (wal_->append(current_lsn_, current_term_, 1, key, value)) {
        map_[key] = value;
        return true;
    }
    return false;
}

bool KVStore::del(const std::string& key, uint64_t& out_lsn) {
    std::lock_guard<std::mutex> lock(mu_);
    current_lsn_++;
    out_lsn = current_lsn_;
    
    if (wal_->append(current_lsn_, current_term_, 2, key, "")) {
        map_.erase(key);
        return true;
    }
    return false;
}

bool KVStore::get(const std::string& key, std::string& out_val) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = map_.find(key);
    if (it != map_.end()) {
        out_val = it->second;
        return true;
    }
    return false;
}

uint64_t KVStore::get_lsn() {
    std::lock_guard<std::mutex> lock(mu_);
    return current_lsn_;
}

uint64_t KVStore::get_term() {
    std::lock_guard<std::mutex> lock(mu_);
    return current_term_;
}
