#pragma once
#include <string>
#include <cstdint>
#include <fstream>
#include <mutex>
using namespace std;

class WALWriter {
public:
    WALWriter(const string& directory);
    ~WALWriter();

    // Appends a record to the WAL and fsyncs.
    // Returns true on success.
    bool append(uint64_t lsn, uint64_t term, uint8_t op_type, const string& key, const string& value);

private:
#ifdef _WIN32
    int fd_;
#else
    int fd_;
#endif
    string filepath_;
};
