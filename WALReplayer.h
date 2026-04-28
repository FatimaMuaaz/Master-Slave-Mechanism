#pragma once
#include <string>
#include <cstdint>
using namespace std;

class KVStore;

class WALReplayer {
public:
    WALReplayer(const string& directory);
    ~WALReplayer();

    // Replays the WAL and rebuilds the KVStore state.
    // Returns the number of records successfully replayed.
    size_t replay(KVStore* store);

private:
    string filepath_;
};
