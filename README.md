# Master-Slave Replication Engine

This project implements a fully replicated, fault-tolerant key-value store in C++. It operates as a three-node cluster over TCP, featuring Write-Ahead Logging (WAL) for durability, Raft-like leader elections, and dynamic log truncation for node recovery.

## Build Instructions

Requirements:
- C++20 compatible compiler (GCC/Clang/MSVC)
- CMake 3.10+

```bash
# Generate build files
cmake -B build -S .

# Build the project
cmake --build build --config Release
```

The executables `kvdb` and `kvdb-cli` will be generated in `build/Release/` (on Windows) or `build/` (on Linux).

## Deployment Options

### 1. Three Processes on One Machine (Local Development)
You can run all three nodes locally on different ports.
```bash
./kvdb --node-id 1 --port 6001 --repl-port 7001 --peers 2@localhost:7002,3@localhost:7003 --data ./data1 &
./kvdb --node-id 2 --port 6002 --repl-port 7002 --peers 1@localhost:7001,3@localhost:7003 --data ./data2 &
./kvdb --node-id 3 --port 6003 --repl-port 7003 --peers 1@localhost:7001,2@localhost:7002 --data ./data3 &
```

### 2. Docker Compose
A `docker-compose.yml` is provided for containerized deployment.
```bash
docker-compose up --build
```
This spins up three isolated nodes (`node1`, `node2`, `node3`) communicating over a dedicated Docker network.

### 3. Multiple Machines / VMs
Run the executable on three separate machines, updating the `--peers` list to reflect the IP addresses of the other nodes. Ensure TCP ports are open in the firewall.

## Client Commands

Connect to any node using the client:
```bash
./kvdb-cli 127.0.0.1 6001
```

Supported commands:
- `PUT key value`: Store a pair (must be sent to MASTER).
- `GET key`: Retrieve a value (can be sent to any node).
- `DELETE key`: Remove a key (must be sent to MASTER).
- `\info`: View the node's role, term, LSN, and replication lag.
- `\sync on` / `\sync off`: Toggle synchronous replication mode on the master.
- `QUIT`: Close connection.

## Architecture & Design
Please see `design.pdf` for a detailed breakdown of the replication architecture, WAL formats, and leader election protocols.
