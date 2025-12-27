# Lotus

Lotus is a disaggregated memory OLTP system that uses RDMA for network communication between compute servers and memory servers.

## System Architecture
- **Compute Servers**: Execute transactions and maintain minimal local state
- **Memory Servers**: Store indexes and tables, and support concurrency control
- **RDMA Network**: Provides high-throughput, low-latency communication (two-sided/one-sided)

## Concurrency Control

The system supports multiple concurrency control protocols in 2PL including `NO\_WAIT`, `WAIT\_DIE`, and `WOUND\_WAIT`.


## Directory Structure
- `benchmarks/`: implementation of YCSB benchmark
- `index/`: indexing implementation of Sherman/Deft, DEX, and Lotus
- `txn/`: transaction implementation
- `concurrency/`: concurrency control protocols
- `client/`: compute server implementation
- `server/`: memory server implementation
- `transport/`: RDMA networking and communication protocols
- `storage/`: database table and tuple management
- `system/`: system and workload management
- `utils/`: utility functions and helper classes
- `batch/`: batch processing and decision-making components
- `scripts/`: automation and deployment scripts
- `exp_profile/`: experimental configuration profiles
- `third_party/`: external dependencies


## Prerequisites
- Linux with RDMA-capable network hardware (InfiniBand/RoCE)
- For Sherman/Deft: cityhash, boost, memcached
- For benchmark: jemalloc, python, cmake, MLNX_OFED_LINUX driver
```bash
bash scripts/install.sh # install dependencies
bash scripts/hugepage.sh # allocate hugepage for RDMA MRs
bash scripts/ntp.sh # synchronize NTP among servers
```

## Configuration

Update `ifconfig.txt` with your server IP addresses, and 
modify parameters in `config.h` for different configurations in workloads, concurrenc control protocols, cache sizes, thread counts, RDMA settings


## Build
```bash
mkdir build && cd build
cmake .. && make -j$(nproc)
```


## Usage
```bash
# Run YCSB experiments
bash scripts/run_ycsb_general_tput.sh
bash scripts/run_ycsb_general_latency.sh

# System comparison across all configurations
bash scripts/run_all.sh
```
