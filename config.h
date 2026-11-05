#pragma once

// CPU_FREQ is only a fallback: the TSC frequency is measured at startup
// (g_tsc_ghz in system/global.cpp) and used by get_sys_clock().
#define CPU_FREQ 3.7     // in GHz/s

// Network transport
// ================
// Supported transport: ONE_SIDED, TWO_SIDED
#define ONE_SIDED 1
#define TWO_SIDED 2
#define TRANSPORT TWO_SIDED
#define BATCH 0
// Inter-transaction batching (two-sided only, when BATCH is 1): requests from different
// transactions to the same memory server are combined, with a cap on in-flight messages
// BATCH_DOMAIN_SIZE: client threads that share one batching domain (0 = all client threads on the node)
#define BATCH_DOMAIN_SIZE 0
// BATCH_SLOTS: batches that may be in flight per (domain, server); each slot is one batch QP
#define BATCH_SLOTS 4
// BATCH_ADAPTIVE: when true, a per-domain controller picks the in-flight cap / wait per epoch
#define BATCH_ADAPTIVE true
// BATCH_CONTENTION_GUARD: when the abort rate is high, the controller skips arms with an extra wait
// and prefers no batching unless batching is clearly better. Off by default: on YCSB-A (8 nodes, ~97% aborts)
// batching with a wait still gained ~45%, and the guard cut that to ~13%. The no-batching arm still
// wins on its own whenever batching does not pay off.
#define BATCH_CONTENTION_GUARD false
// BATCH_LIMIT: fixed in-flight cap per (domain, server) when not adaptive (0 = unlimited)
#define BATCH_LIMIT 4
// BATCH_WAIT_NS: fixed extra wait before a sender claims pending requests (ns) when not adaptive
#define BATCH_WAIT_NS 0

// Partitioning in compute layer
// ==========
// When set to true, each client is assigned with a specific set of partitions.
#define PARTITIONED false

// Statistics
// ==========
// COLLECT_LATENCY: when set to true, will collect transaction latency information
#define COLLECT_LATENCY true
#define STATS_ENABLE true
#define TIME_ENABLE true
#define STATS_CP_INTERVAL 1000 // in ms

// Transaction Type
// ================
// Transaction types: STORED_PROCEDURE, INTERACTIVE
#define STORED_PROCEDURE 1
#define INTERACTIVE 2
#define TXN_TYPE STORED_PROCEDURE

// Concurrency Control
// ===================
// Supported concurrency control algorithms: WAIT_DIE, NO_WAIT, WOUND_WAIT
#define NO_WAIT 1
#define WAIT_DIE 2
#define WOUND_WAIT 3
#define CC_ALG WAIT_DIE

// per-row lock/ts management or central lock/ts management
#define BUCKET_CNT 31
#define MAX_NUM_ABORTS 0
#define ABORT_PENALTY 1000000
#define ABORT_BUFFER_SIZE 10
#define ABORT_BUFFER_ENABLE true

// [ INDEX ]
#define INDEX_STRUCT IDX_BTREE


////////////////////////////////////////////////////////////////////////
// Benchmark
////////////////////////////////////////////////////////////////////////
#define WARMUP_TIME 10
#define RUN_TIME 10
#define MAX_TUPLE_SIZE 1024 // in bytes
#define MAX_NUM_THREADS 48
#define NUM_SERVER_THREADS 8
#define NUM_CLIENT_THREADS 40
#define INIT_PARALLELISM NUM_CLIENT_THREADS
#define MAX_NUM_WAITS 64
#define THINK_TIME 0  // in us
#define RPC_RATE 0.1
#define CACHE_ADMISSION_RATE 0.5
#define CACHE_SIZE 160
// WORKLOAD can be YCSB or TPCC
#define WORKLOAD TPCC

///////////////////////////////
// YCSB
///////////////////////////////
// Number of tuples per node
#define SYNTH_TABLE_SIZE 10000000
#define ZIPF_THETA 0.9
#define REQ_PER_QUERY 16
#define READ_PERC 1.0
#define UPDATE_PERC 0.0
#define INSERT_PERC 0.0
#define SCAN_PERC 0.0
// KEY_ORDER: when set to true, each transaction accesses tuples in the primary key order.
#define KEY_ORDER false



///////////////////////////////
// TPCC
///////////////////////////////
// For large warehouse count, the tables do not fit in memory
// small tpcc schemas shrink the table size.
#define TPCC_SMALL false
#define NUM_WH 16
#define PERC_PAYMENT 0.431
#define PERC_NEWORDER 0.445
#define PERC_ORDERSTATUS 0.041
#define PERC_DELIVERY 0.042
#define PERC_STOCKLEVEL 0.041
#define PAYMENT_REMOTE_PERC 15
#define NEW_ORDER_REMOTE_PERC 15
#define FIRSTNAME_MINLEN 8
#define FIRSTNAME_LEN 16
#define LASTNAME_LEN 16
#define DIST_PER_WARE 10

////////////////////////////////////////////////////////////////////////
// Constant
////////////////////////////////////////////////////////////////////////
// index structure
#define IDX_BTREE 1
#define PAGE_SIZE 1024

// WORKLOAD
#define YCSB 1
#define TPCC 2
#define DEFAULT_ROW_SIZE (1024 - 16)
#define DEVICE_MEMORY_SIZE (1024 * 128)

/***********************************************/
// Distributed DBMS
/***********************************************/
#define START_PORT 2123 
#define MAX_NUM_ACTIVE_TXNS 128
#define MAX_MESSAGE_SIZE (1024ULL * 512) // 512 KB
#define QP_DEPTH 512
// SERVER_SEND_SIGNAL_INTERVAL (PARTITIONED two-sided only): the memory server requests a send
// completion (and waits for it) only on every N-th reply per QP; 1 = wait on every reply (old behavior)
#define SERVER_SEND_SIGNAL_INTERVAL 64
#define IB_PORT 1