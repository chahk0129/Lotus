#pragma once

// CPU_FREQ is used to get accurate timing info
// We assume all nodes have the same CPU frequency.
#define CPU_FREQ 3.7     // in GHz/s

// Network transport
// ================
// Supported transport: ONE_SIDED, TWO_SIDED
#define ONE_SIDED 1
#define TWO_SIDED 2
#define TRANSPORT TWO_SIDED
#define BATCH 0

// Partitioning in compute layer
// ==========
// When set to true, each client is assigned with a specific set of partitions.
#define PARTITIONED true

// Statistics
// ==========
// COLLECT_LATENCY: when set to true, will collect transaction latency information
#define COLLECT_LATENCY false
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
#define CACHE_SIZE 128
// WORKLOAD can be YCSB or TPCC
#define WORKLOAD YCSB

///////////////////////////////
// YCSB
///////////////////////////////
// Number of tuples per node
#define SYNTH_TABLE_SIZE 100000000
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
#define PERC_PAYMENT 0.316
#define PERC_NEWORDER 0.331
#define PERC_ORDERSTATUS 0.029
#define PERC_DELIVERY 0.294
#define PERC_STOCKLEVEL 0.03
#define PAYMENT_REMOTE_PERC 15 // percentage of payment transactions that are remote
#define NEW_ORDER_REMOTE_PERC 1 // percentage of new order transactions that are remote
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
#define IB_PORT 1