#pragma once

#include <iomanip>
#include <unistd.h>
#include <cstddef>
#include <cstdlib>
#include <cassert>
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <string.h>
#include <typeinfo>
#include <list>
#include <map>
#include <set>
#include <unordered_map>
#include <string>
#include <vector>
#include <sstream>
#include <time.h>
#include <sys/time.h>
#include <math.h>
#include <pthread.h>
#include <thread>
#include <atomic>
#include <immintrin.h>
#include "config.h"
#include "system/global_address.h"

using namespace std;

namespace onesided {}
namespace twosided {}
class stats_t;
class manager_t;
class txn_table_t;
class transport_t;
class row_t;
class thread_t;
class memory_allocator_t;
class cache_allocator_t;
class batch_manager_t;
class batch_table_t;

typedef uint32_t UInt32;
typedef int32_t SInt32;
typedef uint64_t UInt64;
typedef int64_t SInt64;
typedef uint64_t Key;
typedef uint64_t ts_t; // time stamp type

/******************************************/
// Global Data Structure
/******************************************/
extern stats_t*            stats;
extern manager_t*          global_manager;
extern memory_allocator_t* g_memory_allocator; // mem allocator in server for one-sided RDMA
extern cache_allocator_t** g_cache_allocators; // cache allocators in clients for one-sided RDMA
extern pthread_barrier_t   global_barrier;
extern pthread_mutex_t     global_lock;


/******************************************/
// Global Parameter
/******************************************/
extern bool volatile g_init_done;
extern bool volatile g_warmup_done;
extern double        g_warmup_time;
extern double        g_run_time;
extern uint64_t      g_abort_penalty;
extern uint32_t      g_max_num_active_txns;
extern uint32_t      g_max_num_waits;
extern bool          g_partitioned;
extern uint64_t      g_lock_start_addr; // deft



////////////////////////////
// YCSB
////////////////////////////
extern double   g_read_perc;
extern double   g_update_perc;
extern double   g_insert_perc;
extern double   g_scan_perc;
extern double   g_zipf_theta;
extern uint64_t g_synth_table_size;
extern uint32_t g_req_per_query;
extern bool     g_key_order;
extern std::string g_workload_type;

////////////////////////////
// TPCC
////////////////////////////
extern uint32_t g_num_wh;
extern uint32_t g_max_items;
extern uint32_t g_cust_per_dist;
extern uint32_t g_payment_remote_perc;
extern uint32_t g_new_order_remote_perc;
extern double   g_perc_payment;
extern double   g_perc_new_order;
extern double   g_perc_order_status;
extern double   g_perc_delivery;
extern double   g_perc_stock_level;


extern char* output_file;
extern char* cdf_file;
extern char ifconfig_file[];

enum RC {RCOK, COMMIT, ABORT, WAIT, LOCAL_MISS, ERROR, FINISH};
enum lock_type_t {LOCK_NONE, LOCK_SH, LOCK_EX, LOCK_UPGRADING};

// INDEX
enum latch_t {LATCH_EX, LATCH_SH, LATCH_NONE};
// accessing type determines the latch type on nodes
enum idx_acc_t {IDX_INSERT, IDX_UPDATE, IDX_SEARCH, IDX_DELETE, IDX_SCAN, INDEX_NONE};
enum idx_rpc_type_t { IDX_RPC_NONE, IDX_RPC_WITH_ADMISSION, IDX_RPC_WITHOUT_ADMISSION, IDX_RPC_WRITEBACK };

// LOOKUP, INS and DEL are operations on indexes.
enum access_t {TYPE_NONE, RD, WR, XP, SCAN, INS, DEL, INDEX_CACHE};

#define MSG(str, args...) { \
    printf("[%s : %d] " str, __FILE__, __LINE__, args); } \



#if TRANSPORT == ONE_SIDED
using namespace onesided;
#else
using namespace twosided;
#endif

// concurrency control
class lock_manager_t;
class idx_manager_t;
namespace onesided {
class nowait_t;
class waitdie_t;
}
namespace twosided {
class nowait_t;
class waitdie_t;
class woundwait_t;
}

#ifndef CC_ALG // just in case
#define CC_ALG NO_WAIT // default
#endif

#if PARTITIONED
using CC_MAN = idx_manager_t;
#else
using CC_MAN = lock_manager_t;
#endif

#if CC_ALG == NO_WAIT
using ROW_MAN = nowait_t;
#elif CC_ALG == WAIT_DIE
using ROW_MAN = waitdie_t;
#elif CC_ALG == WOUND_WAIT
using ROW_MAN = woundwait_t;
#endif // end of CC_ALG

// transaction
class txn_t;
class stored_procedure_t;
class interactive_t;
class txn_manager_t;
#if TXN_TYPE == STORED_PROCEDURE
using TXN = stored_procedure_t;
#else // INTERACTIVE
using TXN = interactive_t;
#endif

// index
template <typename T> class idx_wrapper_t;

struct value_t {
    union { 
        row_t* row; 
        global_addr_t addr; 
        uint64_t val; 
    };

    value_t () : val(0) { }
    value_t (uint64_t v) : val(v) { }
    value_t (row_t* r) : row(r) { }
    value_t (global_addr_t r) : addr(r) { }
    value_t (const value_t& v) : val(v.val) { }

    value_t& operator=(const value_t& v) {
        val = v.val;
        return *this;
    }
    value_t& operator=(row_t*& r) {
        row = r;
        return *this;
    }
    value_t& operator=(const global_addr_t& r) {
        addr = r;
        return *this;
    }
    value_t& operator=(const uint64_t& v) {
        val = v;
        return *this;
    }

    bool operator==(const value_t& other) const {
        return val == other.val;
    }

    bool operator!=(const value_t& other) const {
        return val != other.val;
    }
};
using INDEX = idx_wrapper_t<value_t>;


/************************************************/
// constants
/************************************************/
#ifndef UINT64_MAX
#define UINT64_MAX         18446744073709551615UL
#endif // UINT64_MAX


//////////////////////////////////////////////////
// Distributed DBMS
//////////////////////////////////////////////////
extern uint32_t g_init_parallelism;
extern uint32_t g_total_num_threads;
extern uint32_t g_num_batch_threads;
extern uint32_t g_num_client_threads;
extern uint32_t g_num_server_threads;
extern uint32_t g_num_nodes; // for clients, number of servers; for servers, number of clients
extern uint32_t g_num_client_nodes;
extern uint32_t g_num_server_nodes;
extern uint32_t g_node_id;
extern bool     g_is_server;
extern uint32_t g_txn_table_size;
extern double   g_rpc_rate; // dex
extern double   g_admission_rate;
extern uint64_t g_cache_size; // in MB

extern transport_t* transport;
extern thread_t**   threads;
extern txn_table_t* txn_table;


// local stats
extern thread_local uint64_t traffic_request; 
extern thread_local uint64_t traffic_response; 
void reset_traffic_stats();