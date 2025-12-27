#include "system/manager.h"
#include "system/thread.h"
#include "transport/transport.h"
#include "system/query.h"
#include "system/stats.h"
#include "txn/table.h"


stats_t*            stats = nullptr;
manager_t*          global_manager = nullptr;
memory_allocator_t* g_memory_allocator = nullptr; // mem allocator in server for one-sided RDMA
cache_allocator_t** g_cache_allocators = nullptr; // cache allocators in clients for one
pthread_barrier_t   global_barrier;
pthread_mutex_t     global_lock;


////////////////////////////
// Global Parameter
////////////////////////////
bool volatile g_init_done   = false;
bool volatile g_warmup_done = false;
uint64_t g_abort_penalty    = ABORT_PENALTY;
double g_warmup_time        = WARMUP_TIME;
double g_run_time           = RUN_TIME;
uint32_t g_txn_table_size   = 0; // this is tuned in transport.cpp
bool g_partitioned          = false;
uint64_t g_lock_start_addr  = 0; // deft

////////////////////////////
// YCSB
////////////////////////////
std::string g_workload_type = "c";
double g_read_perc          = READ_PERC;
double g_update_perc        = UPDATE_PERC;
double g_insert_perc        = INSERT_PERC;
double g_scan_perc          = SCAN_PERC;
double g_zipf_theta         = ZIPF_THETA;
uint64_t g_synth_table_size = SYNTH_TABLE_SIZE;
uint32_t g_req_per_query    = REQ_PER_QUERY;
bool g_key_order            = false;

////////////////////////////
// TPCC
////////////////////////////
uint32_t g_num_wh                = NUM_WH;
double g_perc_payment            = PERC_PAYMENT;
double g_perc_new_order          = PERC_NEWORDER;
double g_perc_order_status       = PERC_ORDERSTATUS;
double g_perc_delivery           = PERC_DELIVERY;
double g_perc_stock_level        = PERC_STOCKLEVEL;
uint32_t g_payment_remote_perc   = PAYMENT_REMOTE_PERC;
uint32_t g_new_order_remote_perc = NEW_ORDER_REMOTE_PERC;
#if TPCC_SMALL
uint32_t g_max_items             = 10000;
uint32_t g_cust_per_dist         = 2000;
#else
uint32_t g_max_items             = 100000;
uint32_t g_cust_per_dist         = 3000;
#endif

char* output_file      = nullptr;
char* cdf_file         = nullptr;
char ifconfig_file[80] = "../ifconfig.txt";


//////////////////////////////////////////////////
// Distributed DBMS
//////////////////////////////////////////////////
uint32_t g_init_parallelism   = INIT_PARALLELISM;
uint32_t g_total_num_threads  = 0;
uint32_t g_num_batch_threads  = 0;
uint32_t g_num_client_threads = NUM_CLIENT_THREADS;
uint32_t g_num_server_threads = NUM_SERVER_THREADS;
uint32_t g_max_num_waits      = MAX_NUM_WAITS;
uint32_t g_num_nodes          = 0;
uint32_t g_num_client_nodes   = 0;
uint32_t g_num_server_nodes   = 0;
uint32_t g_node_id            = (uint32_t)-1;
bool     g_is_server          = false;
double   g_rpc_rate           = RPC_RATE; // dex
double   g_admission_rate     = CACHE_ADMISSION_RATE;
uint64_t g_cache_size         = CACHE_SIZE; // in MB
uint32_t g_cc_alg             = CC_ALG;

transport_t* transport    = nullptr;
thread_t**   threads      = nullptr;
txn_table_t* txn_table    = nullptr;




// local stats
thread_local uint64_t traffic_request = 0;
thread_local uint64_t traffic_response = 0;

void reset_traffic_stats() {
    traffic_request = 0;
    traffic_response = 0;
}