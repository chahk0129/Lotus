#pragma once
#include "system/global.h"
#include "utils/helper.h"

enum stats_float_t {
    // txn
    STAT_run_time,
    STAT_txn_latency,

    // high-level breakdown of txn time
    STAT_time_process_txn,
    STAT_time_wait,
    STAT_time_abort,
    STAT_time_idle,

    // low-level breakdown of txn time
    STAT_time_index,
    STAT_time_lock,
    STAT_time_prepare,
    STAT_time_commit,

    NUM_FLOAT_STATS
};

enum stats_int_t {
    // txn
    STAT_num_commits,
    STAT_num_aborts,
    STAT_num_waits,

    // network 
    STAT_bytes_sent,
    STAT_bytes_received,
    STAT_bytes_read,
    STAT_bytes_written,
    STAT_bytes_cas,
    STAT_bytes_faa,

    // network TXN
    STAT_bytes_request,
    STAT_bytes_response,
    STAT_bytes_abort,

    // cache
    STAT_num_cache_hits,
    STAT_num_cache_misses,
    STAT_num_cache_evictions,

    NUM_INT_STATS
};

class stats_thd_t {
public:
    stats_thd_t();
    void copy_from(stats_thd_t* stats_thd);

    void init(uint32_t tid);
    void clear();

    double*   _float_stats;
    uint64_t* _int_stats;
    uint64_t* _batch_stats;
    std::vector<double> all_latency;
};

class stats_t {
public:
    stats_t();
    // PER THREAD statistics
    stats_thd_t** _stats;

    double last_cp_bytes_sent(double &dummy_bytes);
    void init();
    void init(uint32_t tid);
    void clear(uint32_t tid);
    void print();
    void print_lat_distr();

    void checkpoint();
    void copy_from(stats_t* stats);

    void output(std::ostream* os);
    void output_cdf();

    std::string stats_float_name[NUM_FLOAT_STATS] = {
        // worker thread
        "run_time",
        "average_latency",
        
        "time_process_txn",
        "time_wait",
        "time_abort",
        "time_idle",

       
        "time_index",
        "time_lock",
        "time_prepare",
        "time_commit",
    };

    std::string stats_int_name[NUM_INT_STATS] = {
        "num_commits",
        "num_aborts",
        "num_waits",

        "bytes_sent",
        "bytes_received",
        "bytes_read",
        "bytes_written",
        "bytes_cas",
        "bytes_faa",

        "bytes_request",
        "bytes_response",
        "bytes_abort",

        "num_cache_hits",
        "num_cache_misses",
        "num_cache_evictions",
    };
private:
    std::vector<double>   _aggregate_latency;
    std::vector<stats_t*> _checkpoints;
    uint32_t              _num_checkpoints;
};
