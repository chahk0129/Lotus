#pragma once

#include "utils/helper.h"
#include "system/global.h"
#include "batch/manager.h"
#include <thread>

class row_t;
class workload_t;
class transport_t;
class batch_table_t;


// Global Manager shared by all the threads.
// For distributed version, Manager is shared by all threads on a single node.
class manager_t {
    public:
        manager_t();
        ~manager_t();

        void            init();
        uint64_t        get_ts();

        // per-thread random number generator
        void            init_rand(int tid) {  srand48_r(tid, &_buffer); }
        uint64_t        rand_uint64();
        uint64_t        rand_uint64(uint64_t max);
        uint64_t        rand_uint64(uint64_t min, uint64_t max);
        double          rand_double();

        // node id
        void            set_node_id(uint32_t node_id) { _node_id = node_id; }
        uint32_t        get_node_id() { return _node_id; }

        // thread id
        void            set_tid(uint64_t thread_id) { _thread_id = thread_id; }
        uint64_t        get_tid() { return _thread_id; }

        // workload
        void            set_workload(workload_t* wl) { _wl = wl; }
        workload_t*     get_workload() { return _wl; }

        // transport
        void            set_transport(transport_t* transport) { _transport = transport; }
        transport_t*    get_transport() { return _transport; }

        // batch table
        void            set_batch_table(batch_table_t* table) { _batch_table = table; }
        batch_table_t*  get_batch_table() { return _batch_table; }
        void             set_batch_manager(batch_manager_t* batch_manager) { _batch_manager = batch_manager; }
        batch_manager_t* get_batch_manager() { return _batch_manager; }

        // global synchronization
        bool            is_warmup_done();
        bool            is_sim_done();
        void            set_remote_done() { _remote_done = true; }
        bool            get_remote_done() { return _remote_done; }
        void            set_sync_done();
        bool            get_sync_done();
        void            warmup_thread_done();
        void            worker_thread_done();
        void            remote_node_done();
        bool            are_all_threads_done() { return _num_finished_threads.load() == g_total_num_threads; }

    private:
        bool volatile           _remote_done;
        std::atomic<uint32_t>   _num_finished_threads;
        std::atomic<uint32_t>   _num_finished_nodes;
        std::atomic<uint32_t>   _num_warmup_threads;
        std::atomic<uint32_t>   _num_sync_nodes;

        // per-thread random number
        static thread_local drand48_data _buffer;

        uint32_t        _node_id; 
        // thread id
        static thread_local uint32_t _thread_id;

        // workload
        workload_t*     _wl;

        // transport
        transport_t*    _transport;

        // batch table
        batch_table_t*  _batch_table;
        static thread_local batch_manager_t* _batch_manager;
};
