#include "system/manager.h"
#include "system/workload.h"
#include "transport/transport.h"
#include "batch/table.h"
#include "batch/manager.h"

thread_local drand48_data manager_t::_buffer;
thread_local uint32_t manager_t::_thread_id;
thread_local batch_manager_t* manager_t::_batch_manager = nullptr;

manager_t::manager_t() {
    init();
}

manager_t::~manager_t() { }

void manager_t::init() {
    _num_finished_threads.store(0);
    _num_finished_nodes.store(0);
    _num_warmup_threads.store(0);
    _num_sync_nodes.store(0);
    _remote_done = true;
    if (g_is_server)  {
        _remote_done = false;
        _num_finished_threads.store(g_total_num_threads);
    }
}

uint64_t manager_t::get_ts() {
    return get_priority();
}

uint64_t manager_t::rand_uint64() {
    int64_t rint64 = 0;
    lrand48_r(&_buffer, &rint64);
    return rint64;
}

uint64_t manager_t::rand_uint64(uint64_t max) {
    return rand_uint64() % max;
}

uint64_t manager_t::rand_uint64(uint64_t min, uint64_t max) {
    return min + rand_uint64(max - min + 1);
}

double manager_t::rand_double() {
    double r = 0;
    drand48_r(&_buffer, &r);
    return r;
}

void manager_t::worker_thread_done() {
    _num_finished_threads.fetch_add(1);
}

void manager_t::remote_node_done() {
    _num_finished_nodes.fetch_add(1);
    if (_num_finished_nodes.load() == g_num_nodes)
        set_remote_done();
}

void manager_t::warmup_thread_done() {
    _num_warmup_threads.fetch_add(1);
}

bool manager_t::is_warmup_done() {
    return _num_warmup_threads.load() == g_total_num_threads;
}

bool manager_t::is_sim_done() {
    return _remote_done && are_all_threads_done();
}

void manager_t::set_sync_done() {
    _num_sync_nodes.fetch_add(1);
}

bool manager_t::get_sync_done() {
    return _num_sync_nodes.load() == g_num_nodes;
}