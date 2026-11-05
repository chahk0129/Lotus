#include "system/global.h"
#include "system/manager.h"
#include "system/stats.h"
#include "client/thread.h"
#include "client/transport.h"
#include "server/thread.h"
#include "server/transport.h"
#include "utils/options.h"
#include "txn/table.h"
#include "utils/debug.h"

#include "batch/limits.h"
#include "batch/combiner.h"

#include "benchmarks/tpcc/workload.h"
#include "benchmarks/ycsb/workload.h"
#include "benchmarks/ycsb/query.h"

#include <signal.h>

workload_t* workload = nullptr;
void run() {
    // initialization
    stats = new stats_t();
    global_manager = new manager_t();
    // global_manager->init();
    
    if (WORKLOAD == YCSB) 
        workload = new ycsb_workload_t();
    else 
        workload = new tpcc_workload_t();
    global_manager->set_workload(workload);
    
    debug::notify_info("TSC frequency: %.3f GHz (CPU_FREQ fallback %.3f)", g_tsc_ghz, (double)CPU_FREQ);
    debug::notify_info("Initializing transport ...");
    if (g_is_server) { // server initialization
        assert(g_total_num_threads == g_num_server_threads);
        transport = new server_transport_t();
        g_txn_table_size = g_num_client_nodes * g_num_client_threads;
        threads = new thread_t* [g_num_server_threads];
        for (uint32_t i=0; i<g_num_server_threads; i++)
            threads[i] = new server_thread_t(i);
    }
    else { // client initialization
        // one-sided: g_num_client_threads may exceed the worker count (see utils/options.h)
        assert(g_total_num_threads <= g_num_client_threads);
        transport = new client_transport_t();
        g_txn_table_size = g_num_client_threads;
        threads = new thread_t* [g_total_num_threads];
        for (uint32_t i=0; i<g_total_num_threads; i++)
            threads[i] = new client_thread_t(i);
    }

    global_manager->set_transport(transport);
    txn_table = new txn_table_t(g_txn_table_size);

    #if BATCH
    if (!g_is_server)
        batch_domain_t::create_all();
    debug::notify_info("Batching: %u domain(s) of %u client threads, %u slot(s) each, adaptive=%d, limit=%d, wait=%dns",
                       g_batch_num_domains, g_batch_domain_size, (uint32_t)BATCH_SLOTS, (int)BATCH_ADAPTIVE,
                       (int)BATCH_LIMIT, (int)BATCH_WAIT_NS);
    #endif
    pthread_barrier_init(&global_barrier, nullptr, g_total_num_threads);
    pthread_mutex_init(&global_lock, nullptr);
    g_warmup_done = false;
    
    debug::notify_info("Loading the database ...");
    if (WORKLOAD == YCSB)
        ycsb_query_t::preprocess();
    workload->init();

    debug::notify_info("Executing the workload ...");
    // execution
    struct timespec ts_start, ts_end;
    clock_gettime(CLOCK_REALTIME, &ts_start);
    
    auto func = [](thread_t* thd) { thd->run(); };
    std::vector<std::thread> vec;
    for (uint32_t i=1; i<g_total_num_threads; i++)
        vec.push_back(std::thread(func, threads[i]));
    func(threads[0]);
    for (auto& t: vec) t.join();
    uint64_t end = get_sys_clock();

    clock_gettime(CLOCK_REALTIME, &ts_end);
    uint64_t elapsed = ts_end.tv_nsec - ts_start.tv_nsec + (ts_end.tv_sec - ts_start.tv_sec) * 1000000000;
    std::cout << "Elapsed time (sec)   : " << elapsed / 1000000000.0 << std::endl;

    // print stats
    if (STATS_ENABLE)
        stats->print();
    #if BATCH
    if (!g_is_server)
        batch_domain_t::print_all();
    #endif

    // cleanup
    delete txn_table;
    for (uint32_t i = 0; i < g_total_num_threads; i++)
        delete threads[i];
    delete[] threads;

    delete workload;
    delete transport;
    delete stats;
    delete global_manager;
}

void cleanup_handler(int signal) {
    debug::notify_info("Cleaning up resources ...");
    if (txn_table) delete txn_table;
    if (threads) {
        for (uint32_t i = 0; i < g_total_num_threads; i++)
            if (threads[i]) delete threads[i];
        delete[] threads;
    }
    if (workload) delete workload;
    if (stats) delete stats;
    if (transport) delete transport;
}

int main(int argc, char* argv[]) {
    signal(SIGINT, cleanup_handler);
    #ifdef SERVER
    g_is_server = true;
    #endif
    parse_args(argc, argv);
    run();
    return 0;
}