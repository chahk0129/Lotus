#include "benchmarks/ycsb/workload.h"
#include "benchmarks/ycsb/stored_procedure.h"
#include "benchmarks/ycsb/interactive.h"
#include "benchmarks/ycsb/query.h"
#include "utils/helper.h"
#include "system/thread.h"
#include "system/manager.h"
#include "system/query.h"
#include "system/workload.h"
#include "storage/table.h"
#include "storage/catalog.h"
#include "storage/row.h"
#include "client/transport.h"
#include "index/idx_wrapper.h"
#include "index/onesided/dex/leanstore_tree.h"
#include "index/onesided/sherman/Tree.h"
#include "index/twosided/partitioned/idx.h"
#include "index/twosided/non_partitioned/idx.h"

#if WORKLOAD == YCSB

RC ycsb_workload_t::init() {
    workload_t::init();
    string schema_file = "../benchmarks/ycsb/schema.txt";
    cout << "Reading schema file: " << schema_file.c_str() << endl;
    if (!init_schema(schema_file)) {
        assert(false);
        return ERROR;
    }
    set_partition();
    init_table();
    return RCOK;
}

catalog_t* ycsb_workload_t::init_schema(string schema_file) {
    the_schema = workload_t::init_schema(schema_file);
    the_table = _tables[0];
    the_index = _indexes[0];
    return the_schema;
}

void ycsb_workload_t::init_table() {
#if PARTITIONED // DEX requires rpc rate to be zero during bulk load
    if (!g_is_server) the_index->set_rpc_ratio(0);
#endif
    std::vector<std::thread> thd;
    for(uint32_t i=1; i<g_init_parallelism; i++)
        thd.push_back(std::thread(thread_init_table, this, i));
    thread_init_table(this, 0);
    for(auto& t: thd) t.join();
#if PARTITIONED // DEX setting requirements
    if (!g_is_server) {
        if (g_node_id == 0) {
            std::vector<Key> bound;
            for (uint32_t i=0; i<g_num_nodes-1; i++)
                bound.push_back(_partitions[0][i+1]);
            the_index->set_shared(bound);
        }
        uint64_t left_bound = _partitions[0][g_node_id];
        uint64_t right_bound = _partitions[0][g_node_id + 1];
        if (g_node_id == g_num_server_nodes - 1) 
            right_bound = g_synth_table_size;
        the_index->set_bound(left_bound, right_bound);
        the_index->reset_buffer_pool(true);
        // newest root will be fetched during execution
        // the_index->get_newest_root(); 
        the_index->set_rpc_ratio(g_rpc_rate);
    }
#endif
}

void ycsb_workload_t::init_table_parallel(uint32_t tid) {
    assert(!PARTITIONED);
#if TRANSPORT == ONE_SIDED
    if (g_is_server) return;
    // for one-sided RDMA, each client initializes the table
    uint32_t num_nodes = g_num_client_nodes;
    // Sherman has synchronization issue ...
    // it crashes if the DB is loaded with multiple nodes and large number of threads
    num_nodes = 1;
    if (g_node_id > 0) return;
    // g_init_parallelism = g_init_parallelism > 4 ? 4 : g_num_client_threads;
    // if (tid > g_init_parallelism) return;
#else // TWO_SIDED
    if (!g_is_server) return;
    // for two-sided RDMA, each server initializes the table
    uint32_t num_nodes = g_num_server_nodes;
#endif
    global_manager->set_tid(tid);
    bind_core(tid);
    uint64_t chunk_per_node = g_synth_table_size / num_nodes;
    uint64_t chunk_per_thread = chunk_per_node / g_init_parallelism;
    uint64_t from = chunk_per_node * g_node_id + chunk_per_thread * tid + 1;
    uint64_t to = chunk_per_node * g_node_id + chunk_per_thread * (tid + 1);
    if (tid == g_init_parallelism - 1) {
        if (g_node_id == num_nodes - 1) to = g_synth_table_size;
        else to = chunk_per_node * (g_node_id + 1);
    }

    auto row_size = the_table->get_tuple_size() + 16;
    assert(row_size == sizeof(row_t));
    auto schema = the_schema;
#if TRANSPORT == ONE_SIDED
    char* buffer = transport->get_buffer();
    for (uint64_t key=from; key<=to; key++) {
        uint32_t node_id = get_partition_by_key(key);
        auto row_addr = rpc_alloc(node_id, row_size);
        auto row = reinterpret_cast<row_t*>(buffer);
        // memset(row, 0, row_size);
        row->init_manager();
        row->set_value(schema, 0, &key);
        for(uint32_t fid=1; fid<schema->get_field_cnt(); fid++){
            char value[schema->get_field_size(fid)] = {0};
            strncpy(value, "hello", schema->get_field_size(fid));
            row->set_value(schema, fid, value);
        }
        transport->write(buffer, row_addr, row_size);
        value_t value;
        value.addr = row_addr;
        the_index->insert(key, value);
    }
#else // TWO_SIDED
    for (uint64_t key=from; key<=to; key++) {
        uint32_t node_id = get_partition_by_key(key);
        row_t* row = nullptr;
        uint64_t primary_key = key;
        RC rc = the_table->get_new_row(row);
        assert(rc == RCOK);
        row->init_manager();
        row->set_value(schema, 0, &primary_key);
        for(uint32_t fid=1; fid<schema->get_field_cnt(); fid++){
            // char value[6] = "hello";
            char value[schema->get_field_size(fid)] = {0};
            strncpy(value, "hello", schema->get_field_size(fid));
            row->set_value(schema, fid, value);
        }

        the_index->insert(key, row);
    }
#endif
}

void ycsb_workload_t::init_index_parallel(uint32_t tid) {
    assert(PARTITIONED);
#if PARTITIONED
    uint32_t num_nodes = 0;
    if (TRANSPORT == ONE_SIDED) { // DEX requires to bulk load with single node + single thread
        if (g_is_server || g_node_id > 0) return;
        num_nodes = 1;
        g_init_parallelism = 1;
        if (tid >= g_init_parallelism) return;
    }
    else { // TWO_SIDED
        if (!g_is_server) return;
        num_nodes = g_num_server_nodes;
    }
    global_manager->set_tid(tid);
    bind_core(tid);

    uint64_t chunk_per_node = g_synth_table_size / num_nodes;
    uint64_t chunk_per_thread = chunk_per_node / g_init_parallelism;
    uint64_t from = chunk_per_node * g_node_id + chunk_per_thread * tid + 1;
    uint64_t to = chunk_per_node * g_node_id + chunk_per_thread * (tid + 1);
    if (tid == g_init_parallelism - 1) {
        if (g_node_id == num_nodes - 1) to = g_synth_table_size;
        else to = chunk_per_node * (g_node_id + 1);
    }

    auto bulk_keys = new uint64_t[to - from + 1];
    for (uint64_t key=from; key<=to; key++)
        bulk_keys[key - from] = key;
    the_index->bulk_load(bulk_keys, to - from + 1);
    delete[] bulk_keys;
#endif
}

// server
txn_t* ycsb_workload_t::create_interactive(txn_id_t txn_id) {
    return new ycsb_interactive_t(txn_id);
}

// server
txn_t* ycsb_workload_t::create_stored_procedure(txn_id_t txn_id) {
    return new ycsb_stored_procedure_t(txn_id);
}

// client
txn_t* ycsb_workload_t::create_txn(base_query_t* query) {
    if constexpr (std::is_same_v<TXN, stored_procedure_t>)
        return new ycsb_stored_procedure_t(query);
    else
        return new ycsb_interactive_t(query);
}

base_query_t* ycsb_workload_t::gen_query() {
    auto query = new ycsb_query_t();
    query->gen_requests();
    return query;
}

base_query_t* ycsb_workload_t::gen_query(base_query_t* query) {
    auto q = static_cast<ycsb_query_t*>(query);
    auto new_query = new ycsb_query_t(q->get_requests(), q->get_request_count());
    return new_query;
}

uint64_t ycsb_workload_t::get_primary_key(row_t* row, uint32_t table_id) {
    uint64_t key = -1;
    row->get_value(the_schema, 0, &key);
    assert(key != -1);
    return key;
}

void ycsb_workload_t::set_partition(uint32_t table_id) {
    uint64_t partition_size = g_synth_table_size / g_num_server_nodes;
    _partition_size[table_id] = partition_size;

    auto& partition = _partitions[table_id];
    assert(partition.size() == 0);

    partition.push_back(std::numeric_limits<uint64_t>::min());
    for (uint32_t i=0; i<g_num_server_nodes-1; i++)
        partition.push_back(partition_size + partition[i]);
    partition.push_back(std::numeric_limits<uint64_t>::max());
}

#endif // end of WORKLOAD == YCSB
