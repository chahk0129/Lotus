#pragma once

#include "system/global.h"
#include "system/global_address.h"
#include "txn/id.h"
#include <vector>
#include <map>

class table_t;
class txn_t;
class base_query_t;
class row_t;
class message_t;
class catalog_t;

class workload_t {
public:

    virtual RC              init();
    virtual catalog_t*      init_schema(string path);

    virtual txn_t*          create_txn(base_query_t* query) = 0;
    virtual txn_t*          create_stored_procedure(txn_id_t txn_id) = 0;
    virtual txn_t*          create_interactive(txn_id_t txn_id) = 0;

    virtual base_query_t*   gen_query() = 0;
    virtual base_query_t*   gen_query(base_query_t* query) = 0;

    virtual uint64_t        get_primary_key(row_t* row, uint32_t table_id = 0) = 0;
    virtual uint64_t        get_index_key(row_t* row, uint32_t index_id) = 0;
    
    virtual INDEX*          get_index(uint32_t index_id) { return nullptr; }
    virtual table_t*        get_table(uint32_t table_id) { return nullptr; }

    uint32_t                get_partition_by_key(uint64_t key, uint32_t table_id = 0);
    uint32_t                get_partition_by_node(uint32_t node_id, uint32_t table_id = 0);
    virtual void            set_partition(uint32_t table_id = 0) = 0;

    uint64_t                rpc_alloc_chunk(uint32_t node_id, uint64_t size);
    global_addr_t           rpc_alloc(uint32_t node_id, uint64_t size);
    global_addr_t           rpc_alloc(uint64_t size);

    void                    set_root(global_addr_t addr, uint32_t index_id);
    global_addr_t           get_root(uint32_t index_id, uint32_t node_id);

    void                    set_index_root(uint64_t addr, uint32_t index_id);
    uint64_t                get_index_root(uint32_t index_id);

protected:
    INDEX*                  create_index(uint32_t index_id);
    void                    index_insert(INDEX* index, uint64_t key, value_t value);
    // tables
    std::vector<table_t*>             _tables;

    // indexes
    std::vector<INDEX*>               _indexes;
    std::map<uint32_t, uint64_t>      _index_roots;

    // partition info
    std::vector<uint64_t>              _partition_size;
    std::vector<std::vector<uint64_t>> _partitions;
};