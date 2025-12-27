#pragma once

#include "system/global.h"
#include "system/workload.h"
#include "utils/helper.h"
#include "txn/id.h"

class base_query_t;
class txn_manager_t;
class stored_procedure_t;
class interactive_t;
class catalog_t;
class row_t;

class ycsb_workload_t : public workload_t {
public:
    RC         init();
    void       init_table();
    catalog_t* init_schema(string schema_file);

    txn_t*     create_txn(base_query_t* query);
    txn_t*     create_stored_procedure(txn_id_t txn_id);
    txn_t*     create_interactive(txn_id_t txn_id);

    base_query_t* gen_query();
    base_query_t* gen_query(base_query_t* query);

    uint64_t get_primary_key(row_t* row, uint32_t table_id = 0);
    uint64_t get_index_key(row_t* row, uint32_t index_id) { return get_primary_key(row); }

    INDEX*   get_index(uint32_t index_id) { return the_index; }
    table_t* get_table(uint32_t table_id) { return the_table; }

    void     set_partition(uint32_t table_id = 0);

    catalog_t*       the_schema;
    INDEX*           the_index;
    table_t*         the_table;

private:
    void  init_table_parallel(uint32_t tid);
    void  init_index_parallel(uint32_t tid);
    static void* thread_init_table(ycsb_workload_t* wl, uint32_t tid) {
        #if PARTITIONED
        wl->init_index_parallel(tid);
        #else
        wl->init_table_parallel(tid);
        #endif
        return nullptr;
    }


};