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

class tpcc_workload_t : public workload_t {
public:
    RC         init();
    void       init_table();
    catalog_t* init_schema(string schema_file);

    txn_t*     create_txn(base_query_t* query);
    txn_t*     create_stored_procedure(txn_id_t txn_id);
    txn_t*     create_interactive(txn_id_t txn_id);

    base_query_t* gen_query();
    base_query_t* gen_query(base_query_t* query);

    uint32_t key_to_node(uint64_t key, uint32_t table_id = 0);
    uint32_t index_to_table(uint32_t index_id);
    void     table_to_indexes(uint32_t table_id, set<INDEX*>* indexes);

    uint64_t get_primary_key(row_t* row, uint32_t table_id = 0);

    uint64_t get_index_key(row_t* row, uint32_t index_id);
    table_t* get_table(uint32_t table_id) { return _tables[table_id]; }
    INDEX*   get_index(uint32_t index_id) { return _indexes[index_id]; }

    void     set_partition(uint32_t table_id = 0);

    catalog_t* schema;

    table_t* t_warehouse;
    table_t* t_district;
    table_t* t_customer;
    table_t* t_history;
    table_t* t_neworder;
    table_t* t_order;
    table_t* t_orderline;
    table_t* t_item;
    table_t* t_stock;

    INDEX*   i_item;
    INDEX*   i_warehouse;
    INDEX*   i_district;
    INDEX*   i_customer_id;
    INDEX*   i_customer_last;
    INDEX*   i_stock;
    INDEX*   i_order; // key = (w_id, d_id, o_id)
    INDEX*   i_order_cust;
    INDEX*   i_orderline; // key = (w_id, d_id, o_id)
    INDEX*   i_neworder;

    bool ** delivering;
    uint32_t next_tid;

private:
    uint64_t num_wh;
    void init_tab_item();
    void init_tab_wh(uint64_t wid);
    void init_tab_dist(uint64_t w_id);
    void init_tab_stock(uint64_t w_id);
    void init_tab_cust(uint64_t d_id, uint64_t w_id);
    void init_tab_hist(uint64_t c_id, uint64_t d_id, uint64_t w_id);
    void init_tab_order(uint64_t d_id, uint64_t w_id);

    void init_permutation(uint64_t * perm_c_id, uint64_t wid);

    void init_table_parallel(uint32_t tid);
    static void* thread_init_table(tpcc_workload_t* wl, uint32_t tid) {
        wl->init_table_parallel(tid);
        return nullptr;
    }
    // static void * threadInitWarehouse(void * This);
};
