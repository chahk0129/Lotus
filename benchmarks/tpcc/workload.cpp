#include "benchmarks/tpcc/workload.h"
#include "benchmarks/tpcc/const.h"
#include "benchmarks/tpcc/stored_procedure.h"
#include "benchmarks/tpcc/interactive.h"
#include "benchmarks/tpcc/query.h"
#include "benchmarks/tpcc/helper.h"
#include "utils/helper.h"
#include "system/thread.h"
#include "system/manager.h"
#include "system/workload.h"
#include "system/query.h"
#include "system/global.h"
#include "storage/table.h"
#include "storage/catalog.h"
#include "storage/row.h"
#include "client/transport.h"
#include "index/idx_wrapper.h"
#include "index/onesided/dex/leanstore_tree.h"
#include "index/onesided/sherman/Tree.h"
#include "index/twosided/partitioned/idx.h"
#include "index/twosided/non_partitioned/idx.h"

#if WORKLOAD == TPCC

RC tpcc_workload_t::init() {
    workload_t::init();
    string schema_file = "../benchmarks/tpcc/";
#if TPCC_SMALL
    schema_file += "short_schema.txt";
#else
    schema_file += "full_schema.txt";
#endif
    cout << "reading schema file: " << schema_file.c_str() << endl;
    if (!init_schema(schema_file)) {
        assert(false);
        return ERROR;
    }

    for(uint32_t table_id=TAB_WAREHOUSE; table_id<=TAB_STOCK; table_id++)
        set_partition(table_id);
    init_table();
    return RCOK;
}

catalog_t* tpcc_workload_t::init_schema(string schema_file) {
    schema = workload_t::init_schema(schema_file);

    t_warehouse = _tables[TAB_WAREHOUSE];
    t_district = _tables[TAB_DISTRICT];
    t_customer = _tables[TAB_CUSTOMER];
    t_history = _tables[TAB_HISTORY];
    t_neworder = _tables[TAB_NEWORDER];
    t_order = _tables[TAB_ORDER];
    t_orderline = _tables[TAB_ORDERLINE];
    t_item = _tables[TAB_ITEM];
    t_stock = _tables[TAB_STOCK];

    i_item = _indexes[IDX_ITEM];
    i_warehouse = _indexes[IDX_WAREHOUSE];
    i_district = _indexes[IDX_DISTRICT];
    i_customer_id = _indexes[IDX_CUSTOMER_ID];
    i_customer_last = _indexes[IDX_CUSTOMER_LAST];
    i_stock = _indexes[IDX_STOCK];
    i_order = _indexes[IDX_ORDER];
    i_order_cust = _indexes[IDX_ORDER_CUST];
    i_orderline = _indexes[IDX_ORDERLINE];
    i_neworder = _indexes[IDX_NEWORDER];

    return schema;
}

void tpcc_workload_t::table_to_indexes(uint32_t table_id, set<INDEX *> * indexes) {
    switch(table_id) {
        case TAB_WAREHOUSE:
            indexes->insert(i_warehouse);
            return;
        case TAB_DISTRICT:
            indexes->insert(i_district);
            return;
        case TAB_CUSTOMER:
            indexes->insert(i_customer_id);
            indexes->insert(i_customer_last);
            return;
        case TAB_HISTORY:
            return;
        case TAB_NEWORDER:
            indexes->insert(i_neworder);
            return;
        case TAB_ORDER:
            indexes->insert(i_order);
            indexes->insert(i_order_cust);
            return;
        case TAB_ORDERLINE:
            indexes->insert(i_orderline);
            return;
        case TAB_ITEM:
            indexes->insert(i_item);
            return;
        case TAB_STOCK:
            indexes->insert(i_stock);
            return;
        default:
            assert(false);
    }
}


uint32_t tpcc_workload_t::index_to_table(uint32_t index_id) {
    switch(index_id) {
        case IDX_ITEM:
            return TAB_ITEM;
        case IDX_WAREHOUSE:
            return TAB_WAREHOUSE;
        case IDX_DISTRICT:
            return TAB_DISTRICT;
        case IDX_CUSTOMER_ID:
        case IDX_CUSTOMER_LAST:
            return TAB_CUSTOMER;
        case IDX_STOCK:
            return TAB_STOCK;
        case IDX_ORDER:
        case IDX_ORDER_CUST:
            return TAB_ORDER;
        case IDX_NEWORDER:
            return TAB_NEWORDER;
        case IDX_ORDERLINE:
            return TAB_ORDERLINE;
        default: {
            assert(false);
            return 0;
        }
    }
}

uint32_t tpcc_workload_t::key_to_node(uint64_t key, uint32_t table_id) {
    assert(false);
    if (table_id == TAB_WAREHOUSE)
        return tpcc_helper_t::wh_to_node(key);
    else if (table_id == TAB_DISTRICT)
        return tpcc_helper_t::wh_to_node(key / DIST_PER_WARE);
    else
        assert(false);
    return 0;
}

void tpcc_workload_t::init_table() {
    num_wh = g_num_wh;

/******** fill in data ************/
// data filling process:
//- item
//- wh
//    - stock
//     - dist
//      - cust
//          - hist
//        - order
//        - new order
//        - order line
/**********************************/
    std::vector<std::thread> thd;
    for(uint32_t i=1; i<g_init_parallelism; i++)
        thd.push_back(std::thread(thread_init_table, this, i));
    thread_init_table(this, 0);
    for(auto& t: thd) t.join();
    
    // pthread_t * p_thds = new pthread_t[g_num_wh - 1];
    // for (uint32_t i = 0; i < g_num_wh - 1; i++)
    //     pthread_create(&p_thds[i], NULL, threadInitWarehouse, this);
    // threadInitWarehouse(this);
    // for (uint32_t i = 0; i < g_num_wh - 1; i++)
    //     pthread_join(p_thds[i], NULL);
}


void tpcc_workload_t::init_tab_item() {
    char* buffer = transport->get_buffer();
    auto schema = t_item->get_schema();
    for (uint64_t i=1; i<=g_max_items; i++) {
        row_t * row = nullptr;
        uint32_t node_id = get_partition_by_key(i, TAB_ITEM);
        #if TRANSPORT == ONE_SIDED
        auto row_addr = rpc_alloc(node_id, sizeof(row_t));
        row = reinterpret_cast<row_t*>(buffer);
        #else // TWO_SIDED
        RC rc = t_item->get_new_row(row);
        assert(rc == RCOK);
        #endif
        row->init_manager();

        row->set_value(schema, I_ID, &i);
        uint64_t id = URand(1L,10000L);
        row->set_value(schema, I_IM_ID, &id);
        char name[24];
        MakeAlphaString(14, 24, name);
        row->set_value(schema, I_NAME, name);
        id = URand(1, 100);
        row->set_value(schema, I_PRICE, &id);
        char data[50];
        MakeAlphaString(26, 50, data);
        if (RAND(10) == 0)
            strcpy(data, "original");
        row->set_value(schema, I_DATA, data);

        value_t value;
        #if TRANSPORT == ONE_SIDED
        transport->write(buffer, row_addr, sizeof(row_t));
        value.addr = row_addr;
        #else // TWO_SIDED
        value.row = row;
        #endif
        index_insert(i_item, i, value);
    }
}

// TODO: adjust the size of row based on tpcc table schema
void tpcc_workload_t::init_tab_wh(uint64_t wid) {
    auto schema = t_warehouse->get_schema();
    row_t* row = nullptr;
    uint32_t node_id = get_partition_by_key(wid, TAB_WAREHOUSE);
#if TRANSPORT == ONE_SIDED
    char* buffer = transport->get_buffer();
    auto row_addr = rpc_alloc(node_id, sizeof(row_t));
    row = reinterpret_cast<row_t*>(buffer);
#else // TWO_SIDED
    RC rc = t_warehouse->get_new_row(row);
    assert(rc == RCOK);
#endif
    row->init_manager();
    row->set_value(schema, W_ID, &wid);
    char name[10];
    MakeAlphaString(6, 10, name);
    row->set_value(schema, W_NAME, name);
    char street[20];
    MakeAlphaString(10, 20, street);
    row->set_value(schema, W_STREET_1, street);
    MakeAlphaString(10, 20, street);
    row->set_value(schema, W_STREET_2, street);
    MakeAlphaString(10, 20, street);
    row->set_value(schema, W_CITY, street);
    char state[2];
    MakeAlphaString(2, 2, state);
    row->set_value(schema, W_STATE, state);
    char zip[9];
    MakeNumberString(9, 9, zip);
    row->set_value(schema, W_ZIP, zip);
    double tax = (double)URand(0L, 200L) / 1000.0;
    double w_ytd=300000.00;
    row->set_value(schema, W_TAX, &tax);
    row->set_value(schema, W_YTD, &w_ytd);

    value_t value;
#if TRANSPORT == ONE_SIDED
    transport->write(buffer, row_addr, sizeof(row_t));
    value.addr = row_addr;
#else // TWO_SIDED
    value.row = row;
#endif
    index_insert(i_warehouse, wid, value);
    return;
}

void tpcc_workload_t::init_tab_dist(uint64_t wid) {
    char* buffer = transport->get_buffer();
    auto schema = t_district->get_schema();
    for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
        row_t* row = nullptr;
        uint64_t key = distKey(wid, did);
        #if TRANSPORT == ONE_SIDED
        uint32_t node_id = get_partition_by_key(key, TAB_DISTRICT);
        auto row_addr = rpc_alloc(node_id, sizeof(row_t));
        row = reinterpret_cast<row_t*>(buffer);
        #else // TWO_SIDED
        RC rc = t_district->get_new_row(row);
        assert(rc == RCOK);
        #endif
        row->init_manager();

        row->set_value(schema, D_ID, &did);
        row->set_value(schema, D_W_ID, &wid);
        char name[10];
        MakeAlphaString(6, 10, name);
        row->set_value(schema, D_NAME, name);
        char street[20];
        MakeAlphaString(10, 20, street);
        row->set_value(schema, D_STREET_1, street);
        MakeAlphaString(10, 20, street);
        row->set_value(schema, D_STREET_2, street);
        MakeAlphaString(10, 20, street);
        row->set_value(schema, D_CITY, street);
        char state[2];
        MakeAlphaString(2, 2, state);
        row->set_value(schema, D_STATE, state);
        char zip[9];
        MakeNumberString(9, 9, zip);
        row->set_value(schema, D_ZIP, zip);
        double tax = (double)URand(0L, 200L) / 1000.0;
        double w_ytd=30000.00;
        row->set_value(schema, D_TAX, &tax);
        row->set_value(schema, D_YTD, &w_ytd);
        int64_t id = 3001;
        row->set_value(schema, D_NEXT_O_ID, &id);

        value_t value;
        #if TRANSPORT == ONE_SIDED
        transport->write(buffer, row_addr, sizeof(row_t));
        value.addr = row_addr;
        #else // TWO_SIDED
        value.row = row;
        #endif
        index_insert(i_district, key, value);
    }
}

void tpcc_workload_t::init_tab_stock(uint64_t wid) {
    char* buffer = transport->get_buffer();
    auto schema = t_stock->get_schema();
    for (uint64_t iid = 1; iid <= g_max_items; iid++) {
        row_t* row = nullptr;
        uint64_t key = stockKey(wid, iid);
        uint32_t node_id = get_partition_by_key(key, TAB_STOCK);
        #if TRANSPORT == ONE_SIDED
        auto row_addr = rpc_alloc(node_id, sizeof(row_t));
        row = reinterpret_cast<row_t*>(buffer);
        #else // TWO_SIDED
        RC rc = t_stock->get_new_row(row);
        assert(rc == RCOK);
        #endif
        row->init_manager();

        row->set_value(schema, S_I_ID, &iid);
        row->set_value(schema, S_W_ID, &wid);
        int64_t quantity = URand(10, 100);
        int64_t remote_cnt = 0;
        row->set_value(schema, S_QUANTITY, &quantity);
        row->set_value(schema, S_REMOTE_CNT, &remote_cnt);
#if !TPCC_SMALL
        int64_t ytd = 0;
        int64_t order_cnt = 0;
        row->set_value(schema, S_YTD, &ytd);
        row->set_value(schema, S_ORDER_CNT, &order_cnt);
        char s_data[50];
        int len = MakeAlphaString(26, 50, s_data);
        if (rand() % 100 < 10) {
            int idx = URand(0, len - 8);
            strcpy(&s_data[idx], "original");
        }
        row->set_value(schema, S_DATA, s_data);
#endif
        value_t value;
        #if TRANSPORT == ONE_SIDED
        transport->write(buffer, row_addr, sizeof(row_t));
        value.addr = row_addr;
        #else // TWO_SIDED
        value.row = row;
        #endif
        index_insert(i_stock, key, value);
    }
}

void tpcc_workload_t::init_tab_cust(uint64_t did, uint64_t wid) {
    assert(g_cust_per_dist >= 1000);
    auto schema = t_customer->get_schema();
    char* buffer = transport->get_buffer();
    for (uint64_t cid = 1; cid <= g_cust_per_dist; cid++) {
        row_t* row = nullptr;
        uint64_t key = custKey(wid, did, cid);
        uint32_t node_id = get_partition_by_key(key, TAB_CUSTOMER);
        #if TRANSPORT == ONE_SIDED
        auto row_addr = rpc_alloc(node_id, sizeof(row_t));
        row = reinterpret_cast<row_t*>(buffer);
        #else // TWO_SIDED
        RC rc = t_customer->get_new_row(row);
        assert(rc == RCOK);
        #endif
        row->init_manager();

        row->set_value(schema, C_ID, &cid);
        row->set_value(schema, C_D_ID, &did);
        row->set_value(schema, C_W_ID, &wid);
        char c_last[LASTNAME_LEN];
        if (cid <= 1000)
            Lastname(cid - 1, c_last);
        else
            Lastname(NURand(255, 0, 999), c_last);
        row->set_value(schema, C_LAST, c_last);
#if !TPCC_SMALL
        char tmp[3] = "OE";
        row->set_value(schema, C_MIDDLE, tmp);
        char c_first[FIRSTNAME_LEN];
        MakeAlphaString(FIRSTNAME_MINLEN, sizeof(c_first), c_first);
        row->set_value(schema, C_FIRST, c_first);
        char street[20];
        MakeAlphaString(10, 20, street);
        row->set_value(schema, C_STREET_1, street);
        MakeAlphaString(10, 20, street);
        row->set_value(schema, C_STREET_2, street);
        MakeAlphaString(10, 20, street);
        row->set_value(schema, C_CITY, street);
        char state[2];
        MakeAlphaString(2, 2, state);
        row->set_value(schema, C_STATE, state);
        char zip[9];
        MakeNumberString(9, 9, zip);
        row->set_value(schema, C_ZIP, zip);
        char phone[16];
        MakeNumberString(16, 16, phone);
        row->set_value(schema, C_PHONE, phone);
        int64_t since = 0;
        int64_t credit_lim = 50000;
        int64_t delivery_cnt = 0;
        row->set_value(schema, C_SINCE, &since);
        row->set_value(schema, C_CREDIT_LIM, &credit_lim);
        row->set_value(schema, C_DELIVERY_CNT, &delivery_cnt);
        char c_data[500];
        MakeAlphaString(300, 500, c_data);
        row->set_value(schema, C_DATA, c_data);
#endif
        if (RAND(10) == 0) {
            char tmp[] = "GC";
            row->set_value(schema, C_CREDIT, tmp);
        } else {
            char tmp[] = "BC";
            row->set_value(schema, C_CREDIT, tmp);
        }
        double discount = RAND(5000) / 10000.0;
        row->set_value(schema, C_DISCOUNT, &discount);
        double balance = -10;
        double payment = 10;
        int64_t cnt = 1;
        row->set_value(schema, C_BALANCE, &balance);
        row->set_value(schema, C_YTD_PAYMENT, &payment);
        row->set_value(schema, C_PAYMENT_CNT, &cnt);

        value_t value;
        #if TRANSPORT == ONE_SIDED
        transport->write(buffer, row_addr, sizeof(row_t));
        value.addr = row_addr;
        #else // TWO_SIDED
        value.row = row;
        #endif

        key = custNPKey(c_last, did, wid);
        index_insert(i_customer_last, key, value);
        key = custKey(wid, did, cid);
        index_insert(i_customer_id, key, value);
    }
}

void tpcc_workload_t::init_tab_hist(uint64_t c_id, uint64_t d_id, uint64_t w_id) {
#if 0
    char* buffer = transport->get_buffer();
    auto schema = t_history->get_schema();
    row_t* row = nullptr;
    uint64_t key = histKey(w_id, d_id, c_id);
    uint32_t node_id = get_partition_by_key(key, TAB_HISTORY);
    #if TRANSPORT == ONE_SIDED
    auto row_addr = rpc_alloc(node_id, sizeof(row_t));
    row = reinterpret_cast<row_t*>(buffer);
    #else // TWO_SIDED
    RC rc = t_history->get_new_row(row);
    assert(rc == RCOK);
    #endif
    row->init_manager();

    row->set_value(schema, H_C_ID, &c_id);
    row->set_value(schema, H_C_D_ID, &d_id);
    row->set_value(schema, H_D_ID, &d_id);
    row->set_value(schema, H_C_W_ID, &w_id);
    row->set_value(schema, H_W_ID, &w_id);
    int64_t date = 0;
    row->set_value(schema, H_DATE, &date);
    double amount = 10;
    row->set_value(schema, H_AMOUNT, &amount);
#if !TPCC_SMALL
    char h_data[24];
    MakeAlphaString(12, 24, h_data);
    row->set_value(schema, H_DATA, h_data);
#endif

    value_t value;
    #if TRANSPORT == ONE_SIDED
    transport->write(buffer, row_addr, sizeof(row_t));
    value.addr = row_addr;
    #else // TWO_SIDED
    value.row = row;
    #endif
#endif
}

void tpcc_workload_t::init_tab_order(uint64_t did, uint64_t wid) {
    char* buffer = transport->get_buffer();
    catalog_t* o_schema = t_order->get_schema();
    catalog_t* ol_schema = t_orderline->get_schema();
    catalog_t* no_schema = t_neworder->get_schema();
    uint64_t perm[g_cust_per_dist];
    init_permutation(perm, wid); /* initialize permutation of customer numbers */
    for (uint64_t oid=1; oid<=g_cust_per_dist; oid++) {
        row_t* row = nullptr;
        uint64_t key = orderKey(wid, did, oid);
        #if TRANSPORT == ONE_SIDED
        uint32_t node_id = get_partition_by_key(key, TAB_ORDER);
        auto row_addr = rpc_alloc(node_id, sizeof(row_t));
        row = reinterpret_cast<row_t*>(buffer);
        #else // TWO_SIDED
        RC rc = t_order->get_new_row(row);
        assert(rc == RCOK);
        #endif
        row->init_manager();

        uint64_t o_ol_cnt = 1;
        uint64_t cid = perm[oid - 1];
        row->set_value(o_schema, O_ID, &oid);
        row->set_value(o_schema, O_C_ID, &cid);
        row->set_value(o_schema, O_D_ID, &did);
        row->set_value(o_schema, O_W_ID, &wid);
        uint64_t o_entry = 2013;
        row->set_value(o_schema, O_ENTRY_D, &o_entry);
        int64_t id = (oid < 2101)? URand(1, 10) : 0;

        row->set_value(o_schema, O_CARRIER_ID, &id);

        o_ol_cnt = URand(5, 15);
        row->set_value(o_schema, O_OL_CNT, &o_ol_cnt);
        int64_t all_local = 1;
        row->set_value(o_schema, O_ALL_LOCAL, &all_local);

        value_t value;
        #if TRANSPORT == ONE_SIDED
        transport->write(buffer, row_addr, sizeof(row_t));
        value.addr = row_addr;
        #else // TWO_SIDED
        value.row = row;
        #endif

        key = orderKey(wid, did, oid);
        index_insert(i_order, key, value);

        key = custKey(wid, did, cid);
        index_insert(i_order_cust, key, value);

        // ORDER-LINE
#if !TPCC_SMALL
        for (uint64_t ol = 1; ol <= o_ol_cnt; ol++) {
            row = nullptr;
            key = orderlineKey(wid, did, oid);
            #if TRANSPORT == ONE_SIDED
            node_id = get_partition_by_key(key, TAB_ORDERLINE);
            row_addr = rpc_alloc(node_id, sizeof(row_t));
            row = reinterpret_cast<row_t*>(buffer);
            #else // TWO_SIDED
            RC rc = t_orderline->get_new_row(row);
            assert(rc == RCOK);
            #endif
            row->init_manager();

            row->set_value(ol_schema, OL_O_ID, &oid);
            row->set_value(ol_schema, OL_D_ID, &did);
            row->set_value(ol_schema, OL_W_ID, &wid);
            row->set_value(ol_schema, OL_NUMBER, &ol);
            int64_t id = URand(1, 100000);
            row->set_value(ol_schema, OL_I_ID, &id);
            row->set_value(ol_schema, OL_SUPPLY_W_ID, &wid);
            double amount = 0;
            int64_t date = 0;
            if (oid < 2101) {
                row->set_value(ol_schema, OL_DELIVERY_D, &o_entry);
                row->set_value(ol_schema, OL_AMOUNT, &amount);
            } else {
                row->set_value(ol_schema, OL_DELIVERY_D, &date);
                amount = URand(1, 999999)/100.0;
                row->set_value(ol_schema, OL_AMOUNT, &amount);
            }
            int64_t quantity = 5;
            row->set_value(ol_schema, OL_QUANTITY, &quantity);
            char ol_dist_info[24];
            MakeAlphaString(24, 24, ol_dist_info);
            row->set_value(ol_schema, OL_DIST_INFO, ol_dist_info);

            #if TRANSPORT == ONE_SIDED
            transport->write(buffer, row_addr, sizeof(row_t));
            value.addr = row_addr;
            #else // TWO_SIDED
            value.row = row;
            #endif
            index_insert(i_orderline, key, value);
        }
#endif
        // NEW ORDER
        if (oid > 2100) {
            row = nullptr;
            key = neworderKey(wid, did);
            #if TRANSPORT == ONE_SIDED
            node_id = get_partition_by_key(key, TAB_NEWORDER);
            row_addr = rpc_alloc(node_id, sizeof(row_t));
            row = reinterpret_cast<row_t*>(buffer);
            #else // TWO_SIDED
            rc = t_neworder->get_new_row(row);
            assert(rc == RCOK);
            #endif
            row->init_manager();

            row->set_value(no_schema, NO_O_ID, &oid);
            row->set_value(no_schema, NO_D_ID, &did);
            row->set_value(no_schema, NO_W_ID, &wid);

            #if TRANSPORT == ONE_SIDED
            transport->write(buffer, row_addr, sizeof(row_t));
            value.addr = row_addr;
            #else // TWO_SIDED
            value.row = row;
            #endif
            index_insert(i_neworder, key, value);
        }
    }
}

/*==================================================================+
| ROUTINE NAME
| InitPermutation
+==================================================================*/

void tpcc_workload_t::init_permutation(uint64_t* perm_c_id, uint64_t wid) {
    // Init with consecutive values
    for(uint32_t i=0; i<g_cust_per_dist; i++)
        perm_c_id[i] = i+1;

    // shuffle
    for(uint32_t i=0; i<g_cust_per_dist-1; i++) {
        uint64_t j = URand(i+1, g_cust_per_dist-1);
        uint64_t tmp = perm_c_id[i];
        perm_c_id[i] = perm_c_id[j];
        perm_c_id[j] = tmp;
    }
}


/*==================================================================+
| ROUTINE NAME
| GetPermutation
+==================================================================*/
void tpcc_workload_t::init_table_parallel(uint32_t tid) {
#if TRANSPORT == ONE_SIDED
    if (g_is_server) return;
    // for one-sided RDMA, each client intiializes all tables
    uint32_t num_nodes = g_num_client_nodes;
    // Sherman has synchronization issue ...
    // it crashes if the DB is loaded with multiple nodes and large number of threads
    num_nodes = 1;
    if (g_node_id > 0) return;
    // g_init_parallelism = g_init_parallelism > 4 ? 4 : g_init_parallelism;
    // if (tid > g_init_parallelism) return;
#else // TWO_SIDED
    if (!g_is_server) return;
    // for two-sided RDMA, each server initializes its own partitioned tables
    uint32_t num_nodes = g_num_server_nodes;
#endif
    global_manager->set_tid(tid);
    bind_core(tid);

    uint64_t w_per_node = g_num_wh * g_num_server_nodes / num_nodes;
    uint64_t w_per_thread = w_per_node / g_init_parallelism;
    uint64_t from = w_per_node * g_node_id + w_per_thread * tid + 1;
    uint64_t to = w_per_node * g_node_id + w_per_thread * (tid + 1);
    if (tid == g_init_parallelism - 1) {
        if (g_node_id == num_nodes - 1) to = g_num_wh * g_num_server_nodes;
        else to = w_per_node * (g_node_id + 1);
    }

#if TRANSPORT == ONE_SIDED
    if (g_node_id == 0) {
        if (tid == 0)
            init_tab_item();
    }
#else // TWO_SIDED
    if (tid == 0)
        init_tab_item();
#endif
    for (uint64_t wid=from; wid<=to; wid++) {
        init_tab_wh(wid);
        init_tab_dist(wid);
        init_tab_stock(wid);
        for (uint64_t did=1; did<=DIST_PER_WARE; did++) {
            init_tab_cust(did, wid);
            init_tab_order(did, wid);
            for (uint64_t cid=1; cid<=g_cust_per_dist; cid++)
                init_tab_hist(cid, did, wid);
        }
    }
}

// server
txn_t* tpcc_workload_t::create_stored_procedure(txn_id_t txn_id) {
    return new tpcc_stored_procedure_t(txn_id);
}

// server
txn_t* tpcc_workload_t::create_interactive(txn_id_t txn_id) {
    return new tpcc_interactive_t(txn_id);
}

// client
txn_t* tpcc_workload_t::create_txn(base_query_t* query) {
    if constexpr (std::is_same_v<TXN, stored_procedure_t>)
        return new tpcc_stored_procedure_t(query);
    else
        return new tpcc_interactive_t(query);  
}

base_query_t* tpcc_workload_t::gen_query() {
    while (true) {
        double x = global_manager->rand_double();
        if (x < g_perc_payment)
            return new tpcc_payment_t();
        x -= g_perc_payment;
        if (x < g_perc_new_order)
            return new tpcc_new_order_t();
        x -= g_perc_new_order;
        if (x < g_perc_order_status)
            return new tpcc_order_status_t();
        x -= g_perc_order_status;
        if (x < g_perc_delivery)
            return new tpcc_delivery_t();
        x -= g_perc_delivery;
        if (x < PERC_STOCKLEVEL)
            return new tpcc_stock_level_t();
        assert(false);
    }
    return nullptr;
}

base_query_t* tpcc_workload_t::gen_query(base_query_t* query) {
    tpcc_query_t* q = (tpcc_query_t*)query;
    switch(q->type) {
        case TPCC_PAYMENT:      return new tpcc_payment_t((static_cast<tpcc_payment_t*>(query)));
        case TPCC_NEW_ORDER:    return new tpcc_new_order_t((static_cast<tpcc_new_order_t*>(query)));
        case TPCC_ORDER_STATUS: return new tpcc_order_status_t((static_cast<tpcc_order_status_t*>(query)));
        case TPCC_DELIVERY:     return new tpcc_delivery_t((static_cast<tpcc_delivery_t*>(query)));
        case TPCC_STOCK_LEVEL:  return new tpcc_stock_level_t((static_cast<tpcc_stock_level_t*>(query)));
        default:                assert(false);
    }
}

uint64_t tpcc_workload_t::get_primary_key(row_t* row, uint32_t table_id) {
    int64_t wid, did, cid, oid, iid, olnum;
    switch(table_id) {
        case TAB_WAREHOUSE: {
            row->get_value(t_warehouse->get_schema(), W_ID, &wid);
            return wid;
        }
        case TAB_DISTRICT: {
            row->get_value(t_district->get_schema(), D_ID, &did);
            row->get_value(t_district->get_schema(), D_W_ID, &wid);
            return distKey(wid, did);
        }
        case TAB_CUSTOMER: {
            row->get_value(t_customer->get_schema(), C_ID, &cid);
            row->get_value(t_customer->get_schema(), C_D_ID, &did);
            row->get_value(t_customer->get_schema(), C_W_ID, &wid);
            return custKey(wid, did, cid);
        }
        case TAB_HISTORY: {
            assert(false);
        }
        case TAB_NEWORDER: {
            row->get_value(t_neworder->get_schema(), NO_O_ID, &oid);
            row->get_value(t_neworder->get_schema(), NO_D_ID, &did);
            row->get_value(t_neworder->get_schema(), NO_W_ID, &wid);
            return orderKey(wid, did, oid);
        }
        case TAB_ORDER: {
            row->get_value(t_order->get_schema(), O_ID, &oid);
            row->get_value(t_order->get_schema(), O_D_ID, &did);
            row->get_value(t_order->get_schema(), O_W_ID, &wid);
            return orderKey(wid, did, oid);
        }
        case TAB_ORDERLINE: {
            row->get_value(t_orderline->get_schema(), OL_O_ID, &oid);
            row->get_value(t_orderline->get_schema(), OL_D_ID, &did);
            row->get_value(t_orderline->get_schema(), OL_W_ID, &wid);
            row->get_value(t_orderline->get_schema(), OL_NUMBER, &olnum);
            return orderlinePrimaryKey(wid, did, oid, olnum);
        }
        case TAB_ITEM: {
            row->get_value(t_item->get_schema(), I_ID, &iid);
            return iid;
        }
        case TAB_STOCK: {
            row->get_value(t_stock->get_schema(), S_I_ID, &iid);
            row->get_value(t_stock->get_schema(), S_W_ID, &wid);
            return stockKey(wid, iid);
        }
        default:
            assert(false);
    }
}

uint64_t tpcc_workload_t::get_index_key(row_t* row, uint32_t index_id) {
    int64_t wid, did, cid, oid;
    char * c_last;
    switch(index_id) {
        case IDX_ITEM:
            return get_primary_key(row, TAB_ITEM);
        case IDX_WAREHOUSE:
            return get_primary_key(row, TAB_WAREHOUSE);
        case IDX_DISTRICT:
            return get_primary_key(row, TAB_DISTRICT);
        case IDX_CUSTOMER_ID:
            return get_primary_key(row, TAB_CUSTOMER);
        case IDX_STOCK:
            return get_primary_key(row, TAB_STOCK);
        case IDX_ORDER:
            return get_primary_key(row, TAB_ORDER);
        case IDX_NEWORDER:
            row->get_value(t_neworder->get_schema(), NO_W_ID, &wid);
            row->get_value(t_neworder->get_schema(), NO_D_ID, &did);
            return neworderKey(wid, did);
        case IDX_CUSTOMER_LAST:
            row->get_value(t_customer->get_schema(), C_W_ID, &wid);
            row->get_value(t_customer->get_schema(), C_D_ID, &did);
            c_last = row->get_value(t_customer->get_schema(), C_LAST);
            return custNPKey(c_last, did, wid);
        case IDX_ORDER_CUST:
            row->get_value(t_order->get_schema(), O_W_ID, &wid);
            row->get_value(t_order->get_schema(), O_D_ID, &did);
            row->get_value(t_order->get_schema(), O_C_ID, &cid);
            return custKey(wid, did, cid);
        case IDX_ORDERLINE:
            row->get_value(t_orderline->get_schema(), OL_W_ID, &wid);
            row->get_value(t_orderline->get_schema(), OL_D_ID, &did);
            row->get_value(t_orderline->get_schema(), OL_O_ID, &oid);
            return orderlineKey(wid, did, oid);
        default:
            assert(false);
    }
}

void tpcc_workload_t::set_partition(uint32_t table_id) {
    uint64_t partition_size = 0;
    switch (table_id) {
        case TAB_WAREHOUSE: partition_size = (g_num_wh * g_num_server_nodes) / g_num_server_nodes; break;
        case TAB_DISTRICT:  partition_size = distKey(DIST_PER_WARE, g_num_wh) / g_num_server_nodes; break;
        case TAB_CUSTOMER:  partition_size = custKey(g_num_wh, DIST_PER_WARE, g_cust_per_dist) / g_num_server_nodes; break;
        case TAB_STOCK:     partition_size = stockKey(g_num_wh, g_max_items) / g_num_server_nodes; break;
        case TAB_ORDER:     partition_size = orderKey(g_num_wh, DIST_PER_WARE, g_cust_per_dist) / g_num_server_nodes; break;
        case TAB_ORDERLINE: partition_size = orderlineKey(g_num_wh, DIST_PER_WARE, g_cust_per_dist) / g_num_server_nodes; break;
        case TAB_NEWORDER:  partition_size = neworderKey(g_num_wh, DIST_PER_WARE) / g_num_server_nodes; break;
        case TAB_HISTORY:   return;
        // case TAB_HISTORY:   partition_size = histKey(g_num_wh, DIST_PER_WARE, g_cust_per_dist) / g_num_server_nodes; break;
        case TAB_ITEM:      partition_size = g_max_items / g_num_server_nodes; break;
        default:            assert(false);
    }
    
    _partition_size[table_id] = partition_size;
    auto& partition = _partitions[table_id];
    assert(partition.size() == 0);
    partition.push_back(std::numeric_limits<uint64_t>::min());
    for (uint32_t i=0; i<g_num_server_nodes-1; i++)
        partition.push_back(partition_size + partition[i]);
    partition.push_back(std::numeric_limits<uint64_t>::max());
}
#endif
