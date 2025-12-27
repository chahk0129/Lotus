#include "system/query.h"
#include "benchmarks/tpcc/query.h"
#include "benchmarks/tpcc/const.h"
#include "benchmarks/tpcc/helper.h"
#include "benchmarks/tpcc/workload.h"
#include "storage/table.h"
#include "system/manager.h"

#if WORKLOAD == TPCC

// In distributed system, w_id ranges from [0, g_num_wh * g_num_nodes)
// global_w_id = local_w_id + g_node_id * g_num_wh

tpcc_query_t::tpcc_query_t() : base_query_t() {
    // generate the local warehouse id.
    uint32_t local_w_id = URand(1, g_num_wh);
    // global warehouse id
    w_id = g_num_wh * g_node_id + local_w_id;
}

tpcc_query_t::tpcc_query_t(tpcc_query_t* query) {
    type = query->type;
    w_id = query->w_id;
    d_id = query->d_id;
    c_id = query->c_id;
}

///////////////////////////////////////////
// Payment
///////////////////////////////////////////
tpcc_payment_t::tpcc_payment_t(): tpcc_query_t() { gen_requests(); }

void tpcc_payment_t::gen_requests() {
    type = TPCC_PAYMENT;
    d_w_id = w_id;

    d_id = URand(1, DIST_PER_WARE);
    uint32_t x = URand(1, 100);
    uint32_t y = URand(1, 100);

    if(x >= g_payment_remote_perc) {
        // home warehouse
        c_d_id = d_id;
        c_w_id = w_id;
    } else {
        // remote warehouse
        c_d_id = URand(1, DIST_PER_WARE);
        if(g_num_wh * g_num_nodes > 1) {
            do {
                c_w_id = URand(1, g_num_wh * g_num_nodes);
            } while(c_w_id == w_id);
        } else
            c_w_id = w_id;
    }
    if(y <= 60) {
        // by last name
        by_last_name = true;
        Lastname( NURand(255, 0, 999), c_last );
    } else {
        // by cust id
        by_last_name = false;
        c_id = NURand(1023, 1, g_cust_per_dist);
    }
    h_amount = URand(1, 5000);
}

///////////////////////////////////////////
// New Order
///////////////////////////////////////////

tpcc_new_order_t::tpcc_new_order_t() : tpcc_query_t() { gen_requests(); }

void tpcc_new_order_t::gen_requests() { 
    type = TPCC_NEW_ORDER;

    d_id = URand(1, DIST_PER_WARE);
    c_id = NURand(1023, 1, g_cust_per_dist);
    uint32_t rbk = URand(1, 100);
    ol_cnt = URand(5, 15);
    o_entry_d = 2013;
    items = new Item_no[ol_cnt];
    remote = false;

    for (uint32_t oid = 0; oid < ol_cnt; oid ++) {
        items[oid].ol_i_id = NURand(8191, 1, g_max_items);
        // handle roll back. invalid ol_i_id.
        if (oid == ol_cnt - 1 && rbk == 1)
            items[oid].ol_i_id = 0;
        uint32_t x = URand(1, 100);
        if (x > g_new_order_remote_perc || (g_num_wh == 1 && g_num_nodes == 1))
            items[oid].ol_supply_w_id = w_id;
        else  {
            do {
                items[oid].ol_supply_w_id = RAND(g_num_wh * g_num_nodes) + 1;
            } while (items[oid].ol_supply_w_id == w_id);
            remote = true;
        }
        items[oid].ol_quantity = URand(1, 10);
    }
    // Remove duplicate items
    for (uint32_t i = 0; i < ol_cnt; i ++) {
        for (uint32_t j = 0; j < i; j++) {
            if (items[i].ol_i_id == items[j].ol_i_id) {
                items[i] = items[ol_cnt - 1];
                ol_cnt --;
                i--;
            }
        }
    }
#if DEBUG_ASSERT
    for (uint32_t i = 0; i < ol_cnt; i ++)
        for (uint32_t j = 0; j < i; j++)
            assert(items[i].ol_i_id != items[j].ol_i_id);
#endif
}

tpcc_new_order_t::~tpcc_new_order_t() {
    assert(items);
    delete items;
}

///////////////////////////////////////////
// Order Status
///////////////////////////////////////////
tpcc_order_status_t::tpcc_order_status_t() : tpcc_query_t() { gen_requests(); }

void tpcc_order_status_t::gen_requests() {
    type = TPCC_ORDER_STATUS;
    d_id = URand(1, DIST_PER_WARE);
    uint32_t y = URand(1, 100);
    if(y <= 60) {
        // by last name
        by_last_name = true;
        Lastname( NURand(255, 0, 999), c_last );
    } else {
        // by cust id
        by_last_name = false;
        c_id = NURand(1023, 1, g_cust_per_dist);
    }
}

///////////////////////////////////////////
// Delivery
///////////////////////////////////////////
tpcc_delivery_t::tpcc_delivery_t() : tpcc_query_t() { gen_requests(); }

void tpcc_delivery_t::gen_requests() {
    // generate the local warehouse id.
    type = TPCC_DELIVERY;
    d_id = URand(1, DIST_PER_WARE);
    o_carrier_id = URand(1, 10);
    ol_delivery_d = 2017;
}

///////////////////////////////////////////
// Stock Level
///////////////////////////////////////////
tpcc_stock_level_t::tpcc_stock_level_t(): tpcc_query_t() { gen_requests(); }

void tpcc_stock_level_t::gen_requests() {
    // generate the local warehouse id.
    type = TPCC_STOCK_LEVEL;
    d_id = URand(1, DIST_PER_WARE);
    threshold = URand(10, 20);
}

#endif
