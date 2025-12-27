#pragma once

#include "system/global.h"
#include "utils/helper.h"
#include "system/query.h"

// items of new order transaction
struct Item_no {
    uint64_t ol_i_id;
    uint64_t ol_supply_w_id;
    uint64_t ol_quantity;
};

class tpcc_query_t : public base_query_t { 
public:
    tpcc_query_t();
    tpcc_query_t(tpcc_query_t * query);
    virtual ~tpcc_query_t() { };

    virtual void gen_requests() { }

    uint32_t type;

    uint64_t w_id;
    uint64_t d_id;
    uint64_t c_id;
};

class tpcc_payment_t : public tpcc_query_t {
public:
    tpcc_payment_t();
    tpcc_payment_t(tpcc_payment_t* query) : tpcc_query_t(query) {
        d_w_id = query->d_w_id;
        c_w_id = query->c_w_id;
        c_d_id = query->c_d_id;
        by_last_name = query->by_last_name;
        memcpy(c_last, query->c_last, LASTNAME_LEN);
        h_amount = query->h_amount;
    };
    tpcc_payment_t(char * data);
    ~tpcc_payment_t() = default;

    void gen_requests();

    uint64_t d_w_id;
    uint64_t c_w_id;
    uint64_t c_d_id;
    bool by_last_name;
    char c_last[LASTNAME_LEN];
    double h_amount;
};

class tpcc_new_order_t : public tpcc_query_t {
public:
    tpcc_new_order_t();
    tpcc_new_order_t(tpcc_new_order_t * query) : tpcc_query_t(query) {
        remote = query->remote;
        ol_cnt = query->ol_cnt;
        o_entry_d = query->o_entry_d;
        items = new Item_no[ol_cnt];
        memcpy(items, query->items, sizeof(Item_no) * ol_cnt);
    }
    tpcc_new_order_t(char * data);
    ~tpcc_new_order_t();

    void gen_requests();

    uint32_t serialize(char * &raw_data);

    Item_no* items;
    bool remote;
    uint64_t ol_cnt;
    uint64_t o_entry_d;
};

class tpcc_order_status_t : public tpcc_query_t {
public:
    tpcc_order_status_t();
    tpcc_order_status_t(tpcc_order_status_t * query) : tpcc_query_t(query) {
        by_last_name = query->by_last_name;
        memcpy(c_last, query->c_last, LASTNAME_LEN);
    }
    ~tpcc_order_status_t() = default;

    void gen_requests();

    bool by_last_name;
    char c_last[LASTNAME_LEN];
};

class tpcc_delivery_t : public tpcc_query_t {
public:
    tpcc_delivery_t();
    tpcc_delivery_t(tpcc_delivery_t * query) : tpcc_query_t(query) {
        d_id = query->d_id;
        o_carrier_id = query->o_carrier_id;
        ol_delivery_d = query->ol_delivery_d;
    }
    ~tpcc_delivery_t() = default;

    void gen_requests();

    uint64_t d_id;
    int64_t o_carrier_id;
    int64_t ol_delivery_d;
};

class tpcc_stock_level_t : public tpcc_query_t {
public:
    tpcc_stock_level_t();
    tpcc_stock_level_t(tpcc_stock_level_t * query) : tpcc_query_t(query) {
        threshold = query->threshold;
    }

    void gen_requests();

    int64_t threshold;
};
