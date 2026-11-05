#pragma once

#include "txn/id.h"
#include "txn/txn.h"
#include "txn/interactive.h"

class tpcc_interactive_t : public interactive_t {
public:
    tpcc_interactive_t(base_query_t* query);
    tpcc_interactive_t(txn_id_t txn_id);
    ~tpcc_interactive_t() = default;

    RC execute();

private:
    void init();

    RC execute_payment();
    RC execute_new_order();
    RC execute_order_status();
    RC execute_delivery();
    RC execute_stock_level();

    uint32_t get_txn_type();

#define LOAD_VALUE(type, var, schema, data, col) \
    type var = *(type *)row_t::get_value_v(schema, col, data);
#define STORE_VALUE(var, schema, data, col) \
    row_t::set_value_v(schema, col, data, (char *)&var);

    // Need to maintain some states so a txn can be pended and restarted.
    uint32_t  _phase;
    uint32_t  _curr_step;
    uint32_t  _curr_ol_number;

    // for remote txn return value
    uint64_t  _s_quantity;

    // for NEW ORDER
    int64_t   _o_id;
    int64_t   _i_price[15];
    double    _w_tax;
    double    _d_tax;
    double    _c_discount;
    int64_t   _ol_num;

    // for ORDER STATUS
    int64_t   _c_id;
    uint32_t  _ol_cnt;

    // for DELIVERY
    uint32_t  _curr_dist;
    double    _ol_amount;

    // For STOCK LEVEL
    set<int64_t> _items;
};
