#include "system/global.h"
#include "system/manager.h"
#include "system/cc_manager.h"
#include "system/workload.h"
#include "benchmarks/tpcc/interactive.h"
#include "benchmarks/tpcc/workload.h"
#include "benchmarks/tpcc/query.h"
#include "benchmarks/tpcc/helper.h"
#include "benchmarks/tpcc/const.h"
#include "storage/row.h"
#include "storage/table.h"
#include "storage/catalog.h"
#include "txn/id.h"
#include "txn/txn.h"
#include "txn/interactive.h"

#if WORKLOAD == TPCC

// client constructor
tpcc_interactive_t::tpcc_interactive_t(base_query_t* query)
    : interactive_t(query) { init(); }

// server constructor
tpcc_interactive_t::tpcc_interactive_t(txn_id_t txn_id)
    : interactive_t(txn_id) { init(); }
      
void tpcc_interactive_t::init() {
    interactive_t::init();
    _curr_step = 0;
    _phase = 0;
    _curr_ol_number = 0;
    _curr_dist = 0;
    _ol_amount = 0;
    _ol_num = 0;
    _items.clear();
}

uint32_t tpcc_interactive_t::get_txn_type() {
    return ((tpcc_query_t*)_query)->type;
}

RC tpcc_interactive_t::execute() {
    assert(g_is_server == false);
    auto query = static_cast<tpcc_query_t*>(_query);
    if (query->type == TPCC_PAYMENT)           return execute_payment();
    else if (query->type == TPCC_NEW_ORDER)    return execute_new_order();
    else if (query->type == TPCC_ORDER_STATUS) return execute_order_status();
    else if (query->type == TPCC_DELIVERY)     return execute_delivery();
    else if (query->type == TPCC_STOCK_LEVEL)  return execute_stock_level();
    else assert(false);
    return ERROR;
}

RC tpcc_interactive_t::execute_payment() {
    auto query = static_cast<tpcc_payment_t*>(_query);
    auto wl = static_cast<tpcc_workload_t*>(GET_WORKLOAD);

    RC rc = RCOK;
    uint64_t key;
    catalog_t* schema = nullptr;
    char* data = nullptr;
    uint32_t node_id = 0;

    if (_curr_step == 0) {
        // register access to WAREHOUSE
        key = query->w_id;
        node_id = wl->get_partition_by_key(key, TAB_WAREHOUSE);
        _cc_manager->register_access(node_id, WR, key, TAB_WAREHOUSE, IDX_WAREHOUSE);
        _curr_step++;
        return RCOK;
    }

    if (_curr_step == 1) {
        // access WAREHOUSE table
        key = query->w_id;
        schema = wl->t_warehouse->get_schema();
        data = _cc_manager->get_data(key, TAB_WAREHOUSE);
        /*====================================================+
            EXEC SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount
            WHERE w_id=:w_id;
        +====================================================*/
        /*===================================================================+
            EXEC SQL SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
            INTO :w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_name
            FROM warehouse
            WHERE w_id=:w_id;
        +===================================================================*/
        LOAD_VALUE(double, w_ytd, schema, data, W_YTD);
        w_ytd += query->h_amount;
        STORE_VALUE(w_ytd, schema, data, W_YTD);

        __attribute__((unused)) LOAD_VALUE(char*, w_name, schema, data, W_NAME);
        __attribute__((unused)) LOAD_VALUE(char*, w_street_1, schema, data, W_STREET_1);
        __attribute__((unused)) LOAD_VALUE(char*, w_street_2, schema, data, W_STREET_2);
        __attribute__((unused)) LOAD_VALUE(char*, w_city, schema, data, W_CITY);
        __attribute__((unused)) LOAD_VALUE(char*, w_state, schema, data, W_STATE);
        __attribute__((unused)) LOAD_VALUE(char*, w_zip, schema, data, W_ZIP);

        _curr_step++;
    }

    if (_curr_step == 2) {
        // register access to DISTRICT
        key = distKey(query->d_w_id, query->d_id);
        node_id = wl->get_partition_by_key(key, TAB_DISTRICT);
        _cc_manager->register_access(node_id, WR, key, TAB_DISTRICT, IDX_DISTRICT);

        _curr_step++;
        return RCOK;
    }
    
    if (_curr_step == 3) {
        // access DISTRICT table
        key = distKey(query->d_w_id, query->d_id);
        schema = wl->t_district->get_schema();
        data = _cc_manager->get_data(key, TAB_DISTRICT);
        /*=====================================================+
            EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount
            WHERE d_w_id=:w_id AND d_id=:d_id;
        +=====================================================*/
        LOAD_VALUE(double, d_ytd, schema, data, D_YTD);
        d_ytd += query->h_amount;
        STORE_VALUE(d_ytd, schema, data, D_YTD);

        __attribute__((unused)) LOAD_VALUE(char*, d_name, schema, data, D_NAME);
        __attribute__((unused)) LOAD_VALUE(char*, d_street_1, schema, data, D_STREET_1);
        __attribute__((unused)) LOAD_VALUE(char*, d_street_2, schema, data, D_STREET_2);
        __attribute__((unused)) LOAD_VALUE(char*, d_city, schema, data, D_CITY);
        __attribute__((unused)) LOAD_VALUE(char*, d_state, schema, data, D_STATE);
        __attribute__((unused)) LOAD_VALUE(char*, d_zip, schema, data, D_ZIP);

        _curr_step++;
    }

    if (_curr_step == 4) {
        // register access to CUSTOMER
        bool by_last_name = query->by_last_name;
        key = by_last_name ?
              custNPKey(query->c_last, query->c_d_id, query->c_w_id)
              : custKey(query->c_w_id, query->c_d_id, query->c_id);
        node_id = wl->get_partition_by_key(key, TAB_CUSTOMER);
        uint32_t index_id = by_last_name ? IDX_CUSTOMER_LAST : IDX_CUSTOMER_ID;
        _cc_manager->register_access(node_id, WR, key, TAB_CUSTOMER, index_id);

        _curr_step++;
        return RCOK;
    }

    if (_curr_step == 5) {
        // access CUSTOMER table
        schema = wl->t_customer->get_schema();
        key = query->by_last_name ?
              custNPKey(query->c_last, query->c_d_id, query->c_w_id)
              : custKey(query->c_w_id, query->c_d_id, query->c_id);
        data = _cc_manager->get_data(key, TAB_CUSTOMER);
        LOAD_VALUE(double, c_balance, schema, data, C_BALANCE);
        c_balance -= query->h_amount;
        STORE_VALUE(c_balance, schema, data, C_BALANCE);

        LOAD_VALUE(double, c_ytd_payment, schema, data, C_YTD_PAYMENT);
        c_ytd_payment -= query->h_amount;
        STORE_VALUE(c_ytd_payment, schema, data, C_YTD_PAYMENT);

        LOAD_VALUE(uint64_t, c_payment_cnt, schema, data, C_PAYMENT_CNT);
        c_payment_cnt += 1;
        STORE_VALUE(c_payment_cnt, schema, data, C_PAYMENT_CNT);

        __attribute__((unused)) LOAD_VALUE(char*, c_first, schema, data, C_FIRST);
        __attribute__((unused)) LOAD_VALUE(char*, c_middle, schema, data, C_MIDDLE);
        __attribute__((unused)) LOAD_VALUE(char*, c_street_1, schema, data, C_STREET_1);
        __attribute__((unused)) LOAD_VALUE(char*, c_street_2, schema, data, C_STREET_2);
        __attribute__((unused)) LOAD_VALUE(char*, c_city, schema, data, C_CITY);
        __attribute__((unused)) LOAD_VALUE(char*, c_state, schema, data, C_STATE);
        __attribute__((unused)) LOAD_VALUE(char*, c_zip, schema, data, C_ZIP);
        __attribute__((unused)) LOAD_VALUE(char*, c_phone, schema, data, C_PHONE);
        __attribute__((unused)) LOAD_VALUE(int64_t, c_since, schema, data, C_SINCE);
        __attribute__((unused)) LOAD_VALUE(char*, c_credit, schema, data, C_CREDIT);
        __attribute__((unused)) LOAD_VALUE(int64_t, c_credit_lim, schema, data, C_CREDIT_LIM);
        __attribute__((unused)) LOAD_VALUE(int64_t, c_discount, schema, data, C_DISCOUNT);
        return FINISH;
    }
    assert(false);
    return ERROR;
}

RC tpcc_interactive_t::execute_new_order() {
    auto query = static_cast<tpcc_new_order_t*>(_query);
    auto wl = static_cast<tpcc_workload_t*>(GET_WORKLOAD);

    RC rc = RCOK;
    uint64_t key;
    //itemid_t * item;
    catalog_t* schema = nullptr;
    char* data = nullptr;
    uint32_t node_id = 0;

    uint64_t w_id = query->w_id;
    uint64_t d_id = query->d_id;
    uint64_t c_id = query->c_id;
    uint64_t ol_cnt = query->ol_cnt;
    Item_no* items = query->items;

    if (_curr_step == 0) {
        // register access to WAREHOUSE
        key = w_id;
        node_id = wl->get_partition_by_key(w_id, TAB_WAREHOUSE);
        _cc_manager->register_access(node_id, RD, key, TAB_WAREHOUSE, IDX_WAREHOUSE);
        _curr_step++;
        return RCOK;
    }

    if (_curr_step == 1) {
        //////////////////////////////////////////////////////////////////
        //    EXEC SQL SELECT c_discount, c_last, c_credit, w_tax
        //    INTO :c_discount, :c_last, :c_credit, :w_tax
        //    FROM customer, warehouse
        //    WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id AND c_id = :c_id;
        //////////////////////////////////////////////////////////////////
        // access WAREHOUSE table
        schema = wl->t_warehouse->get_schema();
        data = _cc_manager->get_data(query->w_id, TAB_WAREHOUSE);
        LOAD_VALUE(double, w_tax, schema, data, W_TAX);
        _w_tax = w_tax;
        _curr_step++;
    }

    if (_curr_step == 2) {
        // register access to CUSTOMER
        key = custKey(w_id, d_id, c_id);
        node_id = wl->get_partition_by_key(key, TAB_CUSTOMER);
        _cc_manager->register_access(node_id, RD, key, TAB_CUSTOMER, IDX_CUSTOMER_ID);
        _curr_step++;
        return RCOK;
    }

    if (_curr_step == 3) {
        // access CUSTOMER table
        key = custKey(w_id, d_id, c_id);
        schema = wl->t_customer->get_schema();
        data = _cc_manager->get_data(key, TAB_CUSTOMER);
        LOAD_VALUE(uint64_t, c_discount, schema, data, C_DISCOUNT);
        _c_discount = c_discount;
        __attribute__((unused)) LOAD_VALUE(char*, c_last, schema, data, C_LAST);
        __attribute__((unused)) LOAD_VALUE(char*, c_credit, schema, data, C_CREDIT);
        _curr_step++;
    }

    if (_curr_step == 4) {
        // register access to DISTRICT
        key = distKey(w_id, d_id);
        node_id = wl->get_partition_by_key(key, TAB_DISTRICT);
        _cc_manager->register_access(node_id, WR, key, TAB_DISTRICT, IDX_DISTRICT);

        _curr_step++;
        return RCOK;
    }

    if (_curr_step == 5) {
        // access DISTRICT table
        //////////////////////////////////////////////////////////////////
        //     EXEC SQL SELECT d_next_o_id, d_tax INTO :d_next_o_id, :d_tax
        //     FROM district
        //     WHERE d_id = :d_id AND d_w_id = :w_id;

        //     EXEC SQL UPDATE district SET d_next_o_id = :d_next_o_id + 1
        //     WHERE d_id = :d_id AND d_w_id = :w_id;
        //////////////////////////////////////////////////////////////////
        key = distKey(w_id, d_id);
        schema = wl->t_district->get_schema();
        data = _cc_manager->get_data(key, TAB_DISTRICT);
        LOAD_VALUE(double, d_tax, schema, data, D_TAX);
        _d_tax = d_tax;
        LOAD_VALUE(int64_t, o_id, schema, data, D_NEXT_O_ID);
        _o_id = o_id;
        o_id++;
        STORE_VALUE(o_id, schema, data, D_NEXT_O_ID);
        _curr_step++;
    }   

    if (_curr_step == 6) {
        // register access to ORDER
        key = orderKey(w_id, d_id, _o_id);
        node_id = wl->get_partition_by_key(key, TAB_ORDER);
        _cc_manager->register_access(node_id, WR, key, TAB_ORDER, IDX_ORDER);
        _curr_step++;
        return RCOK;
    }

    if (_curr_step == 7) {
        // insert to ORDER table
        /////////////////////////////////////////////////////////////////
        //    EXEC SQL INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
        //    VALUES (:o_id, :d_id, :w_id, :c_id, :datetime, :o_ol_cnt, :o_all_local);
        /////////////////////////////////////////////////////////////////
        key = orderKey(w_id, d_id, _o_id);
        schema = wl->t_order->get_schema();
        data = _cc_manager->get_data(key, TAB_ORDER);

        STORE_VALUE(_o_id, schema, data, O_ID);
        STORE_VALUE(d_id, schema, data, O_D_ID);
        STORE_VALUE(w_id, schema, data, O_W_ID);
        STORE_VALUE(c_id, schema, data, O_C_ID);
        STORE_VALUE(query->o_entry_d, schema, data, O_ENTRY_D);
        STORE_VALUE(ol_cnt, schema, data, O_OL_CNT);
        int64_t all_local = (query->remote? 0 : 1);
        STORE_VALUE(all_local, schema, data, O_ALL_LOCAL);
        _curr_step++;
    }

    if (_curr_step == 8) {
        // register access to NEW_ORDER
        key = neworderKey(w_id, d_id);
        node_id = wl->get_partition_by_key(key, TAB_NEWORDER);
        _cc_manager->register_access(node_id, WR, key, TAB_NEWORDER, IDX_NEWORDER);
        _curr_step++;
        return RCOK;
    }

    if (_curr_step == 9) {
        // insert to NEWORDER
        /////////////////////////////////////////////////////////////////
        //    EXEC SQL INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)
        //    VALUES (:o_id, :d_id, :w_id);
        /////////////////////////////////////////////////////////////////
        key = neworderKey(w_id, d_id);
        schema = wl->t_neworder->get_schema();
        data = _cc_manager->get_data(key, TAB_NEWORDER);
        STORE_VALUE(_o_id, schema, data, NO_O_ID);
        STORE_VALUE(d_id, schema, data, NO_D_ID);
        STORE_VALUE(w_id, schema, data, NO_W_ID);
        _curr_step++;
    }

    if (_curr_step == 10) {
        if (_phase == 0) {
            // register access to ITEM 
            assert(_curr_ol_number == 0);
            for (; _curr_ol_number<ol_cnt; _curr_ol_number++) {
                key = items[_curr_ol_number].ol_i_id;
                if (key == 0) continue;
                node_id = wl->get_partition_by_key(key, TAB_ITEM);
                _cc_manager->register_access(node_id, RD, key, TAB_ITEM, IDX_ITEM);
                
            }
            _curr_ol_number = 0;
            _phase = 1;
            return RCOK;
        }
        else {
            assert(_phase == 1);
            // access ITEM tables
            /*===========================================+
                EXEC SQL SELECT i_price, i_name , i_data
                INTO :i_price, :i_name, :i_data
                FROM item
                WHERE i_id = :ol_i_id;
            +===========================================*/
            for (; _curr_ol_number<ol_cnt; _curr_ol_number++) {
                key = items[_curr_ol_number].ol_i_id;
                if (key == 0) continue;
                schema = wl->t_item->get_schema();
                data = _cc_manager->get_data(key, TAB_ITEM);
                __attribute__((unused)) LOAD_VALUE(int64_t, i_price, schema, data, I_PRICE);
                _i_price[_curr_ol_number] = i_price;
                __attribute__((unused)) LOAD_VALUE(char *, i_name, schema, data, I_NAME);
                __attribute__((unused)) LOAD_VALUE(char *, i_data, schema, data, I_DATA);
            }
            _curr_ol_number = 0;
            _phase = 0;
            _curr_step++;
        }
    }

    if (_curr_step == 11) {
        if (_phase == 0) {
            // register access to STOCK
            for (; _curr_ol_number<ol_cnt; _curr_ol_number++) {
                Item_no* it = &items[_curr_ol_number];
                key = stockKey(it->ol_supply_w_id, it->ol_i_id);
                node_id = wl->get_partition_by_key(key, TAB_STOCK);
                _cc_manager->register_access(node_id, RD, key, TAB_STOCK, IDX_STOCK);
            }

            _curr_ol_number = 0;
            _phase = 1;
            return RCOK;
        }
        else {
            assert(_phase == 1);
            // access STOCK tables
            for (; _curr_ol_number<ol_cnt; _curr_ol_number++) {
                Item_no* it = &items[_curr_ol_number];
                key = stockKey(it->ol_supply_w_id, it->ol_i_id);
                schema = wl->t_stock->get_schema();
                data = _cc_manager->get_data(key, TAB_STOCK);

                LOAD_VALUE(uint64_t, s_quantity, schema, data, S_QUANTITY);
                if (s_quantity > it->ol_quantity + 10)
                    s_quantity = s_quantity - it->ol_quantity;
                else
                    s_quantity = s_quantity - it->ol_quantity + 91;
                STORE_VALUE(s_quantity, schema, data, S_QUANTITY);
                __attribute__((unused)) LOAD_VALUE(int64_t, s_remote_cnt, schema, data, S_REMOTE_CNT);
#if !TPCC_SMALL
                LOAD_VALUE(int64_t, s_ytd, schema, data, S_YTD);
                s_ytd += it->ol_quantity;
                STORE_VALUE(s_ytd, schema, data, S_YTD);

                LOAD_VALUE(int64_t, s_order_cnt, schema, data, S_ORDER_CNT);
                s_order_cnt ++;
                STORE_VALUE(s_order_cnt, schema, data, S_ORDER_CNT);

                __attribute__((unused)) LOAD_VALUE(char *, s_data, schema, data, S_DATA);
#endif
                if (it->ol_supply_w_id != w_id) {
                    LOAD_VALUE(int64_t, s_remote_cnt, schema, data, S_REMOTE_CNT);
                    s_remote_cnt ++;
                    STORE_VALUE(s_remote_cnt, schema, data, S_REMOTE_CNT);
                }
            }
            _curr_ol_number = 0;
            _phase = 0;
            _curr_step++;
        }
    }

    if (_curr_step == 12) {
        // register access to orderline
        if (_phase == 0) {
            for (_ol_num=0; _ol_num<(int64_t)ol_cnt; _ol_num ++) {
                Item_no* it = &items[_ol_num];
                key = orderlinePrimaryKey(w_id, d_id, _o_id, _ol_num + 1);
                node_id = wl->get_partition_by_key(key, TAB_ORDERLINE);
                _cc_manager->register_access(node_id, WR, key, TAB_ORDERLINE, IDX_ORDERLINE);
            }
            _ol_num = 0;
            _phase = 1;
            return RCOK;
        }
        else {
            assert(_phase == 1);
            // insert to ORDERLINE table
            /*============================================================+
                EXEC SQL INSERT INTO ORDERLINE
                    (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id,
                     ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d)
                VALUES
                    (:o_id, :d_id, :w_id, :ol_number, :ol_i_id,
                     :ol_supply_w_id, :ol_quantity, :ol_amount, NULL);
            +============================================================*/
            schema = wl->t_orderline->get_schema();
            for (_ol_num=0; _ol_num<(int64_t)ol_cnt; _ol_num ++) {
                Item_no* it = &items[_ol_num];
                key = orderlinePrimaryKey(w_id, d_id, _o_id, _ol_num + 1);
                schema = wl->t_orderline->get_schema();

                data = _cc_manager->get_data(key, TAB_ORDERLINE);
                double ol_amount = it->ol_quantity * _i_price[_ol_num] * (1 + _w_tax + _d_tax) * (1 - _c_discount);
                STORE_VALUE(_o_id, schema, data, OL_O_ID);
                STORE_VALUE(d_id, schema, data, OL_D_ID);
                STORE_VALUE(w_id, schema, data, OL_W_ID);
                int64_t ol_number = _ol_num + 1;
                STORE_VALUE(ol_number, schema, data, OL_NUMBER);
                STORE_VALUE(it->ol_i_id, schema, data, OL_I_ID);
                STORE_VALUE(it->ol_supply_w_id, schema, data, OL_SUPPLY_W_ID);
                STORE_VALUE(it->ol_quantity, schema, data, OL_QUANTITY);
                STORE_VALUE(ol_amount, schema, data, OL_AMOUNT);
            }
            return FINISH;
        }
        
    }
    assert(false);
    return ERROR;
}

RC tpcc_interactive_t::execute_order_status() {
    auto query = static_cast<tpcc_order_status_t*>(_query);
    auto wl = static_cast<tpcc_workload_t*>(GET_WORKLOAD);

    RC rc = RCOK;
    uint64_t key;
    uint32_t node_id = 0;
    catalog_t* schema = nullptr;
    char* data = nullptr;

    if (_curr_step == 0) {
        // register access to CUSTOMER
        key = (query->by_last_name)?
              custNPKey(query->c_last, query->d_id, query->w_id)
              : custKey(query->w_id, query->d_id, query->c_id);
        node_id = wl->get_partition_by_key(key, TAB_CUSTOMER);
        _cc_manager->register_access(node_id, RD, key, TAB_CUSTOMER,
                                      query->by_last_name ? IDX_CUSTOMER_LAST : IDX_CUSTOMER_ID);
        _curr_step++;
        return RCOK;
    }

    if (_curr_step == 1) {
        // access CUSTOMER table
        key = (query->by_last_name)?
              custNPKey(query->c_last, query->d_id, query->w_id)
              : custKey(query->w_id, query->d_id, query->c_id);
        schema = wl->t_customer->get_schema();
        data = _cc_manager->get_data(key, TAB_CUSTOMER);
        if (query->by_last_name) {
            LOAD_VALUE(int64_t, c_id, schema, data, C_ID);
            _c_id = c_id;
        } else {
            _c_id = query->c_id;
            __attribute__((unused)) LOAD_VALUE(double, c_balance, schema, data, C_BALANCE);
            __attribute__((unused)) LOAD_VALUE(char*, c_first, schema, data, C_FIRST);
            __attribute__((unused)) LOAD_VALUE(char*, c_middle, schema, data, C_MIDDLE);
            __attribute__((unused)) LOAD_VALUE(char*, c_last, schema, data, C_LAST);
        }
        _curr_step++;
    }

    if (_curr_step == 2) {
        // ORDER
        key = custKey(query->w_id, query->d_id, _c_id);
        node_id = wl->get_partition_by_key(key, TAB_ORDER);
        _cc_manager->register_access(node_id, RD, key, TAB_ORDER, IDX_ORDER);
        _curr_step++;
        return RCOK;
    }

    if (_curr_step == 3) {
        // access ORDER table
        key = custKey(query->w_id, query->d_id, _c_id);
        data = _cc_manager->get_data(key, TAB_ORDER);
        schema = wl->t_order->get_schema();
        // TODO. should pick the largest O_ID.
        // Right now, picking the last one which should also be the largest.
        LOAD_VALUE(int64_t, o_id, schema, data, O_ID);
        _o_id = o_id;
        __attribute__((unused)) LOAD_VALUE(int64_t, o_entry_d, schema, data, O_ENTRY_D);
        __attribute__((unused)) LOAD_VALUE(int64_t, o_carrier_id, schema, data, O_CARRIER_ID);
        _curr_step++;
    }
    
    if (_curr_step == 4) {
        // ORDER-LINE
        _ol_cnt = URand(5, 15);
        for (uint32_t i=0; i<_ol_cnt; i++) {
            key = orderlineKey(query->w_id, query->d_id, _o_id + i);
            node_id = wl->get_partition_by_key(key, TAB_ORDERLINE);
            _cc_manager->register_access(node_id, SCAN, key, TAB_ORDERLINE, IDX_ORDERLINE);
        }
        _curr_step++;
        return RCOK;
    }

    if (_curr_step == 5) {
        schema = wl->t_orderline->get_schema();
        for (uint32_t i=0; i<_ol_cnt; i++) {
            key = orderlineKey(query->w_id, query->d_id, _o_id + i);
            data = _cc_manager->get_data(key, TAB_ORDERLINE);
            __attribute__((unused)) LOAD_VALUE(int64_t, ol_i_id, schema, data, OL_I_ID);
            __attribute__((unused)) LOAD_VALUE(int64_t, ol_supply_w_id, schema, data, OL_SUPPLY_W_ID);
            __attribute__((unused)) LOAD_VALUE(int64_t, ol_quantity, schema, data, OL_QUANTITY);
            __attribute__((unused)) LOAD_VALUE(double, ol_amount, schema, data, OL_AMOUNT);
            __attribute__((unused)) LOAD_VALUE(int64_t, ol_delivery_d, schema, data, OL_DELIVERY_D);
        }
        return FINISH;
    }
    assert(false);
    return ERROR;
}


RC tpcc_interactive_t::execute_delivery() {
    auto query = static_cast<tpcc_delivery_t*>(_query);
    auto wl = static_cast<tpcc_workload_t*>(GET_WORKLOAD);

    RC rc = RCOK;
    uint64_t key;
    uint32_t node_id = 0;
    catalog_t* schema = nullptr;
    char* data = nullptr; 
    
    _curr_dist = query->d_id;
    if (_curr_step == 0) {
        // register access to NEW ORDER
        key = neworderKey(query->w_id, _curr_dist);
        node_id = wl->get_partition_by_key(key, TAB_NEWORDER);
        // TODO. should pick the row with the lower NO_O_ID. need to do a scan here.
        // However, HSTORE just pick one row using LIMIT 1. So we also just pick a row.
        _cc_manager->register_access(node_id, RD, key, TAB_NEWORDER, IDX_NEWORDER);
        _curr_step++;
        return RCOK;
    }

    if (_curr_step == 1) {
        // access NEW ORDER table
        key = neworderKey(query->w_id, _curr_dist);
        schema = wl->t_neworder->get_schema();
        data = _cc_manager->get_data(key, TAB_NEWORDER);
        LOAD_VALUE(int64_t, o_id, schema, data, NO_O_ID);
        _o_id = o_id;
        // rc = get_cc_manager()->row_delete( curr_row );
        _curr_step++;
    }

    if (_curr_step == 2) {
        // register access to ORDER
        key = orderKey(query->w_id, _curr_dist, _o_id);
        node_id = wl->get_partition_by_key(key, TAB_ORDER);
        _cc_manager->register_access(node_id, WR, key, TAB_ORDER, IDX_ORDER);
        _curr_step++;
        return RCOK;
    }

    if (_curr_step == 3) {
        // access the ORDER table
        key = orderKey(query->w_id, _curr_dist, _o_id);
        schema = wl->t_order->get_schema();
        data = _cc_manager->get_data(key, TAB_ORDER);
        LOAD_VALUE(int64_t, o_c_id, schema, data, O_C_ID);
        _c_id = o_c_id;
        STORE_VALUE(query->o_carrier_id, schema, data, O_CARRIER_ID);
        _curr_step++;
    }

    if (_curr_step == 4) {
        // register access to ORDER LINE
        _ol_cnt = URand(5, 15);
        for (uint32_t i = 0; i < _ol_cnt; i++) {
            key = orderlineKey(query->w_id, _curr_dist, _o_id + i);
            node_id = wl->get_partition_by_key(key, TAB_ORDERLINE);
            _cc_manager->register_access(node_id, RD, key, TAB_ORDERLINE, IDX_ORDERLINE);
        }
        _curr_step++;
        return RCOK;
    }

    if (_curr_step == 5) {
        // access the ORDER LINE table
        schema = wl->t_orderline->get_schema();
        for (uint32_t i=0; i<_ol_cnt; i++) {
            key = orderlineKey(query->w_id, _curr_dist, _o_id + i);
            data = _cc_manager->get_data(key, TAB_ORDERLINE);
            STORE_VALUE(query->ol_delivery_d, schema, data, OL_DELIVERY_D);

            LOAD_VALUE(double, ol_amount, schema, data, OL_AMOUNT);
            _ol_amount += ol_amount;
        }
        _curr_step++;
    }

    if (_curr_step == 6) {
        // register access to CUSTOMER
        key = custKey(query->w_id, _curr_dist, _c_id);
        node_id = wl->get_partition_by_key(key, TAB_CUSTOMER);
        _cc_manager->register_access(node_id, WR, key, TAB_CUSTOMER, IDX_CUSTOMER_ID);
        _curr_step++;
        return RCOK;
    }

    if (_curr_step == 7) {
        // update CUSTOMER
        key = custKey(query->w_id, _curr_dist, _c_id);
        schema = wl->t_customer->get_schema();
        data = _cc_manager->get_data(key, TAB_CUSTOMER);

        LOAD_VALUE(double, c_balance, schema, data, C_BALANCE);
        c_balance += _ol_amount;
        STORE_VALUE(c_balance, schema, data, C_BALANCE);

        LOAD_VALUE(int64_t, c_delivery_cnt, schema, data, C_DELIVERY_CNT);
        c_delivery_cnt++;
        STORE_VALUE(c_delivery_cnt, schema, data, C_DELIVERY_CNT);
        return FINISH;
    }
    assert(false);
    return ERROR;
}

RC tpcc_interactive_t::execute_stock_level() {
    auto query = static_cast<tpcc_stock_level_t*>(_query);
    auto wl = static_cast<tpcc_workload_t*>(GET_WORKLOAD);

    RC rc = RCOK;
    uint64_t key;
    uint32_t node_id = 0;
    catalog_t* schema = nullptr;
    char* data = nullptr; 

    if (_curr_step == 0) {
        // register DISTRICT access
        key = distKey(query->w_id, query->d_id);
        node_id = wl->get_partition_by_key(key, TAB_DISTRICT);
        _cc_manager->register_access(node_id, RD, key, TAB_DISTRICT, IDX_DISTRICT);
        _curr_step++;
        return RCOK;
    }

    if (_curr_step == 1) {
        // Read DISTRICT table
        key = distKey(query->w_id, query->d_id);
        data = _cc_manager->get_data(key, TAB_DISTRICT);
        schema = wl->t_district->get_schema();

        LOAD_VALUE(int64_t, o_id, schema, data, D_NEXT_O_ID);
        o_id = 3001;
        _o_id = o_id;
        _curr_ol_number = o_id - 20;
        _curr_step++;
    }

    if (_curr_step == 2) {
        // register ORDER LINE access
        uint32_t temp_ol_number = _curr_ol_number;
        for (; _curr_ol_number<_o_id; _curr_ol_number++) {
            key = orderlineKey(query->w_id, query->d_id, _curr_ol_number);
            node_id = wl->get_partition_by_key(key, TAB_ORDERLINE);
            _cc_manager->register_access(node_id, RD, key, TAB_ORDERLINE, IDX_ORDERLINE);
        }
        _curr_ol_number = temp_ol_number;
        _curr_step++;
        return RCOK;
    }

    if (_curr_step == 3) {
        // Read ORDER LINE table
        for (; _curr_ol_number<_o_id; _curr_ol_number++) {
            key = orderlineKey(query->w_id, query->d_id, _curr_ol_number);
            schema = wl->t_orderline->get_schema();
            data = _cc_manager->get_data(key, TAB_ORDERLINE);
            LOAD_VALUE(int64_t, ol_i_id, schema, data, OL_I_ID);
            _items.insert(ol_i_id);
        }
        assert(_items.size() > 0);
        _curr_step++;
    }

    if (_curr_step == 4) {
        // register STOCK access
        for (auto it=_items.begin(); it!=_items.end(); it++) {
            key = stockKey(query->w_id, *it);
            node_id = wl->get_partition_by_key(key, TAB_STOCK);
            _cc_manager->register_access(node_id, RD, key, TAB_STOCK, IDX_STOCK);
        }
        _curr_step++;
        return RCOK;
    }

    if (_curr_step == 5) {
        // Read STOCK table
        for (auto it=_items.begin(); it!=_items.end(); it++) {
            key = stockKey(query->w_id, *it);
            schema = wl->t_stock->get_schema();
            data = _cc_manager->get_data(key, TAB_STOCK);
            __attribute__((unused)) LOAD_VALUE(int64_t, s_quantity, schema, data, S_QUANTITY);
        }
        return FINISH;
    }
    assert(false);
    return ERROR;
}

#endif // WORKLOAD == TPCC
