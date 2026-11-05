#pragma once

#include "txn/id.h"
#include "txn/interactive.h"
class row_t;
class base_query_t;

class ycsb_interactive_t: public interactive_t {
public:
    ycsb_interactive_t(base_query_t* query);
    ycsb_interactive_t(txn_id_t id);
    ~ycsb_interactive_t() = default;

    RC execute();

    void init();
    
private:
    uint32_t _curr_step;
    uint32_t _curr_idx;
};
