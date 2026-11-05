#pragma once

#include "txn/id.h"
#include "txn/txn.h"
#include "txn/stored_procedure.h"

class row_t;
class base_query_t;

class ycsb_stored_procedure_t: public stored_procedure_t {
public:
    ycsb_stored_procedure_t(base_query_t* query);
    ycsb_stored_procedure_t(txn_id_t txn_id);
    ~ycsb_stored_procedure_t() = default;

    RC execute();

    void init();

private:
    uint32_t _curr_step;
};
