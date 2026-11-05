#pragma once
#include "system/global.h"
#include "utils/helper.h"

class base_query_t {
public:
    base_query_t() {}
    virtual ~base_query_t() = default;

    virtual void gen_requests() = 0; 
    uint64_t     get_ts() { return _txn_ts; }
    void         set_ts(uint64_t ts) { _txn_ts = ts; }

protected:
    uint64_t _txn_ts;
};