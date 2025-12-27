#pragma once

#include "system/global.h"
#include "txn/txn.h"

namespace twosided {

inline bool lock_conflict(lock_type_t lt1, lock_type_t lt2) {
    if (lt1 == LOCK_NONE)
        return false;
    else if (lt1 == LOCK_EX)
        return true;
    else // lt1 == LOCK_SH
        if (lt2 == LOCK_EX)
            return true;
    return false;
}

struct wait_entry_t {
    lock_type_t type;
    txn_t*      txn;

    wait_entry_t(lock_type_t type, txn_t* txn)
        : type(type), txn(txn) { }
};

struct lock_comparator_t {
    inline bool operator() (txn_t* t1, txn_t* t2) const { 
        if (t1->get_ts() != t2->get_ts())
            return t1->get_ts() < t2->get_ts(); 
        return t1 < t2;
    }
};

struct wait_comparator_t {
    inline bool operator() (const wait_entry_t& w1, const wait_entry_t& w2) const {
        // first compare timestamps
        if (w1.txn->get_ts() != w2.txn->get_ts())
            return w1.txn->get_ts() < w2.txn->get_ts();
        // if timestamps are equal, compare txn pointers
        if (w1.txn != w2.txn)
            return w1.txn < w2.txn;
        // same txn, compare lock types
        return w1.type < w2.type;
    }
};
} // namespace twosided