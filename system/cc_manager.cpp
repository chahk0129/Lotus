#include "system/cc_manager.h"
#include "concurrency/lock_manager.h"
#include "concurrency/idx_manager.h"
#include "txn/txn.h"

cc_manager_t* cc_manager_t::create(txn_t* txn) {
    return new CC_MAN(txn);
}

cc_manager_t::cc_manager_t(txn_t* txn)
    : _txn(txn) {
    clear();
}

void cc_manager_t::clear() {
    _inserts.clear();
    _deletes.clear();
}