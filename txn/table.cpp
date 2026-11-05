#include "txn/table.h"
#include "txn/txn.h"
#include "system/manager.h"

// txn_table implementation
txn_table_t::txn_table_t(uint32_t size) {
    if (size == 0) size = g_txn_table_size;
    assert(size > 0);
    _table = new cuckoohash_map<uint64_t, txn_t*>(size);
    //_table = new cuckoohash_map<txn_id_t, txn_t*>(size);
}

txn_table_t::~txn_table_t() {
    if (_table) {
        delete _table;
        _table = nullptr;
    }
}

void txn_table_t::insert(txn_id_t key, txn_t* value) {
    assert(value != nullptr);
    uint64_t k = key.to_uint64();
    bool ret = _table->insert(k, value);
    //bool ret = _table->insert(key, value);
    if (!ret) { // debug
        assert(false);
    }
    assert(ret);
}

txn_t* txn_table_t::find(txn_id_t key) {
    txn_t* value = nullptr;
    uint64_t k = key.to_uint64();
    bool ret = _table->find(k, value);
    if (ret) return value;
    return nullptr;
}

bool txn_table_t::remove(txn_id_t key) {
    uint64_t k = key.to_uint64();
    return _table->erase(k);
}


void txn_table_t::print() {
    auto ltable = _table->lock_table();
    for(auto it=ltable.begin(); it!=ltable.end(); ++it) {
        uint64_t k = it->first;
        txn_id_t key(k >> 32, k & 0xFFFFFFFF);
        txn_t* value = it->second;
        printf("Transaction ID: node %u thd %u, Pointer: %p\n", key.node_id, key.tid, value);
    }
}