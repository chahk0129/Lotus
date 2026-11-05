#pragma once

#include "system/global.h"
#include "txn/id.h"
#include "index/libcuckoo/libcuckoo/cuckoohash_map.hh"

class txn_t;

namespace std {
    /*
    template <>
    struct hash<txn_id_t> {
        size_t operator()(const txn_id_t& k) const {
            return std::hash<uint64_t>()((static_cast<uint64_t>(k.node_id) << 32) | k.tid);
        }
    };
    */
}

class txn_table_t {
public:
    txn_table_t(uint32_t size);
    ~txn_table_t();

    void   insert(txn_id_t key, txn_t* value);
    txn_t* find(txn_id_t key);
    bool   remove(txn_id_t key);

    void   print();
private:

    cuckoohash_map<uint64_t, txn_t*>* _table;
    // cuckoohash_map<txn_id_t, txn_t*>* _table;
};