#pragma once

#include "system/global.h"
#include "system/global_address.h"
#include "utils/helper.h"

class txn_t;
class UnstructuredBuffer;

class cc_manager_t {
public:
    static cc_manager_t* create(txn_t* txn);

    cc_manager_t(txn_t* txn);
    virtual ~cc_manager_t() = default;

    struct RowAccess {
        RowAccess() : processed(false), value(), data_size(0), cache(0), 
                      data(nullptr), cache_data(nullptr) {  }

        uint32_t node_id;
        bool     processed;
        access_t type;
        uint64_t key;
        uint32_t table_id;
        uint32_t index_id;
        uint32_t data_size;
        value_t  value;
        uint64_t cache;
        char*    data;
        char*    cache_data;

        bool waiting = false;
    };

    struct IndexAccess {
        IndexAccess(): index(nullptr), value() { }

        INDEX*   index;
        uint64_t key;
        access_t type;
        value_t  value;
    };

    struct InsertOp {
        InsertOp(INDEX* idx, uint64_t k, value_t v) : index(idx), key(k), value(v) { }

        INDEX*   index;
        uint64_t key;
        value_t  value;
    };

    struct DeleteOp {
        DeleteOp(INDEX* idx, uint64_t k) : index(idx), key(k) { }

        INDEX*   index;
        uint64_t key;
    };

    virtual void       register_access(uint32_t node_id, access_t type, uint64_t key, uint32_t table_id, uint32_t index_id, uint64_t value=0) = 0; 
    virtual void       register_access(access_t type, uint64_t key, uint32_t table_id, uint32_t index_id, uint64_t cache, uint64_t value=0, uint64_t evict_key=0, uint64_t evict_value=0) = 0;
    virtual RowAccess* get_last_access() = 0;
    virtual int        get_last_access_idx() = 0;

    virtual void       get_resp_data_last(char*& data, uint32_t& size) = 0;
    virtual void       get_resp_data(uint32_t from, uint32_t to, char*& data, uint32_t& size) = 0;
    virtual void       get_resp_data(uint32_t access_idx) = 0;
    virtual void       get_resp_data() = 0;

    virtual uint32_t   get_row_cnt() = 0;
    virtual RC         get_row(uint32_t access_idx) = 0;
    virtual RC         get_row(RowAccess* access) = 0;
    virtual RC         get_remote_row(uint32_t access_idx) = 0;
    virtual RC         get_remote_row(RowAccess* access) = 0;

    virtual RC         process_prepare(char* data, uint32_t size) = 0;
    virtual RC         process_commit() = 0;
    virtual void       cleanup(RC rc) = 0;
    virtual void       abort(uint32_t abort_access_idx) = 0;
    virtual void       abort(RowAccess* access) = 0;

    virtual void       serialize_commit(uint32_t node_id, char*& data, uint32_t& size) = 0;
    virtual void       serialize(uint32_t node_id, std::vector<RowAccess*>& access_set, char*& data, uint32_t& size) = 0;
    virtual void       deserialize(uint32_t node_id, std::vector<RowAccess*>& access_set, char* data, uint32_t size) = 0;
    virtual void       serialize_last(uint32_t node_id, char*& data, uint32_t& size) = 0;
    virtual void       deserialize_last(uint32_t node_id, char* data, uint32_t size) = 0;

    virtual uint32_t   get_last_node_involved() = 0;
    virtual uint32_t   get_nodes_involved(std::vector<uint32_t>& nodes) = 0;
    virtual uint32_t   get_commit_nodes(std::vector<uint32_t>& nodes) = 0;
    virtual void       clear_nodes_involved() = 0;
    virtual void       clear_nodes_involved(std::vector<uint32_t>& nodes_aborted) = 0;
    virtual void       abort() = 0;
    virtual char*      get_data(uint64_t key, uint32_t table_id) = 0;

    

protected:
    // TODO. different CC algorithms should have different ways to handle index consistency.
    // For now, just ignore index concurrency control.
    // Since this is not a problem for YCSB and TPCC.
    virtual void     commit_insdel() = 0;
    virtual void     clear();

    txn_t*           _txn;
    vector<InsertOp> _inserts;
    vector<DeleteOp> _deletes;
};