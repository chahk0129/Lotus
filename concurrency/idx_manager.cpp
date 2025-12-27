#include "concurrency/idx_manager.h"
#include "index/idx_wrapper.h"
#include "system/cc_manager.h"
#include "system/manager.h"
#include "system/workload.h"
#include "system/query.h"
#include "system/global_address.h"
#include "txn/txn.h"
#include "storage/row.h"
#include "storage/table.h"
#include "storage/catalog.h"
#include "utils/packetize.h"
#include "utils/helper.h"
#include "client/transport.h"

#if PARTITIONED
///////////////////////
// common operations //
///////////////////////
idx_manager_t::idx_manager_t(txn_t* txn)
    : cc_manager_t(txn), _last_access(nullptr), _last_access_idx(-1) { }

void idx_manager_t::clear() {
    _last_access = nullptr;
    _last_access_idx = -1;
    _access_set.clear();
    _index_access_set.clear();
    _cache_set.clear();
    cc_manager_t::clear();
}

cc_manager_t::RowAccess* idx_manager_t::get_last_access() {
    if (_last_access_idx != -1)
        return &_access_set[_last_access_idx];
    else
        return nullptr;
    return _last_access;
}

int idx_manager_t::get_last_access_idx() {
    return _last_access_idx;
}

char* idx_manager_t::get_data(uint64_t key, uint32_t table_id) {
    auto access = get_access(key, table_id);
    assert(access != nullptr);
    return reinterpret_cast<char*>(&access->value.val);
}

cc_manager_t::RowAccess* idx_manager_t::get_access(uint64_t key, uint32_t table_id) {
    for (auto & access : _access_set) {
        if (access.key == key && access.table_id == table_id)
            return &access;
    }
    return nullptr;
}

uint32_t idx_manager_t::get_access_idx(uint64_t key, uint32_t table_id) {
    for (uint32_t i=0; i<_access_set.size(); i++) {
        auto& access = _access_set[i];
        if (access.key == key && access.table_id == table_id)
            return i;
    }
    assert(false);
    return -1;
}

cc_manager_t::RowAccess* idx_manager_t::get_write_access(uint32_t& idx_writes) {
    while (idx_writes < _access_set.size()) {
        auto access = &_access_set[idx_writes];
        idx_writes++;
        if (access->type == WR)
            return access;
    }
    return nullptr;
}


void idx_manager_t::commit_insdel() {
    for (auto & ins: _inserts) {
        auto index = ins.index;
        index->insert(ins.key, ins.value);
    }

    for (auto & del: _deletes) {
        auto index = del.index;
        index->remove(del.key);
    }
}


///////////////////////////////
// two-sided RDMA operations //
///////////////////////////////

///////////////////////
// client operations //
///////////////////////

void idx_manager_t::register_access(uint32_t node_id, access_t type, uint64_t key, uint32_t table_id, uint32_t index_id, uint64_t value) {
     for (auto& access: _access_set) {
        if (access.key == key && access.table_id == table_id) {
            printf("TXN %lu duplicate access key %lu table %u\n", _txn->get_id().to_uint64(), key, table_id);
            assert(false);
            return; // duplicate access 
        }
    }
    cc_manager_t::RowAccess ac;
    _access_set.push_back(ac);
    auto access = &_access_set.back();
    _last_access = access;

    access->node_id = node_id;
    access->type = type;
    access->key = key;
    access->value = value;
    access->table_id = table_id;
    access->index_id = index_id;
    access->cache = 0;
    access->data_size = PAGE_SIZE;
    access->data = new char[access->data_size];
    access->cache_data = new char[access->data_size];
    access->processed = false;
#if TRANSPORT == ONE_SIDED
    get_remote_row(access);
#else // TWO_SIDED
    process_cache(access);
#endif
    
    // printf("TXN %lu register access %u (key %lu)\n", _txn->get_id().to_uint64(), _access_set.size()-1, key);
    if (_nodes_involved.find(node_id) == _nodes_involved.end())
        _nodes_involved.insert(node_id);
}

void idx_manager_t::process_cache(cc_manager_t::RowAccess* access) {
    auto index = GET_WORKLOAD->get_index(access->index_id);
    int ret = 0;
    auto type = access->type;
    auto key = access->key;

    auto it = _cache_set.find(key);
    if (it != _cache_set.end()) {
        if (type == RD)
            access->value.val = it->second.val;
        else if (type == WR)
            it->second.val = access->value.val;
        else { // TODO
            assert(false);
        }
        ret = 1;
    }
    else {
        UnstructuredBuffer buffer(access->cache_data); // for eviction
        if (type == INS) 
            ret = index->insert(key, access->value, buffer);
        else if (type == WR) 
            ret = index->update(key, access->value, buffer);
        else if (type == DEL)
            ret = index->remove(key, buffer);
        else if (type == RD) {
            ret = index->lookup(key, access->value, buffer);
        }
        else {
            assert(type == SCAN);
        }
    }

    if (ret == 1)
        access->processed = true;
    else if (ret == 2) {
        UnstructuredBuffer evict_buffer(access->cache_data);
        uint64_t evict_key, evict_value;
        evict_buffer.get(&evict_key);
        evict_buffer.get(&evict_value);
        _cache_set[evict_key] = evict_value;
    }
    access->cache = static_cast<uint64_t>(ret);
}

// serialize access request for stored procedure txn
void idx_manager_t::serialize(uint32_t node_id, std::vector<cc_manager_t::RowAccess*>& access_set, char*& data, uint32_t& size) {
    // stored_procedure [ num | (access_type | key | index_id | table_id) * num ]
    UnstructuredBuffer buffer(data);

    for (auto& access : _access_set) {
        if (access.node_id == node_id && access.cache != 1)
            access_set.push_back(&access);
    }

    uint32_t num = access_set.size();
    if (num > 0) {
        buffer.put(&num);
        for (auto& access : access_set) {
            uint64_t cache_op = access->cache;
            buffer.put(&cache_op);
            buffer.put(&access->type);
            buffer.put(&access->key);
            buffer.put(&access->index_id);
            buffer.put(&access->table_id);
            buffer.put(&access->value.val);
            if (cache_op == 2) { // cache admission + eviction
                buffer.put(access->cache_data, sizeof(uint64_t) * 2); // evict key + evict value
            }
        }
        size = buffer.size();
    }
    else
        size = 0;
}

// deserialize access response for stored procedure txn
void idx_manager_t::deserialize(uint32_t node_id, std::vector<cc_manager_t::RowAccess*>& access_set, char* data, uint32_t size) {
    // stored_procedure [ num | (cache stale check) data * num ]
    UnstructuredBuffer buffer(data);
    uint32_t num = 0;
    buffer.get(&num);
    for (auto& access: access_set) {
        buffer.get(&access->value.val);
        // assert(access->value.val == access->key);
        assert(access->data != nullptr);
        if (access->cache != 0) { // cache admission
            auto index = GET_WORKLOAD->get_index(access->index_id);
            UnstructuredBuffer cache_buffer(access->cache_data);
            char* cache_ptr = nullptr;
            if (access->cache == 2) { // admission + eviction
                uint64_t evict_key, evict_value;
                cache_buffer.get(&evict_key);
                cache_buffer.get(&evict_value);
                cache_buffer.get(&cache_ptr);
                index->invalidate(evict_key, cache_ptr);
                index->add_to_cache(access->key, access->value.val);
            }
            else { // just admission
                assert(access->cache == 3);
                index->add_to_cache(access->key, access->value.val);
            }
        }
    }
    assert(buffer.size() == size);
}

// serialize last access request for interactive txn
void idx_manager_t::serialize_last(uint32_t node_id, char*& data, uint32_t& size) {
    // interactive [ access_type | key | index_id | table_id ]
    UnstructuredBuffer buffer(data);
    assert(_last_access != nullptr);
    assert(_last_access->node_id == node_id);

    uint64_t cache_op = _last_access->cache;
    if (cache_op != 1) {
        uint32_t num = 1;
        buffer.put(&num);
        buffer.put(&cache_op);
        buffer.put(&_last_access->type);
        buffer.put(&_last_access->key);
        buffer.put(&_last_access->index_id);
        buffer.put(&_last_access->table_id);
        buffer.put(&_last_access->value.val);
        if (cache_op == 2) // admission + eviction
            buffer.put(_last_access->cache_data, sizeof(uint64_t) * 2); // evict key + evict value
        size = buffer.size();
    }
    else 
        size = 0;
}

// deserialize last access response for interactive txn
void idx_manager_t::deserialize_last(uint32_t node_id, char* data, uint32_t size) {
    // interactive [ data ]
    UnstructuredBuffer buffer(data);
    uint32_t num = 0;
    buffer.get(&num);
    assert(num == 1);

    buffer.get(&_last_access->value.val);
    if (_last_access->cache != 0) { // cache admission
        auto index = GET_WORKLOAD->get_index(_last_access->index_id);
        UnstructuredBuffer cache_buffer(_last_access->cache_data);
        char* cache_ptr = nullptr;
        if (_last_access->cache == 2) { // cache admission + eviction
            uint64_t evict_key, evict_value;
            cache_buffer.get(&evict_key);
            cache_buffer.get(&evict_value);
            cache_buffer.get(&cache_ptr);
            index->invalidate(evict_key, cache_ptr);
            index->add_to_cache(_last_access->key, _last_access->value.val);
        }
        else { // cache admission
            assert(_last_access->cache == 3);
            index->add_to_cache(_last_access->key, _last_access->value.val);
        }
    }
    assert(buffer.size() == size);
    _cache_set.clear();
}

// get last node involved for interactive txn
uint32_t idx_manager_t::get_last_node_involved() {
    return _last_access->node_id;
}

// get nodes involved for stored procedure txn
uint32_t idx_manager_t::get_nodes_involved(std::vector<uint32_t>& nodes) {
    for (auto node_id: _nodes_involved)
        nodes.push_back(node_id);
    return _nodes_involved.size();
}

// get nodes involved for commit
uint32_t idx_manager_t::get_commit_nodes(std::vector<uint32_t>& nodes) {
    for (auto node_id: _nodes_involved)
        nodes.push_back(node_id);
    return _nodes_involved.size();
}

///////////////////////
// server operations //
///////////////////////

void idx_manager_t::register_access(access_t type, uint64_t key, uint32_t table_id, uint32_t index_id, uint64_t cache, uint64_t value, uint64_t evict_key, uint64_t evict_value) {
    assert(TRANSPORT == TWO_SIDED);
    cc_manager_t::RowAccess ac;
    _access_set.push_back(ac);
    auto access = &_access_set.back();
    _last_access = access;

    access->node_id = g_node_id;
    access->type = type;
    access->key = key;
    access->value = value;
    access->table_id = table_id;
    access->index_id = index_id;
    access->cache = cache;
    access->data_size = 0;
    access->data = reinterpret_cast<char*>(evict_key);
    access->cache_data = reinterpret_cast<char*>(evict_value);

    process_index(access);
}


// get response data for stored procedure txn
void idx_manager_t::get_resp_data(uint32_t from, uint32_t to, char*& data, uint32_t &size) {
    assert(from < to);
    // stored_procedure [ num | (data) * num ]
    UnstructuredBuffer buffer(data);
    uint32_t num_tuples = to - from;
    buffer.put(&num_tuples);
    for (uint32_t i=from; i<to; i++) {
        auto& access = _access_set[i];
        buffer.put(&access.value.val);
    }
    size = buffer.size();
}

// get response data for interactive txn
void idx_manager_t::get_resp_data_last(char*& data, uint32_t &size) {
    // interactive [ data ]
    UnstructuredBuffer buffer(data);
    assert(_last_access != nullptr);
    buffer.put(&_last_access->value.val);
    size = buffer.size();
}

row_t* idx_manager_t::process_index(cc_manager_t::RowAccess* access) {
    auto index = GET_WORKLOAD->get_index(access->index_id);
    assert(index != nullptr);

    // process index to get the row pointer
    uint64_t cache = access->cache;
    bool res = false;
    value_t value;

    // do the rpc first
    if (access->type == INS)
        index->insert(access->key, access->value);
    else if (access->type == WR)
        res = index->update(access->key, access->value);
    else if (access->type == DEL)
        res = index->remove(access->key);
    else if (access->type == RD)
        res = index->lookup(access->key, access->value);

    if (cache == 2) { // handle write-back for dirty data (eviction)
        uint64_t evict_key = reinterpret_cast<uint64_t>(access->data);
        uint64_t evict_value = reinterpret_cast<uint64_t>(access->cache_data);
        index->update(evict_key, evict_value);
    }


    return nullptr;
}

void idx_manager_t::cleanup(RC rc) {
    assert(rc == COMMIT);
    // assert(rc == COMMIT || rc == ABORT);
    if (rc == COMMIT)
        commit_insdel();

    for (auto & access : _access_set) {
        if (access.data && access.data_size > 0) {
            delete[] access.data;
            access.data = nullptr;
        }
        if (access.cache_data && access.data_size > 0) {
            delete[] access.cache_data;
            access.cache_data = nullptr;
        }
    }
    clear();
}



///////////////////////////////
// one-sided RDMA operations //
///////////////////////////////

// get row data after acquiring its lock for stored procedure txn
void idx_manager_t::get_resp_data() {  }

// get data after acquiring its lock for interactive txn
void idx_manager_t::get_resp_data(uint32_t access_idx) { }

// get row lock
RC idx_manager_t::get_remote_row(uint32_t access_idx) {
    assert(TRANSPORT == ONE_SIDED);
    auto access = &_access_set[access_idx];
    return get_remote_row(access);
}

// get row lock
RC idx_manager_t::get_remote_row(cc_manager_t::RowAccess* access) {
    assert(TRANSPORT == ONE_SIDED);
    RC rc = RCOK;
    _last_access = access;

    auto index = GET_WORKLOAD->get_index(access->index_id);
    if (access->type == INS)
        index->insert(access->key, access->value);
    else if (access->type == WR)
        index->update(access->key, access->value);
    else if (access->type == DEL)
        index->remove(access->key);
    else if (access->type == RD) {
        value_t value;
        bool found = index->lookup(access->key, value);
        if (found)
            access->value = value;
    }
    return rc;
}


#endif // PARTITIONED