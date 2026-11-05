#include "concurrency/idx_manager.h"
#include "system/manager.h"
#include "system/workload.h"
#include "system/query.h"
#include "system/global_address.h"
#include "index/idx_wrapper.h"
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
idx_manager_t::idx_manager_t(txn_t* txn) : cc_manager_t(txn) { }

void idx_manager_t::clear() {
    _cache_set.clear();
    cc_manager_t::clear();
}

// PARTITIONED is an index-only benchmark (same as DEX): every key maps to an
// 8B value_t, never to a row_t. The "data" of an access is that 8B value.
char* idx_manager_t::get_data(uint64_t key, uint32_t table_id) {
    auto access = get_access(key, table_id);
    assert(access != nullptr);
    return reinterpret_cast<char*>(&access->value.val);
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

    if (type == WR || type == INS || type == DEL)
        _num_write_accesses++;

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
    access->processed = false;
    access->data_size = GET_WORKLOAD->get_table(table_id)->get_schema()->get_tuple_size();
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
        // this transaction evicted `key`'s dirty entry earlier in its own
        // registration loop -- it's no longer resident in the live cache, so
        // serve it from the staged value instead of a redundant RPC
        assert(type == RD || type == WR); // INS/DEL/SCAN on an evicted-this-txn key: not modeled
        if (type == RD) {
            access->value = it->second;
            ret = 1;
        }
        else { // WR: the staged write-back value is already in the evicting access's
               // evict_buf, so send this write as a plain miss. Accesses are serialized
               // in registration order, so the server applies the older write-back first.
            it->second = access->value;
            ret = 0;
        }
    }
    else {
        UnstructuredBuffer buffer(reinterpret_cast<char*>(access->evict_buf)); // for eviction
        // index-only: the 8B value travels as is (no row_t). A cache hit applies
        // the write locally (entry marked dirty, written back on eviction); a miss
        // sends the value and the server applies it in process_index().
        if (type == INS)
            ret = index->insert(key, access->value, buffer);
        else if (type == WR)
            ret = index->update(key, access->value, buffer);
        else if (type == RD)
            ret = index->lookup(key, access->value, buffer);
        else if (type == DEL)
            ret = index->remove(key, buffer);
        else { // TODO
            assert(type == SCAN);
        }
    }

    if (ret == 1)
        access->processed = true;
    else if (ret == 2)
        _cache_set[access->evict_buf[0]] = access->evict_buf[1];
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
                buffer.put(reinterpret_cast<char*>(access->evict_buf), sizeof(uint64_t) * 2); // evict key + evict value
            }
        }
        size = buffer.size();
    }
    else
        size = 0;
}

// deserialize access response for stored procedure txn
void idx_manager_t::deserialize(uint32_t node_id, std::vector<cc_manager_t::RowAccess*>& access_set, char* data, uint32_t size) {
    // stored_procedure [ num | (8B value) * num ]
    UnstructuredBuffer buffer(data);
    uint32_t num = 0;
    buffer.get(&num);
    for (auto& access: access_set) {
        buffer.get(&access->value.val);
        if (access->cache != 0) { // cache admission
            auto index = GET_WORKLOAD->get_index(access->index_id);
            if (access->cache == 2) { // admission + eviction
                index->invalidate(access->evict_buf[0], reinterpret_cast<char*>(access->evict_buf[2]));
                index->add_to_cache(access->key, access->value);
            }
            else { // just admission
                assert(access->cache == 3);
                index->add_to_cache(access->key, access->value);
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
            buffer.put(reinterpret_cast<char*>(_last_access->evict_buf), sizeof(uint64_t) * 2); // evict key + evict value
        size = buffer.size();
    }
    else
        size = 0;
}

// deserialize last access response for interactive txn
void idx_manager_t::deserialize_last(uint32_t node_id, char* data, uint32_t size) {
    // interactive [ num (=1) | 8B value ] (the server replies via get_resp_data(from, to))
    UnstructuredBuffer buffer(data);
    uint32_t num = 0;
    buffer.get(&num);
    assert(num == 1);

    buffer.get(&_last_access->value.val);
    if (_last_access->cache != 0) { // cache admission
        auto index = GET_WORKLOAD->get_index(_last_access->index_id);
        if (_last_access->cache == 2) { // cache admission + eviction
            index->invalidate(_last_access->evict_buf[0], reinterpret_cast<char*>(_last_access->evict_buf[2]));
            index->add_to_cache(_last_access->key, _last_access->value);
        }
        else { // cache admission
            assert(_last_access->cache == 3);
            index->add_to_cache(_last_access->key, _last_access->value);
        }
    }
    assert(buffer.size() == size);
    _cache_set.clear();
}

// get nodes involved for stored procedure txn
uint32_t idx_manager_t::get_nodes_involved(std::vector<uint32_t>& nodes) {
    for (auto node_id: _nodes_involved)
        nodes.push_back(node_id);
    return _nodes_involved.size();
}

// get nodes involved for commit -- only nodes with something that genuinely
// can't be resolved purely locally (an uncached WR, or a DEL) get a commit
// broadcast; a pure-RD/cache-admitted transaction returns an empty list
uint32_t idx_manager_t::get_commit_nodes(std::vector<uint32_t>& nodes) {
    std::set<uint32_t> commit_nodes;
    for (auto& access : _access_set) {
        if (access.type == DEL || (access.type == WR && access.cache == 0))
            commit_nodes.insert(access.node_id);
    }
    for (auto node_id : commit_nodes)
        nodes.push_back(node_id);
    return commit_nodes.size();
}

// build node_id's commit-broadcast payload: (key, index_id, 8B value)
// triples for that node's WR-with-cache==0 accesses. DEL contributes no
// data. Note: the PARTITIONED commit path (txn_t::process_commit) sends no
// broadcast; the server already applied these in process_index().
void idx_manager_t::serialize_commit(uint32_t node_id, char*& data, uint32_t& size) {
    UnstructuredBuffer buffer(data);
    std::vector<cc_manager_t::RowAccess*> targets;
    for (auto& access : _access_set) {
        if (access.node_id == node_id && access.type == WR && access.cache == 0)
            targets.push_back(&access);
    }
    uint32_t num = targets.size();
    buffer.put(&num);
    for (auto access : targets) {
        buffer.put(&access->key);
        buffer.put(&access->index_id);
        buffer.put(&access->value.val); // index-only: 8B value
    }
    size = buffer.size();
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
    access->data_size = GET_WORKLOAD->get_table(table_id)->get_schema()->get_tuple_size();
    access->evict_buf[0] = evict_key;
    access->evict_buf[1] = evict_value;

    process_index(access);
}


// get response data for stored procedure txn
void idx_manager_t::get_resp_data(uint32_t from, uint32_t to, char*& data, uint32_t &size) {
    assert(from < to);
    // stored_procedure [ num | (8B value) * num ]
    UnstructuredBuffer buffer(data);
    uint32_t num_tuples = to - from;
    buffer.put(&num_tuples);
    for (uint32_t i=from; i<to; i++)
        buffer.put(&_access_set[i].value.val);
    size = buffer.size();
}

// get response data for interactive txn (unused: the server replies to both txn
// types via get_resp_data(from, to); same framing kept so deserialize_last matches)
void idx_manager_t::get_resp_data_last(char*& data, uint32_t &size) {
    // interactive [ num (=1) | 8B value ]
    UnstructuredBuffer buffer(data);
    assert(_last_access != nullptr);
    uint32_t num = 1;
    buffer.put(&num);
    buffer.put(&_last_access->value.val);
    size = buffer.size();
}

row_t* idx_manager_t::process_index(cc_manager_t::RowAccess* access) {
    auto index = GET_WORKLOAD->get_index(access->index_id);
    assert(index != nullptr);

    uint64_t cache = access->cache;

    // dirty eviction write-back first: it carries an older value than any
    // access in this request (upsert semantics -- insert() overwrites in place)
    if (cache == 2)
        index->insert(access->evict_buf[0], access->evict_buf[1]);

    // index-only (same as DEX): every access that reaches the server is a
    // client-side cache miss, applied directly to the 8B value
    if (access->type == INS)
        index->insert(access->key, access->value);
    else if (access->type == WR) {
        __attribute__((unused)) bool found = index->update(access->key, access->value);
        assert(found);
    }
    else if (access->type == DEL)
        index->remove(access->key);
    else if (access->type == RD)
        index->lookup(access->key, access->value);

    return nullptr;
}

RC idx_manager_t::process_prepare(char* data, uint32_t size) {
    // one-phase commit broadcast payload: [ num | (key, index_id, 8B value) * num ]
    // this message *is* the commit decision -- nothing left to defer, apply directly
    UnstructuredBuffer buffer(data);
    uint32_t num = 0;
    buffer.get(&num);
    for (uint32_t i=0; i<num; i++) {
        uint64_t key = 0;
        uint32_t index_id = 0;
        value_t value;
        buffer.get(&key);
        buffer.get(&index_id);
        buffer.get(&value.val);
        auto index = GET_WORKLOAD->get_index(index_id);
        index->insert(key, value); // upsert: insert() overwrites in place on a duplicate key
    }
    assert(buffer.size() == size);
    return RCOK;
}

RC idx_manager_t::process_commit() {
    cleanup(COMMIT, false);
    return COMMIT;
}

void idx_manager_t::cleanup(RC rc, bool undo) {
    assert(rc == COMMIT);
    // assert(rc == COMMIT || rc == ABORT);
    // index-only: WR/INS/DEL were already applied when registered (client cache
    // hit) or by the server (miss) -- nothing is deferred to commit, and no
    // row_t is ever allocated, so there is nothing to free here
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
