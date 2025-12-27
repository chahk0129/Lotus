#include "concurrency/lock_manager.h"
#include "concurrency/onesided/nowait.h"
#include "concurrency/onesided/waitdie.h"
#include "concurrency/twosided/nowait.h"
#include "concurrency/twosided/waitdie.h"
#include "concurrency/twosided/woundwait.h"
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
#include "storage/row.h"
#include "storage/table.h"
#include "storage/catalog.h"

#if !PARTITIONED
///////////////////////
// common operations //
///////////////////////

lock_manager_t::lock_manager_t(txn_t* txn)
    : cc_manager_t(txn), _last_access(nullptr), _last_access_idx(-1) { }

void lock_manager_t::clear() {
    _last_access = nullptr;
    _last_access_idx = -1;
    _access_set.clear();
    _index_access_set.clear();
    _remote_nodes.clear();
    _nodes_involved.clear();
    cc_manager_t::clear();
}

cc_manager_t::RowAccess* lock_manager_t::get_last_access() {
    if (_last_access_idx != -1)
        return &_access_set[_last_access_idx];
    else
        return nullptr;
    return _last_access;
}

int lock_manager_t::get_last_access_idx() {
    return _last_access_idx;
}

char* lock_manager_t::get_data(uint64_t key, uint32_t table_id) {
    auto access = get_access(key, table_id);
    assert(access != nullptr);
    return access->data;
}

cc_manager_t::RowAccess* lock_manager_t::get_access(uint64_t key, uint32_t table_id) {
    for (auto & access : _access_set) {
        if (access.key == key && access.table_id == table_id)
            return &access;
    }
    return nullptr;
}

uint32_t lock_manager_t::get_access_idx(uint64_t key, uint32_t table_id) {
    for (uint32_t i=0; i<_access_set.size(); i++) {
        auto& access = _access_set[i];
        if (access.key == key && access.table_id == table_id)
            return i;
    }
    assert(false);
    return -1;
}

cc_manager_t::RowAccess* lock_manager_t::get_write_access(uint32_t& idx_writes) {
    while (idx_writes < _access_set.size()) {
        auto access = &_access_set[idx_writes];
        idx_writes++;
        if (access->type == WR)
            return access;
    }
    return nullptr;
}


void lock_manager_t::commit_insdel() {
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

void lock_manager_t::register_access(uint32_t node_id, access_t type, uint64_t key, uint32_t table_id, uint32_t index_id, uint64_t value) {
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
    access->processed = false;
    access->value = 0;
    access->type = type;
    access->key = key;
    access->value = value;
    access->table_id = table_id;
    access->index_id = index_id;
    access->data_size = GET_WORKLOAD->get_table(table_id)->get_schema()->get_tuple_size();
    access->data = new char[access->data_size];
    access->cache_data = new char[PAGE_SIZE];
    process_cache(access);
    
    // printf("TXN %lu register access %u (key %lu)\n", _txn->get_id().to_uint64(), _access_set.size()-1, key);
    if (_remote_nodes.find(node_id) == _remote_nodes.end())
        _remote_nodes.insert(node_id);
    if (_nodes_involved.find(node_id) == _nodes_involved.end())
        _nodes_involved.insert(node_id);
}

void lock_manager_t::process_cache(cc_manager_t::RowAccess* access) {
    auto index = GET_WORKLOAD->get_index(access->index_id);
    UnstructuredBuffer buffer(access->cache_data);
    int ret = index->search_cache(access->key, buffer);
    if (ret == 1) // cache miss, but admit to cache
        access->cache = static_cast<uint64_t>(1); // no cache ptr
    else if (ret == 2) { // cache hit
        // memcpy(&access->value.val, buffer.data(), sizeof(uint64_t)); // cache entry pointer for stale check
        // memcpy(&access->cache, buffer.data()+sizeof(uint64_t), sizeof(uint64_t));
        memcpy(&access->cache, buffer.data(), sizeof(uint64_t));
        // access->cache = reinterpret_cast<uint64_t>(buffer.data() + sizeof(uint64_t)); // cache ptr
    }
    else
        access->cache = static_cast<uint64_t>(0); // no cache hint
}

// serialize commit request
void lock_manager_t::serialize_commit(uint32_t node_id, char*& data, uint32_t& size) {
    // stored_procedure [ num | (write_data * num) ]
    UnstructuredBuffer buffer(data);
    for (auto& access: _access_set) {
        if (access.node_id == node_id && access.type == WR) {
            char* ptr = access.data;
            buffer.put(ptr, access.data_size);
        }
    }
    size = buffer.size();
}

// serialize access request for stored procedure txn
void lock_manager_t::serialize(uint32_t node_id, std::vector<cc_manager_t::RowAccess*>& access_set, char*& data, uint32_t& size) {
    // stored_procedure [ num | (access_type | key | index_id | table_id) * num ]
    UnstructuredBuffer buffer(data);
#if CC_ALG == WAIT_DIE || CC_ALG == WOUND_WAIT
    // if waitdie, add timestamp at the beginning
    uint64_t timestamp = _txn->get_ts();
    buffer.put(&timestamp);
#endif

    for (auto& access : _access_set) {
        if ((access.node_id == node_id) && (access.processed == false)) {
            access.processed = true;
            access_set.push_back(&access);
        }
    }

    uint32_t num = access_set.size();
    buffer.put(&num);
    for (auto& access : access_set) {
        // examine client cache first
        buffer.put(&access->type);
        buffer.put(&access->key);
        buffer.put(&access->index_id);
        buffer.put(&access->table_id);
        buffer.put(&access->cache);
    }
    size = buffer.size();
}

// deserialize access response for stored procedure txn
void lock_manager_t::deserialize(uint32_t node_id, std::vector<cc_manager_t::RowAccess*>& access_set, char* data, uint32_t size) {
    // stored_procedure [ num | (cache stale check) data * num ]
    UnstructuredBuffer buffer(data);
    uint32_t num = 0;
    buffer.get(&num);
    for (auto& access: access_set) {
        assert(access->data != nullptr);
        if (access->cache > 1) {  // cache hint was provided
            int stale = 0;
            buffer.get(&stale);
            if (stale == 1) { // cache is stale
                auto index = GET_WORKLOAD->get_index(access->index_id);
                index->invalidate(reinterpret_cast<char*>(access->value.val));
                char* cache_ptr = access->cache_data;
                buffer.get(&cache_ptr);
                index->add_to_cache(access->key, cache_ptr);
            }
            else {
                assert(stale == 0);
            }
        }
        else if (access->cache == 1) { // cache admission
            char* cache_ptr = access->cache_data;
            buffer.get(&cache_ptr);
            auto index = GET_WORKLOAD->get_index(access->index_id);
            index->add_to_cache(access->key, cache_ptr);
        }
        // else no cache hint, just get the data

        char* ptr = access->data;
        buffer.get(ptr, access->data_size);
    }
    assert(buffer.size() == size);
}

// serialize last access request for interactive txn
void lock_manager_t::serialize_last(uint32_t node_id, char*& data, uint32_t& size) {
    // interactive [ access_type | key | index_id | table_id ]
    UnstructuredBuffer buffer(data);
    assert(_last_access != nullptr);
    assert(_last_access->node_id == node_id);

#if CC_ALG == WAIT_DIE || CC_ALG == WOUND_WAIT
    uint64_t timestamp = _txn->get_ts();
    // if waitdie, add timestamp at the beginning
    buffer.put(&timestamp);
#endif

    uint32_t num = 1;
    buffer.put(&num);
    buffer.put(&_last_access->type);
    buffer.put(&_last_access->key);
    buffer.put(&_last_access->index_id);
    buffer.put(&_last_access->table_id);
    buffer.put(&_last_access->cache);
    size = buffer.size();
}

// deserialize last access response for interactive txn
void lock_manager_t::deserialize_last(uint32_t node_id, char* data, uint32_t size) {
    // interactive [ data ]
    UnstructuredBuffer buffer(data);
    uint32_t num = 0;
    buffer.get(&num);
    assert(num == 1);

    if (_last_access->cache > 1) {  // cache hint was provided
        int stale = 0;
        buffer.get(&stale);
        if (stale == 1) { // cache is stale
            auto index = GET_WORKLOAD->get_index(_last_access->index_id);
            index->invalidate(reinterpret_cast<char*>(_last_access->value.val));
            char* cache_ptr = _last_access->cache_data;
            buffer.get(&cache_ptr);
            index->add_to_cache(_last_access->key, cache_ptr);
        }
        else {
            assert(stale == 0);
        }
    }
    else if (_last_access->cache == 1) { // cache admission
        char* cache_ptr = _last_access->cache_data;
        buffer.get(&cache_ptr);
        auto index = GET_WORKLOAD->get_index(_last_access->index_id);
        index->add_to_cache(_last_access->key, cache_ptr);
    }

    char* ptr = _last_access->data;
    buffer.get(ptr, _last_access->data_size);
    assert(buffer.size() == size);
}

// get last node involved for interactive txn
uint32_t lock_manager_t::get_last_node_involved() {
    return _last_access->node_id;
}

// get nodes involved for stored procedure txn
uint32_t lock_manager_t::get_nodes_involved(std::vector<uint32_t>& nodes) {
    for (auto node_id: _remote_nodes)
        nodes.push_back(node_id);
    return _remote_nodes.size();
}

// get nodes involved for commit
uint32_t lock_manager_t::get_commit_nodes(std::vector<uint32_t>& nodes) {
    for (auto node_id: _nodes_involved)
        nodes.push_back(node_id);
    return nodes.size();
}

void lock_manager_t::abort() {
    _txn->set_state(txn_t::state_t::ABORTING);
    for (auto & access : _access_set) {
        if (access.data) {
            delete[] access.data;
            access.data = nullptr;
        }
        if (access.cache_data) {
            delete[] access.cache_data;
            access.cache_data = nullptr;
        }
    }
    clear();
}


///////////////////////
// server operations //
///////////////////////

void lock_manager_t::register_access(access_t type, uint64_t key, uint32_t table_id, uint32_t index_id, uint64_t cache, uint64_t value, uint64_t evict_key, uint64_t evict_value) {
    assert(TRANSPORT == TWO_SIDED);
    for (auto& access: _access_set) {
        if (access.key == key && access.table_id == table_id) {
            printf("TXN %lu duplicate access key %lu table %u\n", _txn->get_id().to_uint64(), key, table_id);
            assert(false);
            return; // duplicate access 
        }
    }
    // register row access information
    cc_manager_t::RowAccess ac;
    _access_set.push_back(ac);
    auto access = &_access_set.back();
    _last_access = access;

    access->node_id = g_node_id;
    access->type = type;
    access->key = key;
    access->table_id = table_id;
    access->index_id = index_id;
    access->value = 0;
    access->cache = cache;
    access->data_size = GET_WORKLOAD->get_table(table_id)->get_schema()->get_tuple_size();
    if (type == WR)
        access->data = new char[access->data_size];
    else
        access->data = nullptr;
}

// get response data for stored procedure txn
void lock_manager_t::get_resp_data(uint32_t from, uint32_t to, char*& data, uint32_t &size) {
    assert(from < to);
    // stored_procedure [ num | (data) * num ]
    UnstructuredBuffer buffer(data);
    uint32_t num_tuples = to - from;
    buffer.put(&num_tuples);
    for (uint32_t i = from; i < to; i++) {
        auto& access = _access_set[i];
        if (access.cache == 1) { // cache admission
            buffer.put(&access.value.val);
        }
        else if (access.cache > 1) { // cache hint was provided
            int stale = 0;
            if (access.cache_data == nullptr) // cache is up-to-date
                buffer.put(&stale);
            else { // cache is stale
                stale = 1;
                buffer.put(&stale);
                buffer.put(&access.value.val);
            }
        }
        char* ptr = access.value.row->get_data();
        buffer.put(ptr, access.data_size);
    }
    size = buffer.size();
}

// get response data for interactive txn
void lock_manager_t::get_resp_data_last(char*& data, uint32_t &size) {
    // interactive [ data ]
    UnstructuredBuffer buffer(data);
    assert(_last_access != nullptr);
    if (_last_access->cache == 1) { // cache admission
        buffer.put(&_last_access->value.val);
    }
    else if (_last_access->cache > 1) { // cache hint was provided
        int stale = 0;
        if (_last_access->cache_data == nullptr) // cache is up-to-date
            buffer.put(&stale);
        else { // cache is stale
            stale = 1;
            buffer.put(&stale);
            buffer.put(&_last_access->value.val);
        }
    }

    char* ptr = _last_access->value.row->get_data();
    buffer.put(ptr, _last_access->data_size);
    size = buffer.size();
}

void lock_manager_t::prefetch_row(char* ptr, uint32_t size) {
    // uint32_t num = size / 64; // get the number of cache lines to prefetch
    uint32_t num = 4;
    if (size % 64 != 0) num += 1;
    for (uint32_t i=0; i<num; i++) {
        __builtin_prefetch(ptr + i * 64, 0, 3);
    }
}
row_t* lock_manager_t::process_index(cc_manager_t::RowAccess* access) {
    auto index = GET_WORKLOAD->get_index(access->index_id);
    assert(index != nullptr);

    // process index to get the row pointer
    uint64_t cache = access->cache;
    bool need_admission = false;
    bool found = false;
    value_t value;
    if (cache > 1) { // cache hint was provided
        // prefetch_row(reinterpret_cast<char*>(cache), 16 + access->data_size);
        row_t* cache_ptr = reinterpret_cast<row_t*>(cache);
        uint64_t cache_key = GET_WORKLOAD->get_primary_key(cache_ptr);
        if (cache_key == access->key) {
            value.row = cache_ptr;
            found = true;
        }
        else
            need_admission = true;
    }
    else if (cache == 1) // cache admission requested
        need_admission = true;

    if (need_admission) {
        assert(found == false);
        access->cache_data = new char[PAGE_SIZE];
    }

    if (!found)
        found = index->lookup(access->key, value);

    if (!found) { // this is an insert
        row_t* new_row = nullptr;
        RC rc = GET_WORKLOAD->get_table(access->table_id)->get_new_row(new_row);
        assert(rc == RCOK);
        new_row->set_value(GET_WORKLOAD->get_table(access->table_id)->get_schema(), 0, &access->key);
        // columns other than primary key are left uninitialized
        // they will be later updated from the compute side when committing
        value = new_row;

        InsertOp ins(index, access->key, value);
        _inserts.push_back(ins);
    }

    // printf("TXN %lu register access %u (key %lu)\n", _txn->get_id().to_uint64(), _access_set.size()-1, key);
    access->value = value;
    return value.row;
}

// acquire a lock on the row
RC lock_manager_t::get_row(uint32_t idx) {
    RC rc = RCOK;
#if TRANSPORT == TWO_SIDED
    auto access = &_access_set[idx];
    rc = get_row(access);
    if (rc != ABORT) {
        access->processed = true;
        if (rc == WAIT)
            access->waiting = true;
    }
#else // TRANSPORT == ONE_SIDED
    assert(TRANSPORT == ONE_SIDED);
    assert(!g_is_server);
    rc = get_remote_row(idx);
#endif
    return rc;
}

// acquire a lock on the row
RC lock_manager_t::get_row(cc_manager_t::RowAccess* access) {
    RC rc = RCOK;
#if TRANSPORT == TWO_SIDED
    assert(g_is_server);
    auto txn_state = _txn->get_state();
    if (txn_state != txn_t::state_t::RUNNING){ // wounded by another txn
        assert(txn_state == txn_t::state_t::ABORTING);
        rc = ABORT;
        cleanup(rc);
        // abort(access);
        return rc;
    }

    assert(access != nullptr); 
    row_t* row = process_index(access);
    auto lock_type = (access->type == RD) ? lock_type_t::LOCK_SH : lock_type_t::LOCK_EX;
    rc = row->manager->lock_get(lock_type, _txn);
    if (rc == ABORT) {
        _txn->set_state(txn_t::state_t::ABORTING);
        cleanup(rc);
    }
    else {
        access->processed = true;
        if (rc == WAIT)
            access->waiting = true;
    }
        // abort(access);
#else // TRANSPORT == ONE_SIDED
    assert(TRANSPORT == ONE_SIDED);
    assert(!g_is_server);
    rc = get_remote_row(access);
#endif
    return rc;
}

void lock_manager_t::abort(uint32_t idx) {
    auto txn_state = _txn->get_state();
    if (txn_state != txn_t::state_t::ABORTING)
        _txn->set_state(txn_t::state_t::ABORTING); // allow other txns to wound this txn

#if TRANSPORT == ONE_SIDED
    char* buffer = transport->get_buffer();
    for (uint32_t i=0; i<idx; i++) {
        auto access = &_access_set[i];
        auto ltype = (access->type == RD) ? lock_type_t::LOCK_SH : lock_type_t::LOCK_EX;
        row_t* row = reinterpret_cast<row_t*>(buffer);
        // auto row_man = new ((ROW_MAN*)&row->manager) ROW_MAN();
        auto row_man = (ROW_MAN*)&row->manager;
        row_man->lock_release(ltype, _txn, access->value.addr);
        if (access->cache_data) {
            delete[] access->cache_data;
            access->cache_data = nullptr;
        }
        if (access->data) {
            delete[] access->data;
            access->data = nullptr;
        }
    }

    for (uint32_t i=idx; i<_access_set.size(); i++) {
        auto access = &_access_set[i];
        if (access->cache_data) {
            delete[] access->cache_data;
            access->cache_data = nullptr;
        }
        if (access->data) {
            delete[] access->data;
            access->data = nullptr;
        }
    }
#else // TRANSPORT == TWO_SIDED
    assert(false);
    for (auto& access: _access_set) {
        auto ltype = (access.type == RD) ? lock_type_t::LOCK_SH : lock_type_t::LOCK_EX;
        access.value.row->manager->lock_release(ltype, _txn);
        if (access.cache_data) {
            delete[] access.cache_data;
            access.cache_data = nullptr;
        }
        if (access.data) {
            delete[] access.data;
            access.data = nullptr;
        }
    }
#endif
    clear();
}

void lock_manager_t::abort(cc_manager_t::RowAccess* access) {
    assert(false);
    uint32_t access_idx = 0;
    for (auto it=_access_set.begin(); it!=_access_set.end(); it++) {
        if (&(*it) == access)
            break;
        access_idx++;
    }
    cleanup(ABORT);
    // abort(access_idx);
}

// 2PC prepare phase
RC lock_manager_t::process_prepare(char* data, uint32_t size) {
    auto txn_state = _txn->get_state();
    if (txn_state == txn_t::state_t::ABORTING) {
        cleanup(ABORT);
        return ABORT;
    }
    assert(txn_state == txn_t::state_t::RUNNING);

    bool success = _txn->update_state(txn_state, txn_t::state_t::COMMITTING);
    if (!success) { // wounded by another txn
        assert(_txn->get_state() == txn_t::state_t::ABORTING);
        cleanup(ABORT);
        return ABORT;
    }

    if (size > 0) { // deserialize write data
        UnstructuredBuffer buffer(data);
        uint32_t access_idx = 0;
        while (buffer.size() < size) {
            auto access = get_write_access(access_idx);
            assert(access != nullptr);
            char* temp = access->data;
            buffer.get(temp, access->data_size);
        }
        assert(size == buffer.size());
    }
    
    return RCOK;
}

// 2PC commit phase
RC lock_manager_t::process_commit() {
    RC rc = COMMIT;
    auto txn_state = _txn->get_state();
    // read-only single-node TXN may not have entered COMMITTING state yet
    if (txn_state == txn_t::state_t::ABORTING) // wounded
        rc = ABORT;
    else if (txn_state == txn_t::state_t::RUNNING) {
        assert(txn_state == txn_t::state_t::RUNNING);
        bool success = _txn->update_state(txn_state, txn_t::state_t::COMMITTING);
        if (!success) { // wounded by another txn
            assert(_txn->get_state() == txn_t::state_t::ABORTING);
            rc = ABORT;
        }
    }
    else
        assert(txn_state == txn_t::state_t::COMMITTING);
    cleanup(rc);
    return rc;
}

void lock_manager_t::cleanup(RC rc) {
    assert(rc == COMMIT || rc == ABORT);
    if (rc == COMMIT)
        commit_insdel();
    auto txn_state = _txn->get_state();
    assert(txn_state == txn_t::state_t::COMMITTING || txn_state == txn_t::state_t::ABORTING);
#if TRANSPORT == ONE_SIDED
    char* buffer = transport->get_buffer();
    uint32_t offset = 0;
    for (uint32_t i=0; i<_access_set.size(); i++) {
        auto access = &_access_set[i];
        auto ltype = (access->type == RD) ? lock_type_t::LOCK_SH : lock_type_t::LOCK_EX;
        bool processed = access->processed;
        row_t* row = reinterpret_cast<row_t*>(buffer);
        // row_t* row = reinterpret_cast<row_t*>(buffer + offset);
        global_addr_t row_addr = access->value.addr;
        if (access->type == WR && rc == COMMIT && processed) {
            auto schema = GET_WORKLOAD->get_table(access->table_id)->get_schema();
            row->copy(schema, access->data);
            global_addr_t write_addr = GADD(row_addr, 16); // skip lock part
            transport->write(reinterpret_cast<char*>(row) + 16, write_addr, access->data_size, i==(_access_set.size()-1) ? true : false);
        }
        // auto row_man = new ((ROW_MAN*)&row->manager) ROW_MAN();
        auto row_man = (ROW_MAN*)&row->manager;
        if (processed)
            row_man->lock_release(ltype, _txn, access->value.addr);

        if (access->cache_data) {
            delete[] access->cache_data;
            access->cache_data = nullptr;
        }
        if (access->data) {
            delete[] access->data;
            access->data = nullptr;
        }
        offset += sizeof(row_t);
    }
#else // TRANSPORT == TWO_SIDED
    for (auto & access : _access_set) {
        if (g_is_server) { 
            auto ltype = (access.type == RD) ? lock_type_t::LOCK_SH : lock_type_t::LOCK_EX;
            if (access.type == WR && rc == COMMIT) {
                auto schema = GET_WORKLOAD->get_table(access.table_id)->get_schema();
                access.value.row->copy(schema, access.data);
            }
            if (access.value.row != nullptr)
                access.value.row->manager->lock_release(ltype, _txn, access.value.addr);
        }
        if (access.cache_data) {
            delete[] access.cache_data;
            access.cache_data = nullptr;
        }
        if (access.data) {
            delete[] access.data;
            access.data = nullptr;
        }
    }
#endif
    clear();
}



///////////////////////////////
// one-sided RDMA operations //
///////////////////////////////

// get row data after acquiring its lock for stored procedure txn
void lock_manager_t::get_resp_data() {
    assert(TRANSPORT == ONE_SIDED);
    char* buffer = transport->get_buffer();
    uint32_t offset = 0;
    for (uint32_t i=0; i<_access_set.size(); i++) {
        auto access = &_access_set[i];
        global_addr_t row_addr = access->value.addr;
        global_addr_t data_addr = GADD(row_addr, 16); // skip lock part
        // printf("[data %u] row_addr %lu data_addr %lu\n", i, row_addr.addr, data_addr.addr);
        char* data = buffer + offset;
        uint32_t data_size = access->data_size;
        assert(data_size > 0 && access->data != nullptr);
        transport->read(data, data_addr, data_size);
        offset += data_size;
    }

    offset = 0;
    for (auto& access: _access_set) {
        assert(access.data != nullptr);
        char* data = buffer + offset;
        memcpy(access.data, buffer + offset, access.data_size);
        offset += access.data_size;
    }
}

// get data after acquiring its lock for interactive txn
void lock_manager_t::get_resp_data(uint32_t access_idx) {
    assert(TRANSPORT == ONE_SIDED);
    char* buffer = transport->get_buffer();
    auto access = &_access_set[access_idx];
    global_addr_t row_addr = access->value.addr;
    global_addr_t data_addr = GADD(row_addr, 16); // skip lock part
    assert(access->data_size > 0 && access->data != nullptr);
    transport->read(buffer, data_addr, access->data_size);
    memcpy(access->data, buffer, access->data_size);
}

// get row lock
RC lock_manager_t::get_remote_row(uint32_t access_idx) {
    assert(TRANSPORT == ONE_SIDED);
    RC rc = RCOK;
    auto access = &_access_set[access_idx];
    _last_access = access;
    if (_txn->get_state() != txn_t::state_t::RUNNING)
        return ABORT;

    auto index = GET_WORKLOAD->get_index(access->index_id);
    row_t* row = reinterpret_cast<row_t*>(transport->get_buffer());
    value_t value;
    bool found = index->lookup(access->key, value);
    if (!found) { // this is an insert
        uint64_t key = access->key;
        value.addr = GET_WORKLOAD->rpc_alloc(access->node_id, sizeof(row_t));
        row->init_manager();
        auto schema = GET_WORKLOAD->get_table(access->table_id)->get_schema();
        row->set_value(schema, 0, &key);
        for (uint32_t fid=1; fid<schema->get_field_cnt(); fid++){
            char value[schema->get_field_size(fid)] = {0};
            strncpy(value, "hello", schema->get_field_size(fid));
            row->set_value(schema, fid, value);
        }
        transport->write(reinterpret_cast<char*>(row), value.addr, access->data_size);
        InsertOp ins(index, key, value);
        _inserts.push_back(ins);
    }
    assert(value.addr != global_addr_t::null());
    access->value = value;

    auto lock_type = (access->type == RD) ? lock_type_t::LOCK_SH : lock_type_t::LOCK_EX;
    auto row_man = (ROW_MAN*)&row->manager;
    rc = row_man->lock_get(lock_type, _txn, access->value.addr);
    if (rc != ABORT) {
        access->processed = true;
        if (rc == WAIT)
            access->waiting = true;
    }

    return rc;
}

// get row lock
RC lock_manager_t::get_remote_row(cc_manager_t::RowAccess* access) {
    assert(TRANSPORT == ONE_SIDED);
    RC rc = RCOK;
    _last_access = access;
    if (_txn->get_state() != txn_t::state_t::RUNNING)
        return ABORT;

    auto index = GET_WORKLOAD->get_index(access->index_id);
    row_t* row = reinterpret_cast<row_t*>(transport->get_buffer());
    value_t value;
    bool found = index->lookup(access->key, value);
    if (!found) { // this is an insert
        uint64_t key = access->key;
        value.addr = GET_WORKLOAD->rpc_alloc(access->node_id, sizeof(row_t));
        row->init_manager();
        auto schema = GET_WORKLOAD->get_table(access->table_id)->get_schema();
        row->set_value(schema, 0, &key);
        for (uint32_t fid=1; fid<schema->get_field_cnt(); fid++){
            char value[schema->get_field_size(fid)] = {0};
            strncpy(value, "hello", schema->get_field_size(fid));
            row->set_value(schema, fid, value);
        }
        transport->write(reinterpret_cast<char*>(row), value.addr, access->data_size);
        InsertOp ins(index, key, value);
        _inserts.push_back(ins);
    }
    access->value = value;

    auto lock_type = (access->type == RD) ? lock_type_t::LOCK_SH : lock_type_t::LOCK_EX;
    auto row_man = (ROW_MAN*)&row->manager;
    rc = row_man->lock_get(lock_type, _txn, access->value.addr);
    if (rc != ABORT) {
        access->processed = true;
        if (rc == WAIT)
            access->waiting = true;
    }

    return rc;
}


#endif // !PARTITIONED