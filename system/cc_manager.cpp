#include "system/cc_manager.h"
#include "concurrency/lock_manager.h"
#include "concurrency/idx_manager.h"
#include "index/idx_wrapper.h"
#include "storage/row.h"
#include "txn/txn.h"

cc_manager_t* cc_manager_t::create(txn_t* txn) {
    return new CC_MAN(txn);
}

cc_manager_t::cc_manager_t(txn_t* txn)
    : _txn(txn) {
    clear();
}

void cc_manager_t::clear() {
    _num_write_accesses = 0;
    _last_access_idx = -1;
    _last_access = nullptr;
    _access_set.clear();
    _index_access_set.clear();
    _nodes_involved.clear();
    _inserts.clear();
    _deletes.clear();
}

void cc_manager_t::commit_insdel() {
    for (auto & ins: _inserts) {
        auto index = ins.index;
        index->insert(ins.key, ins.value);
    }

    for (auto & del: _deletes) {
        auto index = del.index;
#if TRANSPORT == TWO_SIDED
        // tombstone the row while its EX lock is still held so a stale cache
        // hint to this row fails validation (rows are never freed or reused)
        del.value.row->set_deleted();
#endif
        index->remove(del.key);
    }
}


uint32_t cc_manager_t::get_access_idx(uint64_t key, uint32_t table_id) {
    for (uint32_t i=0; i<_access_set.size(); i++) {
        auto& access = _access_set[i];
        if (access.key == key && access.table_id == table_id)
            return i;
    }
    assert(false);
    return -1;
}

cc_manager_t::RowAccess* cc_manager_t::get_write_access(uint32_t& idx_writes) {
    if (_num_write_accesses == 0)
        return nullptr;
    while (idx_writes < _access_set.size()) {
        auto access = &_access_set[idx_writes];
        idx_writes++;
        if (access->type == WR)
            return access;
    }
    assert(false);
    return nullptr;
}

cc_manager_t::RowAccess* cc_manager_t::get_access(uint64_t key, uint32_t table_id) {
    for (auto & access : _access_set) {
        if (access.key == key && access.table_id == table_id)
            return &access;
    }
    return nullptr;
}


