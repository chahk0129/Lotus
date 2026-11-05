#include "system/global.h"
#include "system/manager.h"
#include "system/cc_manager.h"
#include "system/workload.h"
#include "benchmarks/ycsb/interactive.h"
#include "benchmarks/ycsb/workload.h"
#include "benchmarks/ycsb/query.h"
#include "concurrency/lock_manager.h"
#include "transport/message.h"
#include "storage/row.h"
#include "storage/table.h"
#include "storage/catalog.h"
#include "txn/id.h"
#include "txn/txn.h"
#include "txn/interactive.h"

#if WORKLOAD == YCSB

// client constructor
ycsb_interactive_t::ycsb_interactive_t(base_query_t* query)
    : interactive_t(query) { init(); }

// server constructor
ycsb_interactive_t::ycsb_interactive_t(txn_id_t txn_id)
    : interactive_t(txn_id) { init(); }

void ycsb_interactive_t::init() {
    interactive_t::init();
    _curr_step = 0;
    _curr_idx = 0;
}

// client function
RC ycsb_interactive_t::execute() {
    assert(g_is_server == false);
    RC rc = RCOK;
    uint32_t table_id = 0;
    uint32_t index_id = 0;
    auto wl = GET_WORKLOAD;
    auto index = wl->get_index(index_id);
    auto query = static_cast<ycsb_query_t*>(_query);
    auto cc_man = get_cc_manager();
    access_t type = TYPE_NONE;
    assert(query);

    uint32_t request_cnt = query->get_request_count();
    auto requests = query->get_requests();
    ycsb_query_t::request_t* req = nullptr;
    if (_curr_step > 0 || _curr_idx > 0) { // get data for the last registered access
        if (_curr_idx == 0) // RD/WR
            req = &requests[_curr_step - 1];
        else // SCAN
            req = &requests[_curr_step];
        type = req->rtype;
        if (type == RD) {
            char *data = cc_man->get_data(req->key, table_id);
            assert(data != nullptr);
            #if PARTITIONED 
            __attribute__((unused)) uint64_t val = *(uint64_t *)data;
            #else
            for (int fid=0; fid<10; fid++)
                __attribute__((unused)) uint64_t fval = *(uint64_t *)(&data[fid * 100]);
            #endif
        } 
        else if (type == WR) {
            char *data = cc_man->get_data(req->key, table_id);
            assert(data != nullptr);
            #if PARTITIONED 
            *(uint64_t *)data = req->value;
            #else
            uint64_t txn_id = _txn_id.to_uint64();
            for (int fid=1; fid<10; fid++)
                *(uint64_t *)(&data[fid * 100]) = txn_id;
            #endif
        }
        else {
            uint64_t key = req->key + _curr_idx;
            char* data = cc_man->get_data(key, table_id);
            assert(data != nullptr);
            #if PARTITIONED 
            __attribute__((unused)) uint64_t val = *(uint64_t *)data;
            #else
            for (int fid=0; fid<10; fid++)
                __attribute__((unused)) uint64_t fval = *(uint64_t *)(&data[fid * 100]);
            #endif
            _curr_idx++;
            if (_curr_idx == req->value) {
                _curr_step++;
                _curr_idx = 0; // reset for next request
            }
        }

        // processed all the requests, proceed to commit
        if (_curr_step == request_cnt)
            return FINISH; // proceed to commit
    }
    // register the next request to the request list
    if (_curr_step < request_cnt) {
        req = &requests[_curr_step];
        uint32_t node_id = wl->get_partition_by_key(req->key);
        type = req->rtype;
        if (type == SCAN) { 
            // HACK: we don't actually scan an index, 
            //       just perform read a key at a time, 
            //       because Sherman has a strong assumption that
            //       all the nodes are cached for scan, which is too good to be true
            uint64_t key = req->key + _curr_idx;
            uint32_t node_id = wl->get_partition_by_key(key);
            cc_man->register_access(node_id, RD, key, table_id, index_id, 0);
            if (_curr_idx == 0)
                _curr_step++; // move to next request only when the first key is registered
            _curr_idx++;
        }
        else { // RD or WR
            uint32_t node_id = wl->get_partition_by_key(req->key);
            cc_man->register_access(node_id, type, req->key, table_id, index_id, req->value);
            _curr_step++;
        }
    }
    
    return RCOK;
}

#endif // end of WORKLOAD == YCSB