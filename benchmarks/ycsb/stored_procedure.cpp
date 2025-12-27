#include "system/global.h"
#include "system/manager.h"
#include "system/cc_manager.h"
#include "system/workload.h"
#include "benchmarks/ycsb/stored_procedure.h"
#include "benchmarks/ycsb/workload.h"
#include "benchmarks/ycsb/query.h"
#include "concurrency/lock_manager.h"
#include "transport/message.h"
#include "storage/row.h"
#include "storage/table.h"
#include "storage/catalog.h"
#include "txn/id.h"
#include "txn/txn.h"
#include "txn/stored_procedure.h"

#if WORKLOAD == YCSB

// client constructor
ycsb_stored_procedure_t::ycsb_stored_procedure_t(base_query_t* query)
    : stored_procedure_t(query) { init(); }

// server constructor
ycsb_stored_procedure_t::ycsb_stored_procedure_t(txn_id_t txn_id)
    : stored_procedure_t(txn_id) { init(); }

void ycsb_stored_procedure_t::init() {
    stored_procedure_t::init();
    _curr_step = 0;
}

// client function
RC ycsb_stored_procedure_t::execute() {
    assert(g_is_server == false);
    RC rc = RCOK;
    uint32_t table_id = 0;
    uint32_t index_id = 0;
    auto wl = GET_WORKLOAD;
    auto index = wl->get_index(index_id);
    auto query = static_cast<ycsb_query_t*>(_query);
    auto cc_man = get_cc_manager();
    assert(query);

    uint32_t request_cnt = query->get_request_count();
    auto requests = query->get_requests();
    if (_curr_step != 0) { // get data for the last registered access
        for (uint32_t i=0; i<request_cnt; i++) {
            auto req = &requests[i];
            char* data = nullptr;
            if (req->rtype == RD) {
                data = cc_man->get_data(req->key, table_id);
                assert(data != nullptr);
                #if PARTITIONED 
                __attribute__((unused)) uint64_t val = *(uint64_t *)data;
                #else
                for (int fid=0; fid<10; fid++)
                    __attribute__((unused)) uint64_t fval = *(uint64_t *)(&data[fid * 100]);
                #endif
            }
            else if (req->rtype == WR) {
                data = cc_man->get_data(req->key, table_id);
                assert(data != nullptr);
                #if PARTITIONED 
                *(uint64_t *)data = req->value;
                #else
                uint64_t txn_id = _txn_id.to_uint64();
                for (int fid=1; fid<10; fid++)
                    *(uint64_t *)(&data[fid * 100]) = txn_id;
                #endif
            }
            else { // SCAN
                for (uint32_t j=0; j<req->value; j++) {
                    uint64_t key = req->key + j;
                    data = cc_man->get_data(key, table_id);
                    assert(data != nullptr);
                    #if PARTITIONED 
                    __attribute__((unused)) uint64_t val = *(uint64_t *)data;
                    #else
                    for (int fid=0; fid<10; fid++)
                        __attribute__((unused)) uint64_t fval = *(uint64_t *)(&data[fid * 100]);
                    #endif
                }
            }
        }
        return FINISH; // proceed to commit
    }
    else { // register all the request information 
        for (uint32_t i=0; i<request_cnt; i++) {
            auto req = &requests[i];
            auto type = req->rtype;
            if (type == SCAN) {
                // HACK: we don't actually scan an index, 
                //       just perform read a key at a time, 
                //       because Sherman has a strong assumption that
                //       all the nodes are cached for scan, which is too good to be true
                for (uint32_t j=0; j<req->value; j++) {
                    uint64_t key = req->key + j;
                    uint32_t node_id = wl->get_partition_by_key(key);
                    cc_man->register_access(node_id, RD, key, table_id, index_id, 0);
                }
            }
            else { // RD or WR
                uint32_t node_id = wl->get_partition_by_key(req->key);
                cc_man->register_access(node_id, req->rtype, req->key, table_id, index_id, req->value);
            }
            // printf("TXN %lu register access %u (key %lu)\n", _txn_id.to_uint64(), i, req->key);
        }
        _curr_step = request_cnt;
        return RCOK;
    }
}

#endif // end of WORKLOAD == YCSB
