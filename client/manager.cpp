#include "storage/row.h"
#include "client/manager.h"
#include "client/transport.h"
#include "client/thread.h"
#include "system/workload.h"
#include "system/stats.h"
#include "system/manager.h"
#include "utils/debug.h"
#include "utils/helper.h"
#include "txn/txn.h"
#include "txn/stored_procedure.h"
#include "txn/interactive.h"
#include "txn/table.h"
#include "benchmarks/ycsb/stored_procedure.h"
#include "benchmarks/ycsb/interactive.h"
#include "benchmarks/ycsb/workload.h"

client_txn_manager_t::client_txn_manager_t(thread_t* thread)
    : txn_manager_t(thread), _txn(nullptr) { }

client_txn_manager_t::~client_txn_manager_t() {
    clear_txn();
}

void client_txn_manager_t::init(base_query_t* query) {
    clear_txn();
    _txn = GET_WORKLOAD->create_txn(query);
}

RC client_txn_manager_t::run() {
    assert(_txn != nullptr);
    // printf("TXN %lu begins (ts %lu)\n", _txn->get_id().to_uint64(), _txn->get_ts());
    // fflush(stdout);
    _txn->init();
    txn_table->insert(_txn->get_id(), _txn);
    RC rc = execute();
    txn_table->remove(_txn->get_id());
    if (rc == RCOK) { // txn has committed
    //     printf("TXN %lu commits (ts %lu)\n", _txn->get_id().to_uint64(), _txn->get_ts());
    // fflush(stdout);
        delete _txn;
        _txn = nullptr;
    }
    return rc;
}

RC client_txn_manager_t::execute() { 
    RC rc = _txn->execute();
    if (rc == RCOK) {
        rc = _txn->process_request();
        if (rc == RCOK)
            rc = execute();
    }
    else if (rc == FINISH)
        rc = _txn->process_commit();
    return rc;   
}

void client_txn_manager_t::global_sync() {
    if (GET_THD_ID == 0) {
        char* base_ptr = transport->get_buffer();
        uint32_t base_size = sizeof(message_t) - sizeof(char*);
        for (uint32_t i=0; i<g_num_server_nodes; i++) {
            char* send_ptr = base_ptr + i * base_size;
            auto msg = new (send_ptr) message_t(message_t::type_t::SYNC, 0, 0);
            transport->send(send_ptr, i, base_size);
        }

        struct ibv_wc wc[g_num_server_nodes];
        uint32_t num = transport->poll(wc, g_num_server_nodes);
        assert(num == g_num_server_nodes);
        // post receives for future communication
        for (uint32_t i=0; i<g_num_server_nodes; i++) {
            char* recv_buffer = transport->get_recv_buffer(i);
            transport->post_recv(recv_buffer, i, 0, MAX_MESSAGE_SIZE);
            global_manager->set_sync_done();
        }
    }
    else {
        while (!global_manager->get_sync_done())
            PAUSE
    }
}

void client_txn_manager_t::terminate() {
    char* base_ptr = transport->get_buffer();
    uint32_t base_size = sizeof(message_t) - sizeof(char*);
    for (uint32_t i=0; i<g_num_server_nodes; i++) {
        char* send_ptr = base_ptr + i * base_size;
        auto msg = new (send_ptr) message_t(message_t::type_t::TERMINATE, 0, 0);
        transport->send(send_ptr, i, base_size);
    }
}

void client_txn_manager_t::clear_txn() {
    if (_txn) {
        if (txn_table->find(_txn->get_id()) != nullptr)
            txn_table->remove(_txn->get_id());
        _txn->force_abort();
        delete _txn;
        _txn = nullptr;
    }
}
