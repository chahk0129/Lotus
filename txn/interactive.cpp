#include "txn/id.h"
#include "txn/interactive.h"
#include "system/query.h"
#include "transport/transport.h"
#include "system/cc_manager.h"
#include "concurrency/lock_manager.h"
#include "utils/packetize.h"
#include "batch/table.h"
#include <algorithm>

interactive_t::interactive_t(txn_id_t txn_id)
    : txn_t(txn_id) { }

interactive_t::interactive_t(base_query_t* query)
    : txn_t(query) { }

void interactive_t::init() {
    txn_t::init();
}

uint32_t interactive_t::get_nodes_involved(std::vector<uint32_t>& nodes) {
    return _cc_manager->get_last_node_involved();
}

#if TRANSPORT == TWO_SIDED
RC interactive_t::process_request() {
    RC rc = RCOK;
    uint32_t qp_id = get_id().get_qp_id();
    uint32_t data_size = 0;
    uint32_t base_size = sizeof(message_t) - sizeof(char*);

    uint32_t node_id = _cc_manager->get_last_node_involved();
    char* base_ptr = transport->get_buffer(node_id);
    auto send_msg = new (base_ptr) message_t(message_t::type_t::REQUEST_INTERACTIVE, qp_id, 0);
    char* send_data = send_msg->get_data_ptr();
    _cc_manager->serialize_last(node_id, send_data, data_size);
    if (data_size == 0)
        return rc;
    send_msg->set_data_size(data_size);

    char* recv_buffer = transport->get_recv_buffer(node_id);
    auto recv_msg = reinterpret_cast<message_t*>(recv_buffer);
#if BATCH
    auto batch_man = global_manager->get_batch_manager();
    auto decision = batch_man->get_decision();
    uint32_t wait_time = 0;
    bool batch_status = false;
    uint64_t latency = get_sys_clock();

    if (decision == batch_decision_t::WAIT_FOR_BATCH) {
        wait_time = batch_man->submit_request(node_id, base_size + data_size);
        std::vector<uint32_t> nodes{node_id};
        if (batch_man->is_leader()) {
            batch_status = handle_batching_leader(nodes, &wait_time);
            batch_man->check_leader_expiration_status();
            batch_man->report_leader(get_sys_clock() - latency, batch_status);
        }
        else {
            batch_status = handle_batching_member(nodes, &wait_time);
            batch_man->report_member(get_sys_clock() - latency, true, batch_status);
        }
    }
    else { // SEND_IMMEDIATELY
        transport->send(base_ptr, node_id, base_size + data_size);
        struct ibv_wc wc;
        while (true) {
            int polled = transport->poll_once(&wc, 1);
            if (polled > 0) {
                assert(polled == 1);
                assert(wc.wr_id == node_id);
                break;
            }
            else {
                if (GET_THD_ID % batch_table_t::MAX_GROUP_SIZE == 0) // this thread is assigned in the same core as batch thread
                    std::this_thread::yield(); // leader yields CPU to avoid busy waiting
            }
        }
        recv_buffer = transport->get_recv_buffer(node_id);
        recv_msg = reinterpret_cast<message_t*>(recv_buffer);
        transport->post_recv(recv_buffer, node_id, MAX_MESSAGE_SIZE); // prepost receive
        batch_man->report_member(get_sys_clock() - latency);
    }
#else // no BATCH
    transport->send(base_ptr, node_id, base_size + data_size);
    struct ibv_wc wc;
    uint32_t num = transport->poll(&wc, 1);
    assert(num == 1);
    assert(wc.wr_id == node_id);
    transport->post_recv(recv_buffer, node_id, MAX_MESSAGE_SIZE); // prepost
    recv_buffer = transport->get_recv_buffer(node_id);
    recv_msg = reinterpret_cast<message_t*>(recv_buffer);
#endif

    auto type = recv_msg->get_type();
    assert(recv_msg->get_node_id() == node_id);
    if (type != message_t::type_t::ACK) { // ABORT
        assert(type == message_t::type_t::RESPONSE_ABORT);
        std::vector<uint32_t> nodes{node_id};
        _cc_manager->clear_nodes_involved(nodes);
        process_abort_nodes();
        rc = ABORT;
    }
    else { 
        char* recv_data = recv_msg->get_data_ptr();
        uint32_t recv_size = recv_msg->get_data_size();
        _cc_manager->deserialize_last(node_id, recv_data, recv_size);
    }

    return rc;
}
#else // TRANSPORT == ONE_SIDED
RC interactive_t::process_request() {
    auto access = _cc_manager->get_last_access();
    RC rc = _cc_manager->get_row(access);
    if (rc == ABORT) { // lock conflict
        int completed_access_idx = _cc_manager->get_last_access_idx();
        process_abort(completed_access_idx);
    }
    return rc;
}
#endif