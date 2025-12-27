#include "txn/id.h"
#include "txn/stored_procedure.h"
#include "system/workload.h"
#include "system/query.h"
#include "transport/transport.h"
#include "system/cc_manager.h"
#include "concurrency/lock_manager.h"
#include "utils/packetize.h"
#include "batch/manager.h"
#include "batch/decision.h"

stored_procedure_t::stored_procedure_t(txn_id_t txn_id)
    : txn_t(txn_id) { }

stored_procedure_t::stored_procedure_t(base_query_t* query)
    : txn_t(query) { }

void stored_procedure_t::init() {
    txn_t::init();
}


/////////////////////
// client operations
/////////////////////
uint32_t stored_procedure_t::get_nodes_involved(std::vector<uint32_t>& nodes) {
    return _cc_manager->get_nodes_involved(nodes);
}

#if TRANSPORT == TWO_SIDED
RC stored_procedure_t::process_request() {
    std::vector<uint32_t> nodes_involved;
    std::vector<uint32_t> nodes_aborted;
    bool aborting = false;
    uint32_t base_size = sizeof(message_t) - sizeof(char*);
    uint32_t qp_id = get_id().get_qp_id();

    uint32_t num_resp_expected = _cc_manager->get_nodes_involved(nodes_involved);
    std::vector<cc_manager_t::RowAccess*> access_sets[g_num_server_nodes];
#if BATCH
    auto batch_man = global_manager->get_batch_manager();
    auto decision = batch_man->get_decision();
    uint32_t wait_time[num_resp_expected];
    bool batch_status = false;
    uint64_t latency = get_sys_clock();
#endif

    uint32_t total_size = 0;
    uint32_t sent_num = 0;
    for (uint32_t i=0; i<num_resp_expected; i++) {
        uint32_t data_size = 0;
        char* send_ptr = transport->get_buffer(nodes_involved[i]);
        auto msg = new (send_ptr) message_t(message_t::type_t::REQUEST_STORED_PROCEDURE, qp_id, 0);
        char* send_data = msg->get_data_ptr();

        // _cc_manager->serialize(nodes_involved[i], access_sets[i], send_data, data_size);
        _cc_manager->serialize(nodes_involved[i], access_sets[nodes_involved[i]], send_data, data_size);
        msg->set_data_size(data_size);
        if (data_size == 0) {
            continue;
        }
        sent_num++;

#if BATCH
        if (decision == batch_decision_t::WAIT_FOR_BATCH)
            wait_time[i] = batch_man->submit_request(nodes_involved[i], data_size + base_size);
        else {
            assert(decision == batch_decision_t::SEND_IMMEDIATELY);
#endif
        transport->send(send_ptr, nodes_involved[i], base_size + data_size);
#if BATCH
        }
#endif
    }

#if BATCH
    if (decision == batch_decision_t::WAIT_FOR_BATCH && sent_num > 0) {
        if (batch_man->is_leader()) {
            batch_status = handle_batching_leader(nodes_involved, wait_time);
            batch_man->check_leader_expiration_status();
            batch_man->report_leader(get_sys_clock() - latency, batch_status);
        }
        else {
            batch_status = handle_batching_member(nodes_involved, wait_time);
            batch_man->report_member(get_sys_clock() - latency, true, batch_status);
        }
        // handle_batching(nodes_involved, wait_time);
        for (auto& node_id: nodes_involved) {
            char* recv_buffer = transport->get_recv_buffer(node_id);
            auto recv_msg = reinterpret_cast<message_t*>(recv_buffer);
            auto type = recv_msg->get_type();
            if (type == message_t::type_t::RESPONSE_ABORT) {
                aborting = true;
                nodes_aborted.push_back(node_id);
            }
            else {
                assert(type == message_t::type_t::ACK); 
                if (!aborting) {
                    char* recv_data = recv_msg->get_data_ptr();
                    uint32_t recv_size = recv_msg->get_data_size();
                    _cc_manager->deserialize(node_id, access_sets[node_id], recv_data, recv_size);
                }
            }
        }
    }
    else { // SEND_IMMEDIATELY
            assert(decision == batch_decision_t::SEND_IMMEDIATELY || sent_num == 0);
#endif
    struct ibv_wc wc[sent_num];
    uint32_t num = 0;
    while (num < sent_num) { 
        int polled = transport->poll_once(wc, sent_num - num);
        if (polled > 0) {
            for (uint32_t i=0; i<polled; i++) {
                uint64_t node_id = wc[i].wr_id;
                char* recv_buffer = transport->get_recv_buffer(node_id);
                auto recv_msg = reinterpret_cast<message_t*>(recv_buffer);
                if (recv_msg->get_type() != message_t::type_t::ACK) {
                    assert(recv_msg->get_type() == message_t::type_t::RESPONSE_ABORT);
                    aborting = true;
                    nodes_aborted.push_back(node_id);
                }
                else {
                    if (!aborting) {
                        char* recv_data = recv_msg->get_data_ptr();
                        uint32_t recv_size = recv_msg->get_data_size();
                        _cc_manager->deserialize(node_id, access_sets[node_id], recv_data, recv_size);
                    }
                }
                transport->post_recv(recv_buffer, node_id, MAX_MESSAGE_SIZE); // prepost
            }
            num += polled;
        }
    }
#if BATCH
    batch_man->report_member(get_sys_clock() - latency);
    }
#endif

    if (aborting) { // some nodes voted abort
        _cc_manager->clear_nodes_involved(nodes_aborted);
        process_abort_nodes();
        return ABORT;
    }
    _cc_manager->clear_nodes_involved();
    return RCOK; 
}

#else // TRANSPORT == ONE_SIDED

RC stored_procedure_t::process_request() {
    assert(TRANSPORT == ONE_SIDED);
    uint32_t num_accesses = _cc_manager->get_row_cnt();
    RC rc = RCOK;
    uint32_t completed_access_idx = num_accesses;
    for (uint32_t i=0; i<num_accesses; i++) {
        rc = _cc_manager->get_row(i);
        if (rc == ABORT) { // lock conflict
            completed_access_idx = i;
            break;
        }
    }

    if (completed_access_idx < num_accesses) { // abort
        assert(rc == ABORT);
        process_abort(completed_access_idx);
        return rc;
    }

    _cc_manager->get_resp_data();
    return RCOK;
}
#endif