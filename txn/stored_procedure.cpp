#include "txn/id.h"
#include "txn/stored_procedure.h"
#include "system/workload.h"
#include "system/query.h"
#include "transport/transport.h"
#include "system/cc_manager.h"
#include "concurrency/lock_manager.h"
#include "utils/packetize.h"

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
    std::vector<uint32_t> dispatched_nodes; // subset of nodes_involved actually sent to (a node can serialize empty -- see idx_manager_t::serialize, client cache hits)
    std::vector<uint32_t> nodes_aborted;
    bool aborting = false;
    uint32_t base_size = sizeof(message_t) - sizeof(char*);
    uint32_t qp_id = get_id().get_qp_id();

    uint32_t num_resp_expected = _cc_manager->get_nodes_involved(nodes_involved);
    std::vector<cc_manager_t::RowAccess*> access_sets[g_num_server_nodes];
    dispatched_nodes.reserve(num_resp_expected);

    send_begin();
    for (uint32_t i=0; i<num_resp_expected; i++) {
        uint32_t node_id = nodes_involved[i];
        uint32_t data_size = 0;
        char* send_ptr = transport->get_buffer(node_id);
        auto msg = new (send_ptr) message_t(message_t::type_t::REQUEST_STORED_PROCEDURE, qp_id, 0);
        char* send_data = msg->get_data_ptr();

        _cc_manager->serialize(node_id, access_sets[node_id], send_data, data_size);
        msg->set_data_size(data_size);
        if (data_size == 0)
            continue;

        send_dispatch(node_id, send_ptr, base_size, data_size);
        dispatched_nodes.push_back(node_id);
    }
    send_end(dispatched_nodes.size());

    for (auto& node_id: dispatched_nodes) {
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
    // resume after the accesses locked in previous rounds: the one-sided lock word
    // carries no owner id, so re-acquiring an owned EX lock self-conflicts and a
    // re-acquired SH lock leaks a reader count on release
    for (uint32_t i=_curr_offset; i<num_accesses; i++) {
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

    // fetch row data only for this round's accesses
    for (uint32_t i=_curr_offset; i<num_accesses; i++)
        _cc_manager->get_resp_data(i);
    _curr_offset = num_accesses;
    return RCOK;
}
#endif