#include "txn/id.h"
#include "txn/interactive.h"
#include "system/query.h"
#include "transport/transport.h"
#include "system/cc_manager.h"
#include "concurrency/lock_manager.h"
#include "utils/packetize.h"
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
// A step may register several accesses before returning (e.g. TPC-C NewOrder's
// ITEM/STOCK/ORDERLINE loops); the interactive txn sends each one in its own round trip.
RC interactive_t::process_request() {
    RC rc = RCOK;
    while (_cc_manager->next_unsent_access() != nullptr) {
        rc = process_last_access();
        if (rc != RCOK)
            break;
    }
    return rc;
}

RC interactive_t::process_last_access() {
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
    send_begin();
    send_dispatch(node_id, base_ptr, base_size, data_size);
    send_end(1);
    recv_buffer = transport->get_recv_buffer(node_id);
    recv_msg = reinterpret_cast<message_t*>(recv_buffer);
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
// Like the one-sided stored procedure: lock and fetch every access registered since the
// last call (a step may register several, e.g. NewOrder's ITEM/STOCK/ORDERLINE loops).
// get_last_access() is not usable here: its index is only maintained by the two-sided path.
RC interactive_t::process_request() {
    uint32_t num_accesses = _cc_manager->get_row_cnt();
    for (uint32_t i=_curr_offset; i<num_accesses; i++) {
        RC rc = _cc_manager->get_row(i);
        if (rc == ABORT) { // lock conflict
            process_abort(i);
            return rc;
        }
        _cc_manager->get_resp_data(i);
    }
    _curr_offset = num_accesses;
    return RCOK;
}
#endif