#include "txn/txn.h"
#include "system/query.h"
#include "system/manager.h"
#include "system/cc_manager.h"
#include "concurrency/lock_manager.h"
#include "transport/message.h"
#include "client/transport.h"
#include "server/transport.h"
#include "utils/helper.h"
#include "batch/table.h"
#include "batch/decision.h"
#include "batch/manager.h"
#include "utils/packetize.h"

// server constructor 
txn_t::txn_t(txn_id_t txn_id): _txn_id(txn_id), _query(nullptr),
                               _timestamp(0), _prev_offset(0), _curr_offset(0) {
    assert(g_is_server);
    _cc_manager = cc_manager_t::create(this);
}

// client constructor
txn_t::txn_t(base_query_t* query): _txn_id(txn_id_t(g_node_id, GET_THD_ID)), 
                                   _query(query), _timestamp(get_priority()),
                                   _prev_offset(0), _curr_offset(0) {
    assert(!g_is_server);
    _cc_manager = cc_manager_t::create(this);
}

txn_t::~txn_t() {
    if (_query) delete _query;
    if (_cc_manager) delete _cc_manager;
}

void txn_t::init() {
    _prev_offset = 0;
    _curr_offset = 0;
    _state.store(RUNNING);
}

void txn_t::set_query(base_query_t* query) {
    if(_query) delete _query;
    _query = query;
}

//////////////////////////////////////////////////
////////////// server operations /////////////////
//////////////////////////////////////////////////

void txn_t::parse_request(message_t* msg) {
    uint32_t size = msg->get_data_size();
    UnstructuredBuffer buffer(msg->get_data_ptr());
#if (CC_ALG == WAIT_DIE || CC_ALG == WOUND_WAIT) && !PARTITIONED
    uint64_t timestamp = 0;
    buffer.get(&timestamp);
    assert(timestamp != 0);
    if (_timestamp == 0) 
        _timestamp = timestamp;
#endif
    uint32_t num = 0;
    buffer.get(&num);

    for (uint32_t i=0; i<num; i++) {
        uint64_t key = -1;
        uint32_t index_id = -1, table_id = -1;
        access_t type = TYPE_NONE;
        uint64_t cache_hint = 0;
#if PARTITIONED // index manager
        uint64_t evict_key, evict_value;
        uint64_t value;
        buffer.get(&cache_hint);
        buffer.get(&type);
        buffer.get(&key);
        buffer.get(&index_id);
        buffer.get(&table_id);
        buffer.get(&value);
        if (cache_hint == 2) { // cache admission + eviction
            buffer.get(&evict_key);
            buffer.get(&evict_value);
        }
        _cc_manager->register_access(type, key, table_id, index_id, cache_hint, value, evict_key, evict_value);
#else // CC manager
        buffer.get(&type);
        buffer.get(&key);
        buffer.get(&index_id);
        buffer.get(&table_id);
        buffer.get(&cache_hint);
        _cc_manager->register_access(type, key, table_id, index_id, cache_hint);
#endif
    }
    assert(buffer.size() == size);
}

RC txn_t::process_prepare(char* data, uint32_t size) {
    return _cc_manager->process_prepare(data, size);
}

RC txn_t::process_commit(char* data, uint32_t size) {
    RC rc = RCOK;
    if (size > 0) { // this is not 2PC, single-node commit
        rc = _cc_manager->process_prepare(data, size);
        if (rc != RCOK) {
            assert(rc == ABORT);
            return rc;
        }
    }
    rc = _cc_manager->process_commit();
    return rc;
}

RC txn_t::process_request(char*& data, uint32_t& size) {
    RC rc = RCOK;
    uint32_t num_accesses = _cc_manager->get_row_cnt();
    while (_curr_offset < num_accesses) {
        rc = _cc_manager->get_row(_curr_offset);
        if (rc != RCOK) { // lock conflict
            if (rc == ABORT)
                return rc;
            assert(rc == WAIT);
            _curr_offset++;
            return rc;
        }
        _curr_offset++;
    }

    _cc_manager->get_resp_data(_prev_offset, _curr_offset, data, size);
    _prev_offset = _curr_offset;
    return rc;
}

void txn_t::process_abort() {
    set_state(state_t::ABORTING);
    _cc_manager->cleanup(ABORT);
    // auto last_access = _cc_manager->get_last_access();
    // _cc_manager->abort(last_access);
    _prev_offset = 0;
    _curr_offset = 0;
}

//////////////////////////////////////////////////
////////////// client operations /////////////////
//////////////////////////////////////////////////

void txn_t::force_abort() {
    process_abort();
}

// abort for one-sided RDMA
void txn_t::process_abort(uint32_t abort_access_idx) {
    _cc_manager->abort(abort_access_idx);
    // _cc_manager->cleanup(ABORT);
    _prev_offset = 0;
    _curr_offset = 0;
}

///////////////////////////////
// two-sided RDMA operations //
///////////////////////////////
void txn_t::process_abort_nodes() {
    set_state(state_t::ABORTING);
    std::vector<uint32_t> all_nodes_involved;
    uint32_t num_resp_expected = _cc_manager->get_commit_nodes(all_nodes_involved);
#if BATCH
    auto batch_man = global_manager->get_batch_manager();
    auto decision = batch_man->get_decision();
    uint32_t wait_time[num_resp_expected];
#endif

    uint32_t qp_id = get_id().get_qp_id();
    uint32_t base_size = sizeof(message_t) - sizeof(char*);
    for (uint32_t i=0; i<num_resp_expected; i++) {
        uint32_t node_id = all_nodes_involved[i];
        // std::cout << "  sending abort request to node " << node_id << " (ts " << _timestamp << ")" << std::endl;
        char* send_ptr = transport->get_buffer(node_id);
        auto send_msg = new (send_ptr) message_t(message_t::type_t::REQUEST_ABORT, qp_id, 0);
#if BATCH
        if (decision == batch_decision_t::WAIT_FOR_BATCH)
            wait_time[i] = batch_man->submit_request(node_id, base_size);
        else {
#endif
        transport->send(send_ptr, node_id, base_size);
#if BATCH
        }
#endif
    }

#if BATCH
    if (decision == batch_decision_t::WAIT_FOR_BATCH) {
        if (batch_man->is_leader()) {
            handle_batching_leader(all_nodes_involved, wait_time);
            batch_man->check_leader_expiration_status();
        }
        else {
            handle_batching_member(all_nodes_involved, wait_time);
        }
    }
    else { // SEND_IMMEDIATELY
#endif
    struct ibv_wc wc[num_resp_expected];
    uint32_t num = 0;
    do {
        int polled = transport->poll_once(wc, num_resp_expected - num);
        if (polled > 0) {
            num += polled;
            for (uint32_t i=0; i<polled; i++) {
                uint64_t node_id = wc[i].wr_id;
                char* recv_buffer = transport->get_recv_buffer(node_id);
                transport->post_recv(recv_buffer, node_id, MAX_MESSAGE_SIZE); // prepost receive
            }
        }
    } while (num < num_resp_expected);
#if BATCH
    }    
#endif
    _cc_manager->abort();
    _prev_offset = 0;
    _curr_offset = 0;
}

RC txn_t::process_commit() {
    RC rc = RCOK;
    set_state(state_t::COMMITTING);
#if !PARTITIONED
#if TRANSPORT == TWO_SIDED
    std::vector<uint32_t> all_nodes_involved;
    uint32_t size = _cc_manager->get_commit_nodes(all_nodes_involved);
    if (size == 1) // single node commit
        rc = process_single_commit(all_nodes_involved);
    else { // 2PC commit
        rc = process_2pc_prepare(all_nodes_involved);
        if (rc == RCOK)
            process_2pc_commit(all_nodes_involved);
    }
    _prev_offset = 0;
    _curr_offset = 0;
#endif
#endif // !PARTITIONED
    if (rc == RCOK) 
        _cc_manager->cleanup(COMMIT);
    else {
        assert(rc == ABORT);
        _cc_manager->cleanup(rc);
    }
// #if !PARTITIONED
// #if TRANSPORT == ONE_SIDED
//     _cc_manager->cleanup(COMMIT);
// #else // TRANSPORT == TWO_SIDED
//     std::vector<uint32_t> all_nodes_involved;
//     uint32_t size = _cc_manager->get_commit_nodes(all_nodes_involved);
//     if (size == 1) // single node commit
//         rc = process_single_commit(all_nodes_involved);
//     else { // 2PC commit
//         rc = process_2pc_prepare(all_nodes_involved);
//         if (rc == RCOK)
//             process_2pc_commit(all_nodes_involved);
//     }

//     _prev_offset = 0;
//     _curr_offset = 0;
// #endif // TRANSPORT
// #else // PARTITIONED
//     _cc_manager->cleanup(COMMIT);
// #endif // !PARTITIONED
    return rc;
}

RC txn_t::process_2pc_prepare(std::vector<uint32_t>& nodes_involved) {
    uint32_t total_size = 0;
    uint32_t base_size = sizeof(message_t) - sizeof(char*);
    uint32_t qp_id = get_id().get_qp_id();

    uint32_t num_nodes = nodes_involved.size();
    assert(num_nodes > 1);
    std::vector<uint32_t> nodes_aborted;
    bool aborting = false;
#if BATCH
    auto batch_man = global_manager->get_batch_manager();
    auto decision = batch_man->get_decision();
    uint32_t wait_time[num_nodes];
    uint64_t latency = get_sys_clock();
    bool batch_status = false;
#endif
    for (uint32_t i=0; i<num_nodes; i++) {
        uint32_t node_id = nodes_involved[i];
        char* data_ptr = transport->get_buffer(node_id);
        uint32_t data_size = 0;
        message_t* send_msg = new (data_ptr) message_t(message_t::type_t::REQUEST_PREPARE, qp_id, 0);
        char* send_data = send_msg->get_data_ptr();
        _cc_manager->serialize_commit(node_id, send_data, data_size);
        if (data_size != 0)
            send_msg->set_data_size(data_size);

#if BATCH
        if (decision == batch_decision_t::WAIT_FOR_BATCH)
            wait_time[i] = batch_man->submit_request(node_id, data_size + base_size);
        else {
#endif
        transport->send(data_ptr, node_id, base_size + data_size);
#if BATCH
        }
#endif
    }

#if BATCH
    if (decision == batch_decision_t::WAIT_FOR_BATCH) {
        bool is_leader = batch_man->is_leader();
        if (is_leader) {
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
            assert(recv_msg->get_data_size() == 0);
            assert(recv_msg->get_node_id() == node_id);
            if (recv_msg->get_type() != message_t::type_t::RESPONSE_PREPARE) { // this node voted ABORT
                assert(recv_msg->get_type() == message_t::type_t::RESPONSE_ABORT);
                aborting = true;
                nodes_aborted.push_back(node_id);
            }
        }
    }
    else { // SEND_IMMEDIATELY
#endif 
    struct ibv_wc wc[num_nodes];
    uint32_t num = 0;
    do { 
        int polled = transport->poll_once(wc, num_nodes - num);
        if (polled > 0) {
            num += polled;
            for (uint32_t i=0; i<polled; i++) {
                uint64_t node_id = wc[i].wr_id;
                char* recv_buffer = transport->get_recv_buffer(node_id);
                auto recv_msg = reinterpret_cast<message_t*>(recv_buffer);
                assert(recv_msg->get_data_size() == 0);
                assert(recv_msg->get_node_id() == node_id);
                if (recv_msg->get_type() != message_t::type_t::RESPONSE_PREPARE) { // this node voted ABORT
                    assert(recv_msg->get_type() == message_t::type_t::RESPONSE_ABORT);
                    aborting = true;
                    nodes_aborted.push_back(node_id);
                }
                transport->post_recv(recv_buffer, node_id, MAX_MESSAGE_SIZE); // prepost receive
            }
        }
    } while (num < num_nodes);
#if BATCH
    batch_man->report_member(get_sys_clock() - latency);
    }
#endif

    if (aborting) { // some nodes voted ABORT
        _cc_manager->clear_nodes_involved(nodes_aborted);
        process_abort_nodes();
        return ABORT;
    }
    else // all nodes voted COMMIT
        return RCOK;
}

void txn_t::process_2pc_commit(std::vector<uint32_t>& nodes_involved) {
    uint32_t total_size = 0;
    uint32_t base_size = sizeof(message_t) - sizeof(char*);
    uint32_t qp_id = get_id().get_qp_id();

    uint32_t num_nodes = nodes_involved.size();
    assert(num_nodes > 1);
#if BATCH
    auto batch_man = global_manager->get_batch_manager();
    auto decision = batch_man->get_decision();
    uint32_t wait_time[num_nodes];
    bool batch_status = false;
    uint64_t latency = get_sys_clock();
#endif
    for (uint32_t i=0; i<num_nodes; i++) {
        uint32_t node_id = nodes_involved[i];
        char* send_ptr = transport->get_buffer(node_id);
        message_t* send_msg = new (send_ptr) message_t(message_t::type_t::REQUEST_COMMIT, qp_id, 0);
#if BATCH
        if (decision == batch_decision_t::WAIT_FOR_BATCH)
            wait_time[i] = batch_man->submit_request(node_id, base_size);
        else {
#endif
        transport->send(send_ptr, node_id, base_size);
#if BATCH
        }
#endif
    }

#if BATCH
    if (decision == batch_decision_t::WAIT_FOR_BATCH) {
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
            assert(recv_msg->get_type() == message_t::type_t::RESPONSE_COMMIT); // 2PC commit phase should never fail unless a node failure
        }
    }
    else { // no batching
#endif
    struct ibv_wc wc[num_nodes];
    uint32_t num = 0;
    do {
        int polled = transport->poll_once(wc, num_nodes - num);
        if (polled > 0) {
            num += polled;
            for (uint32_t i=0; i<polled; i++) {
                uint64_t node_id = wc[i].wr_id;
                char* recv_buffer = transport->get_recv_buffer(node_id);
                auto recv_msg = reinterpret_cast<message_t*>(recv_buffer);
                assert(recv_msg->get_type() == message_t::type_t::RESPONSE_COMMIT); // 2PC commit phase should never fail unless a node failure
                transport->post_recv(recv_buffer, node_id, MAX_MESSAGE_SIZE); // prepost receive
            }
        }
    } while(num < num_nodes);
#if BATCH
    batch_man->report_member(get_sys_clock() - latency);
    }
#endif
}

RC txn_t::process_single_commit(std::vector<uint32_t>& nodes_involved) {
    assert(nodes_involved.size() == 1);
    uint32_t node_id = nodes_involved[0];
    uint32_t qp_id = get_id().get_qp_id();
    char* send_buffer = transport->get_buffer(node_id);
    uint32_t base_size = sizeof(message_t) - sizeof(char*);
    uint32_t send_size = 0;

    message_t* send_msg = new (send_buffer) message_t(message_t::type_t::REQUEST_COMMIT, qp_id, 0);
    char* send_data = send_msg->get_data_ptr();
    _cc_manager->serialize_commit(node_id, send_data, send_size);
    if (send_size != 0)
        send_msg->set_data_size(send_size);
#if BATCH
    auto batch_man = global_manager->get_batch_manager();
    auto decision = batch_man->get_decision();
    uint32_t wait_time = 0;
    bool batch_status = false;
    uint64_t latency = get_sys_clock();
    if (decision == batch_decision_t::WAIT_FOR_BATCH) {
        wait_time = batch_man->submit_request(node_id, send_size + base_size);
        if (batch_man->is_leader()) {
            batch_status = handle_batching_leader(nodes_involved, &wait_time);
            batch_man->check_leader_expiration_status();
            batch_man->report_leader(get_sys_clock() - latency, batch_status);
        }
        else {
            batch_status = handle_batching_member(nodes_involved, &wait_time);
            batch_man->report_member(get_sys_clock() - latency, true, batch_status);
        }
        // handle_batching(nodes_involved, &wait_time);
    }
    else { // SEND_IMMEDIATELY
            assert(decision == batch_decision_t::SEND_IMMEDIATELY);
#endif // no BATCH
    transport->send(send_buffer, node_id, base_size + send_size);
    struct ibv_wc wc;
    uint32_t num = transport->poll(&wc, 1);
    assert(num == 1);
    assert(wc.wr_id == node_id);
    transport->post_recv(transport->get_recv_buffer(node_id), node_id, MAX_MESSAGE_SIZE); // prepost receive
#if BATCH
    batch_man->report_member(get_sys_clock() - latency);
    }
#endif

    char* recv_buffer = transport->get_recv_buffer(node_id);
    auto recv_msg = reinterpret_cast<message_t*>(recv_buffer);
    auto type = recv_msg->get_type();
    assert(recv_msg->get_node_id() == node_id);
    if (type != message_t::type_t::RESPONSE_COMMIT) { // ABORT
        assert(type == message_t::type_t::RESPONSE_ABORT);
        // the involved single node has already aborted
        // _cc_manager->abort();
        _cc_manager->clear_nodes_involved(nodes_involved);
        process_abort_nodes();
        return ABORT;
    }
    return RCOK;
}

bool txn_t::handle_batching_leader(std::vector<uint32_t>& nodes_involved, uint32_t* wait_time) {
    assert(BATCH == 1);
    auto batch_man = global_manager->get_batch_manager();
    uint32_t base_size = sizeof(message_t) - sizeof(char*);
    uint32_t group_id = GET_THD_ID / batch_table_t::MAX_GROUP_SIZE;

    bool batch_result = false;
    uint32_t nodes_sent_count = 0;
    for (uint32_t i=0; i<g_num_server_nodes; i++) {
        std::vector<std::pair<uint32_t, uint32_t>> requests;
        batch_man->collect_batch_requests(i, requests);
        if (requests.size() == 0)
            continue;
        nodes_sent_count++;
        char* send_buffer = transport->get_batch_buffer(i, group_id);
        auto batch_msg = new (send_buffer) message_t(message_t::type_t::REQUEST_BATCHED_STORED_PROCEDURE, group_id, requests.size());
        assert(batch_msg->get_data_size() == requests.size());
        assert(batch_msg->get_qp_id() == group_id);
        char* batch_data_ptr = batch_msg->get_data_ptr();
        uint32_t total_size = 0;
        for (auto& entry: requests) {
            char* member_buffer = transport->get_buffer(i, entry.first);
            memcpy(batch_data_ptr + total_size, member_buffer, entry.second);
            total_size += entry.second;
        }
        assert(batch_msg->get_data_size() == requests.size());
        transport->send_batch(send_buffer, i, base_size + total_size);

        uint32_t batch_size = requests.size();
        if (batch_size > batch_table_t::MAX_GROUP_SIZE / 2)
            batch_result = true; 
        INC_BATCH_STATS(batch_size, 1);
    }
    // for (uint32_t i=0; i<nodes_involved.size(); i++) {
    //     uint32_t node_id = nodes_involved[i];
    //     std::vector<std::pair<uint32_t, uint32_t>> requests;
    //     batch_man->collect_batch_requests(node_id, requests);
    //     char* send_buffer = transport->get_batch_buffer(node_id, group_id);
    //     auto batch_msg = new (send_buffer) message_t(message_t::type_t::REQUEST_BATCHED_STORED_PROCEDURE, group_id, requests.size());
    //     assert(batch_msg->get_data_size() == requests.size());
    //     assert(batch_msg->get_qp_id() == group_id);
    //     char* batch_data_ptr = batch_msg->get_data_ptr();
    //     uint32_t total_size = 0;
    //     for (auto& entry: requests) {
    //         char* member_buffer = transport->get_buffer(node_id, entry.first);
    //         memcpy(batch_data_ptr + total_size, member_buffer, entry.second);
    //         total_size += entry.second;
    //     }
    //     assert(batch_msg->get_data_size() == requests.size());
    //     transport->send_batch(send_buffer, node_id, base_size + total_size);
    //     INC_BATCH_STATS(requests.size(), 1);
    //     batch_data.push_back(requests);
    // }

    // uint32_t sent_num = nodes_involved.size();
    uint32_t sent_num = nodes_sent_count;
    struct ibv_wc wc[sent_num];
    uint32_t recv_node_id, recv_qp_id;
    uint32_t recv_num = 0;
    do {
        int polled = transport->poll_once_batch(wc, sent_num - recv_num);
        if (polled > 0) {
            recv_num += polled;
            for (uint32_t i=0; i<polled; i++) {
                transport->deserialize_node_info(wc[i].wr_id, recv_node_id, recv_qp_id);
                
                char* recv_buffer = transport->get_recv_batch_buffer(recv_node_id, recv_qp_id);
                assert(recv_qp_id == group_id);
                auto recv_msg = reinterpret_cast<message_t*>(recv_buffer);
                assert(recv_msg->get_type() == message_t::type_t::ACK); // batch ACK

                uint32_t num_responses = recv_msg->get_data_size();
                char* base_data = recv_msg->get_data_ptr();
                uint32_t offset = 0;
                // dispatch responses to batch members
                for (uint32_t j=0; j<num_responses; j++) {
                    char* member_ptr = base_data + offset;
                    auto member_msg = reinterpret_cast<message_t*>(member_ptr);
                    uint32_t member_data_size = member_msg->get_data_size();
                    uint32_t member_id = member_msg->get_qp_id();
                    if (member_msg->get_type() == message_t::type_t::RESPONSE_WAIT) {
                        // need to notify member for WAIT response
                        assert(member_data_size == 0);
                        batch_man->update_status(recv_node_id, member_id, true);
                    }
                    else {
                        char* member_buffer = transport->get_recv_buffer(recv_node_id, member_id);
                        memcpy(member_buffer, member_ptr, base_size + member_data_size);
                        batch_man->update_status(recv_node_id, member_id, false);
                    }
                    offset += base_size + member_data_size;
                }
                transport->post_recv_batch(recv_buffer, recv_node_id, recv_qp_id, MAX_MESSAGE_SIZE * batch_table_t::MAX_GROUP_SIZE); // prepost receive
            }
        }
    } while (recv_num < sent_num);

    // leader also need to reset responses for itself
    uint32_t need_recv_count = 0;
    for (auto& node_id : nodes_involved) {
        if (batch_man->wait_for_completion(node_id))
            need_recv_count++;
    }

    // wait for WAIT responses
    if (need_recv_count > 0) {
        wait_for_responses(need_recv_count);
    }
    return batch_result;
}

bool txn_t::handle_batching_member(std::vector<uint32_t>& nodes_involved, uint32_t* wait_time) {
    assert(BATCH == 1);
    auto batch_man = global_manager->get_batch_manager();
    uint32_t base_size = sizeof(message_t) - sizeof(char*);
    uint32_t need_recv_count = 0;
    bool timeout = false;
    for (uint32_t i=0; i<nodes_involved.size(); i++) {
        uint32_t node_id = nodes_involved[i];
        bool need_individual_send = batch_man->examine_status(node_id, wait_time[i]);
        if (need_individual_send) { // timeout, send individual request
            char* send_buffer = transport->get_buffer(node_id);
            auto send_msg = reinterpret_cast<message_t*>(send_buffer);
            transport->send(send_buffer, node_id, base_size + send_msg->get_data_size());
            need_recv_count++;
            timeout = true;
        }
    }

    // wait for batch completion
    for (auto& node_id : nodes_involved) {
        if (batch_man->wait_for_completion(node_id))
            need_recv_count++;
    }

    if (need_recv_count > 0)
        wait_for_responses(need_recv_count);
    return timeout;
}

void txn_t::wait_for_responses(uint32_t num_responses) {
    uint32_t tid = GET_THD_ID;
    struct ibv_wc wc[num_responses];
    uint32_t num = 0;
    do {
        int polled = transport->poll_once(wc, num_responses - num);
        if (polled > 0) {
            num += polled;
            for (uint32_t i=0; i<polled; i++) {
                uint64_t node_id = wc[i].wr_id;
                char* recv_buffer = transport->get_recv_buffer(node_id);
                auto recv_msg = reinterpret_cast<message_t*>(recv_buffer);
                assert(recv_msg->get_type() != message_t::type_t::RESPONSE_WAIT); // should not receive WAIT response here
                transport->post_recv(recv_buffer, node_id, MAX_MESSAGE_SIZE); // prepost receive
            }
        }
    } while (num < num_responses);
}
/*
bool txn_t::twotree_rpc_insert(INDEX* index, char* addr, uint64_t version, uint64_t key, uint64_t value, char* data) {
    // we currently do not consider RPC pushdown for insert
    index->insert(key, value, data);
    return true;
}

bool txn_t::twotree_rpc_update(INDEX* index, char* addr, uint64_t key, uint64_t value, char* data) {
    if (!addr) { // no cache, directly index operation
        return index->update(key, value, data);
    }

restart:
    bool needRestart = false;
    auto leaf = reinterpret_cast<twosided::BTreeLeaf<uint64_t, uint64_t>*>(addr);
    leaf->writeLockOrRestart(needRestart);
    if (needRestart) goto restart;

    if (!leaf->rangeValid(key)) { // cache is stale
        leaf->writeUnlock();
        return index->update(key, value, data);
    }
    // else cache is up-to-date
    bool ret = leaf->update(key, value);
    leaf->writeUnlock();

    return ret;
}

bool txn_t::twotree_rpc_remove(INDEX* index, char* addr, uint64_t key, char* data) {
    if (!addr) { // no cache, directly index operation
        return index->remove(key, data);
    }

restart:
    bool needRestart = false;
    auto leaf = reinterpret_cast<twosided::BTreeLeaf<uint64_t, uint64_t>*>(addr);
    leaf->writeLockOrRestart(needRestart);
    if (needRestart) goto restart;

    if (!leaf->rangeValid(key)) { // cache is stale
        leaf->writeUnlock();
        return index->remove(key, data);
    }
    // else cache is up-to-date
    bool ret = leaf->remove(key);
    leaf->writeUnlock();

    return ret;
}

bool txn_t::twotree_rpc_update(INDEX* index, char* addr, uint64_t key, uint64_t value, char* data) {
    if (!addr) { // no cache, directly index operation
        return index->update(key, value, data);
    }

restart:
    bool needRestart = false;
    auto leaf = reinterpret_cast<twosided::BTreeLeaf<uint64_t, uint64_t>*>(addr);
    leaf->writeLockOrRestart(needRestart);
    if (needRestart) goto restart;

    if (!leaf->rangeValid(key)) { // cache is stale
        leaf->writeUnlock();
        return index->update(key, value, data);
    }
    // else cache is up-to-date
    bool ret = leaf->update(key, value);
    leaf->writeUnlock();

    return ret;
}

bool txn_t::twotree_rpc_search(INDEX* index, char* addr, uint64_t key, uint64_t& value, char* data) {
    if (!addr) { // no cache, directly index operation
        return index->lookup(key, value, data);
    }

restart:
    bool needRestart = false;
    auto leaf = reinterpret_cast<twosided::BTreeLeaf<uint64_t, uint64_t>*>(addr);
    leaf->readLockOrRestart(needRestart);
    if (needRestart) goto restart;

    if (!leaf->rangeValid(key)) { // cache is stale
        leaf->readUnlock();
        return index->lookup(key, value, data);
    }
    // else cache is up-to-date
    bool ret = leaf->lookup(key, value);
    leaf->readUnlock();

    return ret;
}
    */
