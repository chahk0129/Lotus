#include "txn/txn.h"
#include "system/query.h"
#include "system/manager.h"
#include "system/cc_manager.h"
#include "concurrency/lock_manager.h"
#include "transport/message.h"
#include "client/transport.h"
#include "server/transport.h"
#include "utils/helper.h"
#include "batch/combiner.h"
#include "utils/packetize.h"

// server constructor 
txn_t::txn_t(txn_id_t txn_id): _txn_id(txn_id), _query(nullptr),
                               _timestamp(0), _prev_offset(0), _curr_offset(0) {
    assert(g_is_server);
    _cc_manager = cc_manager_t::create(this);
}

// client constructor
txn_t::txn_t(base_query_t* query): _txn_id(txn_id_t(g_node_id, GET_THD_ID)),
                                   _query(query), _timestamp(get_priority(g_node_id, GET_THD_ID)),
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
        uint64_t evict_key = 0, evict_value = 0;
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
        if (rc == COMMIT)
            return rc;
        else if (rc == ABORT)
            return rc;
        // RCOK with single-node commit, continue to commit phase
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
    // COMMITTING means this node already voted YES in prepare and dirty-wrote its
    // writes there; the CC manager protects COMMITTING owners from being wounded,
    // so this state is only ever overwritten by our own set_state() call below.
    bool undo = (get_state() == state_t::COMMITTING);
    set_state(state_t::ABORTING);
    _cc_manager->cleanup(ABORT, undo);
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
    uint32_t qp_id = get_id().get_qp_id();
    uint32_t base_size = sizeof(message_t) - sizeof(char*);

    send_begin();
    for (uint32_t i=0; i<num_resp_expected; i++) {
        uint32_t node_id = all_nodes_involved[i];
        // std::cout << "  sending abort request to node " << node_id << " (ts " << _timestamp << ")" << std::endl;
        char* send_ptr = transport->get_buffer(node_id);
        new (send_ptr) message_t(message_t::type_t::REQUEST_ABORT, qp_id, 0);
        send_dispatch(node_id, send_ptr, base_size, 0);
    }
    send_end(num_resp_expected);

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
        if (rc == RCOK) {
            // some nodes self-committed (read-only) during prepare and were excluded;
            // fetch the refreshed list of nodes that still need the commit phase
            std::vector<uint32_t> commit_nodes;
            _cc_manager->get_commit_nodes(commit_nodes);
            process_2pc_commit(commit_nodes);
        }
    }
    _prev_offset = 0;
    _curr_offset = 0;
#endif
#endif // !PARTITIONED
    if (rc == ABORT)
        _cc_manager->cleanup(rc);
    else {
        assert(rc == RCOK || rc == COMMIT);
        _cc_manager->cleanup(COMMIT);
        // COMMIT here means every participant self-committed read-only during prepare;
        // callers only distinguish committed (RCOK) vs ABORT
        rc = RCOK;
    }
    return rc;
}

RC txn_t::process_2pc_prepare(std::vector<uint32_t>& nodes_involved) {
    uint32_t base_size = sizeof(message_t) - sizeof(char*);
    uint32_t qp_id = get_id().get_qp_id();

    uint32_t num_nodes = nodes_involved.size();
    assert(num_nodes > 1);
    std::vector<uint32_t> nodes_aborted;
    std::vector<uint32_t> nodes_readonly; // nodes that self-committed (read-only) during prepare
    bool aborting = false;

    send_begin();
    for (uint32_t i=0; i<num_nodes; i++) {
        uint32_t node_id = nodes_involved[i];
        char* data_ptr = transport->get_buffer(node_id);
        uint32_t data_size = 0;
        message_t* send_msg = new (data_ptr) message_t(message_t::type_t::REQUEST_PREPARE, qp_id, 0);
        char* send_data = send_msg->get_data_ptr();
        _cc_manager->serialize_commit(node_id, send_data, data_size);
        if (data_size != 0)
            send_msg->set_data_size(data_size);
        send_dispatch(node_id, data_ptr, base_size, data_size);
    }
    send_end(num_nodes);

    for (auto& node_id: nodes_involved) {
        char* recv_buffer = transport->get_recv_buffer(node_id);
        auto recv_msg = reinterpret_cast<message_t*>(recv_buffer);
        assert(recv_msg->get_data_size() == 0);
        assert(recv_msg->get_node_id() == node_id);
        auto msg_type = recv_msg->get_type();
        if (msg_type == message_t::type_t::RESPONSE_ABORT) { // this node voted ABORT
            aborting = true;
            nodes_aborted.push_back(node_id);
        }
        else {
            assert(msg_type == message_t::type_t::RESPONSE_PREPARE || msg_type == message_t::type_t::RESPONSE_COMMIT);
            if (msg_type == message_t::type_t::RESPONSE_COMMIT)
                nodes_readonly.push_back(node_id);
        }
    }

    // read-only nodes already fully committed and released everything during prepare;
    // exclude them from any further commit/abort messaging
    if (!nodes_readonly.empty())
        _cc_manager->clear_nodes_involved(nodes_readonly);

    if (aborting) { // some nodes voted ABORT
        _cc_manager->clear_nodes_involved(nodes_aborted);
        process_abort_nodes();
        return ABORT;
    }

    // all nodes voted YES
    if (nodes_readonly.size() == num_nodes) // every node was read-only, nothing left to commit
        return COMMIT;
    else
        return RCOK;
}

void txn_t::process_2pc_commit(std::vector<uint32_t>& nodes_involved) {
    uint32_t base_size = sizeof(message_t) - sizeof(char*);
    uint32_t qp_id = get_id().get_qp_id();

    uint32_t num_nodes = nodes_involved.size();
    // num_nodes can be as low as 1 here: the original txn spanned >1 nodes, but
    // some may have self-committed as read-only during prepare and been excluded.
    assert(num_nodes >= 1);

    send_begin();
    for (uint32_t i=0; i<num_nodes; i++) {
        uint32_t node_id = nodes_involved[i];
        char* send_ptr = transport->get_buffer(node_id);
        new (send_ptr) message_t(message_t::type_t::REQUEST_COMMIT, qp_id, 0);
        send_dispatch(node_id, send_ptr, base_size, 0);
    }
    send_end(num_nodes);

    for (auto& node_id: nodes_involved) {
        char* recv_buffer = transport->get_recv_buffer(node_id);
        auto recv_msg = reinterpret_cast<message_t*>(recv_buffer);
        assert(recv_msg->get_type() == message_t::type_t::RESPONSE_COMMIT); // 2PC commit phase should never fail unless a node failure
    }
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

    send_begin();
    send_dispatch(node_id, send_buffer, base_size, send_size);
    send_end(1);

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

void txn_t::send_begin() {
#if BATCH
    batch_combiner_t::get()->begin_round();
#endif
}

void txn_t::send_dispatch(uint32_t node_id, char* send_ptr, uint32_t base_size, uint32_t data_size) {
#if BATCH
    // request is already built in transport->get_buffer(node_id); the combiner sends it
    assert(send_ptr == transport->get_buffer(node_id));
    batch_combiner_t::get()->submit(node_id, base_size + data_size);
#else
    transport->send(send_ptr, node_id, base_size + data_size);
#endif
}

void txn_t::send_end(uint32_t num_sent) {
#if BATCH
    // the combiner tracks which nodes were submitted this round
    (void)num_sent;
    batch_combiner_t::get()->finish_round();
#else
    struct ibv_wc wc[num_sent];
    uint32_t num = 0;
    while (num < num_sent) {
        int polled = transport->poll_once(wc, num_sent - num);
        for (int i=0; i<polled; i++) {
            uint64_t node_id = wc[i].wr_id;
            char* recv_buffer = transport->get_recv_buffer(node_id);
            transport->post_recv(recv_buffer, node_id, MAX_MESSAGE_SIZE); // prepost receive
        }
        if (polled > 0)
            num += polled;
    }
#endif
}
