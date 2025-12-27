#include "client/transport.h"
#include "transport/message.h"
#include "system/manager.h"
#include "system/cache_allocator.h"
#include "system/memory_region.h"
#include "utils/helper.h"
#include "batch/table.h"

client_transport_t::client_transport_t()
    : transport_t() {
    if(!init()) exit(0);
	if(!connect()) exit(0);

    // sleep(1);
	// all the setup is complete, prepost RECVs for all server nodes
	for(uint32_t i=0; i<g_num_server_nodes; i++) {
		for (uint32_t j=0; j<g_num_client_threads; j++) {
			auto recv_ptr = get_recv_buffer(i, j);
			post_recv(recv_ptr, i, j, MAX_MESSAGE_SIZE);
		}
        for (uint32_t j=0; j<g_num_batch_threads; j++) {
            auto recv_ptr = get_recv_batch_buffer(i, j);
            post_recv_batch(recv_ptr, i, j, MAX_MESSAGE_SIZE * batch_table_t::MAX_GROUP_SIZE);
        }
	}

    // init memory allocator for one-sided RDMA access
#if TRANSPORT == ONE_SIDED
    g_cache_allocators = new cache_allocator_t*[g_num_server_nodes];
    for (uint32_t i=0; i<g_num_server_nodes; i++)
        g_cache_allocators[i] = new cache_allocator_t();
#endif
}

client_transport_t::~client_transport_t() {
    cleanup();
}

bool client_transport_t::init(){
    assert(!g_is_server);
    if(!transport_t::init())
        return cleanup();

    if(!init_rdma()) 
        return cleanup();

    // create CQs for each client thread
    _send_cqs = new ibv_cq*[g_num_client_threads];
    _recv_cqs = new ibv_cq*[g_num_client_threads];
    for(uint32_t i=0; i<g_num_client_threads; i++) {
        _send_cqs[i] = create_cq(_context.ctx);
        _recv_cqs[i] = create_cq(_context.ctx);
        if (!_send_cqs[i] || !_recv_cqs[i])
            return cleanup();
    }
    
    // create QPs
    _qps = new ibv_qp**[g_num_server_nodes];
    for(uint32_t i=0; i<g_num_server_nodes; i++) {
        _qps[i] = new ibv_qp*[g_num_client_threads];
        for(uint32_t j=0; j<g_num_client_threads; j++) {
            _qps[i][j] = create_qp(_context.pd, _send_cqs[j], _recv_cqs[j]);
            if (!_qps[i][j]) return cleanup();
            if (!modify_qp_state_to_init(_qps[i][j])) return cleanup();
        }
    }

    // create CQs for batch threads
    _batch_send_cqs = new ibv_cq*[g_num_batch_threads];
    _batch_recv_cqs = new ibv_cq*[g_num_batch_threads];
    for(uint32_t i=0; i<g_num_batch_threads; i++) {
        _batch_send_cqs[i] = create_cq(_context.ctx);
        _batch_recv_cqs[i] = create_cq(_context.ctx);
        if (!_batch_send_cqs[i] || !_batch_recv_cqs[i])
            return cleanup();
    }

    // create QPs for batch threads
    _batch_qps = new ibv_qp**[g_num_server_nodes];
    for(uint32_t i=0; i<g_num_server_nodes; i++) {
        _batch_qps[i] = new ibv_qp*[g_num_batch_threads];
        for(uint32_t j=0; j<g_num_batch_threads; j++) {
            _batch_qps[i][j] = create_qp(_context.pd, _batch_send_cqs[j], _batch_recv_cqs[j]);
            if (!_batch_qps[i][j]) return cleanup();
            if (!modify_qp_state_to_init(_batch_qps[i][j])) return cleanup();
        }
    }

    // create MR
    _mr = register_mr(_context.pd, _memory_region->ptr(), _memory_region->size());
    if (!_mr) return cleanup();

    return true;
}

bool client_transport_t::cleanup(){
    debug::notify_error("Error occurred during initialization --- cleaning up ...");
    if (_mr) ibv_dereg_mr(_mr);

    if (_batch_qps) {
        for(int i=0; i<g_num_server_nodes; i++) {
            if(_batch_qps[i]) {
                for(int j=0; j<g_num_batch_threads; j++) 
                    if(_batch_qps[i][j]) ibv_destroy_qp(_batch_qps[i][j]);
                delete[] _batch_qps[i];
            }
        }
        delete[] _batch_qps;
    }

    if (_batch_send_cqs) {
        for (int i=0; i<g_num_batch_threads; i++)
            if (_batch_send_cqs[i])
                ibv_destroy_cq(_batch_send_cqs[i]);
        delete[] _batch_send_cqs;
    }

    if (_batch_recv_cqs) {
        for (int i=0; i<g_num_batch_threads; i++)
            if (_batch_recv_cqs[i])
                ibv_destroy_cq(_batch_recv_cqs[i]);
        delete[] _batch_recv_cqs;
    }

	if (_qps) {
		for(int i=0; i<g_num_server_nodes; i++) {
			if(_qps[i]) {
				for(int j=0; j<g_num_client_threads; j++) 
					if(_qps[i][j]) ibv_destroy_qp(_qps[i][j]);
				delete[] _qps[i];
			}
		}
		delete[] _qps;
	}

	if (_send_cqs) {
		for (int i=0; i<g_num_client_threads; i++)
			if (_send_cqs[i])
				ibv_destroy_cq(_send_cqs[i]);
		delete[] _send_cqs;
	}
    if (_recv_cqs) {
        for (int i=0; i<g_num_client_threads; i++)
            if (_recv_cqs[i])
                ibv_destroy_cq(_recv_cqs[i]);
        delete[] _recv_cqs;
    }
    return transport_t::cleanup();
}

bool client_transport_t::connect() {
    if (!transport_t::connect())
        return cleanup();
    struct rdma_meta local;
    uint32_t data_size = sizeof(struct rdma_meta);

    memset(&local, 0, data_size);
    local.gid_idx = _context.gid_idx;
    memcpy(&local.gid, &_context.gid, sizeof(union ibv_gid));
    local.lid = _context.port_attr.lid;

    _meta = new rdma_meta[g_num_server_nodes];
    memset(_meta, 0, data_size * g_num_server_nodes);
    char* data = new char[data_size];

    // exchange QP information with servers
    for (uint32_t i=0; i<g_num_server_nodes; i++) {
        message_t* recv_msg = nullptr;
        do {
            recv_msg = recvMsg();
            PAUSE
        } while (!recv_msg);
        assert(recv_msg->get_type() == message_t::type_t::QP_SETUP);
        assert(recv_msg->get_data_size() == data_size);

        uint32_t node_id = recv_msg->get_node_id();
        char* qp_info = recv_msg->get_data();
        memcpy(&_meta[node_id], qp_info, data_size);
        // modify QP states
        for (uint32_t j=0; j<g_num_client_threads; j++) {
            if(!modify_qp_state_to_rtr(_qps[node_id][j], _meta[node_id].gid, _meta[node_id].gid_idx, _meta[node_id].lid, _meta[node_id].qpn[j])){
                debug::notify_error("Failed to modify qp state to RTR");
                return cleanup();
            }
            if(!modify_qp_state_to_rts(_qps[node_id][j])){
                debug::notify_error("Failed to modify qp state to RTS");
                return cleanup();
            }
        }

        for (uint32_t j=0; j<g_num_batch_threads; j++) {
            if(!modify_qp_state_to_rtr(_batch_qps[node_id][j], _meta[node_id].gid, _meta[node_id].gid_idx, _meta[node_id].lid, _meta[node_id].batch_qpn[j])){
                debug::notify_error("Failed to modify batch qp state to RTR");
                return cleanup();
            }
            if(!modify_qp_state_to_rts(_batch_qps[node_id][j])){
                debug::notify_error("Failed to modify batch qp state to RTS");
                return cleanup();
            }
        }

        delete recv_msg;
        for (uint32_t j=0; j<g_num_client_threads; j++)
            local.qpn[j] = _qps[i][j]->qp_num;
        for (uint32_t j=0; j<g_num_batch_threads; j++)
            local.batch_qpn[j] = _batch_qps[i][j]->qp_num;
        char* data = new char[data_size];
        memcpy(data, &local, data_size);

        auto send_msg = new message_t(message_t::type_t::QP_SETUP, 0, data_size, data);
        sendMsg(send_msg, i);
        delete send_msg;
    }
    return true;
}

// prepost RDMA RECV for the given QP
void client_transport_t::post_recv(char* ptr, uint32_t node_id, uint32_t size){
    rdma_recv_prepost(_qps[node_id][GET_THD_ID], ptr, size, _mr->lkey, node_id);
}

void client_transport_t::post_recv(char* ptr, uint32_t node_id, uint32_t qp_id, uint32_t size){
    rdma_recv_prepost(_qps[node_id][qp_id], ptr, size, _mr->lkey, node_id);
}

int client_transport_t::poll(struct ibv_wc* wc, int num) {
    int ret = poll_cq(_recv_cqs[GET_THD_ID], num, wc);
    uint32_t recv_size = 0;
    for (int i=0; i<ret; i++)
        recv_size += wc[i].byte_len;
    INC_INT_STATS(bytes_received, recv_size);
    traffic_response += recv_size;
    return ret;
}

int client_transport_t::poll_once(struct ibv_wc* wc, int num) {
    int ret = poll_cq_once(_recv_cqs[GET_THD_ID], num, wc);
    uint32_t recv_size = 0;
    for (int i=0; i<ret; i++)
        recv_size += wc[i].byte_len;
    INC_INT_STATS(bytes_received, recv_size);
    traffic_response += recv_size;
    return ret;
}

void client_transport_t::recv(char* ptr, uint32_t node_id, uint32_t size){
    uint32_t qp_id = GET_THD_ID;
    rdma_recv(_qps[node_id][qp_id], _recv_cqs[qp_id], ptr, size, _mr->lkey);
}

void client_transport_t::send(char* ptr, uint32_t node_id, uint32_t size, bool signaled){
    uint32_t qp_id = GET_THD_ID;
    rdma_send(_qps[node_id][qp_id], _send_cqs[qp_id], ptr, size, _mr->lkey, signaled);
    INC_INT_STATS(bytes_sent, size);
}

void client_transport_t::post_recv_batch(char* ptr, uint32_t node_id, uint32_t size){
    rdma_recv_prepost(_batch_qps[node_id][GET_THD_ID], ptr, size, _mr->lkey, serialize_node_info(node_id, GET_THD_ID / batch_table_t::MAX_GROUP_SIZE));
}

void client_transport_t::post_recv_batch(char* ptr, uint32_t node_id, uint32_t qp_id, uint32_t size){
    rdma_recv_prepost(_batch_qps[node_id][qp_id], ptr, size, _mr->lkey, serialize_node_info(node_id, qp_id));
}

int client_transport_t::poll_batch(struct ibv_wc* wc, int num) {
    int ret = poll_cq(_batch_recv_cqs[GET_THD_ID / batch_table_t::MAX_GROUP_SIZE], num, wc);
    uint32_t recv_size = 0;
    for (int i=0; i<ret; i++)
        recv_size += wc[i].byte_len;
    INC_INT_STATS(bytes_received, recv_size);
    traffic_response += recv_size;
    return ret;
}

int client_transport_t::poll_once_batch(struct ibv_wc* wc, int num) {
    int ret = poll_cq_once(_batch_recv_cqs[GET_THD_ID / batch_table_t::MAX_GROUP_SIZE], num, wc);
    uint32_t recv_size = 0;
    for (int i=0; i<ret; i++)
        recv_size += wc[i].byte_len;
    INC_INT_STATS(bytes_received, recv_size);
    traffic_response += recv_size;
    return ret;
}

void client_transport_t::recv_batch(char* ptr, uint32_t node_id, uint32_t size){
    uint32_t qp_id = GET_THD_ID / batch_table_t::MAX_GROUP_SIZE;
    rdma_recv(_batch_qps[node_id][qp_id], _batch_recv_cqs[qp_id], ptr, size, _mr->lkey);
}

void client_transport_t::send_batch(char* ptr, uint32_t node_id, uint32_t size, bool signaled){
    uint32_t qp_id = GET_THD_ID / batch_table_t::MAX_GROUP_SIZE;
    rdma_send(_batch_qps[node_id][qp_id], _batch_send_cqs[qp_id], ptr, size, _mr->lkey, signaled);
    INC_INT_STATS(bytes_sent, size);
    traffic_request += size;
}

void client_transport_t::write(char* src, global_addr_t dest, uint32_t size, bool signaled){
    uint32_t qp_id = GET_THD_ID;
    rdma_write(_qps[dest.node_id][qp_id], _send_cqs[qp_id], src, dest.addr, size, _mr->lkey, _meta[dest.node_id].rkey, signaled);
    INC_INT_STATS(bytes_written, size);
    traffic_request += size;
}

void client_transport_t::read(char* src, global_addr_t dest, uint32_t size, bool signaled){ 
    uint32_t qp_id = GET_THD_ID;
    rdma_read(_qps[dest.node_id][qp_id], _send_cqs[qp_id], src, dest.addr, size, _mr->lkey, _meta[dest.node_id].rkey, signaled);
    INC_INT_STATS(bytes_read, size);
    traffic_request += size;
}

bool client_transport_t::cas(char* src, global_addr_t dest, uint64_t cmp, uint64_t swap, uint32_t size, bool signaled){
    uint32_t qp_id = GET_THD_ID;
    bool ret = rdma_cas(_qps[dest.node_id][qp_id], _send_cqs[qp_id], src, dest.addr, cmp, swap, size, _mr->lkey, _meta[dest.node_id].rkey, signaled);
    INC_INT_STATS(bytes_cas, size);
    traffic_request += size;
    return ret;
}

bool client_transport_t::cas_mask(char* src, global_addr_t dest, uint64_t cmp, uint64_t swap, uint64_t mask, uint32_t size, bool signaled){
    uint32_t qp_id = GET_THD_ID;
    bool ret = rdma_cas_mask(_qps[dest.node_id][qp_id], _send_cqs[qp_id], src, dest.addr, cmp, swap, mask, size, _mr->lkey, _meta[dest.node_id].rkey, signaled);
    INC_INT_STATS(bytes_cas, size);
    traffic_request += size;
    return ret;
}

void client_transport_t::faa(char* src, global_addr_t dest, uint64_t add, uint32_t size, bool signaled){
    uint32_t qp_id = GET_THD_ID;
    rdma_faa(_qps[dest.node_id][qp_id], _send_cqs[qp_id], src, dest.addr, add, size, _mr->lkey, _meta[dest.node_id].rkey, signaled);
    INC_INT_STATS(bytes_faa, size);
    traffic_request += size;
}

void client_transport_t::faa_bound(char* src, global_addr_t dest, uint64_t add, uint64_t boundary, uint32_t size, bool signaled){
    uint32_t qp_id = GET_THD_ID;
    rdma_faa_bound(_qps[dest.node_id][qp_id], _send_cqs[qp_id], src, dest.addr, add, boundary, size, _mr->lkey, _meta[dest.node_id].rkey, signaled);
    INC_INT_STATS(bytes_faa, size);
    traffic_request += size;
}

void client_transport_t::read_dm(char* src, global_addr_t dest, uint32_t size, bool signaled){
    assert(_meta[dest.node_id].dm_rkey != 0);
    uint32_t qp_id = GET_THD_ID;
    rdma_read(_qps[dest.node_id][qp_id], _send_cqs[qp_id], src, _meta[dest.node_id].dm_base + dest.addr, size, _mr->lkey, _meta[dest.node_id].dm_rkey, signaled);
    INC_INT_STATS(bytes_read, size);
    traffic_request += size;
}

void client_transport_t::write_dm(char* src, global_addr_t dest, uint32_t size, bool signaled){
    assert(_meta[dest.node_id].dm_rkey != 0);
    uint32_t qp_id = GET_THD_ID;
    rdma_write(_qps[dest.node_id][qp_id], _send_cqs[qp_id], src, _meta[dest.node_id].dm_base + dest.addr, size, _mr->lkey, _meta[dest.node_id].dm_rkey, signaled);
    INC_INT_STATS(bytes_written, size);
    traffic_request += size;
}

bool client_transport_t::cas_dm(char* src, global_addr_t dest, uint64_t cmp, uint64_t swap, uint32_t size, bool signaled){
    assert(_meta[dest.node_id].dm_rkey != 0);
    uint32_t qp_id = GET_THD_ID;
    bool ret = rdma_cas(_qps[dest.node_id][qp_id], _send_cqs[qp_id], src, _meta[dest.node_id].dm_base + dest.addr, cmp, swap, size, _mr->lkey, _meta[dest.node_id].dm_rkey, signaled);
    INC_INT_STATS(bytes_cas, size);
    traffic_request += size;
    return ret;
}

bool client_transport_t::cas_dm_mask(char* src, global_addr_t dest, uint64_t cmp, uint64_t swap, uint64_t mask, uint32_t size, bool signaled){
    assert(_meta[dest.node_id].dm_rkey != 0);
    uint32_t qp_id = GET_THD_ID;
    INC_INT_STATS(bytes_cas, size);
    traffic_request += size;
    return rdma_cas_mask(_qps[dest.node_id][qp_id], _send_cqs[qp_id], src, _meta[dest.node_id].dm_base + dest.addr, cmp, swap, mask, size, _mr->lkey, _meta[dest.node_id].dm_rkey, signaled);
}

void client_transport_t::faa_dm(char* src, global_addr_t dest, uint64_t add, uint32_t size, bool signaled){
    assert(_meta[dest.node_id].dm_rkey != 0);
    uint32_t qp_id = GET_THD_ID;
    rdma_faa(_qps[dest.node_id][qp_id], _send_cqs[qp_id], src, _meta[dest.node_id].dm_base + dest.addr, add, size, _mr->lkey, _meta[dest.node_id].dm_rkey, signaled);
    INC_INT_STATS(bytes_faa, size);
    traffic_request += size;
}

void client_transport_t::faa_dm_bound(char* src, global_addr_t dest, uint64_t add, uint64_t boundary, uint32_t size, bool signaled){
    assert(_meta[dest.node_id].dm_rkey != 0);
    uint32_t qp_id = GET_THD_ID;
    rdma_faa_bound(_qps[dest.node_id][qp_id], _send_cqs[qp_id], src, _meta[dest.node_id].dm_base + dest.addr, add, boundary, size, _mr->lkey, _meta[dest.node_id].dm_rkey, signaled);
    INC_INT_STATS(bytes_faa, size);
    traffic_request += size;
}