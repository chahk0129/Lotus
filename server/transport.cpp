#include "server/transport.h"
#include "transport/message.h"
#include "system/memory_allocator.h"
#include "system/memory_region.h"
#include "system/workload.h"
#include "utils/helper.h"
#include "batch/table.h"

server_transport_t::server_transport_t()
    : transport_t() {
    if(!init()) exit(0);
	if(!connect()) exit(0);

	// all the setup is complete, prepost RECVs for all client nodes
	for(uint32_t i=0; i<g_num_client_nodes; i++) {
		for (uint32_t j=0; j<g_num_client_threads; j++) {
			auto recv_ptr = get_recv_buffer(i, j);
			post_recv(recv_ptr, i, j, MAX_MESSAGE_SIZE);
		}
		for (uint32_t j=0; j<g_num_batch_threads; j++) {
			auto recv_ptr = get_recv_batch_buffer(i, j);
			post_recv_batch(recv_ptr, i, j, MAX_MESSAGE_SIZE * batch_table_t::MAX_GROUP_SIZE);
		}
	}
}

server_transport_t::~server_transport_t() {
    cleanup();
}

bool server_transport_t::init() {
	assert(g_is_server);
	if (!transport_t::init())
		return cleanup();

	if (!init_rdma()) 
		return cleanup();

    // create send CQs for client connection
	_send_cqs = new ibv_cq*[g_num_client_threads];
	for (uint32_t i=0; i<g_num_client_threads; i++) {
		_send_cqs[i] = create_cq(_context.ctx);
		if (!_send_cqs[i])
			return cleanup();
	}

	// create send CQs for batch connection
	_batch_send_cqs = new ibv_cq*[g_num_batch_threads];
	for (uint32_t i=0; i<g_num_batch_threads; i++) {
		_batch_send_cqs[i] = create_cq(_context.ctx);
		if (!_batch_send_cqs[i])
			return cleanup();
	}

	// we use a shared CQ for all connections
	_recv_cq = create_cq(_context.ctx);
	if (!_recv_cq) return cleanup();

    // create QPs for each client connection
	_qps = new ibv_qp**[g_num_client_nodes];
	for (uint32_t i=0; i<g_num_client_nodes; i++) {
		_qps[i] = new ibv_qp*[g_num_client_threads];
		for (uint32_t j=0; j<g_num_client_threads; j++) {
			_qps[i][j] = create_qp(_context.pd, _send_cqs[j], _recv_cq);
			if (!_qps[i][j]) return cleanup();
			if (!modify_qp_state_to_init(_qps[i][j])) return cleanup();
		}
	}

	// create QPs for batch connections
	_batch_qps = new ibv_qp**[g_num_client_nodes];
	for (uint32_t i=0; i<g_num_client_nodes; i++) {
		_batch_qps[i] = new ibv_qp*[g_num_batch_threads];
		for (uint32_t j=0; j<g_num_batch_threads; j++) {
			_batch_qps[i][j] = create_qp(_context.pd, _batch_send_cqs[j], _recv_cq);
			if (!_batch_qps[i][j]) return cleanup();
			if (!modify_qp_state_to_init(_batch_qps[i][j])) return cleanup();
		}
	}

    // create MR 
	_mr = nullptr;
	_mr = register_mr(_context.pd, _memory_region->ptr(), _memory_region->size());
	if (!_mr) return cleanup();

	_alloc_mr = nullptr;
#if TRANSPORT == ONE_SIDED
	// create alloc MR for one-sided RDMA
	uint32_t num_indexes = 10 ; // hardcoded, TODO: make it configurable
	// uint32_t num_indexes = g_node_id == 0 ? 10 : 0; // hardcoded, TODO: make it configurable
	_alloc_memory_region = new memory_region_t();
	size_t alloc_memory_size = _alloc_memory_region->size();
	char* alloc_base = reinterpret_cast<char*>(_alloc_memory_region->ptr());
	_alloc_mr = register_mr(_context.pd, alloc_base, alloc_memory_size);
	if (!_alloc_mr) return cleanup();

	for (uint32_t i=0; i<num_indexes; i++) {
		uint64_t* idx_root_ptr = reinterpret_cast<uint64_t*>(alloc_base + sizeof(global_addr_t) * i);
		*idx_root_ptr = 0;
		GET_WORKLOAD->set_index_root(reinterpret_cast<uint64_t>(idx_root_ptr), i);
		printf("index %u root ptr: %lu\n", i, reinterpret_cast<uint64_t>(idx_root_ptr));
	}
	// init memory allocator for one-sided RDMA access
	uint64_t base_addr = reinterpret_cast<uint64_t>(alloc_base + sizeof(global_addr_t) * num_indexes);
	uint64_t base_size = alloc_memory_size - (sizeof(global_addr_t) * num_indexes);
	g_memory_allocator = new memory_allocator_t(base_addr, base_size);

	_dm_size = DEVICE_MEMORY_SIZE;
	_dm_addr = (void*)g_lock_start_addr;
	_dm_mr = register_mr_device(_context.ctx, _context.pd, _dm_addr, _dm_size);
	if (!_dm_mr) return cleanup();
#endif // end of ONE_SIDED

    return true;
}

bool server_transport_t::cleanup(){
    debug::notify_error("Error occurred during initialization --- cleaning up ...");

	if (_dm_mr) ibv_dereg_mr(_dm_mr);
	if (_alloc_mr) ibv_dereg_mr(_alloc_mr);
	if (_alloc_memory_region) delete _alloc_memory_region;
	
	if (_mr) ibv_dereg_mr(_mr);

	if (_batch_qps) {
		for (uint32_t i=0; i<g_num_client_nodes; i++) {
			if(_batch_qps[i]) {
				for (uint32_t j=0; j<g_num_batch_threads; j++) 
					if(_batch_qps[i][j]) ibv_destroy_qp(_batch_qps[i][j]);
				delete[] _batch_qps[i];
			}
		}
		delete[] _batch_qps;
	}

	if (_batch_send_cqs) {
		for (uint32_t i=0; i<g_num_batch_threads; i++)
			if (_batch_send_cqs[i])
				ibv_destroy_cq(_batch_send_cqs[i]);
		delete[] _batch_send_cqs;
	}

	if (_qps) {
		for (uint32_t i=0; i<g_num_client_nodes; i++) {
			if(_qps[i]) {
				for (uint32_t j=0; j<g_num_client_threads; j++) 
					if(_qps[i][j]) ibv_destroy_qp(_qps[i][j]);
				delete[] _qps[i];
			}
		}
		delete[] _qps;
	}
	
	if (_send_cqs) {
		for (uint32_t i=0; i<g_num_client_threads; i++)
			if (_send_cqs[i])
				ibv_destroy_cq(_send_cqs[i]);
		delete[] _send_cqs;
	}
	if (_recv_cq) ibv_destroy_cq(_recv_cq);
	return transport_t::cleanup();
}

bool server_transport_t::connect() {
	if (!transport_t::connect())
		return cleanup();
	struct rdma_meta local;
	uint32_t data_size = sizeof(struct rdma_meta);

	memset(&local, 0, data_size);
	local.gid_idx = _context.gid_idx;
	memcpy(&local.gid, &_context.gid, sizeof(union ibv_gid));
	local.lid = _context.port_attr.lid;
#if TRANSPORT == ONE_SIDED
	local.rkey = _alloc_mr->rkey;
	local.dm_rkey = _dm_mr->rkey;
	local.dm_base = reinterpret_cast<uint64_t>(_dm_addr);
#endif

	_meta = new rdma_meta[g_num_client_nodes];
	memset(_meta, 0, data_size * g_num_client_nodes);

	// exchange QP information with clients
	for (uint32_t i=0; i<g_num_client_nodes; i++) {
		for (uint32_t j=0; j<g_num_client_threads; j++) 
			local.qpn[j] = _qps[i][j]->qp_num;
		for (uint32_t j=0; j<g_num_batch_threads; j++) 
			local.batch_qpn[j] = _batch_qps[i][j]->qp_num;
		char* data = new char[data_size];
		memcpy(data, &local, data_size);

		auto send_msg = new message_t(message_t::type_t::QP_SETUP, 0, data_size, data);
		sendMsg(send_msg, i);
		delete send_msg;

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
	}
	printf("Server connected to %d client nodes ...\n", g_num_client_nodes);

	return true;
}

void server_transport_t::send(char* ptr, uint32_t node_id, uint32_t qp_id, uint32_t size, bool signaled){
    rdma_send(_qps[node_id][qp_id], _send_cqs[qp_id], ptr, size, _mr->lkey, signaled);
}

int server_transport_t::poll(struct ibv_wc* wc, int num) {
	return poll_cq_once(_recv_cq, num, wc);
}

// Pre-post RDMA RECV for the given QP
void server_transport_t::post_recv(char* ptr, uint32_t node_id, uint32_t qp_id, uint32_t size) {
	rdma_recv_prepost(_qps[node_id][qp_id], ptr, size, _mr->lkey, serialize_node_info(node_id, qp_id));
}

void server_transport_t::send_batch(char* ptr, uint32_t node_id, uint32_t qp_id, uint32_t size, bool signaled){
	rdma_send(_batch_qps[node_id][qp_id], _batch_send_cqs[qp_id], ptr, size, _mr->lkey, signaled);
}

void server_transport_t::post_recv_batch(char* ptr, uint32_t node_id, uint32_t qp_id, uint32_t size) {
	rdma_recv_prepost(_batch_qps[node_id][qp_id], ptr, size, _mr->lkey, serialize_node_info(node_id, qp_id + g_num_client_threads));
}
