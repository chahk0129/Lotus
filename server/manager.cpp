#include "system/stats.h"
#include "storage/row.h"
#include "transport/message.h"
#include "server/manager.h"
#include "server/transport.h"
#include "server/thread.h"
#include "system/workload.h"
#include "system/memory_allocator.h"
#include "transport/message.h"
#include "utils/packetize.h"
#include "index/idx_wrapper.h"
#include "index/twosided/partitioned/idx.h"
#include "index/twosided/non_partitioned/idx.h"
#include "index/onesided/dex/leanstore_rpc.h"
#include "txn/txn.h"
#include "txn/table.h"
#include "txn/stored_procedure.h"
#include "txn/interactive.h"
#include <algorithm>

server_txn_manager_t::server_txn_manager_t(thread_t* thread)
	: txn_manager_t(thread) { }

RC server_txn_manager_t::process_msg(message_t* msg) {
	if (msg->is_system_txn()) // system txn for one-sided RDMA operations
		return process_system_txn(msg);
	else
		return process_txn(msg);
}

//////////////////////////////////
// one-sided RDMA operations
//////////////////////////////////
RC server_txn_manager_t::process_system_txn(message_t* msg) {
	auto type = msg->get_type();
	RC rc = RCOK;
	switch (type) {
		case message_t::type_t::REQUEST_RPC_ALLOC:
			rc = rpc_alloc(msg); break;
		case message_t::type_t::REQUEST_IDX_UPDATE_ROOT:
			rc = idx_update_root(msg); break;
		case message_t::type_t::REQUEST_IDX_GET_ROOT:
			rc = idx_get_root(msg); break;
		case message_t::type_t::REQUEST_DEX_INSERT:
			rc = dex_insert(msg); break;
		case message_t::type_t::REQUEST_DEX_LOOKUP:
			rc = dex_lookup(msg); break;
		case message_t::type_t::REQUEST_DEX_UPDATE:
			rc = dex_update(msg); break;
		case message_t::type_t::REQUEST_DEX_DELETE:
			rc = dex_remove(msg); break;
		case message_t::type_t::TERMINATE:
			global_manager->remote_node_done(); break;
		case message_t::type_t::SYNC:
			global_sync(); break;
		default:
		debug::notify_error("Unknown system txn message type %d", msg->get_type());
		assert(false);
	}
	return rc;
}

RC server_txn_manager_t::rpc_alloc(message_t* msg) {
	assert(msg->get_data_size() == sizeof(uint64_t));
	uint64_t alloc_size = *(reinterpret_cast<uint64_t*>(msg->get_data_ptr()));
	assert(alloc_size > 0);
	uint64_t offset = g_memory_allocator->alloc(alloc_size);
	assert(offset != 0);

	uint32_t node_id = msg->get_node_id();
	uint32_t qp_id = msg->get_qp_id();
	uint32_t send_size = sizeof(message_t);
	auto buffer = transport->get_buffer(node_id, qp_id);
	auto send_msg = new (buffer) message_t(message_t::type_t::ACK, sizeof(uint64_t));
	memcpy(send_msg->get_data_ptr(), &offset, sizeof(uint64_t));
	assert(send_msg->get_data_size() == sizeof(uint64_t));
	transport->send(buffer, node_id, qp_id, send_size);
	return RCOK;
}

RC server_txn_manager_t::idx_update_root(message_t* msg) {
	assert(false);
	assert(msg->get_data_size() == sizeof(global_addr_t) + sizeof(uint32_t)); // [ addr | index_id ]
	uint32_t index_id = *reinterpret_cast<uint32_t*>(msg->get_data_ptr());
	global_addr_t addr = *reinterpret_cast<global_addr_t*>(msg->get_data_ptr() + sizeof(uint32_t));
	assert(!addr.is_null());
	GET_WORKLOAD->set_index_root(addr, index_id);

	uint32_t node_id = msg->get_node_id();
	uint32_t qp_id = msg->get_qp_id();
	uint32_t send_size = sizeof(message_t) - sizeof(char*);
	auto buffer = transport->get_buffer(node_id, qp_id);
	auto send_msg = new (buffer) message_t(message_t::type_t::ACK);
	transport->send(buffer, node_id, qp_id, send_size);
	return RCOK;
}

RC server_txn_manager_t::idx_get_root(message_t* msg) {
	assert(msg->get_data_size() == sizeof(uint32_t)); // [ index_id ]
	uint32_t index_id = *(reinterpret_cast<uint32_t*>(msg->get_data_ptr()));
	global_addr_t addr = global_addr_t(g_node_id, GET_WORKLOAD->get_index_root(index_id));
	assert(!addr.is_null());
	printf("idx_get_root: index %u, val(%lu), addr(%lu)\n", index_id, addr.val, addr.addr);

	uint32_t node_id = msg->get_node_id();
	uint32_t qp_id = msg->get_qp_id();
	uint32_t send_size = sizeof(message_t);
	auto buffer = transport->get_buffer(node_id, qp_id);
	auto send_msg = new (buffer) message_t(message_t::type_t::ACK, sizeof(global_addr_t));
	memcpy(send_msg->get_data_ptr(), &addr, sizeof(global_addr_t));
	transport->send(buffer, node_id, qp_id, send_size);
	return RCOK;
}

RC server_txn_manager_t::dex_insert(message_t* msg) {
	assert(msg->get_data_size() == sizeof(global_addr_t) + sizeof(Key) + sizeof(global_addr_t));
	uint32_t node_id = msg->get_node_id();
	uint32_t qp_id = msg->get_qp_id();
	UnstructuredBuffer recv_buffer(msg->get_data_ptr());

	Key k;
	global_addr_t v, leaf_addr;
	recv_buffer.get(&k);
	recv_buffer.get(&v);
	recv_buffer.get(&leaf_addr);
	assert(recv_buffer.size() == msg->get_data_size());

	int ret = cachepush::leanstore_insert<global_addr_t>(leaf_addr, k, v);
	uint32_t base_size = sizeof(message_t) - sizeof(char*);
	auto send_ptr = transport->get_buffer(node_id, qp_id);
	auto send_msg = new (send_ptr) message_t(message_t::type_t::ACK);
	UnstructuredBuffer send_buffer(send_msg->get_data_ptr());
	send_buffer.put(&ret);
	send_buffer.put(&leaf_addr);
	uint32_t data_size = send_buffer.size();
	send_msg->set_data_size(data_size);
	transport->send(send_ptr, node_id, qp_id, data_size + base_size);
	return RCOK;
}

RC server_txn_manager_t::dex_update(message_t* msg) {
	assert(msg->get_data_size() == sizeof(global_addr_t) + sizeof(Key) + sizeof(global_addr_t));
	uint32_t node_id = msg->get_node_id();
	uint32_t qp_id = msg->get_qp_id();
	UnstructuredBuffer recv_buffer(msg->get_data_ptr());
	
	Key k;
	global_addr_t v, leaf_addr;
	recv_buffer.get(&k);
	recv_buffer.get(&v);
	recv_buffer.get(&leaf_addr);
	assert(recv_buffer.size() == msg->get_data_size());

	int ret = cachepush::leanstore_update<global_addr_t>(leaf_addr, k, v);
	uint32_t base_size = sizeof(message_t) - sizeof(char*);
	auto send_ptr = transport->get_buffer(node_id, qp_id);
	auto send_msg = new (send_ptr) message_t(message_t::type_t::ACK);
	UnstructuredBuffer send_buffer(send_msg->get_data_ptr());
	send_buffer.put(&ret);
	send_buffer.put(&leaf_addr);
	uint32_t data_size = send_buffer.size();
	send_msg->set_data_size(data_size);
	transport->send(send_ptr, node_id, qp_id, data_size + base_size);
	return RCOK;
}

RC server_txn_manager_t::dex_lookup(message_t* msg) {
	assert(msg->get_data_size() == sizeof(global_addr_t) + sizeof(Key));
	uint32_t node_id = msg->get_node_id();
	uint32_t qp_id = msg->get_qp_id();
	UnstructuredBuffer recv_buffer(msg->get_data_ptr());

	Key k;
	global_addr_t start_node;
	recv_buffer.get(&k);
	recv_buffer.get(&start_node);
	assert(recv_buffer.size() == msg->get_data_size());

	global_addr_t v, result;
	int level = cachepush::leanstore_lookup<global_addr_t>(start_node, k, v, result);
	uint32_t base_size = sizeof(message_t) - sizeof(char*);
	auto send_ptr = transport->get_buffer(node_id, qp_id);
	auto send_msg = new (send_ptr) message_t(message_t::type_t::ACK);
	UnstructuredBuffer send_buffer(send_msg->get_data_ptr());
	send_buffer.put(&level);
	if (level >= 1)
		send_buffer.put(&v);
	uint32_t data_size = send_buffer.size();
	send_msg->set_data_size(data_size);
	transport->send(send_ptr, node_id, qp_id, data_size + base_size);
	return RCOK;
}

RC server_txn_manager_t::dex_remove(message_t* msg) {
	assert(msg->get_data_size() == sizeof(global_addr_t) + sizeof(Key));
	uint32_t node_id = msg->get_node_id();
	uint32_t qp_id = msg->get_qp_id();
	UnstructuredBuffer recv_buffer(msg->get_data_ptr());

	Key k;
	global_addr_t start_node;
	recv_buffer.get(&k);
	recv_buffer.get(&start_node);
	assert(recv_buffer.size() == msg->get_data_size());

	int ret = cachepush::leanstore_remove<global_addr_t>(start_node, k);
	uint32_t base_size = sizeof(message_t) - sizeof(char*);
	auto send_ptr = transport->get_buffer(node_id, qp_id);
	auto send_msg = new (send_ptr) message_t(message_t::type_t::ACK);
	UnstructuredBuffer send_buffer(send_msg->get_data_ptr());
	send_buffer.put(&ret);
	uint32_t data_size = send_buffer.size();
	send_msg->set_data_size(data_size);
	transport->send(send_ptr, node_id, qp_id, data_size + base_size);
	return RCOK;
}

void server_txn_manager_t::global_sync() {
	global_manager->set_sync_done();
	if (global_manager->get_sync_done()) { // all clients are ready
		char* base_ptr = transport->get_buffer();
		uint32_t base_size = sizeof(message_t) - sizeof(char*);
		for (uint32_t i=0; i<g_num_server_nodes; i++) {
			char* send_ptr = base_ptr + i * base_size;
			auto msg = new (send_ptr) message_t(message_t::type_t::ACK);
			transport->send(send_ptr, i, 0, base_size);
		}
	}
}

//////////////////////////////////
// two-sided RDMA operations
//////////////////////////////////
void server_txn_manager_t::check_wait_buffer() {
	// check waiting txns
	for (auto it = _wait_buffer.begin(); it != _wait_buffer.end(); ) {
		auto txn = *it;
		auto state = txn->get_state();
		if (state == txn_t::state_t::RUNNING) {
			// txn has been promoted to lock owner
			RC rc = continue_execute(txn);
			if (rc == RCOK)
				it = _wait_buffer.erase(it);
			else if(rc == ABORT) {
				it = _wait_buffer.erase(it);
				delete txn;
				INC_INT_STATS(num_aborts, 1);
			}
			else {
				assert(rc == WAIT);
				it++;
				INC_INT_STATS(num_waits, 1);
			}
		}
		else if (state == txn_t::state_t::ABORTING) {
			// txn has been wounded by another txn 
			it = _wait_buffer.erase(it);
			process_abort(txn);
			delete txn;
			INC_INT_STATS(num_aborts, 1);
		}
		else {
			assert(state == txn_t::state_t::WAITING);
			it++;
		}
	}
}

void server_txn_manager_t::remove_wait_buffer(txn_t* txn) {
	auto txn_id = txn->get_id();
	txn->process_abort();
	txn_table->remove(txn_id);

	if (_wait_buffer.size() > 0) {
		auto it = std::find(_wait_buffer.begin(), _wait_buffer.end(), txn);
		if (it != _wait_buffer.end()) {
			_wait_buffer.erase(it);
			delete txn; // native txn for this thread
		}
	}
}

RC server_txn_manager_t::continue_execute(txn_t* txn) {
	assert(txn != nullptr);
	txn_id_t txn_id = txn->get_id();
	uint32_t node_id = txn_id.get_node_id();
	uint32_t qp_id = txn_id.get_qp_id();
	char* buffer = transport->get_buffer(node_id, qp_id);
	auto send_msg = new (buffer) message_t(message_t::type_t::ACK, 0);
	uint32_t send_size = sizeof(message_t) - sizeof(char*);
	uint32_t resp_size = 0;
	char* resp_data = send_msg->get_data_ptr();

	// this txn has been promoted to lock owner no need to acquire the lock
	RC rc = txn->process_request(resp_data, resp_size);
	if (rc == ABORT) {
		// printf("	TXN %lu aborts1 continued (ts %lu)\n", txn->get_id().to_uint64(), txn->get_ts());
		// fflush(stdout);
		send_msg->set_type(message_t::type_t::RESPONSE_ABORT);
		txn_table->remove(txn_id);	
	}
	else if (rc == WAIT) {
		// printf("	TXN %lu waits continued (ts %lu)\n", txn->get_id().to_uint64(), txn->get_ts());
		// fflush(stdout);
		return rc;
	}
	else {
		assert(rc == RCOK);
		if (resp_size > 0)
			send_msg->set_data_size(resp_size);
	}
	
	// printf("[txn %lu] response type [%s]\n", txn_id.to_uint64(), send_msg->get_name().c_str());
	// 	fflush(stdout);
	transport->send(buffer, node_id, qp_id, send_size + resp_size);
	return rc;
}

RC server_txn_manager_t::process_batch_msg(message_t* recv_msg, uint32_t node_id, uint32_t qp_id) {
	uint32_t base_size = sizeof(message_t) - sizeof(char*);
	uint32_t recv_offset = 0;
	uint32_t send_offset = 0;

	uint32_t recv_batch_size = recv_msg->get_data_size();
	char* recv_ptr = recv_msg->get_data_ptr();

	char* send_buffer = transport->get_batch_buffer(node_id, qp_id);
	auto send_msg = new (send_buffer) message_t(message_t::type_t::ACK, 0);
	char* send_ptr = send_msg->get_data_ptr();

	// printf("[batch %lu] %u\n", txn_id_t(recv_msg->get_node_id(), recv_msg->get_qp_id()).to_uint64(), recv_batch_size);
	// 	fflush(stdout);
	for (uint32_t i=0; i<recv_batch_size; i++) {
		auto cur_recv_msg = reinterpret_cast<message_t*>(recv_ptr + recv_offset);
		auto cur_recv_msg_type = cur_recv_msg->get_type();
		char* cur_recv_data = cur_recv_msg->get_data_ptr();
		uint32_t cur_recv_data_size = cur_recv_msg->get_data_size();

		char* cur_send_ptr = send_ptr + send_offset;
		auto cur_send_msg = new (cur_send_ptr) message_t(message_t::type_t::ACK, 0);
		cur_send_msg->set_qp_id(cur_recv_msg->get_qp_id());
		char* cur_send_data = cur_send_msg->get_data_ptr();
		uint32_t cur_send_size = 0;
		RC rc = RCOK;

		txn_t* txn = nullptr;
		txn_id_t txn_id(cur_recv_msg->get_node_id(), cur_recv_msg->get_qp_id());
		// printf("t%u [%u] txn %lu request type [%s] \n", GET_THD_ID, i, txn_id.to_uint64(), cur_recv_msg->get_name().c_str());
		// fflush(stdout);
		if (cur_recv_msg->is_regular_txn()) { // client access requests
			txn = txn_table->find(txn_id);
			if (cur_recv_msg_type == message_t::type_t::REQUEST_STORED_PROCEDURE) {
				assert(txn == nullptr);
				if (!txn) {
					txn = GET_WORKLOAD->create_stored_procedure(txn_id);
					txn_table->insert(txn_id, txn);
				}
				// printf("    TXN %lu begins batch (ts %lu)\n", txn->get_id().to_uint64(), txn->get_ts());
				// fflush(stdout);
			}
			else if (cur_recv_msg_type == message_t::type_t::REQUEST_INTERACTIVE) {
				if (!txn) {
					txn = GET_WORKLOAD->create_interactive(txn_id);
					txn_table->insert(txn_id, txn);
				}
				// printf("    TXN %lu begins batch (ts %lu)\n", txn->get_id().to_uint64(), txn->get_ts());
				// fflush(stdout);
			}
			else assert(false);

			txn->parse_request(cur_recv_msg);
			rc = txn->process_request(cur_send_data, cur_send_size);
#if PARTITIONED // this is just index operation
			assert(rc == RCOK);
			assert(cur_send_size > 0);
			txn_table->remove(txn_id);
			delete txn;
			cur_send_msg->set_data_size(cur_send_size);
#else
			if (rc == ABORT) {
				cur_send_msg->set_type(message_t::type_t::RESPONSE_ABORT);
				txn_table->remove(txn_id);
				// printf("    TXN %lu aborts1 batch (ts %lu)\n", txn->get_id().to_uint64(), txn->get_ts());
				// fflush(stdout);
				delete txn;
				INC_INT_STATS(num_aborts, 1);
			}
			else if (rc == WAIT) {
				cur_send_msg->set_type(message_t::type_t::RESPONSE_WAIT);
				_wait_buffer.push_back(txn);
			// printf("    TXN %lu waits batch (ts %lu)\n", txn->get_id().to_uint64(), txn->get_ts());
			// fflush(stdout);
				INC_INT_STATS(num_waits, 1);
			}
			else {
				assert(rc == RCOK);
				assert(cur_send_size > 0);
				cur_send_msg->set_data_size(cur_send_size);
			}
#endif
		}
		else {  // client txn requests
			txn = txn_table->find(txn_id);
			if (cur_recv_msg_type == message_t::type_t::REQUEST_PREPARE) {
				assert(txn != nullptr);
				rc = txn->process_prepare(cur_recv_data, cur_recv_data_size);
				if (rc != RCOK) {
					assert(rc == ABORT); // cannot be WAIT
				// printf("    TXN %lu aborts2 batch (ts %lu)\n", txn->get_id().to_uint64(), txn->get_ts());
				// fflush(stdout);
					txn_table->remove(txn_id);
					delete txn;
					INC_INT_STATS(num_aborts, 1);
					cur_send_msg->set_type(message_t::type_t::RESPONSE_ABORT);
				}
				else
					cur_send_msg->set_type(message_t::type_t::RESPONSE_PREPARE);
			}
			else if (cur_recv_msg_type == message_t::type_t::REQUEST_ABORT) {
				if (txn) {
				// printf("    TXN %lu aborts3 batch (ts %lu)\n", txn->get_id().to_uint64(), txn->get_ts());
				// fflush(stdout);
					remove_wait_buffer(txn);
					INC_INT_STATS(num_aborts, 1);
				}
				cur_send_msg->set_type(message_t::type_t::RESPONSE_ABORT);
			}
			else if (cur_recv_msg_type == message_t::type_t::REQUEST_COMMIT) {
				assert(txn != nullptr);
				rc = txn->process_commit(cur_recv_data, cur_recv_data_size);
				if (rc == COMMIT) {
				// printf("    TXN %lu commits batch (ts %lu)\n", txn->get_id().to_uint64(), txn->get_ts());
					cur_send_msg->set_type(message_t::type_t::RESPONSE_COMMIT);
					INC_INT_STATS(num_commits, 1);
				}
				else {
				// printf("    TXN %lu aborts4 (ts %lu)\n", txn->get_id().to_uint64(), txn->get_ts());
					cur_send_msg->set_type(message_t::type_t::RESPONSE_ABORT);
					INC_INT_STATS(num_aborts, 1);
				}
				txn_table->remove(txn_id);
				delete txn;
			}
			else {
				debug::notify_error("Unknown message type %d: %s", cur_recv_msg_type, cur_recv_msg->get_name().c_str());
				assert(false);
			}
		}
		// printf("t%u [%u] txn %lu response type [%s] \n", GET_THD_ID, i, txn_id.to_uint64(), cur_send_msg->get_name().c_str());
		// fflush(stdout);

		send_offset += base_size + cur_send_size;
		recv_offset += base_size + cur_recv_data_size;
	}

	send_msg->set_data_size(recv_batch_size);
	// send_msg->set_data_size(send_offset);
	transport->send_batch(send_buffer, node_id, qp_id, base_size + send_offset);
	return RCOK;
}


RC server_txn_manager_t::process_txn(message_t* recv_msg) { 
	uint32_t send_size = sizeof(message_t) - sizeof(char*);
	uint32_t resp_size = 0;
	uint32_t node_id = recv_msg->get_node_id();
	uint32_t qp_id = recv_msg->get_qp_id();
	auto msg_type = recv_msg->get_type();
	txn_t* txn = nullptr;
	RC rc = RCOK;
	txn_id_t txn_id(node_id, qp_id);
	message_t::type_t response_type = message_t::type_t::ACK;

	char* buffer = transport->get_buffer(node_id, qp_id);
	auto send_msg = new (buffer) message_t(response_type, 0);
	auto resp_data = send_msg->get_data_ptr();

	// printf("t%u txn %lu request type [%s] \n", GET_THD_ID, txn_id.to_uint64(), recv_msg->get_name().c_str());
	// printf("[txn %lu] request type [%s]\n", txn_id.to_uint64(), recv_msg->get_name().c_str());
		// fflush(stdout);
	// fflush(stdout);
	if (recv_msg->is_regular_txn()) {  // client access requests
		txn = txn_table->find(txn_id);
		if (msg_type == message_t::type_t::REQUEST_STORED_PROCEDURE) {
			assert(txn == nullptr);
			if (!txn) {
				txn = GET_WORKLOAD->create_stored_procedure(txn_id);
				txn_table->insert(txn_id, txn);
				// printf("    TXN %lu begins\n", txn->get_id().to_uint64());
				// fflush(stdout);
			}
		}
		else if (msg_type == message_t::type_t::REQUEST_INTERACTIVE) {
			if (!txn) {
				txn = GET_WORKLOAD->create_interactive(txn_id);
				txn_table->insert(txn_id, txn);
				// printf("    TXN %lu begins\n", txn->get_id().to_uint64());
				// fflush(stdout);
			}
		}
		else assert(false);

		// TODO: optimized interactive txn
		txn->parse_request(recv_msg);
		rc = txn->process_request(resp_data, resp_size);
#if PARTITIONED // this is just index operation
		assert(rc == RCOK);
		assert(resp_size > 0);
		txn_table->remove(txn_id);
		delete txn;
		send_msg->set_data_size(resp_size);
#else
		if (rc == ABORT) {
			response_type = message_t::type_t::RESPONSE_ABORT;
			txn_table->remove(txn_id);
			// printf("    TXN %lu aborts1 (ts %lu)\n", txn->get_id().to_uint64(), txn->get_ts());
			// fflush(stdout);
			delete txn;
			INC_INT_STATS(num_aborts, 1);
		}
		else if (rc == WAIT) {
			// printf("TXN %lu waits\n", txn->get_id().to_uint64());
			_wait_buffer.push_back(txn);
			INC_INT_STATS(num_waits, 1);
			// printf("    TXN %lu waits (ts %lu)\n", txn->get_id().to_uint64(), txn->get_ts());
			// fflush(stdout);
			return rc; // waiting txn will send the response later
		}
		else {
			assert(rc == RCOK);
			assert(resp_size > 0);
			send_msg->set_data_size(resp_size);
		}
#endif
	}
	else { // client txn requests
		txn = txn_table->find(txn_id);
		if (msg_type == message_t::type_t::REQUEST_PREPARE) {
			assert(txn != nullptr);
			// printf("    TXN %lu prepares\n", txn->get_id().to_uint64());
			rc = txn->process_prepare(recv_msg->get_data_ptr(), recv_msg->get_data_size());
			if (rc != RCOK) {
				assert(rc == ABORT); // cannot be WAIT
				// printf("    TXN %lu aborts2 (ts %lu)\n", txn->get_id().to_uint64(), txn->get_ts());
				// fflush(stdout);
				txn_table->remove(txn_id);
				delete txn;
				INC_INT_STATS(num_aborts, 1);
				response_type = message_t::type_t::RESPONSE_ABORT;
			}
			else 
				response_type = message_t::type_t::RESPONSE_PREPARE;
			// txn will be removed after commit/abort (2PC)
		}
		else if (msg_type == message_t::type_t::REQUEST_ABORT) {
			if (txn) {
				// printf("    TXN %lu aborts3 (ts %lu)\n", txn->get_id().to_uint64(), txn->get_ts());
				// fflush(stdout);
				remove_wait_buffer(txn);
				INC_INT_STATS(num_aborts, 1);
			}
			response_type = message_t::type_t::RESPONSE_ABORT;
			// return RCOK;
		}
		else if (msg_type == message_t::type_t::REQUEST_COMMIT) {
			assert(txn != nullptr);
			rc = txn->process_commit(recv_msg->get_data_ptr(), recv_msg->get_data_size());
			if (rc == COMMIT) { 
				// printf("    TXN %lu commits (ts %lu)\n", txn->get_id().to_uint64(), txn->get_ts());
				response_type = message_t::type_t::RESPONSE_COMMIT;
				INC_INT_STATS(num_commits, 1); 
			}
			else {
				// printf("    TXN %lu aborts4 (ts %lu)\n", txn->get_id().to_uint64(), txn->get_ts());
				response_type = message_t::type_t::RESPONSE_ABORT;
				INC_INT_STATS(num_aborts, 1); 
			}
			// fflush(stdout);
			txn_table->remove(txn_id);
			delete txn;
		}
		else {
			debug::notify_error("Unknown message type %d: %s", msg_type, recv_msg->get_name().c_str()); 
			assert(false);
			return ERROR;
		}
	}
	send_msg->set_type(response_type);
	// printf("t%u txn %lu response type [%s] \n", GET_THD_ID, txn_id.to_uint64(), send_msg->get_name().c_str());
	// printf("[txn %lu] response type [%s]\n", txn_id.to_uint64(), send_msg->get_name().c_str());
	// 	fflush(stdout);
	// fflush(stdout);
	// if (resp_size > 0) {
	// 	send_msg->set_data_size(resp_size);
	// 	assert(send_msg->get_data_size() == resp_size);
	// }

	// printf("[node %u, tid %u --> node %u, tid %u] send data size = %u \t (%s --> %s)\n", g_node_id, GET_THD_ID, node_id, qp_id, send_msg->get_data_size(), recv_msg->get_name(recv_msg->get_type()).c_str(), send_msg->get_name(send_msg->get_type()).c_str());
	transport->send(buffer, node_id, qp_id, send_size + resp_size);
	return rc;
}

void server_txn_manager_t::process_abort(txn_t* txn) {
	assert(txn != nullptr);
	txn->process_abort();
	auto txn_id = txn->get_id();
	bool need_notification = txn_table->remove(txn_id);
	if (need_notification) {
		// printf("    TXN %lu aborts5 (ts %lu)\n", txn_id.to_uint64(), txn->get_ts());
		// fflush(stdout);

		uint32_t node_id = txn_id.get_node_id();
		uint32_t qp_id = txn_id.get_qp_id();

		char* buffer = transport->get_buffer(node_id, qp_id);
		auto send_msg = new (buffer) message_t(message_t::type_t::RESPONSE_ABORT);
		uint32_t send_size = sizeof(message_t) - sizeof(char*);
		transport->send(buffer, node_id, qp_id, send_size);
	}
}