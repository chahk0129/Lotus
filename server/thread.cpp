#include "server/thread.h"
#include "server/manager.h"
#include "server/transport.h"
#include "transport/message.h"
#include "system/manager.h"
#include "utils/helper.h"
#include "batch/table.h"
#include "txn/txn.h"

server_thread_t::server_thread_t(uint32_t tid) 
	: thread_t(tid, thread_t::type_t::SERVER_THREAD) { }

RC server_thread_t::run() {
	// this assumes running clients and servers in the same machine
	bind_core(g_num_client_threads + get_tid()); // bind to the core after client threads
	// bind_core(g_num_client_threads + g_num_batch_threads + get_tid()); // bind to the core after client threads
	global_manager->init_rand(get_tid());
	global_manager->set_tid(get_tid());
	assert(global_manager->get_tid() == get_tid());

	pthread_barrier_wait(&global_barrier);

	RC rc = RCOK;
	uint32_t node_id, qp_id;

	auto txn_man = new server_txn_manager_t(this); // execute system txns (e.g., micro-operations)
	auto transport = static_cast<server_transport_t*>(GET_TRANSPORT); 
	assert(transport != nullptr);
	
	// int batch_msg_size = 4;
	#if BATCH
	// int batch_msg_size = g_num_client_threads / (batch_table_t::MAX_GROUP_SIZE);
	// int batch_msg_size = g_num_client_threads / g_num_server_threads;
	int batch_msg_size = g_num_client_threads / (batch_table_t::MAX_GROUP_SIZE) / g_num_server_threads;
	#else
	int batch_msg_size = g_num_client_threads / g_num_server_threads;
	#endif
	if (batch_msg_size == 0) batch_msg_size = 1;
    struct ibv_wc wc[batch_msg_size];

	uint64_t last_stats_cp_time = get_sys_clock();

    while (!global_manager->is_sim_done()) {
		// checkpoint every 100 ms
		uint64_t cur_ts = get_sys_clock();
		if (get_tid() == 0 && (cur_ts - last_stats_cp_time > STATS_CP_INTERVAL * 1000 * 1000)) {
			stats->checkpoint();
			last_stats_cp_time += STATS_CP_INTERVAL * 1000 * 1000;
		}

		#if !PARTITIONED
		txn_man->check_wait_buffer();
		#endif

		//struct ibv_wc wc[batch_msg_size];
		int cnt = transport->poll(wc, batch_msg_size);
		assert(cnt <= batch_msg_size);

		if (cnt > 0) {
			// printf("server_thread %u: polled %d messages\n", get_tid(), cnt);
			for(int i=0; i<cnt; i++) {
				assert(wc[i].status == IBV_WC_SUCCESS);
				assert(wc[i].opcode == IBV_WC_RECV);
				uint64_t wr_id = wc[i].wr_id;
				transport->deserialize_node_info(wr_id, node_id, qp_id);
				if (qp_id >= g_num_client_threads) { // this is a batch_request
					assert(qp_id < g_num_client_threads + g_num_batch_threads);
					// printf("[node %u, tid %u] received batch message from node %u, qp_id %u\n", g_node_id, GET_THD_ID, node_id, qp_id);
					qp_id -= g_num_client_threads; // adjust qp_id
					char* recv_buffer = transport->get_recv_batch_buffer(node_id, qp_id);
					auto batch_msg = reinterpret_cast<message_t*>(recv_buffer);
					assert(batch_msg->get_node_id() == node_id);
					txn_man->process_batch_msg(batch_msg, node_id, qp_id);
					transport->post_recv_batch(recv_buffer, node_id, qp_id, (uint64_t)MAX_MESSAGE_SIZE * batch_table_t::MAX_GROUP_SIZE);
				} 
				else { // this is a normal request
					// printf("[node %u, tid %u] received message from node %u, qp_id %u\n", g_node_id, GET_THD_ID, node_id, qp_id);
					char* recv_buffer = transport->get_recv_buffer(node_id, qp_id);
					auto msg = reinterpret_cast<message_t*>(recv_buffer);
					assert(msg->get_node_id() == node_id);
					txn_man->process_msg(msg);
					transport->post_recv(recv_buffer, node_id, qp_id, MAX_MESSAGE_SIZE);
				}
			}
		}

		// if (global_manager->is_sim_done()) {
		// 	break;
		// }
	}

	delete txn_man;
	return FINISH;
}