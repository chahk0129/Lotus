#pragma once

#include "system/global.h"
#include "txn/manager.h"
#include <vector>

class thread_t;
class server_transport_t;
class message_t;
class txn_t;

class server_txn_manager_t: public txn_manager_t {
public:
	server_txn_manager_t(thread_t* thread);
	~server_txn_manager_t() = default;

	RC   execute() { assert(false); return ERROR; }
	RC   process_msg(message_t* msg);
	RC   process_batch_msg(message_t* msg, uint32_t node_id, uint32_t qp_id);
	void check_wait_buffer();
	void global_sync();

private:
	RC   process_system_txn(message_t* msg);
	RC   continue_execute(txn_t* txn);
	RC   process_txn(message_t* msg);
	void process_abort(txn_t* txn);

	RC   rpc_alloc(message_t* msg);
	RC   idx_update_root(message_t* msg);
	RC   idx_get_root(message_t* msg);
	RC   dex_insert(message_t* msg);
	RC   dex_lookup(message_t* msg);
	RC   dex_update(message_t* msg);
	RC   dex_remove(message_t* msg);

	void remove_wait_buffer(txn_t* txn);

	std::vector<txn_t*> _wait_buffer; // txns waiting for the lock
};
