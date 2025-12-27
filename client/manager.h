#pragma once
#include "system/global.h"
#include "system/global_address.h"
#include "txn/manager.h"

class thread_t;
class base_query_t;
class txn_t;
class client_txn_manager_t: public txn_manager_t {
public:
	client_txn_manager_t(thread_t* thread);
	~client_txn_manager_t();

	void init(base_query_t*);
	RC run();
	RC execute();

	void global_sync();
	void terminate();
	void clear_txn();

private:
	txn_t* 			   _txn;
};