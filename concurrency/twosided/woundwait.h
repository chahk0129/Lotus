#pragma once
#include <cstdint>
#include <atomic>
#include <pthread.h>
#include <vector>
#include "system/global.h"
#include "system/global_address.h"
#include "concurrency/twosided/common.h"
class txn_t;
namespace twosided{

class woundwait_t {
public:
	woundwait_t() { pthread_mutex_init(&_latch, nullptr); _lock_type = LOCK_NONE; }
	~woundwait_t() { pthread_mutex_destroy(&_latch); }

	RC   lock_get(lock_type_t type, txn_t* txn, global_addr_t addr=0);
	void lock_release(lock_type_t type, txn_t* txn, global_addr_t addr=0);

private:	
	pthread_mutex_t  						  _latch;
	lock_type_t	 						  	  _lock_type;
	std::set<txn_t*, lock_comparator_t> 	  _owners;
	std::set<wait_entry_t, wait_comparator_t> _waiters;
};

}