#if TRANSPORT == TWO_SIDED
#pragma once
#include <cstdint>
#include <atomic>
#include <pthread.h>
#include <vector>
#include <set>
#include "system/global.h"
#include "system/global_address.h"

class txn_t;
namespace twosided {

class nowait_t {
public:
	nowait_t() { pthread_mutex_init(&_latch, nullptr); _lock_type = LOCK_NONE; }
	~nowait_t() { pthread_mutex_destroy(&_latch); }

	RC   lock_get(lock_type_t type, txn_t* txn, global_addr_t addr=0);
	void lock_release(lock_type_t type, txn_t* txn, global_addr_t addr=0);

private:
	pthread_mutex_t  _latch;
	lock_type_t      _lock_type;
	std::set<txn_t*> _owners;
};
}
#endif