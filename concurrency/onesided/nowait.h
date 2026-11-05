#pragma once
#include <cstdint>
#include <atomic>
#include <pthread.h>
#include "concurrency/onesided/common.h"
#include "system/global_address.h"

class txn_t;
namespace onesided{

class nowait_t {
public:
	nowait_t(): _lock() { }

	RC lock_get(lock_type_t type, txn_t* txn, global_addr_t addr);
	void lock_release(lock_type_t type, txn_t* txn, global_addr_t addr);

private:
	lock_t   _lock;
	uint64_t _timestamp; // not used in nowait but reserved for alignment
};
}