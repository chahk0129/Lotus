#pragma once
#include <cstdint>
#include <atomic>
#include <pthread.h>
#include <vector>
#include "system/global.h"
#include "system/global_address.h"
#include "concurrency/onesided/common.h"

class txn_t;
namespace onesided{

class waitdie_t {
public:
	waitdie_t(): _lock(), _timestamp(0) { }

	RC   lock_get(lock_type_t type, txn_t* txn, global_addr_t addr);
	void lock_release(lock_type_t type, txn_t* txn, global_addr_t addr);

private:
	lock_t   _lock;
	uint64_t _timestamp;
};

}