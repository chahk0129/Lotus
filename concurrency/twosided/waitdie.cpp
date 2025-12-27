#include "concurrency/twosided/waitdie.h"
#include "utils/helper.h"
#include "system/stats.h"
#include "utils/debug.h"
#include "system/global.h"
#include "txn/txn.h"

#if TRANSPORT == TWO_SIDED
namespace twosided{

RC waitdie_t::lock_get(lock_type_t type, txn_t* txn, global_addr_t addr) {
    RC rc = RCOK;
    uint64_t timestamp = txn->get_ts();
	assert(timestamp != 0);
	auto txn_state = txn->get_state();
	if (txn_state != txn_t::state_t::RUNNING) { // wounded by another txn
		assert(txn_state == txn_t::state_t::ABORTING);
		return ABORT;
	}

	assert(txn_state == txn_t::state_t::RUNNING);
    pthread_mutex_lock(&_latch);
	bool conflict = lock_conflict(_lock_type, type);
	if (!conflict) {
		if (!_waiters.empty()) // there are already waiters, must wait/abort 
			conflict = true;
	}

	if (conflict) { // lock conflicts -- cannot be added to the owner's list
		if ((*_owners.begin())->get_ts() < timestamp)
			rc = ABORT;

		if (rc != ABORT) {
			if (txn->update_state(txn_state, txn_t::state_t::WAITING)) {
				_waiters.insert({type, txn});
				rc = WAIT;
			}
			else // the waiting txn has entered ABORTING state
				rc = ABORT;
		}
	}
	else { // no conflict -- add it to owners set
		assert(rc == RCOK);
		_owners.insert(txn);
		_lock_type = type;
	}

    pthread_mutex_unlock(&_latch);
    return rc;
}

void waitdie_t::lock_release(lock_type_t type, txn_t* txn, global_addr_t addr) {
    pthread_mutex_lock(&_latch);

	uint32_t cnt = _owners.erase(txn);
	if (cnt == 0) // not found in owners set, check waiters set
		_waiters.erase({type, txn});

	if (_owners.empty())
	    _lock_type = LOCK_NONE;

    // check waiters for promotion
	do {
		auto it = _waiters.rbegin();
		if (it != _waiters.rend() && !lock_conflict(_lock_type, it->type)) {
			auto state = it->txn->get_state();
			if (state == txn_t::WAITING) {
				if (it->txn->update_state(state, txn_t::RUNNING)) {
					_lock_type = it->type;
					_owners.insert(it->txn);
				}
				// else, the waiting txn has just entered ABORTING state, help it remove
			}
			// else, the waiting txn has already entered ABORTING state, help it remove
			_waiters.erase(--it.base());
		}
		else
			break;
	} while (true);

    pthread_mutex_unlock(&_latch);
}
}
#endif