#include "concurrency/twosided/woundwait.h"
#include "utils/helper.h"
#include "system/stats.h"
#include "txn/txn.h"

#if TRANSPORT == TWO_SIDED
namespace twosided{

RC woundwait_t::lock_get(lock_type_t type, txn_t* txn, global_addr_t addr){
    RC rc = RCOK;
    uint64_t timestamp = txn->get_ts();
	auto txn_state = txn->get_state();
	if (txn_state != txn_t::state_t::RUNNING) { // wounded by another txn
		assert(txn_state == txn_t::state_t::ABORTING);
		return ABORT;
	}

	assert(txn_state == txn_t::state_t::RUNNING);
	// set txn state to WAITING outside critical section
	if (!txn->update_state(txn_state, txn_t::state_t::WAITING)) {
		assert(txn->get_state() == txn_t::state_t::ABORTING);
		return ABORT;
	}

	txn_state = txn_t::state_t::WAITING;
    pthread_mutex_lock(&_latch);
	if (_owners.empty()) { // no current owners, grab the lock
		assert(_waiters.empty());
		if (!txn->update_state(txn_state, txn_t::state_t::RUNNING)) { // the txn was wounded by another txn
			assert(txn->get_state() == txn_t::state_t::ABORTING);
			pthread_mutex_unlock(&_latch);
			return ABORT;
		}
		_lock_type = type;
		_owners.insert(txn);
		pthread_mutex_unlock(&_latch);
		return rc;
	}

	// // add current txn to waiters
	// // if (_waiters.size() > MAX_NUM_WAITS)
	// // 	rc = ABORT;
	// // else if (txn->update_state(txn_state, txn_t::state_t::WAITING)) {

	// if (txn->update_state(txn_state, txn_t::state_t::WAITING)) {
	// 	_waiters.insert({type, txn});
	// 	rc = WAIT;
	// }
	// else { // the txn was wounded by another txn
	// 	assert(txn->get_state() == txn_t::state_t::ABORTING);
	// 	rc = ABORT;
	// 	pthread_mutex_unlock(&_latch);
	// 	return rc;
	// }

	// examine owners for preemption
	for (auto it=_owners.rbegin(); it!=_owners.rend(); ) {
		auto state = (*it)->get_state();
		if (state == txn_t::state_t::ABORTING) { // this txn has already been aborting
			_owners.erase(--it.base()); // help it remove
			continue;
		}
		else if (state == txn_t::state_t::COMMITTING) {
			it++; // cannot preempt committing txns
			continue;
		}

		if (timestamp <= (*it)->get_ts()) { // cur txn has higher priority than this owner
			if (lock_conflict(_lock_type, type)) { // and has conflict
				// try to wound this owner
				if ((*it)->update_state(state, txn_t::state_t::ABORTING)) {
					// successfully wounded
					_owners.erase(--it.base());
				}
				else { // failed, but need to double check (maybe ABORTING / WAITING / COMMITTING)
					if ((*it)->get_state() != txn_t::state_t::COMMITTING)
						continue;
					it++; // skip it if entered commit phase
				}
			}
			else // pass non-conflicting txns
				it++;
		}
		else // cannot preempt txns with higher priority
			break;
	}
	
	if (_owners.empty()) // all active txns have been wounded
		_lock_type = LOCK_NONE;

	_waiters.insert({type, txn});
	rc = WAIT;
	
	bool promotion_done = false;
	bool preemption_done = (rc == WAIT) ? false : true;
	for (auto it=_waiters.begin(); it!=_waiters.end(); ) {
		auto state = it->txn->get_state();
		if (state == txn_t::state_t::ABORTING) { // this txn has already been aborting
			if (it->txn == txn) {
				rc = ABORT;
				preemption_done = true;
			}
			it = _waiters.erase(it); // help it remove
			continue;
		}

		// try to wound conflicting txns
		if (!preemption_done) {
			if (it->txn != txn && timestamp < it->txn->get_ts()) { // cur txn has higher priority than this waiter
				if (lock_conflict(type, it->type)) { 
					it->txn->update_state(state, txn_t::state_t::ABORTING);
					// even if update fails, it has to be ABORTING now
					it = _waiters.erase(it);
					continue;
				}
				else { // pass non-conflicting txn
					if (promotion_done) {
						it++;
						continue;
					}
				}
			}
			else if (it->txn != txn) { // cannot preempt txns with higher priority
				preemption_done = true;
			}
			// else, cannot preempt self
		}

		// examine lock conflict between waiters and owners for promotion
		if (!promotion_done && !lock_conflict(_lock_type, it->type)) { // promote non-conflicting waiters
			if (state == txn_t::state_t::WAITING) {
				if (it->txn->update_state(state, txn_t::state_t::RUNNING)) { // promote this waiter to owner
					_owners.insert(it->txn);
					_lock_type = it->type;
				}
				// else, the waiting txn has just entered ABORTING state, help it remove
			}
			// else, waiting txn got wounded by another txn, help it remove
			it = _waiters.erase(it);
		}
		else { // conflicting waiter, stop promotion
			promotion_done = true;
			it++;
		}

		if (promotion_done && preemption_done) // nothing more to do
			break;
	}

	// check if the current txn has been promoted to an owner
	if (rc == WAIT) {
		if (_owners.find(txn) != _owners.end()) {
			rc = RCOK;
			assert(txn->get_state() != txn_t::state_t::WAITING);
		}
		else if (_waiters.find({type, txn}) == _waiters.end()) { // has been wounded by another txn
			rc = ABORT;
			assert(txn->get_state() == txn_t::state_t::ABORTING);
		}
	}
	
	pthread_mutex_unlock(&_latch);

    return rc;
}

void woundwait_t::lock_release(lock_type_t type, txn_t* txn, global_addr_t addr){
    pthread_mutex_lock(&_latch);

	uint32_t cnt = _owners.erase(txn);
	if (cnt == 0) { // not found in owners set, check waiters set
		cnt = _waiters.erase({type, txn});
		if (cnt == 0) {
			pthread_mutex_unlock(&_latch);
			return; // not found
		}
	}

	if(_owners.empty())
	    _lock_type = LOCK_NONE;

	// examine waiting txns for promotion
	for (auto it=_waiters.begin(); it!=_waiters.end(); ) {
		// examine lock conflict between waiters and lock owners for promotion
		if (!lock_conflict(_lock_type, it->type)) { // waiter not conflicting with lock owners
			auto state = it->txn->get_state();
			if (state == txn_t::state_t::WAITING) {
				if (it->txn->update_state(state, txn_t::state_t::RUNNING)) {
					// successfully promoted
					_owners.insert(it->txn);
					_lock_type = it->type;
				}
				// else, the waiting txn has just entered ABORTING state, help it remove
			}
			// else, waiting txn got wounded by another txn, help it remove
			it = _waiters.erase(it);
		}
		else // conflicting waiter, stop promotion
			break;
	}
	
    pthread_mutex_unlock(&_latch);
}
}
#endif