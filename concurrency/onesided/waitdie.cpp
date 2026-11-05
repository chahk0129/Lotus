#include "concurrency/onesided/waitdie.h"
#include "utils/helper.h"
#include "system/stats.h"
#include "txn/txn.h"
#include "client/transport.h"

namespace onesided{

RC waitdie_t::lock_get(lock_type_t type, txn_t* txn, global_addr_t addr){
    bool retry = false;
    uint64_t timestamp = txn->get_ts();
    global_addr_t lock_addr = addr;
    global_addr_t ts_addr = GADD(addr, sizeof(_lock));
    char* lock_ptr = reinterpret_cast<char*>(&_lock);
    char* ts_ptr = reinterpret_cast<char*>(&_timestamp);
RETRY_LOCK:
    transport->read(lock_ptr, lock_addr, sizeof(waitdie_t));
    bool conflict = _lock.conflict(type);
    if(conflict){ // lock conflicts -- cannot acquire a lock
        if(_timestamp == 0 || timestamp < _timestamp){ // new txn has higher priority or timestamp has not been set yet, can wait
            txn->set_state(txn_t::state_t::WAITING);
            PAUSE;
            goto RETRY_LOCK;
        }
        txn->set_state(txn_t::state_t::ABORTING);
        return ABORT;
    }

    // no conflict -- acquire the lock
    lock_t cur_lock = _lock;
    if(type == LOCK_SH){
        assert(cur_lock.writer == 0);
        lock_t new_lock(cur_lock.readers + 1, 0);
        if(!transport->cas(lock_ptr, lock_addr, cur_lock.val, new_lock.val, sizeof(_lock)))
            goto RETRY_LOCK;
    RETRY_TIMESTAMP:
        if(_timestamp == 0 || timestamp < _timestamp){ // update timestamp
            if(!transport->cas(ts_ptr, ts_addr, _timestamp, timestamp, sizeof(_timestamp))){
                transport->read(ts_ptr, ts_addr, sizeof(_timestamp));
                goto RETRY_TIMESTAMP;
            }
        }
    }
    else{ // LOCK_EX
        assert(cur_lock.writer == 0 && cur_lock.readers == 0);
        lock_t new_lock(0, 1);
        if(!transport->cas(lock_ptr, lock_addr, cur_lock.val, new_lock.val, sizeof(_lock)))
            return ABORT;
        _timestamp = timestamp;
        transport->write(ts_ptr, ts_addr, sizeof(_timestamp));
    }

    txn->set_state(txn_t::state_t::RUNNING);
    return RCOK;
}

void waitdie_t::lock_release(lock_type_t type, txn_t* txn, global_addr_t addr){
    global_addr_t lock_addr = addr;
    global_addr_t ts_addr = GADD(addr, sizeof(_lock));
    uint64_t timestamp = txn->get_ts();

    char* lock_ptr = reinterpret_cast<char*>(&_lock);
    char* ts_ptr = reinterpret_cast<char*>(&_timestamp);

    if(type == LOCK_SH){
        // Leave the row timestamp alone. It is the minimum over the readers that took the
        // lock since the last writer, so it never exceeds the oldest current holder's ts;
        // a stale (smaller) value only makes requesters die more often. Clearing it here
        // when the releasing reader was the oldest left the row "held with ts 0" while other
        // (possibly older-than-requester) readers remained, so younger EX requesters waited
        // on older holders and wait-die deadlocked. The next writer overwrites it.
        (void)timestamp; (void)ts_addr; (void)ts_ptr;
        transport->faa(lock_ptr, addr, static_cast<uint64_t>(-1), sizeof(_lock));
    }
    else{ // LOCK_EX
        _timestamp = 0;
        memset(&_lock, 0, sizeof(_lock));
        transport->write(lock_ptr, addr, sizeof(_lock) + sizeof(_timestamp));
    }
}
}