#include "concurrency/onesided/nowait.h"
#include "client/transport.h"
#include "system/manager.h"
#include "utils/helper.h"
#include "txn/txn.h"

namespace onesided{

RC nowait_t::lock_get(lock_type_t type, txn_t* txn, global_addr_t addr) {
    char* ptr = reinterpret_cast<char*>(&_lock);
RETRY:
    transport->read(ptr, addr, sizeof(_lock));
    bool conflict = _lock.conflict(type);
    if(conflict) // lock conflicts -- cannot acquire a lock
        return ABORT;

    lock_t cur_lock = _lock;
    if(type == LOCK_SH){
        assert(cur_lock.writer == 0);
        lock_t new_lock(cur_lock.readers + 1, 0);
        if(!transport->cas(ptr, addr, cur_lock.val, new_lock.val, sizeof(_lock)))
            goto RETRY;
    }
    else{
        assert(cur_lock.writer == 0 && cur_lock.readers == 0);
        lock_t new_lock(0, 1);
        if(!transport->cas(ptr, addr, cur_lock.val, new_lock.val, sizeof(_lock)))
            return ABORT;
    }
    return RCOK;
}

void nowait_t::lock_release(lock_type_t type, txn_t* txn, global_addr_t addr) {
    char* ptr = reinterpret_cast<char*>(&_lock);
RETRY:
	if (type == LOCK_SH) {
        transport->read(ptr, addr, sizeof(_lock));
        lock_t cur_lock = _lock;
        assert(cur_lock.readers > 0 && cur_lock.writer == 0);
        lock_t new_lock(cur_lock.readers - 1, 0);
        if (!transport->cas(ptr, addr, cur_lock.val, new_lock.val, sizeof(_lock)))
            goto RETRY;
        // transport->faa(ptr, addr, static_cast<uint64_t>(-1), sizeof(_lock));
    }
    else{ // LOCK_EX
        memset(&_lock, 0, sizeof(_lock));
        transport->write(ptr, addr, sizeof(_lock));
    }
}
}