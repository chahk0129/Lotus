#include "concurrency/twosided/nowait.h"
#include "concurrency/twosided/common.h"
#include "utils/helper.h"
#include "txn/txn.h"

#if TRANSPORT == TWO_SIDED
namespace twosided{

RC nowait_t::lock_get(lock_type_t type, txn_t* txn, global_addr_t addr){
    RC rc = RCOK;
    pthread_mutex_lock(&_latch);
    bool conflict = lock_conflict(_lock_type, type);
    if(conflict) // lock conflicts -- cannot be added to the owner list
        rc = ABORT;
    else {
        _owners.insert(txn);
        _lock_type = type;
    }
    
    pthread_mutex_unlock(&_latch);
    return rc;
}

void nowait_t::lock_release(lock_type_t type, txn_t* txn, global_addr_t addr){
    pthread_mutex_lock(&_latch);
    _owners.erase(txn);
    if (_owners.size() == 0)
        _lock_type = LOCK_NONE;
    pthread_mutex_unlock(&_latch);
}

}
#endif