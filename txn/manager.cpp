#include "txn/txn.h"
#include "txn/manager.h"
#include "system/thread.h"


txn_manager_t::txn_manager_t(thread_t* thread) {
    _thread = thread;
}