#include "system/thread.h"
#include "system/manager.h"

thread_t::thread_t(uint32_t tid, thread_t::type_t type) : _tid(tid), _type(type) {
    _wl = global_manager->get_workload();
}