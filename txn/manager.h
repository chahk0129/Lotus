#pragma once

#include <atomic>

#include "system/global.h"
#include "utils/packetize.h"
#include "utils/helper.h"

class thread_t;
class message_t;
class base_query_t;

class txn_manager_t{
    public:
        txn_manager_t(thread_t* thread);

        virtual RC   execute() = 0;
        virtual void global_sync() = 0;

    protected:
        thread_t*               _thread;
};
