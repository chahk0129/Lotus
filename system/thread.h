#pragma once

#include "system/global.h"

class workload_t;

class thread_t{
    public:
        enum type_t {
            CLIENT_THREAD,
            SERVER_THREAD
        };

        thread_t(uint32_t tid, type_t type);

        type_t   get_type() { return _type; }
        uint32_t get_tid() { return _tid; }
        void     set_tid(uint32_t tid) { _tid = tid; }

        virtual RC run() = 0;

    protected:
        uint32_t    _tid;
        type_t      _type;
        workload_t* _wl;
};