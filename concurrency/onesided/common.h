#pragma once

#include "system/global.h"

namespace onesided {

class lock_t {
public:
    lock_t(): val(0) { }
    lock_t(const lock_t& other) {
        readers = other.readers;
        writer  = other.writer;
        // val = other.val;
    }
    lock_t(const uint64_t readers, const uint64_t writer) {
        this->readers = readers;
        this->writer = writer;
    }

    bool conflict(lock_type_t lt) {
        if (lt == LOCK_EX) {
            if (writer > 0 || readers > 0)
                return true;
        }
        else { // LOCK_SH
            if (writer > 0)
                return true;
        }
        return false;
    }

    union {
        uint64_t val;
        struct {
            uint64_t readers : 56;
            uint64_t writer  : 8;
        };
    };
};

} // namespace onesided
