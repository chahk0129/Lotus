#pragma once
#include <atomic>
#include <cstdint>


class version_latch_t {
public:
    version_latch_t() { _version = 0; }
    ~version_latch_t() { }

    bool is_locked() { return _version.load() & 1; }
    bool is_locked(uint64_t version) { return version & 1; }
    
    uint64_t get_version(bool& needRestart) {
        uint64_t version = _version.load();
        if (is_locked(version)) 
            needRestart = true;
        return version;
    }

    bool upgrade_to_writelock(uint64_t old_version) {
        uint64_t version = _version.load();
        if (version != old_version)
            return false;
        if (!_version.compare_exchange_strong(version, version + 1))
            return false;
        return true;
    }

    void release_lock() { _version.fetch_add(1); }

    bool validate(uint64_t old_version) {
        uint64_t version = _version.load();
        return (version == old_version);
    }

private:
    std::atomic<uint64_t> _version;
};