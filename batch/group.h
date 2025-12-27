#pragma once

#include <cstdint>
#include <atomic>
#include <vector>

struct batch_group_t {
    batch_group_t(uint32_t group_id);
    ~batch_group_t() { }
    std::pair<uint32_t, std::atomic<int>*>* get_entry(uint32_t entry_id) { return &_entries[entry_id]; }

    bool is_leader(int thread_id) {
        int cur = _leader.load();
        if (cur != -1) return false;
        return _leader.compare_exchange_strong(cur, thread_id);
    }

    void resign_leader()            { _leader.store(-1); }
    void join()                     { _active_members.fetch_add(1); }
    void leave()                    { _active_members.fetch_sub(1); }
    uint32_t get_active_members()   { return _active_members.load(); }

    std::atomic<int> _leader;
    std::atomic<int> _active_members;
    std::pair<uint32_t, std::atomic<int>*>* _entries;
};