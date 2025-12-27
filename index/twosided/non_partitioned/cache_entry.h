#pragma once

#if !PARTITIONED

namespace nonpartitioned {

struct EntryLock {
    EntryLock() { lock.store(0b100); }
    std::atomic<uint8_t> lock;

    bool isLocked(uint8_t version) { return (version & 0b10) == 0b10; }
    bool isObsolete(uint8_t version) { return (version & 0b1) == 0b1; }
    void setObsoleteState() { lock.store(0b1); }

    uint8_t readLockOrRestart(bool& needRestart) {
        uint8_t version = lock.load();
        if (isLocked(version) || isObsolete(version))
            needRestart = true;
        return version;
    }

    void readUnlockOrRestart(uint8_t startRead, bool& needRestart) {
        needRestart = (startRead != lock.load());
    }
};

template <typename Payload>
struct CacheEntry : public EntryLock {
    std::atomic<uint8_t> frequency;
    uint64_t key;
    Payload data;

    CacheEntry(): EntryLock(), frequency(0), key(0), data() {}
    CacheEntry(Key k, Payload p): EntryLock(), frequency(0), key(k), data(p) {}
    void reset() { frequency.store(0); }
    uint8_t getFrequency() { return frequency.load(); }
    void incrementFrequency() { frequency.fetch_add(1); }

    uint64_t getKey() { return key; }
    uint64_t* getKeyPtr() { return &key; }
    Payload getData(bool update_frequency=false) { 
        if (update_frequency)
            incrementFrequency();
        return data; 
    }

    Payload* getDataPtr(bool update_frequency=false) { 
        if (update_frequency)
            incrementFrequency();
        return &data; 
    }
};
} // namespace non_partitioned
#endif