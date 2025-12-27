#pragma once

// #include "index/twosided/partitioned/cache_node.h"

#if PARTITIONED
namespace partitioned {

struct EntryLock {
	EntryLock() { lock.store(0); }

	std::atomic<uint8_t> lock;

	bool isLocked() { return isLocked(lock.load()); }
	bool isLocked(uint8_t version){ return ((version & 0b100) == 0b100); }
    bool isObsolete(uint8_t version){ return ((version & 0b10) == 0b10); }
    bool isIO(uint8_t version){ return ((version & 1) == 1); }
	bool isIO() { return isIO(lock.load()); }
	bool isObsoleteState() { return isObsolete(lock.load()); }

	void setObsoleteState(){ lock.store(0b10); }
	void setIOState() { lock.store(1); }

    uint8_t readLockOrRestart(bool& needRestart){
		uint8_t version = lock.load();
		if(isLocked(version) || isObsolete(version)){
			_mm_pause();
			needRestart = true;
		}
		return version;
    }

    void writeLockOrRestart(bool& needRestart){
		uint8_t version = lock.load();
		if ((version & 0b111) != 0){
			needRestart = true;
			return;
		}
	
		upgradeToWriteLockOrRestart(version, needRestart);
    }

    void IOLockOrRestart(bool& needRestart){
		uint8_t version = lock.load();
		if ((version & 0b111) != 0){
			needRestart = true;
			return;
		}

		upgradeToIOLockOrRestart(version, needRestart);
    }

    void upgradeToIOLockOrRestart(uint8_t& version, bool& needRestart){
		if ((version & 0b111) != 0){
			needRestart = true;
			return;
		}

		if(lock.compare_exchange_strong(version, version + 1))
			version = version + 1;
		else
			needRestart = true;
    }
    
    void IOUpgradeToWriteLockOrRestart(){
		uint8_t version = lock.load();
		assert(isIO(version));
		lock.store(version - 1 + 0b100);
    }
	
	void IOUpgradeToObsolete(){
		uint8_t version = lock.load();
		assert(isIO(version));
		lock.fetch_add(0b1);
	}

    void upgradeToWriteLockOrRestart(uint8_t& version, bool& needRestart){
		if(isIO(version)){
			needRestart = true;
			return;
		}

		if(lock.compare_exchange_strong(version, version + 0b100))
			version += 0b100;
		else
			needRestart = true;
    }

    void writeUnlock(){ lock.fetch_add(0b100); }
    void writeUnlockObsolete(){ lock.fetch_add(0b110); }
    void readUnlockOrRestart(uint8_t startRead, bool& needRestart) const{ needRestart = ((startRead | 1) != (lock.load() | 1)); }
    void checkOrRestart(uint8_t startRead, bool& needRestart) const{ readUnlockOrRestart(startRead, needRestart); }
    void IOUnlock(){ lock.fetch_sub(1); }
	void resetLock() { lock.store(0); }
};

template <typename Payload>
struct CacheEntry: public EntryLock {
	bool dirty;
	std::atomic<uint16_t> frequency;
	uint64_t key;
	Payload data;

	CacheEntry(): EntryLock(), dirty(false){ lock.store(0); frequency.store(0); }
	CacheEntry(uint64_t k): dirty(false), key(k) { lock.store(0b100); frequency.store(0); }
	CacheEntry(uint64_t k, Payload p): dirty(false), key(k), data(p) { lock.store(0); frequency.store(0); }
	CacheEntry(uint64_t k, Payload p, bool dirty): dirty(dirty), key(k), data(p) { lock.store(0); frequency.store(0); }
	
	bool isDirty() { return dirty; }
	void setDirty() { dirty = true; }
	void unsetDirty() { dirty = false; }
	void reset() { dirty = false; frequency.store(0); resetLock(); }
	uint8_t getFrequency() { return frequency.load(); }
	void incrementFrequency() { frequency.fetch_add(1); }

	bool validateKey(uint64_t k) { return (key == k); }
	uint64_t getKey() { return key; }
	uint64_t* getKeyPtr() { return &key; }

	Payload getData(bool update_frequency=false) { 
		if (update_frequency) 
			incrementFrequency(); 
		return data; 
	}
	Payload* getDataPtr() { return &data; }

	void update(Payload p, bool update_frequency=false) {
		data = p;
		dirty = true;
		if (update_frequency)
			incrementFrequency();
	}

	Payload get(bool update_frequency=false) {
		if (update_frequency)
			incrementFrequency();
		return data;
	}
};

} // namespace partitioned
#endif