#pragma once

#include <atomic>
#include <cassert>
#include <cstring>
#include <iostream>
#include <immintrin.h>
#include <limits>
#include <sched.h>
#include <utility>
#include <functional>
#include "system/global.h"

#if PARTITIONED
namespace partitioned{

enum class PageType: uint8_t { BTreeInner=1, BTreeLeaf=2 };

// static const uint64_t pageSize = 512;
static const uint64_t pageSize = 1024;

struct OptLock{
	OptLock() { typeVersionLockObsolete.store(0); }
    std::atomic<uint64_t> typeVersionLockObsolete{0};

    bool isLocked(uint64_t version){
		return ((version & 0b100) == 0b100);
    }

    bool isObsolete(uint64_t version){
		return ((version & 0b10) == 0b10);
    }

	void setObsoleteState(){ typeVersionLockObsolete.store(0b10); }

    uint64_t readLockOrRestart(bool& needRestart){
		uint64_t version = typeVersionLockObsolete.load();
		if(isLocked(version) || isObsolete(version)){
			_mm_pause();
			needRestart = true;
		}
		return version;
    }

    void writeLockOrRestart(bool& needRestart){
		uint64_t version = typeVersionLockObsolete.load();
		if (isLocked(version) || isObsolete(version)){
			//_mm_pause();
			needRestart = true;
			return;
		}
	
		upgradeToWriteLockOrRestart(version, needRestart);
    }

    void upgradeToWriteLockOrRestart(uint64_t& version, bool& needRestart){
		if(typeVersionLockObsolete.compare_exchange_strong(version, version + 0b100))
			version += 0b100;
		else{
			//_mm_pause();
			needRestart = true;
		}
    }
    
    void writeUnlock(){
		typeVersionLockObsolete.fetch_add(0b100);
    }

    void writeUnlockObsolete(){
		typeVersionLockObsolete.fetch_add(0b110);
    }

    void readUnlockOrRestart(uint64_t startRead, bool& needRestart) const{
		needRestart = (startRead != typeVersionLockObsolete.load());
	}

    void checkOrRestart(uint64_t startRead, bool& needRestart) const{
		readUnlockOrRestart(startRead, needRestart);
    }

	void resetLock() { typeVersionLockObsolete.store(0); }
};

struct NodeBase: public OptLock{
    PageType type;
    uint8_t count;

	NodeBase(PageType t): type(t), count(0) { }
};

struct BTreeLeafBase: public NodeBase{
    static const PageType typeMarker = PageType::BTreeLeaf;

	BTreeLeafBase(): NodeBase(typeMarker) { }
};

struct EntryLock {
	std::atomic<uint8_t> entry_lock;

	EntryLock() { entry_lock.store(0); }

	bool isLocked() { return isLocked(entry_lock.load()); }
	bool isLocked(uint8_t l){ return (l & 0b100) == 0b100; }
	bool isObsolete(uint8_t l){ return (l & 0b10) == 0b10;  }
	bool isIO(uint8_t l){ return (l & 1) == 1; }
	void setObsoleteState(){ entry_lock.store(0b10); }
	void setIOState(){ entry_lock.store(1); }
	void setLockState(){ entry_lock.store(0b100); }

	uint8_t readLockOrRestart(bool& needRestart){
		uint8_t version = entry_lock.load();
		if(isLocked(version) || isObsolete(version)){
			_mm_pause();
			needRestart = true;
		}
		return version;
	}

	void IOLockOrRestart(bool& needRestart){
		uint8_t version = entry_lock.load();
		if (isLocked(version) || isObsolete(version) || isIO(version)){
			//_mm_pause();
			needRestart = true;
			return;
		}
	
		upgradeToIOLockOrRestart(version, needRestart);
	}

	void writeLockOrRestart(bool& needRestart){
		uint8_t version = entry_lock.load();
		if (isLocked(version) || isObsolete(version) || isIO(version)){
			//_mm_pause();
			needRestart = true;
			return;
		}
	
		upgradeToWriteLockOrRestart(version, needRestart);
	}

	void upgradeToWriteLockOrRestart(uint8_t& version, bool& needRestart){
		if(entry_lock.compare_exchange_strong(version, version + 0b100))
			version += 0b100;
		else
			needRestart = true;
	}

	void upgradeToIOLockOrRestart(uint8_t& version, bool& needRestart){
		if(entry_lock.compare_exchange_strong(version, version + 1))
			version = version + 1;
		else
			needRestart = true;
	}

	void writeUnlock(){ entry_lock.fetch_add(0b100); }
	void readUnlockOrRestart(uint8_t startRead, bool& needRestart) const{ needRestart = ((startRead | 1) != (entry_lock.load() | 1)); }
	void IOUnlock(){ entry_lock.fetch_sub(1); }
	void resetLock() { entry_lock.store(0); }
};

template <typename Payload>
struct CacheEntry: public EntryLock {
	bool dirty;
	std::atomic<uint16_t> frequency;
	Payload data;

	CacheEntry(): EntryLock(), dirty(false), frequency(0) { }
	CacheEntry(Payload p): EntryLock(), dirty(false), frequency(0), data(p) { }
	CacheEntry(bool reserve): dirty(false), frequency(0) {
		assert(reserve);
		setLockState();
	}

	void operator=(const CacheEntry& other) {
		dirty = other.dirty;
		frequency.store(other.frequency.load());
		data = other.data;
	}

	Payload get(bool update_freq=false) {
		if (update_freq)
			frequency.fetch_add(1);
		return data;
	}

	Payload* getPtr(bool update_freq=false) { 
		if (update_freq)
			frequency.fetch_add(1);
		return &data; 
	}

	uint16_t getFrequency() {
		return frequency.load();
	}

	bool isDirty() {
		return dirty;
	}

	void insert(Payload p) {
		data = p;
	}

	void update(Payload p, bool update_freq=false) {
		data = p;
		dirty = true;
		if (update_freq)
			frequency.fetch_add(1);
	}
};

template <class Payload>
struct BTreeLeaf: public BTreeLeafBase{
    // This is the element type of the leaf node
    using KeyValueType = std::pair<Key, CacheEntry<Payload>>;
    static const uint64_t maxEntries = (pageSize - sizeof(BTreeLeafBase) - sizeof(BTreeLeaf*)) / sizeof(KeyValueType);

    BTreeLeaf* sibling_ptr;
    // This is the array that we perform search on
    KeyValueType data[maxEntries];
	   

    BTreeLeaf(): BTreeLeafBase(), sibling_ptr(nullptr) { }

    BTreeLeaf(BTreeLeaf* _sibling_ptr): BTreeLeafBase(), sibling_ptr(_sibling_ptr) { }

    bool isFull() { return count == maxEntries; }

    unsigned lowerBound(Key k){
		#ifdef BINARY_SEARCH
		return binarySearch(k);
		#else
		return linearSearch(k);
		#endif
    }

    bool lowerBound(Key& k, Payload& v){
		if(count == 0)
			return false;
		#ifdef BINARY_SEARCH
		unsigned idx = binarySearch(k);
		#else
		unsigned idx = linearSearch(k);
		#endif
		if(idx < count) {
			k = data[idx].first;
			v = data[idx].second;
			return true;
		}
		return false;
    }

    unsigned binarySearch(Key k){
		unsigned lower = 0;
		unsigned upper = count;
		do{
			unsigned mid = ((upper - lower) / 2) + lower;
			// This is the key at the pivot position
			const Key& middle_key = data[mid].first;

			if(k < middle_key){
				upper = mid;
			}
			else if(k > middle_key){
				lower = mid + 1;
			}
			else{
				return mid;
			}
		} while(lower < upper);
		return lower;
    }

    unsigned linearSearch(Key k){
		for(unsigned i=0; i<count; i++){
			if(k <= data[i].first)
			return i;
		}
		return count;
		//return count-1;
    }

	bool reserve(Key k) {
		assert(count < maxEntries);
		if(count){
			unsigned pos = lowerBound(k);
			if((pos < count) && (data[pos].first == k)){ // update
				//data[pos].second = p;
				return false;
			}

			memmove(data+pos+1, data+pos, sizeof(KeyValueType)*(count-pos));
			data[pos].first = k;
			data[pos].second = CacheEntry<Payload>(true);
		}
		else{
			data[0].first = k;
			data[0].second = CacheEntry<Payload>(true);
		}

		count++;
		return true;
	}

    bool insert(Key k, Payload p) {
		assert(count < maxEntries);
		if(count){
			unsigned pos = lowerBound(k);
			if((pos < count) && (data[pos].first == k)){ // update
				//data[pos].second = p;
				return false;
			}

			memmove(data+pos+1, data+pos, sizeof(KeyValueType)*(count-pos));
			data[pos].first = k;
			data[pos].second = CacheEntry<Payload>(p);
		}
		else{
			data[0].first = k;
			data[0].second = CacheEntry<Payload>(p);
		}

		count++;
		return true;
    }

    bool update(Key k, Payload p, bool update_freq=false){
		if(count){
			unsigned pos = lowerBound(k);
			if((pos < count) && (data[pos].first == k)){
				data[pos].second.update(p, update_freq);
				return true;
			}
		}
		return false;
    }

    bool remove(Key k){
		if(count){
			unsigned pos = lowerBound(k);
			if((pos < count) && (data[pos].first == k)){
				memmove(data+pos, data+pos+1, sizeof(KeyValueType)*(count-pos-1));
				count--;
				return true;
			}
		}
		return false;
    }

    BTreeLeaf* split(Key& sep){
		BTreeLeaf* newLeaf = new BTreeLeaf(sibling_ptr);
		int newcnt = count - (count/2);
		newLeaf->count = newcnt;
		count = count - newcnt;
		memcpy(newLeaf->data, data+count, sizeof(KeyValueType)*newcnt);
		sep = data[count-1].first;
		sibling_ptr = newLeaf;
		return newLeaf;
    }

    bool find(Key k, Payload& v, bool update_freq=false){
		unsigned pos = lowerBound(k);
		if(pos < count && data[pos].first == k){
			v = data[pos].second.get(update_freq);
			return true;
		}
		return false;
    }

    bool sanity_check(){
		for(int i=0; i<count-1; i++){
			Key k = data[i].first;
			for(int j=i+1; j<count; j++){
				if(data[j].first <= k){
					debug::notify_error("[sanity_check] data[%d].key %lu is larger than data[%d].key %lu", i, k, j, data[j].first);
					return false;
				}
			}
		}
		return true;
    }
};

struct BTreeInnerBase: public NodeBase{
    static const PageType typeMarker = PageType::BTreeInner;

	BTreeInnerBase(): NodeBase(typeMarker) { }
};

struct BTreeInner: public BTreeInnerBase{
    using KeyValueType = std::pair<Key, NodeBase*>;
    static const uint64_t maxEntries = (pageSize - sizeof(BTreeInnerBase)) / sizeof(KeyValueType);
    KeyValueType data[maxEntries];

    BTreeInner(): BTreeInnerBase() { }

    bool isFull(){ return count == (maxEntries-1); }

    unsigned lowerBound(Key k){
		#ifdef BINARY_SEARCH
		return binarySearch(k);
		#else
		return linearSearch(k);
		#endif
    }

    unsigned binarySearch(Key k){
		unsigned lower = 0;
		unsigned upper = count;
		do{
			unsigned mid = ((upper - lower) / 2) + lower;
			if(k < data[mid].first){
				upper = mid;
			}
			else if(k > data[mid].first){
				lower = mid + 1;
			}
			else{
				return mid;
			}
		} while(lower < upper);
		return lower;
    }

    unsigned linearSearch(Key k){
		for(unsigned i=0; i<count; i++){
			if(k <= data[i].first){
			return i;
			}
		}
		return count;
    }

    void insert(Key k, NodeBase* child){
		assert(count < maxEntries - 1);
		unsigned pos = lowerBound(k);
		memmove(data+pos+1, data+pos, sizeof(KeyValueType)*(count-pos+1));
		data[pos].first = k;
		data[pos].second = child;
		std::swap(data[pos].second, data[pos+1].second);
		count++;
    }

    BTreeInner* split(Key& sep){
		BTreeInner* newInner = new BTreeInner();
		int newcnt = count - (count / 2);
		newInner->count = newcnt;
		count = count - newcnt - 1;
		sep = data[count].first;
		memcpy(newInner->data, data+count+1, sizeof(KeyValueType)*(newInner->count+1));
		return newInner;
    }
};

} // namespace partitioned
#endif