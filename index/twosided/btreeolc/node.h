#pragma once

#include "system/global.h"
#include <atomic>
#include <cstring>
#include <iostream>
#include <immintrin.h>
#include <limits>
#include <sched.h>
#include <utility>
#include <functional>

enum class PageType: uint8_t { BTreeInner=1, BTreeLeaf=2 };

static const uint64_t pageSize = 1024;

struct OptLock{
    std::atomic<uint64_t> typeVersionLockObsolete{0};

    bool isLocked(uint64_t version){
		return ((version & 0b100) == 0b100);
    }

    bool isObsolete(uint64_t version){
		return ((version & 1) == 1);
    }

	void setObsoleteState() { typeVersionLockObsolete.fetch_add(1); }

    uint64_t readLockOrRestart(bool& needRestart){
		uint64_t version = typeVersionLockObsolete.load();
		if(isLocked(version) || isObsolete(version)){
			_mm_pause();
			needRestart = true;
		}
		return version;
    }

    bool readLockOrRestart(uint64_t& _version){
		uint64_t version = typeVersionLockObsolete.load();
		if(isLocked(version) || isObsolete(version)){
			_mm_pause();
			return false;
		}
		_version = version;
		return true;
    }

    void writeLockOrRestart(bool& needRestart){
		uint64_t version = readLockOrRestart(needRestart);
		if(needRestart) return;

		upgradeToWriteLockOrRestart(version, needRestart);
    }

    void upgradeToWriteLockOrRestart(uint64_t& version, bool& needRestart){
		if(typeVersionLockObsolete.compare_exchange_strong(version, version + 0b100)){
			version += 0b100;
		}
		else{
			_mm_pause();
			needRestart = true;
		}
    }
    
    bool upgradeToWriteLockOrRestart(uint64_t& version){
		if(typeVersionLockObsolete.compare_exchange_strong(version, version + 0b100)){
			version += 0b100;
			return true;
		}
		else{
			_mm_pause();
			return false;
		}
    }

    void writeUnlock(){
		typeVersionLockObsolete.fetch_add(0b100);
    }

    void writeUnlockObsolete(){
		typeVersionLockObsolete.fetch_add(0b101);
    }

    void readUnlockOrRestart(uint64_t startRead, bool& needRestart) const{
		needRestart = (startRead != typeVersionLockObsolete.load());
    }

    void checkOrRestart(uint64_t startRead, bool& needRestart) const{
		readUnlockOrRestart(startRead, needRestart);
    }

	void reset() {
		typeVersionLockObsolete.store(0);
	}
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

template <class Payload>
struct BTreeLeaf: public BTreeLeafBase{
    // This is the element type of the leaf node
    using KeyValueType = std::pair<Key, Payload>;
#if !PARTITIONED
    static const uint64_t maxEntries = (pageSize - sizeof(BTreeLeafBase) - sizeof(BTreeLeaf*) - sizeof(uint64_t) - sizeof(Key)*2) / sizeof(KeyValueType);
#else
    static const uint64_t maxEntries = (pageSize - sizeof(BTreeLeafBase) - sizeof(BTreeLeaf*)) / sizeof(KeyValueType);
#endif

#if !PARTITIONED
    Key min_key;
    Key max_key;
	uint64_t index_cache_freq;
#endif
    BTreeLeaf* sibling_ptr;
    // This is the array that we perform search on
    KeyValueType data[maxEntries];
	   

    BTreeLeaf(): BTreeLeafBase(), sibling_ptr(nullptr) {
		#if !PARTITIONED
		min_key = std::numeric_limits<Key>::min();
		max_key = std::numeric_limits<Key>::max();
		index_cache_freq = 0;
		#endif
	}

    BTreeLeaf(BTreeLeaf* _sibling_ptr): BTreeLeafBase(), sibling_ptr(_sibling_ptr) {
		#if !PARTITIONED
		min_key = std::numeric_limits<Key>::min();
		max_key = std::numeric_limits<Key>::max();
		index_cache_freq = 0;
		#endif
	}

    bool isFull() { return count == maxEntries; }

	unsigned lowerBoundExist(Key k) {
		return linearSearchExist(k);
	}

    unsigned lowerBound(Key k){
		return linearSearch(k);
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
    }

	unsigned linearSearchExist(Key k){
		for(unsigned i=0; i<count; i++){
			if(k <= data[i].first)
			return i;
		}
		return -1;
    }

	Key getMaxKey() {
		#if !PARTITIONED
		return max_key;
		#else
		return data[count - 1].first;
		#endif
	}

    bool insert(Key k, Payload p){
		assert(count < maxEntries);
		if(count){
			unsigned pos = lowerBound(k);
			if((pos < count) && (data[pos].first == k)){ // update
				data[pos].second = p;
				return false;
			}

			memmove(data+pos+1, data+pos, sizeof(KeyValueType)*(count-pos));
			data[pos].first = k;
			data[pos].second = p;
		}
		else{
			data[0].first = k;
			data[0].second = p;
		}

		count++;
		return true;
    }

    bool update(Key k, Payload p){
		if(count){
			unsigned pos = lowerBound(k);
			if((pos < count) && (data[pos].first == k)){
				data[pos].second = p;
				return true;
			}
		}
		return false;
    }

	bool upsert(Key k, Payload p, Payload& old_p){
		assert(count < maxEntries);
		if(count){
			unsigned pos = lowerBound(k);
			if((pos < count) && (data[pos].first == k)){ // update
				old_p = data[pos].second;
				data[pos].second = p;
				return true; // indicate update
			}

			memmove(data+pos+1, data+pos, sizeof(KeyValueType)*(count-pos));
			data[pos].first = k;
			data[pos].second = p;
		}
		else{
			data[0].first = k;
			data[0].second = p;
		}

		count++;
		return false; // indicate insert
    }

	bool remove(Key k, Payload p) {
		if(count){
			unsigned pos = lowerBound(k);
			if((pos < count) && (data[pos].first == k)){
				if (data[pos].second == p) {
					memmove(data+pos, data+pos+1, sizeof(KeyValueType)*(count-pos-1));
					count--;
					return true;
				}
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
		#if !PARTITIONED
		newLeaf->min_key = sep;
		newLeaf->max_key = max_key;
		max_key = sep;
		#endif
		return newLeaf;
    }

    bool find(Key k, Payload& v){
		unsigned pos = lowerBound(k);
		if(pos < count && data[pos].first == k){
			v = data[pos].second;
			return true;
		}
		return false;
    }

    int scan(Key k, int num, Payload*& v, int cnt){
		unsigned pos = lowerBound(k);
		if(pos < count && k <= data[pos].first){
			for(int i=pos; i<count; i++){
				v[cnt++] = data[i].second;
				if(cnt == num)
					return i-pos+1;
			}
			return count-pos+1;
		}
		return -1;
    }

    bool rangeValid(Key k){
		#if !PARTITIONED
		if(min_key < k && k <= max_key)
			return true;
		return false;
		#else
		assert(false);
		return true;
		#endif
    }

	void sanity_check(Key k) {
		bool valid = false;
		for(int i=0; i<count-1; i++){
			if (data[i].first == k) {
				valid = true;
				break;
			}
		}

		if (!valid) {
			for (int i=0; i<count-1; i++)
				std::cout << "[" << i << "] key (" << data[i].first << "), value (" << data[i].second << ")\n";
			assert(false);
		}
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