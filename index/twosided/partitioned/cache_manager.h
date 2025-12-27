#pragma once

#include "system/global.h"
#include "utils/helper.h"
#include "utils/packetize.h"
#include "index/idx_wrapper.h"
#include "index/twosided/btreeolc/node.h"
#include "index/twosided/btreeolc/tree.h"
#include "index/twosided/partitioned/cache_entry.h"
#include <random>
#include <atomic>
#include <vector>
#include <queue>

#if PARTITIONED
namespace partitioned {

template <typename Value>
class cache_manager_t: public idx_wrapper_t<Value>{
public:
	using ValueType = CacheEntry<Value>;

	struct ThreadStruct {
		uint32_t slots;
		std::vector<std::pair<ValueType*, uint64_t>> free_set;
	};

	cache_manager_t(uint32_t index_id, uint64_t index_cache_size): index_id(index_id) {
		index_cache = nullptr;
		// free_slots.store(0);
		state.store(0);
		thread_data.resize(g_num_client_threads);
		page_offset.store(0);
		page_num = (index_cache_size * 1024 * 1024) / sizeof(ValueType);
		if (page_num > 0)
			index_cache = new BTree<ValueType*>();
		admission_rate = static_cast<uint64_t>(g_admission_rate * 100);
	}

	// index operations (dummy)
	void insert(Key k, Value v) { assert(false); }
	bool update(Key k, Value v) { assert(false); return false; }
	bool remove(Key k) { assert(false); return false; }
	bool lookup(Key k, Value& v) { assert(false); return false; }
	int scan(Key k, int range, Value*& v) { assert(false); return 0; }
		
	// cache operations (DEX)
	int search_cache(Key k, UnstructuredBuffer& buffer) { assert(false); return 0; }
	void set_bound(Key left, Key right) { from = left; to = right; }
	void set_shared(std::vector<Key>& bound) { }
	void bulk_load(Key* key, uint64_t num) { assert(false); }
	void set_rpc_ratio(double ratio) { }
	void get_newest_root() { }
	void reset_buffer_pool(bool flush_dirty) { }

	int insert(Key k, Value v, UnstructuredBuffer& evict){
		// if no cache or is building the cache just rpc
		if (!index_cache || !g_warmup_done)
			return 0;

		bool update_freq = update_frequency();
	restart:
		bool needRestart = false;
		ValueType* ptr = nullptr;
		bool found = index_cache->lookup(k, ptr);
		if (found) { // cache hit -- this is an update
			assert(ptr != nullptr);
			auto version = ptr->readLockOrRestart(needRestart);
			if (needRestart) goto restart;

			if (!ptr->validateKey(k)) goto restart;
			ptr->upgradeToWriteLockOrRestart(version, needRestart);
			if (needRestart) goto restart;

			ptr->update(v, update_freq);
			ptr->writeUnlock();
			return 1; // cache hit
		}

		bool write_back = get_slot(evict);
		auto slot = new ValueType(k, v, true);
		bool ret = index_cache->insert(k, slot);
		if (!ret) { // insert failed, another thread inserted the same key
			set_local_slot(slot);
			goto restart;
		}
		return write_back ? 2 : 3;
	}

	int update(Key k, Value v, UnstructuredBuffer& buffer) { 
		if (!index_cache) 
			return 0;

		bool update_freq = update_frequency();
	restart:
		bool needRestart = false;
		ValueType* ptr = nullptr;
		bool found = index_cache->lookup(k, ptr);
		if (found) { // cache hit
			assert(ptr != nullptr);
			auto version = ptr->readLockOrRestart(needRestart);
			if (needRestart) goto restart;

			if (!ptr->validateKey(k)) goto restart;
			ptr->upgradeToWriteLockOrRestart(version, needRestart);
			if (needRestart) goto restart;

			ptr->update(v, update_freq);
			ptr->writeUnlock();
			return 1; // cache hit
		}
		// cache miss
		bool admit = admit_to_cache();
		if (!admit)
			return 0; // no admission, just rpc

		bool write_back = get_slot(buffer);
		return write_back ? 2 : 3; 
	}

	int lookup(Key k, Value& v, UnstructuredBuffer& buffer){
		if (!index_cache)
			return 0;

		bool update_freq = update_frequency();
	restart:
		bool needRestart = false;
		ValueType* ptr = nullptr;
		bool found = index_cache->lookup(k, ptr);
		if (found) { // cache hit
			assert(ptr != nullptr);
			auto version = ptr->readLockOrRestart(needRestart);
			if (needRestart) goto restart;

			if (!ptr->validateKey(k)) goto restart;
			auto value = ptr->getData(update_freq);

			ptr->readUnlockOrRestart(version, needRestart);
			if (needRestart) {
				update_freq = false;
				goto restart;
			}

			v = value;
			return 1; // cache hit
		}
		// cache miss
		bool admit = admit_to_cache();
		if (!admit)
			return 0; // cache miss, but no cache admission (just rpc)

		bool write_back = get_slot(buffer);
		return write_back ? 2 : 3;
	}
		
	int remove(Key k, UnstructuredBuffer& buffer){
		if (!index_cache)
			return 0;

	restart:
		bool needRestart = false;
		ValueType* ptr = nullptr;
		bool found = index_cache->lookup(k, ptr);
		if (found) { // cache hit
			assert(ptr != nullptr);
			auto version = ptr->readLockOrRestart(needRestart);
			if (needRestart) goto restart;
			if (!ptr->validateKey(k)) goto restart;
			ptr->upgradeToIOLockOrRestart(version, needRestart);
			if (needRestart) goto restart;

			bool ret = index_cache->remove(k, ptr);
			assert(ret);
			buffer.put(&k);
			buffer.put(&ptr);
			return 1;
		}
		// cache miss
		return 0;
	}

	void add_to_cache(Key k, Value v){
		ValueType* temp = nullptr;
		auto slot = new ValueType(k, v);
		bool ret = index_cache->upsert(k, slot, temp);
		if (ret) // replaced an existing item
			// delete temp;
			set_local_slot(temp, false);
	}
	
	void invalidate(Key k, char* v){
		assert(index_cache);
		invalidate(reinterpret_cast<ValueType*>(v));
	}

private:
	bool evict(UnstructuredBuffer& evict_buffer) {
	restart:
	    bool needRestart = false;
		auto p1 = sample_page();
		auto p2 = sample_page();
		auto p = p1->getFrequency() < p2->getFrequency() ? p1 : p2;
		auto version = p->readLockOrRestart(needRestart);
		if (needRestart) goto restart;

		bool dirty = p->isDirty();
		Key key = p->getKey();
		Value value = p->getData(false);

		// p->readUnlockOrRestart(version, needRestart);
		// if (needRestart) goto restart;

		p->upgradeToIOLockOrRestart(version, needRestart);
		if (needRestart) goto restart;

		if (dirty) { // need a writeback
			evict_buffer.put(&key);
			evict_buffer.put(&value);
			evict_buffer.put(&p);
			return false;
		}
		else {
			bool ret = invalidate(p);
			assert(ret);
		}

		return true;		
	}

	ValueType* sample_page(){
	    static thread_local std::mt19937* gen = nullptr;
	    static thread_local std::uniform_int_distribution<Key> dist(from, to);
	    if(!gen) gen = new std::mt19937(asm_rdtsc() + pthread_self());
	restart:
		Key key = dist(*gen);
		ValueType* value = nullptr;
		bool valid = index_cache->lowerBound(key, value);
		if (!valid)
			goto restart;
		assert(value != nullptr);
		return value;
	}

	bool get_local_slot() {
		if (thread_data[GET_THD_ID].slots > 0) {
			thread_data[GET_THD_ID].slots--;
			return true;
		}
		return false;
	}

	bool invalidate(ValueType* v) {
		auto ret = index_cache->remove(v->getKey(), v);
		if (!ret) // the item has been already removed by other threads
			return false;
		set_local_slot(v);
		return true;
	}

	void set_local_slot(ValueType* p, bool inc=true) {
		free_slots();
		p->setObsoleteState();
		uint64_t cur_time = asm_rdtsc();
		thread_data[GET_THD_ID].free_set.push_back(std::make_pair(p, cur_time));
		if (inc) thread_data[GET_THD_ID].slots++;
	}

	void free_slots() {
		auto& local_free_set = thread_data[GET_THD_ID].free_set;
		if (local_free_set.size() == 0) return;

		int cur_idx = 0;
		uint64_t cur_time = asm_rdtsc();
		for (; cur_idx<local_free_set.size(); cur_idx++) {
			auto p = &local_free_set[cur_idx];
			if (cur_time - p->second < 3000000ull)
				break;
			delete p->first;
		}

		if (cur_idx > 0)
			local_free_set.erase(local_free_set.begin(), local_free_set.begin() + cur_idx);
	}

	bool get_slot(UnstructuredBuffer& buffer){
		bool write_back = false;
		bool alloc = false;
		do {
			if (state.load() == 1) { // dynamic phase
				if (!get_local_slot()) {
					if (!evict(buffer))
						write_back = true; // evicting a dirty entry, need writeback
				}
				alloc = true;
			}
			else { // warmup phase
				auto cur_page_offset = page_offset.load();
				if (cur_page_offset < page_num) {
					auto cur_idx = page_offset.fetch_add(1);
					if (cur_idx >= page_num)
						continue;
					else {
						alloc = true;
						if (cur_idx == page_num - 1) {
							std::cout << "entering dynamic phase" << std::endl;
							state.store(1);
						}
					}
				}
			}
		} while (!alloc);
	    return write_back;
	}

	bool admit_to_cache() { 
		static thread_local std::mt19937* gen = nullptr;
		static thread_local std::uniform_int_distribution<uint64_t>* dist = nullptr;
		if (!gen && !dist) {
			gen = new std::mt19937(asm_rdtsc() + pthread_self());
			dist = new std::uniform_int_distribution<uint64_t>(0, 100);
		}
		uint64_t prob = (*dist)(*gen);
		return prob < admission_rate;
	}

	bool update_frequency() {
		static thread_local std::mt19937* gen = nullptr;
		static thread_local std::uniform_int_distribution<uint64_t>* dist = nullptr;
		if (!gen && !dist) {
			gen = new std::mt19937(asm_rdtsc() + pthread_self());
			dist = new std::uniform_int_distribution<uint64_t>(0, 100);
		}
		uint64_t prob = (*dist)(*gen);
		return prob < 10;
	}

	BTree<ValueType*>* index_cache;
	uint64_t admission_rate;
	uint32_t index_id; 
	uint64_t page_num;
	std::atomic<uint64_t> page_offset;
	Key from, to;
	std::atomic<int> state; // 0: warmup, 1: dynamic

	std::vector<ThreadStruct> thread_data;
};
} // namespace partitioned
#endif