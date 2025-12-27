#pragma once

#include "system/global.h"
#include "utils/helper.h"
#include "utils/packetize.h"
#include "index/idx_wrapper.h"
#include "index/twosided/btreeolc/node.h"
#include "index/twosided/btreeolc/tree.h"
#include "index/twosided/non_partitioned/cache_entry.h"
#if !PARTITIONED
namespace nonpartitioned{

template <typename Value>
class cache_manager_t: public idx_wrapper_t<Value>{
public:
	using ValueType = CacheEntry<Value>;

	struct ThreadStruct {
		uint32_t slots = 0;
		std::vector<std::pair<ValueType*, uint64_t>> free_set;
	};

	cache_manager_t(uint32_t index_id, uint64_t index_cache_size): _index_id(index_id) {
		index_cache = nullptr;
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

	// cache operations
	int search_cache(Key k, UnstructuredBuffer& entry) {
		if (index_cache == nullptr) return 0; // no hint

		uint32_t repeat = 0;
		bool update_freq = update_frequency();
	restart:
		if (repeat++ > 10000000) {
			std::cout << "cache_manager_t::search_cache too many restarts" << std::endl;
			assert(false);
		}
		bool needRestart = false;
		ValueType* ptr = nullptr;
		bool found = index_cache->lookup(k, ptr);
		if (found) { // cache hit
			assert(ptr != nullptr);

			auto version = ptr->readLockOrRestart(needRestart);
			if (needRestart) goto restart;

			auto value = ptr->getData(update_freq);

			ptr->readUnlockOrRestart(version, needRestart);
			if (needRestart) {
				update_freq = false;
				goto restart;
			}

			// entry.put(&ptr);
			entry.put(&value);
			return 2; // cache hit
		}
		// cache miss
		bool admit = admit_to_cache();
		if (!admit)
			return 0; // cache miss, but no cache admission
		return 1; // cache miss, do admit the cache
	}

	void invalidate(char* entry) {
		assert(index_cache);
		invalidate(reinterpret_cast<ValueType*>(entry));
	}

	void add_to_cache(Key k, char* ptr) {
		assert(index_cache);
		get_slot();
		ValueType* temp = nullptr;
		Value v(reinterpret_cast<row_t*>(ptr));
		auto slot = new ValueType(k, v);
		bool ret = index_cache->upsert(k, slot, temp);
		if (ret) // replaced an existing item
			set_local_slot(temp, false);
	}

private:
	void evict() {
	restart:
		auto p1 = sample_page();
		auto p2 = sample_page();
		auto p = p1->getFrequency() < p2->getFrequency() ? p1 : p2;
		if (!invalidate(p))
			goto restart;
	}

	bool invalidate(ValueType* v) {
		auto ret = index_cache->remove(v->getKey(), v);
		if (!ret) // the item has been already removed by other threads
			return false;
		set_local_slot(v);
		return true;
	}

	ValueType* sample_page(){
	    static thread_local std::mt19937* gen = nullptr;
	    static thread_local std::uniform_int_distribution<Key>* dist = nullptr;
		if (!gen && !dist) {
			#if WORKLOAD == YCSB
			from = GET_WORKLOAD->get_partition_by_node(0, _index_id);
			to = g_synth_table_size + 1;
			#else // TPCC
			from = GET_WORKLOAD->get_partition_by_node(0, _index_id);
			to = GET_WORKLOAD->get_partition_by_node(g_num_nodes - 1, _index_id) + 1;
			#endif
			assert(from < to);
			dist = new std::uniform_int_distribution<Key>(from, to);
			gen = new std::mt19937(asm_rdtsc() + pthread_self());
		}
	restart:
	    Key key = (*dist)(*gen);
		ValueType* value = nullptr;
		bool valid = index_cache->lowerBound(key, value);
	    if(!valid)
			goto restart;
		assert(value != nullptr);
		return value;
	}

	bool get_local_slot() {
		auto tid = GET_THD_ID;
		if (thread_data[tid].slots > 0) {
			thread_data[tid].slots--;
			return true;
		}
		return false;
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

	void get_slot() {
		bool alloc = false;
		do {
			if (state.load() == 1) { // dynamic phase
				if (!get_local_slot())
					evict();
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
	uint32_t _index_id;
	uint64_t page_num;
	std::atomic<uint64_t> page_offset;
	Key from, to;
	std::atomic<int> state; // 0: warmup phase, 1: dynamic phase

	std::vector<ThreadStruct> thread_data;
};

} // namespace non-partitioned
#endif // !PARTITIONED