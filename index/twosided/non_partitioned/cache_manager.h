#pragma once

#include "system/global.h"
#include "utils/helper.h"
#include "utils/packetize.h"
#include "index/idx_wrapper.h"
#include "index/twosided/btreeolc/node.h"
#include "index/twosided/btreeolc/tree.h"
#include "index/twosided/non_partitioned/cache_entry.h"
#include <mutex>
#include <random>
#include <limits>
#include <atomic>
#if !PARTITIONED
namespace nonpartitioned{

template <typename Value>
class cache_manager_t: public idx_wrapper_t<Value>{
public:
	using ValueType = CacheEntry<Value>;

	struct ThreadStruct {
		uint32_t slots = 0;
		std::vector<std::pair<ValueType*, uint64_t>> free_set;
		// eviction sampler; lives here (per instance, per thread) so each
		// index samples its own key range — a static thread_local in
		// sample_page() would be shared across all cache instances
		std::uniform_int_distribution<Key> dist;
		bool dist_init = false;
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

			entry.put(&ptr);   // CacheEntry*, kept client-side for stale invalidation
			entry.put(&value); // cached row ptr, sent to the server as the hint
			INC_INT_STATS(num_cache_hits, 1);
			return 2; // cache hit
		}
		// cache miss
		INC_INT_STATS(num_cache_misses, 1);
		bool admit = admit_to_cache();
		if (!admit)
			return 0; // cache miss, but no cache admission
		return 1; // cache miss, do admit the cache
	}

	void invalidate(Key k, char* entry) {
		assert(index_cache);
		// entry may already be freed (the response RTT can outlive the
		// reclamation grace), so it is only used as remove()'s compare value;
		// a successful remove proves it was still live and we own its reclamation
		auto v = reinterpret_cast<ValueType*>(entry);
		if (index_cache->remove(k, v))
			set_local_slot(v);
	}

	void add_to_cache(Key k, char* ptr) {
		assert(index_cache);
		get_slot();
		ValueType* temp = nullptr;
		Value v(reinterpret_cast<row_t*>(ptr));
		auto slot = new ValueType(k, v);
#if WORKLOAD == TPCC
		note_key(k);
#endif
		bool ret = index_cache->upsert(k, slot, temp);
		if (ret) // replaced an existing item: net entries unchanged, so
			set_local_slot(temp); // refund the budget unit get_slot consumed
	}

private:
	void evict() {
	restart:
		auto p1 = sample_page();
		auto p2 = sample_page();
		auto p = p1->getFrequency() < p2->getFrequency() ? p1 : p2;
		if (!invalidate(p))
			goto restart;
		INC_INT_STATS(num_cache_evictions, 1);
	}

	bool invalidate(ValueType* v) {
		auto ret = index_cache->remove(v->getKey(), v);
		if (!ret) // the item has been already removed by other threads
			return false;
		set_local_slot(v);
		return true;
	}

	ValueType* sample_page(){
		static thread_local std::mt19937 gen(asm_rdtsc() + pthread_self());
#if WORKLOAD == TPCC
		// Sample among the keys this cache has held. The partition-based range used for YCSB
		// does not work here: TPC-C routes by warehouse (the key-range partitions are not the
		// key space of each index), and _partitions is indexed by table id, not index id, so
		// e.g. the customer index got HISTORY's empty partition list. When the sampled range
		// lies above every cached key, lowerBound never succeeds and eviction spins forever.
		{
			uint32_t repeat = 0;
		restart_tpcc:
			if (repeat++ > 10000000) {
				std::cout << "cache_manager_t::sample_page too many restarts" << std::endl;
				assert(false);
			}
			Key lo = _min_key.load(std::memory_order_relaxed);
			Key hi = _max_key.load(std::memory_order_relaxed);
			assert(lo <= hi);
			Key key = std::uniform_int_distribution<Key>(lo, hi)(gen);
			ValueType* value = nullptr;
			if (!index_cache->lowerBound(key, value)) // only if the largest keys were evicted
				goto restart_tpcc;
			assert(value != nullptr);
			return value;
		}
#endif
		auto& td = thread_data[GET_THD_ID];
		if (!td.dist_init) {
			// from/to are set once per instance; call_once also orders the
			// stores for threads that lose the race
			std::call_once(range_flag, [this]() {
				#if WORKLOAD == YCSB
				from = GET_WORKLOAD->get_partition_by_node(0, _index_id);
				to = g_synth_table_size + 1;
				#else // TPCC
				from = GET_WORKLOAD->get_partition_by_node(0, _index_id);
				to = GET_WORKLOAD->get_partition_by_node(g_num_nodes - 1, _index_id) + 1;
				#endif
				assert(from < to);
			});
			td.dist = std::uniform_int_distribution<Key>(from, to);
			td.dist_init = true;
		}
		uint32_t repeat = 0;
	restart:
		if (repeat++ > 10000000) {
			std::cout << "cache_manager_t::sample_page too many restarts" << std::endl;
			assert(false);
		}
	    Key key = td.dist(gen);
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
				// evict() refunds the victim's budget unit as a thread-local
				// credit; loop so this admission consumes it (credits + entries
				// must stay == page_num)
				while (!get_local_slot())
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
	std::once_flag range_flag;
#if WORKLOAD == TPCC
	std::atomic<Key> _min_key{std::numeric_limits<Key>::max()};
	std::atomic<Key> _max_key{0};
	void note_key(Key k) {
		Key cur = _min_key.load(std::memory_order_relaxed);
		while (k < cur && !_min_key.compare_exchange_weak(cur, k, std::memory_order_relaxed)) { }
		cur = _max_key.load(std::memory_order_relaxed);
		while (k > cur && !_max_key.compare_exchange_weak(cur, k, std::memory_order_relaxed)) { }
	}
#endif
	std::atomic<int> state; // 0: warmup phase, 1: dynamic phase

	std::vector<ThreadStruct> thread_data;
};

} // namespace non-partitioned
#endif // !PARTITIONED