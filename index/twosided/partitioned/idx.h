#pragma once

#include "system/global.h"
#include "index/idx_wrapper.h"
#include "index/twosided/btreeolc/node.h"
#include "index/twosided/btreeolc/tree.h"

#if PARTITIONED
namespace partitioned{

template <typename Value>
class idx_t: public idx_wrapper_t<Value>{
public:
	idx_t(uint32_t index_id): _index_id(index_id) { index = new BTree<Value>(); }

	// index operations
	void insert(Key k, Value v){ index->insert(k, v); }
	bool update(Key k, Value v){ return index->update(k, v); }
	bool remove(Key k){ return index->remove(k); }
	bool lookup(Key k, Value& v){ return index->lookup(k, v); }
	int scan(Key k, int num, Value*& v) { return index->scan(k, num, v); }

	void bulk_load(Key* key, uint64_t num) {
	    for (uint64_t i = 0; i < num; ++i) {
	        insert(key[i], key[i]);
	    }
	}

	// DEX functions (dummy)
	void set_bound(Key left, Key right) { }
	void set_shared(std::vector<Key>& bound) { }
	void set_rpc_ratio(double ratio) { }
	void reset_buffer_pool(bool flush_dirty) { }
	void get_newest_root() { }
	// cache operations (dummy)
	int insert(Key k, Value v, UnstructuredBuffer& evict) { assert(false); return 0; }
	int update(Key k, Value v, UnstructuredBuffer& evict) { assert(false); return 0; }
	int lookup(Key k, Value& v, UnstructuredBuffer& evict) { assert(false); return 0; }
	int remove(Key k, UnstructuredBuffer& evict) { assert(false); return 0; }
	void add_to_cache(Key k, Value v) { assert(false); }
	void invalidate(Key k, char* ptr) { assert(false); }
	int search_cache(Key k, UnstructuredBuffer& buffer) { assert(false); return 0; }

private:
	BTree<Value>* index;
	uint32_t _index_id;
};

} // namespace partitioned
#endif