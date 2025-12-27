#pragma once

#include "system/global.h"
#include "utils/packetize.h"
#include "index/idx_wrapper.h"
#include "index/twosided/btreeolc/node.h"
#include "index/twosided/btreeolc/tree.h"
// server-side index implementation
#if !PARTITIONED
namespace nonpartitioned{

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

    // cache operations (dummy)
    int search_cache(Key k, UnstructuredBuffer& buffer) { assert(false); return 0; }
    void invalidate(char* entry) { assert(false); }
    void add_to_cache(Key k, char* page) { assert(false); }
    // void add_to_cache(char* page) { assert(false); }
    void bulk_load(Key* key, uint64_t num) { assert(false); }
    void bulk_load(std::vector<std::pair<Key, Value>>& v, uint64_t num, bool sorted=true) { assert(false); }
    void set_shared(std::vector<Key>& bound) {}
    void set_bound(Value left, Value right) {}
    void flush_all() {}

private:
    BTree<Value>* index;
    uint32_t _index_id;
};
} // namespace nonpartitioned
#endif // !PARTITIONED