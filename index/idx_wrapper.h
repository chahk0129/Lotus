#pragma once

#include "system/global.h"
#include "utils/packetize.h"
#include <cstdint>
#include <utility>
#include <vector>

#define PAGE_SIZE 1024
#define MAX_SCAN_PAGE 20

template <class T>
class idx_wrapper_t{
public:
	// index operations
	virtual void insert(Key k, T v) = 0;
	virtual bool update(Key k, T v) = 0;
	virtual bool remove(Key k) = 0;
	virtual bool lookup(Key k, T& v) = 0;
	virtual int scan(Key k, int range, T*& v) = 0;

	// cache operations
	virtual int search_cache(Key k, UnstructuredBuffer& buffer) = 0;
#if PARTITIONED
	virtual int insert(Key k, T v, UnstructuredBuffer& evict) = 0;
	virtual int update(Key k, T v, UnstructuredBuffer& evict) = 0;
	virtual int lookup(Key k, T& v, UnstructuredBuffer& evict) = 0;
	virtual int remove(Key k, UnstructuredBuffer& evict) = 0;
	virtual void add_to_cache(Key k, T v) = 0;
	virtual void invalidate(Key k, char* ptr) = 0;
	virtual void set_bound(Key left, Key right) = 0;
	virtual void set_shared(std::vector<Key>& bound) = 0;
	virtual void bulk_load(Key* key, uint64_t num) = 0;
	virtual void set_rpc_ratio(double ratio) = 0;
	virtual void get_newest_root() = 0;
	virtual void reset_buffer_pool(bool flush_dirty) = 0;
#else
	virtual void add_to_cache(Key k, char* page) = 0;
	// virtual void add_to_cache(char* page) = 0;
	virtual void invalidate(char* ptr) = 0;
#endif

	// one-sided RDMA indexes
	virtual void flush_all() {}
	// virtual void get_newest_root(int tid) {}
	// virtual void reset_buffer_pool(bool flush_dirty) {}
	virtual void set_admission_ratio(double ratio) {}
	virtual void set_partition(Key from, Key to) {}
};
