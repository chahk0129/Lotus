#pragma once
#include "system/global.h"
#include "utils/debug.h"
#include <cassert>
#include <atomic>
#include <cstdint>
// naive memory allocator for one-sided RDMA
// allocates memory in a linear way, starting from the beginning of the memory region
// TODO: support deallocation using freelist

class memory_allocator_t {
public:
	memory_allocator_t(uint64_t start, uint64_t size) : _start(start), _end(start + size) {
		_cur.store(start);
	}

	~memory_allocator_t() { }

	uint64_t alloc(uint64_t size) {
		static int cnt = 0;
		cnt++;
		auto c = _cur.load();
		while (c + size < _end) {
			if (_cur.compare_exchange_strong(c, c + size))
		   		return c;
	       c = _cur.load();
	    }
		    
		// TODO: handle out of memory
		// we assume that the memory region is large enough
	    debug::notify_error("Server runs out of memory !");
	    assert(false);
		exit(1);
	    return 0;
	}
	
	// TODO: free

private:
	uint64_t 			  _start;
	uint64_t 			  _end;
	std::atomic<uint64_t> _cur;
};

