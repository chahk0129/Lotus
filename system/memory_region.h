#pragma once
#include "utils/huge_page.h"
#include "utils/debug.h"

constexpr uint64_t MR_SIZE = 1024ULL * 1024 * 1024 * 32; // 32GB
// constexpr uint64_t MR_SIZE = 1024ULL * 1024 * 1024 * 32; // 32GB

class memory_region_t{
public:
	memory_region_t(): _size(MR_SIZE), addr(nullptr){
	    addr = huge_page_alloc(_size);
	    if(!addr){
			debug::notify_error("failed to allocate memory_region");
			exit(0);
		}

	    memset(addr, 0, _size);
	}

	memory_region_t(uint64_t _size): _size(_size), addr(nullptr){
	    addr = huge_page_alloc(_size);
	    if(!addr){
			debug::notify_error("failed to allocate memory_region");
			exit(0);
		}
	    memset(addr, 0, _size);
	}

	~memory_region_t(){
	    huge_page_dealloc(addr, _size);
	    addr = nullptr;
	}

	void*    ptr()  { return addr; }
	uint64_t size() { return _size; }

private:
	void* addr;
	uint64_t _size;
};

