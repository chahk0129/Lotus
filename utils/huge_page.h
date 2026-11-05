#pragma once
#include <sys/mman.h>
#include <memory.h>
#include <iostream>

inline void* huge_page_alloc(uint64_t size){
    int access_flags = PROT_READ | PROT_WRITE;
    int map_flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB;
    void* addr = mmap(NULL, size, access_flags, map_flags, -1, 0);
    if(addr == MAP_FAILED) {
        std::cerr << "Huge page allocation failed ( " << size / 1024 / 1024 << " MB)" << std::endl;
        return nullptr;
    }
    return addr;
}

inline void huge_page_dealloc(void* addr, uint64_t size){
    int ret = munmap(addr, size);
    if(ret)
        std::cerr << "Failed to dealloc huge_page" << std::endl;
}
