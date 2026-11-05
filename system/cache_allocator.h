#pragma once

#include "system/global.h"
#include "utils/debug.h"
#include <atomic>
#include <cstdint>
#include <vector>
// very naive cache allocator for one-sided RDMA
// it stores a chunk of remote memory in cache and allocates memory in a linear way
// TODO: support deallocation using freelist

class cache_allocator_t {
public:
    cache_allocator_t() : _size(0), _offset(0), _cur(0) { pthread_mutex_init(&_latch, nullptr); }

    ~cache_allocator_t() { pthread_mutex_destroy(&_latch); }

    uint64_t alloc(uint64_t size) {
        pthread_mutex_lock(&_latch);
        uint64_t cur = _cur;
        if (cur + size <= _offset + _size) {
            _cur = cur + size;
            pthread_mutex_unlock(&_latch);
            return cur;
        }
        // latch will be released in add() function
        return 0;
    }

    void add(uint64_t offset, uint64_t size) {
        // latch is already acquired in alloc()
        assert(offset != 0 && size != 0);
        assert(offset > _offset && offset >= _offset + _size);
        _cache_list.push_back({_offset, _size});
        _offset = offset;
        _cur = offset;
        _size = size;
        pthread_mutex_unlock(&_latch);
    }

    // TODO: defragmentation

private:
    pthread_mutex_t       _latch;
    uint64_t              _size;
    uint64_t              _offset;
    uint64_t              _cur;
    std::vector<std::pair<uint64_t, uint64_t>> _cache_list; // <offset, size> of used cache
};