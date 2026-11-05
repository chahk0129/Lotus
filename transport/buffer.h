#pragma once

class rdma_buffer_t {
public:
    rdma_buffer_t() : _buffer(nullptr), _size(0) {}
    rdma_buffer_t(char* ptr, uint64_t size) : _buffer(ptr), _size(size) {}
    ~rdma_buffer_t() {}

    void set_page_buffer(char* buffer, uint32_t size) {
        _page_buffer = buffer;
        _page_buffer_size = size;
        _page_size = 1024; // 1KB
        _cur_page_buffer = 0;
    }

    char* get_page_buffer() {
        _cur_page_buffer = (_cur_page_buffer + 1) % _page_buffer_size;
        return _page_buffer + _cur_page_buffer * _page_size;
    }

    char* get_range_buffer() {
        return _page_buffer;
    }

    char* get_sibling_buffer() {
        _cur_sibling_buffer = (_cur_sibling_buffer + 1) % _sibling_buffer_size;
        return _sibling_buffer + _cur_sibling_buffer * _page_size;
    }

    char* get_send_buffer() {
        return _send_buffer;
    }

    char* get_recv_buffer() {
        return _recv_buffer;
    }

    char* get_cas_buffer() {
        _cur_cas_buffer = (_cur_cas_buffer + 1) % _cas_buffer_size;
        return _cas_buffer + _cur_cas_buffer * 64;
    }

    void set_sibling_buffer(char* buffer, uint32_t size) {
        _sibling_buffer = buffer;
        _sibling_buffer_size = size;
        _cur_sibling_buffer = 0;
    }

    void set_cas_buffer(char* buffer, uint32_t size) {
        _cas_buffer = buffer;
        _cas_buffer_size = size;
        _cur_cas_buffer = 0;
    }

    void set_entry_buffer(char* buffer, uint32_t size) {
        _entry_buffer = buffer;
        _entry_buffer_size = size;
        _cur_entry_buffer = 0;
    }

private:
    char*     _buffer;
    uint64_t  _size;

    // for two-sided RDMA
    char*     _send_buffer;
    uint32_t  _send_buffer_size;

    char*     _recv_buffer;
    uint32_t  _recv_buffer_size;

    // for one-sided RDMA
    char*     _cas_buffer;
    uint32_t  _cas_buffer_cur;
    uint32_t  _cas_buffer_size;

    char*     _page_buffer;
    uint32_t  _page_buffer_cur;
    uint32_t  _page_buffer_size;
    uint32_t  _page_size;

    char*     _sibling_buffer;
    uint32_t  _sibling_buffer_cur;
    uint32_t  _sibling_buffer_size;
};