#pragma once
#include "transport/transport.h"

class message_t;

class client_transport_t : public transport_t{
public:
    client_transport_t();
    ~client_transport_t();

    bool init();

    // two-sided RDMA
    int  poll(struct ibv_wc* wc, int num);
    int  poll_once(struct ibv_wc* wc, int num);
    void post_recv(char* ptr, uint32_t node_id, uint32_t size);
    void post_recv(char* ptr, uint32_t node_id, uint32_t qp_id, uint32_t size);
    void recv(char* ptr, uint32_t node_id, uint32_t size);
    void send(char* ptr, uint32_t node_id, uint32_t size, bool signaled = true);
    void send(char* ptr, uint32_t node_id, uint32_t qp_id, uint32_t size, bool signaled = true) { assert(false); }

    // two-sided RDMA batch
    int  poll_batch(struct ibv_wc* wc, int num);
    int  poll_once_batch(struct ibv_wc* wc, int num);
    void post_recv_batch(char* ptr, uint32_t node_id, uint32_t size);
    void post_recv_batch(char* ptr, uint32_t node_id, uint32_t qp_id, uint32_t size);
    void recv_batch(char* ptr, uint32_t node_id, uint32_t size);
    void send_batch(char* ptr, uint32_t node_id, uint32_t size, bool signaled = true);
    void send_batch(char* ptr, uint32_t node_id, uint32_t qp_id, uint32_t size, bool signaled = true) { assert(false); }

    // one-sided RDMA -- host memory functions
    void read(char* src, global_addr_t dest, uint32_t size, bool signaled = true);
    void write(char* src, global_addr_t dest, uint32_t size, bool signaled = true);
    bool cas(char* src, global_addr_t dest, uint64_t cmp, uint64_t swap, uint32_t size, bool signaled = true);
    bool cas_mask(char* src, global_addr_t dest, uint64_t cmp, uint64_t swap, uint64_t mask, uint32_t size, bool signaled = true);
    void faa(char* src, global_addr_t dest, uint64_t add, uint32_t size, bool signaled = true);
    void faa_bound(char* src, global_addr_t dest, uint64_t add, uint64_t boundary, uint32_t size, bool signaled = true);
    
    // one-sided RDMA -- device memory functions
    void read_dm(char* src, global_addr_t dest, uint32_t size, bool signaled = true);
    void write_dm(char* src, global_addr_t dest, uint32_t size, bool signaled = true);
    bool cas_dm(char* src, global_addr_t dest, uint64_t cmp, uint64_t swap, uint32_t size, bool signaled = true);
    bool cas_dm_mask(char* src, global_addr_t dest, uint64_t cmp, uint64_t swap, uint64_t mask, uint32_t size, bool signaled = true);
    void faa_dm(char* src, global_addr_t dest, uint64_t add, uint32_t size, bool signaled = true);
    void faa_dm_bound(char* src, global_addr_t dest, uint64_t add, uint64_t boundary, uint32_t size, bool signaled = true);

private:
    bool cleanup();
    bool connect();

    struct ibv_qp*** _qps;
    struct ibv_cq**  _send_cqs;
    struct ibv_cq**  _recv_cqs;

    struct ibv_qp*** _batch_qps;
    struct ibv_cq**  _batch_send_cqs;
    struct ibv_cq**  _batch_recv_cqs;
    struct ibv_mr*   _mr;
};



