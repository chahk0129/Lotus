#pragma once
#include "system/global.h"
#include "system/global_address.h"
#include <vector>
#include <string>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <infiniband/verbs.h>
// #include <infiniband/verbs_exp.h>

class message_t;
class memory_region_t;

class transport_t {
public:
    transport_t();
    ~transport_t();

    virtual bool init();
    void         close_sockets();

    void         sendMsg(message_t* msg, uint32_t node_id);
    message_t*   recvMsg();

    // RDMA buffers
    char*        get_buffer();
    char*        get_buffer(uint32_t node_id);
    char*        get_buffer(uint32_t node_id, uint32_t qp_id);
    char*        get_recv_buffer(uint32_t node_id);
    char*        get_recv_buffer(uint32_t node_id, uint32_t qp_id);
    char*        get_batch_buffer();
    char*        get_batch_buffer(uint32_t node_id);
    char*        get_batch_buffer(uint32_t node_id, uint32_t qp_id);
    char*        get_recv_batch_buffer(uint32_t node_id);
    char*        get_recv_batch_buffer(uint32_t node_id, uint32_t qp_id);

    // src/dest helper functions
    uint64_t     serialize_node_info(uint32_t node_id, uint32_t qp_id);
    void         deserialize_node_info(uint64_t data, uint32_t& node_id, uint32_t& qp_id);

    // low-l  evel communication functions
    bool         post_send(struct ibv_qp* qp, char* src, uint32_t size, uint32_t lkey, bool signaled=true);
    bool         post_send_imm(struct ibv_qp* qp, char* src, uint32_t size, uint32_t lkey, uint32_t imm_data, bool signaled=true);
    bool         post_recv(struct ibv_qp* qp, char* src, uint32_t size, uint32_t lkey, uint64_t wr_id=0);
    bool         post_faa(struct ibv_qp* qp, char* src, uint64_t dest, uint64_t add, uint32_t size, uint32_t lkey, uint32_t rkey, bool signaled=true);
    bool         post_faa_bound(struct ibv_qp* qp, char* src, uint64_t dest, uint64_t add, uint64_t bounadry, uint32_t size, uint32_t lkey, uint32_t rkey, bool signaled=true);
    bool         post_cas(struct ibv_qp* qp, char* src, uint64_t dest, uint64_t expected, uint64_t desired, uint32_t size, uint32_t lkey, uint32_t rkey, bool signaled=true);
    bool         post_cas_mask(struct ibv_qp* qp, char* src, uint64_t dest, uint64_t expected, uint64_t desired, uint64_t mask, uint32_t size, uint32_t lkey, uint32_t rkey, bool signaled=true);
    bool         post_read(struct ibv_qp* qp, char* src, uint64_t dest, uint32_t size, uint32_t lkey, uint32_t rkey, bool signaled=true);
    bool         post_write(struct ibv_qp* qp, char* src, uint64_t dest, uint32_t size, uint32_t lkey, uint32_t rkey, bool signaled=true);
    int          poll_cq(struct ibv_cq* cq, int num_entries, struct ibv_wc* wc);
    int          poll_cq_once(struct ibv_cq* cq, int num_entries, struct ibv_wc* wc);

    // communication wrappers
    void         rdma_read(struct ibv_qp* qp, struct ibv_cq* cq, char* src, uint64_t dest, uint32_t size, uint32_t lkey, uint32_t rkey, bool signaled=true);
    void         rdma_write(struct ibv_qp* qp, struct ibv_cq* cq, char* src, uint64_t dest, uint32_t size, uint32_t lkey, uint32_t rkey, bool signaled=true);
    void         rdma_faa(struct ibv_qp* qp, struct ibv_cq* cq, char* src, uint64_t dest, uint64_t add, uint32_t size, uint32_t lkey, uint32_t rkey, bool signaled=true);
    void         rdma_faa_bound(struct ibv_qp* qp, struct ibv_cq* cq, char* src, uint64_t dest, uint64_t add, uint64_t boundary, uint32_t size, uint32_t lkey, uint32_t rkey, bool signaled=true);
    bool         rdma_cas(struct ibv_qp* qp, struct ibv_cq* cq, char* src, uint64_t dest, uint64_t expected, uint64_t desired, uint32_t size, uint32_t lkey, uint32_t rkey, bool signaled=true);
    bool         rdma_cas_mask(struct ibv_qp* qp, struct ibv_cq* cq, char* src, uint64_t dest, uint64_t expected, uint64_t desired, uint64_t mask, uint32_t size, uint32_t lkey, uint32_t rkey, bool signaled=true);
    void         rdma_send(struct ibv_qp* qp, struct ibv_cq* cq, char* src, uint32_t size, uint32_t lkey, bool signaled=true);
    void         rdma_recv(struct ibv_qp* qp, struct ibv_cq* cq, char* src, uint32_t size, uint32_t lkey, uint64_t wr_id=0);
    void         rdma_recv_prepost(struct ibv_qp* qp, char* src, uint32_t size, uint32_t lkey, uint64_t wr_id=0);


    // operation completion
    virtual      int  poll(struct ibv_wc* wc, int num) = 0;
    virtual      int  poll_once(struct ibv_wc* wc, int num) = 0;

    // two-sided RDMA
    virtual      void post_recv(char* ptr, uint32_t node_id, uint32_t size) = 0;
    virtual      void post_recv(char* ptr, uint32_t node_id, uint32_t qp_id, uint32_t size) = 0;
    virtual      void recv(char* ptr, uint32_t node_id, uint32_t size) = 0;
    virtual      void send(char* ptr, uint32_t node_id, uint32_t size, bool signaled = true) = 0;
    virtual      void send(char* ptr, uint32_t node_id, uint32_t qp_id, uint32_t size, bool signaled = true) = 0;

    // two-sided RDMA batch
    virtual      int  poll_batch(struct ibv_wc* wc, int num) = 0;
    virtual      int  poll_once_batch(struct ibv_wc* wc, int num) = 0;
    virtual      void post_recv_batch(char* ptr, uint32_t node_id, uint32_t size) = 0;
    virtual      void post_recv_batch(char* ptr, uint32_t node_id, uint32_t qp_id, uint32_t size) = 0;
    virtual      void recv_batch(char* ptr, uint32_t node_id, uint32_t size) = 0;
    virtual      void send_batch(char* ptr, uint32_t node_id, uint32_t size, bool signaled = true) = 0;
    virtual      void send_batch(char* ptr, uint32_t node_id, uint32_t qp_id, uint32_t size, bool signaled = true) = 0;

    // one-sided RDMA
    virtual      void read(char* src, global_addr_t dest, uint32_t size, bool signaled = true) = 0;
    virtual      void write(char* src, global_addr_t dest, uint32_t size, bool signaled = true) = 0;
    virtual      void faa(char* src, global_addr_t dest, uint64_t add, uint32_t size, bool signaled = true) = 0;
    virtual      void faa_bound(char* src, global_addr_t dest, uint64_t add, uint64_t boundary, uint32_t size, bool signaled = true) = 0;
    virtual      bool cas(char* src, global_addr_t dest, uint64_t cmp, uint64_t swap, uint32_t size, bool signaled = true) = 0;
    virtual      bool cas_mask(char* src, global_addr_t dest, uint64_t cmp, uint64_t swap, uint64_t mask, uint32_t size, bool signaled = true) = 0;
    virtual      void read_dm(char* src, global_addr_t dest, uint32_t size, bool signaled = true) = 0;
    virtual      void write_dm(char* src, global_addr_t dest, uint32_t size, bool signaled = true) = 0;
    virtual      void faa_dm(char* src, global_addr_t dest, uint64_t add, uint32_t size, bool signaled = true) = 0;
    virtual      void faa_dm_bound(char* src, global_addr_t dest, uint64_t add, uint64_t boundary, uint32_t size, bool signaled = true) = 0;
    virtual      bool cas_dm(char* src, global_addr_t dest, uint64_t cmp, uint64_t swap, uint32_t size, bool signaled = true) = 0;
    virtual      bool cas_dm_mask(char* src, global_addr_t dest, uint64_t cmp, uint64_t swap, uint64_t mask, uint32_t size, bool signaled = true) = 0;

protected:
    struct rdma_ctx {
        struct ibv_context* ctx;
        struct ibv_pd* pd;
        struct ibv_port_attr port_attr;
        int gid_idx;
        union ibv_gid gid;
    };

    struct rdma_meta {
        int gid_idx;
        union ibv_gid gid;
        uint32_t lid;
        uint32_t rkey;
        uint32_t dm_rkey;
        uint64_t dm_base;
        uint32_t qpn[MAX_NUM_THREADS];
        uint32_t batch_qpn[MAX_NUM_THREADS];
    };

    virtual bool connect();
    void         server_listen();
    void         client_connect();
    void         read_urls();
    uint32_t     get_port_num(uint32_t node_id, bool is_server);

    bool         init_rdma();
    virtual bool cleanup();

    // rdma configuration functions
    struct ibv_context* open_device();
    struct ibv_pd*      alloc_pd(struct ibv_context* ctx);
    bool                query_attr(struct ibv_context* ctx, struct ibv_port_attr& attr, int& gid_idx, union ibv_gid& gid);
    bool                modify_qp_state_to_init(struct ibv_qp* qp);
    bool                modify_qp_state_to_rtr(struct ibv_qp* qp, union ibv_gid gid, int gid_idx, uint32_t lid, uint32_t qpn);
    bool                modify_qp_state_to_rts(struct ibv_qp* qp);
    struct ibv_cq*      create_cq(struct ibv_context* ctx);
    struct ibv_qp*      create_qp(struct ibv_pd* pd, struct ibv_cq* send_cq, struct ibv_cq* recv_cq);
    struct ibv_mr*      register_mr(struct ibv_pd* pd, void* addr, uint64_t size);
    struct ibv_mr*      register_mr_device(struct ibv_context* ctx, struct ibv_pd* pd, void* addr, uint64_t size);

    // node information
    std::vector<std::string> _client_urls;
    std::vector<std::string> _server_urls;
    
    // for connection establishment (TCP)
    struct addrinfo*        _local_info;
    struct addrinfo*        _remote_info;
    int*                    _sockets;

    char**                  _send_buffer;
    char**                  _recv_buffer;
    uint32_t*               _recv_buffer_lower;
    uint32_t*               _recv_buffer_upper;


    // for RDMA communication
    char***                 _rdma_send_buffer;
    char***                 _rdma_recv_buffer;
    uint32_t                _rdma_send_buffer_size;
    uint32_t                _rdma_recv_buffer_size;

    char***                 _rdma_send_batch_buffer;
    char***                 _rdma_recv_batch_buffer;
    uint32_t                _rdma_send_batch_buffer_size;
    uint32_t                _rdma_recv_batch_buffer_size;

    memory_region_t*        _memory_region; // memory region for RDMA communication
    struct rdma_ctx         _context;
    struct rdma_meta*       _meta;
};