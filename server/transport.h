#pragma once
#include "system/global.h"
#include "transport/transport.h"

class message_t;
class memory_region_t;

class server_transport_t : public transport_t {
    public:
		server_transport_t();
		~server_transport_t();

		bool init();

		int  poll(struct ibv_wc* wc, int num);
		int  poll_once(struct ibv_wc* wc, int num) { assert(false); return 0; };

		// two-sided RDMA
		void send(char* ptr, uint32_t node_id, uint32_t size, bool signaled = true) { assert(false); }
		void send(char* ptr, uint32_t node_id, uint32_t qp_id, uint32_t size, bool signaled = true);
		void post_recv(char* ptr, uint32_t node_id, uint32_t qp_id, uint32_t size);
		void post_recv(char* ptr, uint32_t node_id, uint32_t size) { assert(false); }
		void recv(char* ptr, uint32_t node_id, uint32_t size) { assert(false); }

		// two-sided RDMA batch
		int  poll_batch(struct ibv_wc* wc, int num) { assert(false); return 0; };
		int  poll_once_batch(struct ibv_wc* wc, int num) { assert(false); return 0; };
		void post_recv_batch(char* ptr, uint32_t node_id, uint32_t size) { assert(false); }
		void post_recv_batch(char* ptr, uint32_t node_id, uint32_t qp_id, uint32_t size);
		void recv_batch(char* ptr, uint32_t node_id, uint32_t size) { assert(false); }
		void send_batch(char* ptr, uint32_t node_id, uint32_t qp_id, uint32_t size, bool signaled = true);
		void send_batch(char* ptr, uint32_t node_id, uint32_t size, bool signaled = true) { assert(false); }
		
		// one-sided RDMA
		void read(char* src, global_addr_t dest, uint32_t size, bool signaled = true) { assert(false); }
		void write(char* src, global_addr_t dest, uint32_t size, bool signaled = true) { assert(false); }
		bool cas(char* src, global_addr_t dest, uint64_t expected, uint64_t desired, uint32_t size, bool signaled = true) { assert(false); return false; }
		bool cas_mask(char* src, global_addr_t dest, uint64_t expected, uint64_t desired, uint64_t mask, uint32_t size, bool signaled = true) { assert(false); return false; }
		void faa(char* src, global_addr_t dest, uint64_t add, uint32_t size, bool signaled = true) { assert(false); }
		void faa_bound(char* src, global_addr_t dest, uint64_t add, uint64_t boundary, uint32_t size, bool signaled = true) { assert(false); }

		// one-sided RDMA device functions
		void read_dm(char* src, global_addr_t dest, uint32_t size, bool signaled = true) { assert(false); }
		void write_dm(char* src, global_addr_t dest, uint32_t size, bool signaled = true) { assert(false); }
		bool cas_dm(char* src, global_addr_t dest, uint64_t expected, uint64_t desired, uint32_t size, bool signaled = true) { assert(false); return false; }
		bool cas_dm_mask(char* src, global_addr_t dest, uint64_t expected, uint64_t desired, uint64_t mask, uint32_t size, bool signaled = true) { assert(false); return false; }
		void faa_dm(char* src, global_addr_t dest, uint64_t add, uint32_t size, bool signaled = true) { assert(false); }
		void faa_dm_bound(char* src, global_addr_t dest, uint64_t add, uint64_t boundary, uint32_t size, bool signaled = true) { assert(false); }

	private:
		bool cleanup();
		bool connect();

		struct ibv_qp*** _qps;
		struct ibv_cq**  _send_cqs;
		struct ibv_qp*** _batch_qps;
		struct ibv_cq**  _batch_send_cqs;

		struct ibv_cq*   _recv_cq; // shared recv cq
		struct ibv_mr*   _mr;

		// below are for one-sided RDMA
		// host memory
		struct memory_region_t* _alloc_memory_region;
		struct ibv_mr*  		_alloc_mr;
		// device memory
		struct ibv_mr*  	    _dm_mr;
		void*                   _dm_addr;
		uint64_t                _dm_size;
};