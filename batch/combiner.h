#pragma once

// Inter-transaction batching (BATCH=1, two-sided only).
//
// Client threads are split into batching domains (BATCH_DOMAIN_SIZE threads, or the
// whole node). Per (domain, server) there is a bitmap of pending requests and a cap on
// in-flight messages. A thread puts its request in the bitmap, then whoever still has a
// pending request and wins the destination's send lock claims *everything* pending and
// sends it as one message (or alone, on its own QP, if it is the only one). Requests
// that arrive while the cap is reached pile up and go out together when a slot frees.
// No thread waits on a timer for a leader; the owner of a pending request keeps trying
// until someone claims it or it sends it itself.
//
// Each in-flight batch uses one "slot" = batch QP (domain * BATCH_SLOTS + j) to that
// server. The thread that sent a batch owns the slot until the reply arrives, then hands
// each member its reply and frees the slot.

#include "system/global.h"
#include <atomic>
#include <cstdint>
#include <vector>

#if BATCH

struct alignas(64) combine_dest_t {
    std::atomic<uint64_t> pending{0};   // bit i: domain member i has an unclaimed request
    std::atomic<int>      inflight{0};  // messages to this server in flight (batches + individual)
    std::atomic<int>      lock{0};      // held while claiming + posting one message
    std::atomic<uint32_t> slot_busy{0}; // bit j: slot j has a batch in flight
};

struct alignas(64) combine_req_t {
    enum status_t : int {
        IDLE = 0,
        PENDING,        // in the pending bitmap, not claimed yet
        CLAIMED,        // inside a batch that is in flight
        INDIVIDUAL,     // sent alone on the owner's own QP; reply arrives on the owner's CQ
        WAIT_FALLBACK,  // batch reply said RESPONSE_WAIT; final reply arrives on the owner's CQ
        DONE,           // reply is in the owner's normal recv buffer for this server
    };
    std::atomic<int> status{IDLE};
    uint32_t         size = 0;          // message size in the owner's send buffer for this server
    uint64_t         submit_time = 0;
};

// Per-domain adaptive controller (BATCH_ADAPTIVE). Picks the in-flight cap (and an optional
// extra wait) per epoch from a small set of arms.
//
// All client nodes share the memory servers, so a node's own commit rate depends on what the
// other nodes do, and a node that batches less grabs a bigger share of server time even when
// that lowers cluster throughput. Estimates are therefore taken only in exploration epochs,
// in which every client node and domain runs the same arm: epochs are numbered from the
// startup global sync (g_sync_time), which all client nodes pass within about one round
// trip, and the cycle (one exploration epoch per arm in a fixed order, then EXPLOIT_EPOCHS
// on the best-estimated arm) is the same everywhere.
class combine_controller_t {
public:
    struct arm_t { int limit; uint32_t wait_ns; };

    combine_controller_t();
    void on_round(uint64_t now);       // cheap check at the start of every send round
    void report_commit() { _commits.fetch_add(1, std::memory_order_relaxed); }
    void report_abort()  { _aborts.fetch_add(1, std::memory_order_relaxed); }

    void     print_summary(uint32_t domain_id);  // arm picks and estimates, at end of run
    int      limit()   const { return _limit.load(std::memory_order_relaxed); }
    uint32_t wait_ns() const { return _wait_ns.load(std::memory_order_relaxed); }

private:
    void     start_epoch(uint64_t epoch);
    void     close_epoch(uint64_t now);
    uint32_t best_arm() const;

    std::vector<arm_t>    _arms;
    std::vector<double>   _arm_tput;     // EWMA of commits/s per arm, from exploration epochs
    std::vector<uint32_t> _arm_samples;
    std::vector<uint64_t> _arm_picks;    // epochs each arm was used (whole run)
    double                _abort_rate = 0; // EWMA over measured epochs
    uint32_t              _cur_arm = 0;
    bool                  _cur_explore = false;
    // measured part of the current epoch (after the settle period)
    uint64_t              _measure_start = 0;
    uint64_t              _commits_at_start = 0;
    uint64_t              _aborts_at_start = 0;
    std::atomic<uint64_t> _epoch{UINT64_MAX};
    std::atomic<int>      _measuring{0};  // 0 settling, 1 being set up, 2 measuring
    std::atomic<uint64_t> _commits{0};    // monotonic counters
    std::atomic<uint64_t> _aborts{0};
    std::atomic<int>      _limit;
    std::atomic<uint32_t> _wait_ns;
};

struct batch_domain_t {
    uint32_t              id = 0;
    uint32_t              base_tid = 0;  // first client thread id of this domain
    uint32_t              num_members = 0;
    combine_dest_t*       dests = nullptr; // [g_num_server_nodes]
    combine_req_t*        reqs = nullptr;  // [num_members * g_num_server_nodes]
    combine_controller_t* controller = nullptr;

    combine_req_t& req(uint32_t member, uint32_t node) { return reqs[member * g_num_server_nodes + node]; }
    int      limit()   const;
    uint32_t wait_ns() const;

    static void            create_all();   // after the transport set g_batch_num_domains
    static void            print_all();    // controller summaries (BATCH_ADAPTIVE)
    static batch_domain_t* get(uint32_t tid);
};

// Per-thread handle (one per client thread).
class batch_combiner_t {
public:
    batch_combiner_t(uint32_t tid);

    void begin_round();
    void submit(uint32_t node_id, uint32_t size); // request already built in get_buffer(node_id)
    void finish_round();                          // returns when every submitted request has its reply

    void report_commit() { if (_dom->controller) _dom->controller->report_commit(); }
    void report_abort()  { if (_dom->controller) _dom->controller->report_abort(); }

    static batch_combiner_t* get();               // this thread's handle
    static void              set(batch_combiner_t* c);

private:
    struct owned_slot_t { uint32_t node; uint32_t slot; };

    void try_send(uint32_t node_id, bool force);
    bool poll_owned_slots();
    bool poll_own_cq();
    void dispatch_batch_reply(uint32_t node_id, uint32_t qp_id);
    void send_individual(uint32_t node_id);

    uint32_t                  _tid;
    uint32_t                  _member;   // index inside the domain
    uint64_t                  _bit;
    batch_domain_t*           _dom;
    std::vector<uint32_t>     _nodes;    // servers with a request this round
    std::vector<owned_slot_t> _owned;    // batches this thread sent and must dispatch
};

#endif // BATCH
