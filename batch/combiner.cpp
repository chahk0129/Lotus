#include "batch/combiner.h"

#if BATCH

#include "batch/limits.h"
#include "client/transport.h"
#include "system/manager.h"
#include "system/stats.h"
#include "transport/message.h"
#include "utils/helper.h"
#include <algorithm>
#include <climits>
#include <cmath>

// A pending request that has waited this long is sent even if the in-flight cap is
// reached. Individually sent requests count toward the cap, and the server holds the
// reply of a request that waits for a lock, so without this escape every in-flight
// message to a server could be waiting on locks held by txns that can't send.
static constexpr uint64_t ESCAPE_NS = 1000 * 1000; // 1 ms

static batch_domain_t*                 g_domains = nullptr;
static thread_local batch_combiner_t*  tl_combiner = nullptr;

static inline client_transport_t* ctransport() {
    return static_cast<client_transport_t*>(transport);
}

//////////////////////
// batch_domain_t
//////////////////////

void batch_domain_t::create_all() {
    assert(g_batch_num_domains > 0 && g_batch_domain_size > 0);
    g_domains = new batch_domain_t[g_batch_num_domains];
    for (uint32_t d=0; d<g_batch_num_domains; d++) {
        auto& dom = g_domains[d];
        dom.id = d;
        dom.base_tid = d * g_batch_domain_size;
        dom.num_members = std::min(g_batch_domain_size, g_num_client_threads - dom.base_tid);
        dom.dests = new combine_dest_t[g_num_server_nodes];
        dom.reqs = new combine_req_t[dom.num_members * g_num_server_nodes];
        dom.controller = BATCH_ADAPTIVE ? new combine_controller_t() : nullptr;
    }
}

void batch_domain_t::print_all() {
    for (uint32_t d=0; d<g_batch_num_domains; d++)
        if (g_domains[d].controller)
            g_domains[d].controller->print_summary(d);
}

batch_domain_t* batch_domain_t::get(uint32_t tid) {
    return &g_domains[tid / g_batch_domain_size];
}

int batch_domain_t::limit() const {
    int l = controller ? controller->limit() : BATCH_LIMIT;
    return l <= 0 ? INT_MAX : l; // 0 = unlimited
}

uint32_t batch_domain_t::wait_ns() const {
    return controller ? controller->wait_ns() : BATCH_WAIT_NS;
}

//////////////////////
// batch_combiner_t
//////////////////////

batch_combiner_t::batch_combiner_t(uint32_t tid) : _tid(tid) {
    _dom = batch_domain_t::get(tid);
    _member = tid - _dom->base_tid;
    assert(_member < _dom->num_members && _member < 64);
    _bit = 1ull << _member;
    _nodes.reserve(g_num_server_nodes);
    _owned.reserve(g_num_server_nodes * BATCH_SLOTS);
}

batch_combiner_t* batch_combiner_t::get() { return tl_combiner; }
void batch_combiner_t::set(batch_combiner_t* c) { tl_combiner = c; }

void batch_combiner_t::begin_round() {
    assert(_nodes.empty() && _owned.empty());
    if (_dom->controller)
        _dom->controller->on_round(get_sys_clock());
}

void batch_combiner_t::submit(uint32_t node_id, uint32_t size) {
    auto& r = _dom->req(_member, node_id);
    assert(r.status.load(std::memory_order_relaxed) == combine_req_t::IDLE);
    r.size = size;
    r.submit_time = get_sys_clock();
    r.status.store(combine_req_t::PENDING, std::memory_order_relaxed);
    // release: the request bytes, size and status are visible to whoever claims the bit
    _dom->dests[node_id].pending.fetch_or(_bit, std::memory_order_release);
    _nodes.push_back(node_id);
}

void batch_combiner_t::send_individual(uint32_t node_id) {
    auto& r = _dom->req(_member, node_id);
    transport->send(transport->get_buffer(node_id), node_id, r.size);
}

// Try to send for destination node_id. Called only while this thread's own request to
// node_id is still pending, so the sender is (almost always) inside its own batch.
void batch_combiner_t::try_send(uint32_t node_id, bool force) {
    auto& d = _dom->dests[node_id];
    auto& mine = _dom->req(_member, node_id);
    int limit = _dom->limit();
    if (!force && d.inflight.load(std::memory_order_relaxed) >= limit)
        return;
    int unlocked = 0;
    if (d.lock.load(std::memory_order_relaxed) != 0 ||
        !d.lock.compare_exchange_strong(unlocked, 1, std::memory_order_acquire))
        return;
    if (!force && d.inflight.load(std::memory_order_relaxed) >= limit) {
        d.lock.store(0, std::memory_order_release);
        return;
    }

    uint32_t wait = _dom->wait_ns();
    if (wait > 0) { // optional extra wait to let more requests arrive (adaptive arm)
        uint64_t start = get_sys_clock();
        while (get_sys_clock() - start < wait)
            PAUSE
        INC_INT_STATS(bt_spin_ns, wait);
    }

    uint64_t bits = d.pending.exchange(0, std::memory_order_acq_rel);
    if (bits == 0) { // someone else already claimed everything (including mine)
        d.lock.store(0, std::memory_order_release);
        return;
    }
    if (force) { INC_INT_STATS(bt_member_timeout, 1); }

    if (bits == _bit) { // alone: send on my own QP, no copy
        mine.status.store(combine_req_t::INDIVIDUAL, std::memory_order_relaxed);
        d.inflight.fetch_add(1, std::memory_order_relaxed);
        d.lock.store(0, std::memory_order_release);
        send_individual(node_id);
        return;
    }

    // need a batch slot
    uint32_t busy = d.slot_busy.load(std::memory_order_acquire);
    uint32_t slot = BATCH_SLOTS;
    for (uint32_t j=0; j<BATCH_SLOTS; j++) {
        if (!(busy & (1u << j))) { slot = j; break; }
    }
    if (slot == BATCH_SLOTS) { // no free slot: return the others, send mine alone if it was taken
        uint64_t others = bits & ~_bit;
        if (others)
            d.pending.fetch_or(others, std::memory_order_release);
        if (bits & _bit) {
            mine.status.store(combine_req_t::INDIVIDUAL, std::memory_order_relaxed);
            d.inflight.fetch_add(1, std::memory_order_relaxed);
            d.lock.store(0, std::memory_order_release);
            send_individual(node_id);
        }
        else
            d.lock.store(0, std::memory_order_release);
        return;
    }
    d.slot_busy.fetch_or(1u << slot, std::memory_order_relaxed);

    uint32_t qp_id = _dom->id * BATCH_SLOTS + slot;
    uint32_t base_size = sizeof(message_t) - sizeof(char*);
    char* send_buffer = transport->get_batch_buffer(node_id, qp_id);
    auto batch_msg = new (send_buffer) message_t(message_t::type_t::REQUEST_BATCHED_STORED_PROCEDURE, qp_id, 0);
    char* data = batch_msg->get_data_ptr();
    uint64_t capacity = MAX_MESSAGE_SIZE * batch_limits::BUFFER_MSGS - base_size;
    uint64_t total = 0;
    uint32_t count = 0;
    uint64_t rest = bits;
    while (rest) {
        uint32_t i = __builtin_ctzll(rest);
        rest &= rest - 1;
        auto& m = _dom->req(i, node_id);
        if (total + m.size > capacity) { // doesn't fit: put it and the rest back
            d.pending.fetch_or((1ull << i) | rest, std::memory_order_release);
            break;
        }
        memcpy(data + total, transport->get_buffer(node_id, _dom->base_tid + i), m.size);
        total += m.size;
        count++;
        m.status.store(combine_req_t::CLAIMED, std::memory_order_relaxed);
    }
    assert(count > 0);
    batch_msg->set_data_size(count); // number of member requests
    d.inflight.fetch_add(1, std::memory_order_relaxed);
    d.lock.store(0, std::memory_order_release);

    ctransport()->send_batch_slot(send_buffer, node_id, qp_id, base_size + total);
    _owned.push_back({node_id, slot});
    INC_BATCH_STATS(count, 1);
}

// Hand each member its reply from a batch reply on (node_id, qp_id).
void batch_combiner_t::dispatch_batch_reply(uint32_t node_id, uint32_t qp_id) {
    uint32_t base_size = sizeof(message_t) - sizeof(char*);
    char* recv_buffer = transport->get_recv_batch_buffer(node_id, qp_id);
    auto recv_msg = reinterpret_cast<message_t*>(recv_buffer);
    assert(recv_msg->get_type() == message_t::type_t::ACK);
    uint32_t num_responses = recv_msg->get_data_size();
    char* base_data = recv_msg->get_data_ptr();
    uint64_t offset = 0;
    for (uint32_t k=0; k<num_responses; k++) {
        auto member_msg = reinterpret_cast<message_t*>(base_data + offset);
        uint32_t member_tid = member_msg->get_qp_id();
        uint32_t member_size = member_msg->get_data_size();
        assert(member_tid >= _dom->base_tid && member_tid - _dom->base_tid < _dom->num_members);
        auto& m = _dom->req(member_tid - _dom->base_tid, node_id);
        assert(m.status.load(std::memory_order_relaxed) == combine_req_t::CLAIMED);
        if (member_msg->get_type() == message_t::type_t::RESPONSE_WAIT) {
            // server deferred this request; the final reply comes on the member's own QP
            assert(member_size == 0);
            m.status.store(combine_req_t::WAIT_FALLBACK, std::memory_order_release);
        }
        else {
            memcpy(transport->get_recv_buffer(node_id, member_tid), member_msg, base_size + member_size);
            m.status.store(combine_req_t::DONE, std::memory_order_release);
        }
        offset += base_size + member_size;
    }
    ctransport()->post_recv_batch(recv_buffer, node_id, qp_id, MAX_MESSAGE_SIZE * batch_limits::BUFFER_MSGS);
}

bool batch_combiner_t::poll_owned_slots() {
    bool progress = false;
    for (size_t k=0; k<_owned.size(); ) {
        auto o = _owned[k];
        uint32_t qp_id = _dom->id * BATCH_SLOTS + o.slot;
        struct ibv_wc wc;
        if (ctransport()->poll_once_batch_slot(o.node, qp_id, &wc) > 0) {
            assert(wc.status == IBV_WC_SUCCESS);
            dispatch_batch_reply(o.node, qp_id);
            auto& d = _dom->dests[o.node];
            d.slot_busy.fetch_and(~(1u << o.slot), std::memory_order_release);
            d.inflight.fetch_sub(1, std::memory_order_relaxed);
            _owned[k] = _owned.back();
            _owned.pop_back();
            progress = true;
        }
        else
            k++;
    }
    return progress;
}

// Replies on this thread's own QPs: individual sends and WAIT fallbacks.
bool batch_combiner_t::poll_own_cq() {
    struct ibv_wc wc[8];
    int n = ctransport()->poll_once(wc, 8);
    for (int k=0; k<n; k++) {
        uint32_t node_id = wc[k].wr_id;
        char* recv_buffer = transport->get_recv_buffer(node_id);
        transport->post_recv(recv_buffer, node_id, MAX_MESSAGE_SIZE); // prepost for the next request
        auto recv_msg = reinterpret_cast<message_t*>(recv_buffer);
        assert(recv_msg->get_type() != message_t::type_t::RESPONSE_WAIT);
        auto& r = _dom->req(_member, node_id);
        int st = r.status.load(std::memory_order_acquire);
        if (st == combine_req_t::INDIVIDUAL) {
            _dom->dests[node_id].inflight.fetch_sub(1, std::memory_order_relaxed);
            r.status.store(combine_req_t::DONE, std::memory_order_relaxed);
        }
        else if (st == combine_req_t::WAIT_FALLBACK) {
            r.status.store(combine_req_t::DONE, std::memory_order_relaxed);
        }
        else {
            // the final reply to a deferred request overtook the batch reply carrying its
            // RESPONSE_WAIT (different QPs); remember it until the batch reply is dispatched
            assert(st == combine_req_t::CLAIMED);
            r.size = UINT32_MAX; // marker: final reply already received
        }
    }
    return n > 0;
}

void batch_combiner_t::finish_round() {
    while (true) {
        bool all_done = true;
        uint64_t now = get_sys_clock();
        for (auto node_id : _nodes) {
            auto& r = _dom->req(_member, node_id);
            int st = r.status.load(std::memory_order_acquire);
            if (st == combine_req_t::PENDING) {
                try_send(node_id, now - r.submit_time > ESCAPE_NS);
                st = r.status.load(std::memory_order_acquire);
            }
            if (st == combine_req_t::WAIT_FALLBACK && r.size == UINT32_MAX) {
                r.status.store(combine_req_t::DONE, std::memory_order_relaxed);
                st = combine_req_t::DONE;
            }
            if (st != combine_req_t::DONE)
                all_done = false;
        }
        if (all_done && _owned.empty())
            break;
        bool progress = poll_owned_slots();
        progress |= poll_own_cq();
        if (!progress)
            PAUSE
    }
    for (auto node_id : _nodes)
        _dom->req(_member, node_id).status.store(combine_req_t::IDLE, std::memory_order_relaxed);
    _nodes.clear();
}

//////////////////////////
// combine_controller_t
//////////////////////////

static constexpr uint64_t EPOCH_NS = 10 * 1000 * 1000;  // 10 ms per epoch
// After switching arms, the queue state left by the previous arm (e.g. requests held back
// by a cap and released at once) distorts the start of the epoch; only the rest is measured.
static constexpr uint64_t SETTLE_NS = 3 * 1000 * 1000;  // 3 ms
static constexpr uint64_t EXPLOIT_EPOCHS = 60;           // per cycle, after one epoch per arm
static constexpr double   EWMA_ALPHA = 0.5;              // per exploration sample of an arm
static constexpr double   ABORT_ALPHA = 0.3;             // per measured epoch
static constexpr double   ABORT_RATE_HIGH = 0.15;
static constexpr double   PREFER_OFF_MARGIN = 0.03;      // under contention, batch only if >3% better

combine_controller_t::combine_controller_t() {
    // arm 0 = no batching cap and no wait (requests go out as soon as the send lock is free).
    // On YCSB-C (8 nodes) the extra wait mattered far more than the cap: ~10us was best and
    // K=4/8/16 were within 5% of each other there, so the arms span the wait range.
    _arms = { {0, 0}, {8, 0}, {4, 2000}, {4, 5000}, {4, 10000}, {4, 20000}, {8, 10000}, {2, 10000} };
    _arm_tput.assign(_arms.size(), 0);
    _arm_samples.assign(_arms.size(), 0);
    _arm_picks.assign(_arms.size(), 0);
    _cur_arm = 0;
    _limit.store(_arms[0].limit);
    _wait_ns.store(_arms[0].wait_ns);
}

void combine_controller_t::on_round(uint64_t now) {
    uint64_t sync = g_sync_time;
    if (sync == 0 || now < sync)
        return;
    uint64_t e = (now - sync) / EPOCH_NS;
    uint64_t cur = _epoch.load(std::memory_order_acquire);
    if (e != cur) {
        if (_epoch.compare_exchange_strong(cur, e)) { // this thread switches epochs
            if (cur != UINT64_MAX)
                close_epoch(now);
            start_epoch(e);
        }
        return;
    }
    // settle period over: start measuring this epoch
    if (_measuring.load(std::memory_order_relaxed) == 0 && now - (sync + e * EPOCH_NS) >= SETTLE_NS) {
        int zero = 0;
        if (_measuring.compare_exchange_strong(zero, 1)) {
            _measure_start = now;
            _commits_at_start = _commits.load();
            _aborts_at_start = _aborts.load();
            _measuring.store(2, std::memory_order_release);
        }
    }
}

void combine_controller_t::close_epoch(uint64_t now) {
    if (_measuring.load(std::memory_order_acquire) != 2)
        return; // the epoch never got past its settle period
    double secs = (now - _measure_start) / 1e9;
    if (secs < 1e-3)
        return;
    uint64_t commits = _commits.load() - _commits_at_start;
    uint64_t aborts = _aborts.load() - _aborts_at_start;
    if (commits + aborts > 0)
        _abort_rate = ABORT_ALPHA * ((double)aborts / (commits + aborts)) + (1 - ABORT_ALPHA) * _abort_rate;
    if (!_cur_explore)
        return; // only exploration epochs (same arm on every client node) update estimates
    double tput = commits / secs;
    double& est = _arm_tput[_cur_arm];
    est = (_arm_samples[_cur_arm] == 0) ? tput : EWMA_ALPHA * tput + (1 - EWMA_ALPHA) * est;
    _arm_samples[_cur_arm]++;
}

void combine_controller_t::start_epoch(uint64_t epoch) {
    uint64_t pos = epoch % (_arms.size() + EXPLOIT_EPOCHS);
    _cur_explore = pos < _arms.size();
    _cur_arm = _cur_explore ? (uint32_t)pos : best_arm();
    _arm_picks[_cur_arm]++;
    _limit.store(_arms[_cur_arm].limit, std::memory_order_relaxed);
    _wait_ns.store(_arms[_cur_arm].wait_ns, std::memory_order_relaxed);
    _measuring.store(0, std::memory_order_release);
}

uint32_t combine_controller_t::best_arm() const {
    bool contended = BATCH_CONTENTION_GUARD && _abort_rate > ABORT_RATE_HIGH;
    int best = -1;
    for (uint32_t i=0; i<_arms.size(); i++) {
        if (_arm_samples[i] == 0) continue;
        if (contended && _arms[i].wait_ns != 0) continue; // under contention, never add waiting
        if (best < 0 || _arm_tput[i] > _arm_tput[best]) best = i;
    }
    if (best < 0)
        return 0;
    // under contention lean toward not batching unless batching is clearly better
    if (contended && best != 0 && _arm_samples[0] > 0 && _arm_tput[best] < _arm_tput[0] * (1 + PREFER_OFF_MARGIN))
        best = 0;
    return best;
}

void combine_controller_t::print_summary(uint32_t domain_id) {
    uint64_t total = 0;
    for (auto p : _arm_picks) total += p;
    printf("    controller[domain %u]: abort_rate=%.3f epochs=%lu\n", domain_id, _abort_rate, total);
    for (uint32_t i=0; i<_arms.size(); i++)
        printf("      arm limit=%-3d wait=%-5u picked=%5.1f%%  samples=%-3u est_tput=%.0f/s\n", _arms[i].limit,
               _arms[i].wait_ns, total ? 100.0 * _arm_picks[i] / total : 0.0, _arm_samples[i], _arm_tput[i]);
}

#endif // BATCH
