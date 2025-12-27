#pragma once

#include "batch/decision.h"
#include <vector>
#include <cstdint>

class batch_group_t;
class batch_entry_t;

class batch_manager_t {
public:
    batch_manager_t();
    ~batch_manager_t();

    static constexpr uint32_t LEADER_THRESHOLD = 100000; // in CPU cycles
    static constexpr uint32_t LEADER_WAIT_TIME = 10000; // in CPU cycles

    // consecutive counters for adaptive decision making
    uint32_t stats_conflicts;
    uint32_t stats_timeouts;
    uint32_t stats_batches;

private:
    // batch decision engine
    batch_decision_t decision_engine;
    // batch group
    batch_group_t*   group;
    uint32_t         entry_idx;
    // track request submission time
    uint64_t*        timestamps; 
    bool             leader_active;
    uint64_t         leader_expiration_time;

    void                         try_collect_request(uint32_t node_id, std::vector<std::pair<uint32_t, uint32_t>>& result);
public:
    batch_decision_t::decision_t get_decision();
    uint32_t                     get_wait_time();
    bool                         is_leader();
    void                         check_leader_expiration_status();
    void                         resign_leader();

    // member functions
    uint32_t                     submit_request(uint32_t node_id, uint32_t size);
    bool                         wait_for_completion(uint32_t node_id);
    bool                         examine_status(uint32_t node_id, uint32_t wait_time);

    // leader functions
    void                         collect_batch_requests(uint32_t node_id, std::vector<std::pair<uint32_t, uint32_t>>& result);
    void                         update_status(uint32_t node_id, uint32_t member_id, bool wait_response);

    // Performance feedback methods
    void report_leader(uint64_t latency, bool success) {
        decision_engine.report_batch_outcome(latency, success);
    }
    
    void report_member(uint64_t latency, bool batch=false, bool success=false) {
        if (batch && success) {
            stats_batches++;
            decision_engine.report_batch_outcome(latency, success);
        }
        else if (batch && !success) {
            stats_timeouts++;
            decision_engine.report_timeout();
            decision_engine.report_individual_outcome(latency);
        }
        else
            decision_engine.report_individual_outcome(latency);
    }
    
    void report_conflict() {
        stats_conflicts++;
        decision_engine.report_conflict();
    }
};