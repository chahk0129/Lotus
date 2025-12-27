#include "batch/manager.h"
#include "batch/group.h"
#include "batch/table.h"
#include "batch/decision.h"
#include "system/global.h"
#include "system/manager.h"
#include "utils/helper.h"

batch_manager_t::batch_manager_t() {
    stats_timeouts = 0;
    stats_batches = 0;
    stats_conflicts = 0;

    timestamps = new uint64_t[g_num_server_nodes];
    memset(timestamps, 0, sizeof(uint64_t) * g_num_server_nodes);
    group = GET_BATCH_TABLE->get_group(GET_THD_ID / batch_table_t::MAX_GROUP_SIZE);
    entry_idx = GET_THD_ID % batch_table_t::MAX_GROUP_SIZE;
    assert(group != nullptr);
    leader_active = false;
    leader_expiration_time = 0;
}

batch_manager_t::~batch_manager_t() {
    delete[] timestamps;
}

batch_decision_t::decision_t batch_manager_t::get_decision() {
    if (group->get_active_members() > batch_table_t::MAX_GROUP_SIZE * 2 / 3 &&
        GET_BATCH_TABLE->get_num_groups() > batch_table_t::MAX_GROUP_NUM * 2 / 3) // enough members and groups
        return decision_engine.should_wait_for_batch(this);
    else // not enough members, no batching
        return batch_decision_t::decision_t::SEND_IMMEDIATELY;
}

bool batch_manager_t::is_leader() {
    if (leader_active)
        return true;
    else { // try to become a leader
        auto tid = GET_THD_ID;
        leader_active = group->is_leader(tid);
        if (leader_active)
            leader_expiration_time = get_sys_clock() + LEADER_THRESHOLD;
        return leader_active;
    }
}

void batch_manager_t::check_leader_expiration_status() {
    assert(leader_active == true);
    uint64_t cur_time = get_sys_clock();
    if (cur_time > leader_expiration_time) {
        // my leadership has expired, need to resign
        group->resign_leader();
        leader_active = false;
        leader_expiration_time = 0;
    }
}

void batch_manager_t::resign_leader() {
    if (leader_active) {
        leader_active = false;
        leader_expiration_time = 0;
        group->resign_leader();
    }
}

//////////////////////
// member functions //
//////////////////////

uint32_t batch_manager_t::submit_request(uint32_t node_id, uint32_t size) {
    auto entry = group->get_entry(entry_idx);
    assert(entry->first == GET_THD_ID);
    assert(entry->second[node_id].load() == 0);
    entry->second[node_id].store(size);
    
    uint32_t wait_time = decision_engine.calculate_wait_time(this);
    timestamps[node_id] = get_sys_clock();
    return wait_time;
}


bool batch_manager_t::wait_for_completion(uint32_t node_id) {
    assert(entry_idx == GET_THD_ID % batch_table_t::MAX_GROUP_SIZE);
    auto entry = group->get_entry(entry_idx);
    auto value = entry->second[node_id].load();
    while (value < 0) {
        if (value == -2) { // received WAIT response, need to receive it again itself
            entry->second[node_id].store(0);
            return true;
        }
        assert(value == -1);
        // otherwise, still waiting for response from leader
        PAUSE
        value = entry->second[node_id].load();
    }
    assert(value == 0);
    // completed successfully
    return false;
}

bool batch_manager_t::examine_status(uint32_t node_id, uint32_t wait_time) {
    assert(leader_active == false);
    auto entry = group->get_entry(entry_idx);
    uint64_t deadline = timestamps[node_id] + wait_time;
    uint64_t cur_time = 0;
    while (true) {
        auto value = entry->second[node_id].load();
        if (value <= 0) // successfully batched
            return false;
        
        // otherwise, examine timing constraints
        cur_time = get_sys_clock();
        if (cur_time >= deadline || global_manager->is_sim_done()) {
            assert(value > 0);
            if (entry->second[node_id].compare_exchange_strong(value, 0)) {
                // timeout, try to reclaim the request to send it individually
                return true;
            }

            // otherwise, leader has already reclaimed it
            return false;
        }

        // still waiting ...
        PAUSE
    }
    assert(false);
    return true;
}

//////////////////////
// leader functions //
//////////////////////

uint32_t batch_manager_t::get_wait_time() {
    return LEADER_WAIT_TIME;
}

void batch_manager_t::collect_batch_requests(uint32_t node_id, std::vector<std::pair<uint32_t, uint32_t>>& result) {
    try_collect_request(node_id, result);
}

void batch_manager_t::try_collect_request(uint32_t node_id, std::vector<std::pair<uint32_t, uint32_t>>& result) {
    // collect ready requests from group members
    assert(leader_active == true);
    assert(group->_leader.load() == GET_THD_ID);
    uint64_t cur_time = get_sys_clock();
    uint64_t deadline = cur_time + LEADER_WAIT_TIME;
    std::vector<uint32_t> claimed_members;
    do {
        for (uint32_t i=0; i<batch_table_t::MAX_GROUP_SIZE; i++) {
            auto entry = group->get_entry(i);
            auto value = entry->second[node_id].load();
            if (value > 0) { // try to claim this request
                if (entry->second[node_id].compare_exchange_strong(value, -1)) {
                    // successfully claimed
                    result.push_back({entry->first, value});
                }
                // otherwise, members decided to send it themselves or have not reclaimed its last batch yet
            }
        }
        cur_time = get_sys_clock();
    } while (cur_time < deadline 
             && result.size() < group->get_active_members()
             && global_manager->is_sim_done() == false);
}

void batch_manager_t::update_status(uint32_t node_id, uint32_t member_id, bool wait_response) {
    auto entry = group->get_entry(member_id % batch_table_t::MAX_GROUP_SIZE);
    assert(entry->first == member_id);
    assert(entry->second[node_id].load() == -1);
    assert(member_id / batch_table_t::MAX_GROUP_SIZE == GET_THD_ID / batch_table_t::MAX_GROUP_SIZE);
    entry->second[node_id].store(wait_response ? -2 : 0);
}