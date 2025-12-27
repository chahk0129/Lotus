#include "batch/decision.h"
#include "batch/manager.h"
#include "batch/group.h"
#include "system/manager.h"
#include "utils/helper.h"
#include "utils/timer.h"

batch_decision_t::decision_t batch_decision_t::should_wait_for_batch(batch_manager_t* manager) {
    static uint32_t refresh = 0;
    // Update parameters based on recent performance
    if (refresh++ % 10 == 0)
        update_parameters(manager);
    
    // 1. Calculate scores for batching vs individual sending
    auto batch_score = calculate_batch_score();
    auto individual_score = calculate_individual_score();
    
    // 2. Apply epsilon-greedy exploration
    if (rand_dist(rand_gen) < exploration_rate) {
        // Explore: randomly choose opposite of current best
        return (batch_score > individual_score) ? decision_t::SEND_IMMEDIATELY : decision_t::WAIT_FOR_BATCH;
    }
    
    // 3. Context-aware adjustments
    uint64_t current_time = get_sys_clock();
    uint64_t time_since_last = current_time - last_decision_time;
    last_decision_time = current_time;
    
    // If requests are coming very frequently, batching is more beneficial
    if (time_since_last < 100000) // Less than 100 us between requests
        batch_score *= 1.2;
    
    // 4. Check recent performance patterns
    uint32_t conflicts = manager->stats_conflicts;
    uint32_t timeouts = manager->stats_timeouts;
    uint32_t batches = manager->stats_batches;
    
    // Read-only workload detection: strongly prefer batching when no conflicts
    if (conflicts == 0) {
        batch_score *= 2.0f; // Strong preference for batching under conflict-free conditions
        if (batches > 2) {
            batch_score *= 1.5f; // Even stronger preference with successful batch history
        }
    }
    
    // Adaptive threshold adjustment
    if (conflicts > emergency_threshold) {
        individual_score *= 1.5; // Prefer individual sending
        emergency_threshold = std::min(emergency_threshold + 1.0f, 20.0f);
    } 
    else if (conflicts == 0 && batches > 5)
        emergency_threshold = std::max(emergency_threshold - 0.5f, 5.0f);

    if (timeouts > timeout_threshold) {
        individual_score *= 1.3;
        timeout_threshold = std::min(timeout_threshold + 1.0f, 20.0f);
    } 
    else if (timeouts == 0 && batches > 3)
        timeout_threshold = std::max(timeout_threshold - 0.5f, 3.0f);

    // 5. Make final decision based on scores
    if (batch_score > individual_score)
        return decision_t::WAIT_FOR_BATCH;
    else
        return decision_t::SEND_IMMEDIATELY;
}

uint32_t batch_decision_t::calculate_wait_time(batch_manager_t* manager) {
    // Adaptive wait time based on recent performance and system load
    float base_multiplier = 1.0f;

    // Adjust based on success rates
    if (batch_success_rate > 0.8f)
        base_multiplier *= 1.2f; // Increase wait time if batching is very successful
    else if (batch_success_rate < 0.3f)
        base_multiplier *= 0.7f; // Decrease wait time if batching often fails

    // Adjust based on timeout rate
    if (timeout_rate > 0.2f)
        base_multiplier *= 0.8f; // Reduce wait time if timeouts are frequent
    else if (timeout_rate < 0.05f)
        base_multiplier *= 1.1f; // Increase wait time if timeouts are rare

    // Adjust based on conflict rate
    if (conflict_rate > 0.15f)
        base_multiplier *= 0.9f; // Reduce wait time if conflicts are high

    // Consider recent performance patterns
    uint32_t conflicts = manager->stats_conflicts;
    uint32_t timeouts = manager->stats_timeouts;
    uint32_t batches = manager->stats_batches;

    // Recent performance adjustments
    if (conflicts == 0 && batches > 2) {
        // Read-only workload: increase wait time to collect more requests
        base_multiplier *= 2.0f;
    } else if (conflicts > 3) {
        base_multiplier *= std::max(0.5, 1.0 - (conflicts * 0.1));
    }
    
    if (timeouts > 3)
        base_multiplier *= std::max(0.6, 1.0 - (timeouts * 0.08));
    if (batches > 5)
        base_multiplier *= std::min(1.5, 1.0 + (batches * 0.05));
    
    // Calculate new wait time
    uint32_t new_wait_time = static_cast<uint32_t>(current_wait_time * base_multiplier);
    
    // Smooth the transition using EWMA
    current_wait_time = static_cast<uint32_t>(ALPHA * new_wait_time + (1.0 - ALPHA) * current_wait_time);
    
    // Clamp to reasonable bounds
    current_wait_time = std::clamp(current_wait_time, min_wait_time, max_wait_time);
    
    return current_wait_time;
}

// Performance feedback methods for learning
void batch_decision_t::report_batch_outcome(uint64_t latency, bool success) {
    // Update batch success rate using EWMA
    batch_success_rate = ALPHA * (success ? 1.0f : 0.0f) + (1.0f - ALPHA) * batch_success_rate;
    
    // Update average batch latency
    if (success)
        avg_batch_latency = ALPHA * latency + (1.0f - ALPHA) * avg_batch_latency;

    // Adjust exploration rate based on performance
    if (batch_success_rate > 0.7f)
        exploration_rate = std::max(0.05f, exploration_rate * DECAY_FACTOR);
    else if (batch_success_rate < 0.4f)
        exploration_rate = std::min(0.2f, exploration_rate * 1.05f);
}

void batch_decision_t::report_individual_outcome(uint64_t latency) {
    // Update average individual send latency using EWMA
    avg_individual_latency = ALPHA * latency + (1.0f - ALPHA) * avg_individual_latency;
}

void batch_decision_t::report_timeout() {
    // Update timeout rate using EWMA
    timeout_rate = ALPHA * 1.0 + (1.0 - ALPHA) * timeout_rate;
    
    // Increase exploration when timeouts are frequent
    if (timeout_rate > 0.15f)
        exploration_rate = std::min(0.25f, exploration_rate * 1.1f);
}

void batch_decision_t::report_conflict() {
    // Update conflict rate using EWMA
    conflict_rate = ALPHA * 1.0f + (1.0f - ALPHA) * conflict_rate;

    // Increase exploration when conflicts are frequent
    if (conflict_rate > 0.2f)
        exploration_rate = std::min(0.3f, exploration_rate * 1.15f);
}

void batch_decision_t::update_parameters(batch_manager_t* manager) {
    // Decay rates over time to prevent stale information
    batch_success_rate *= (1.0f - ALPHA * 0.1f);
    timeout_rate *= DECAY_FACTOR;
    conflict_rate *= DECAY_FACTOR;
    
    // Adaptive threshold updates based on overall system performance
    float efficiency = batch_success_rate * (1.0f - timeout_rate) * (1.0f - conflict_rate);

    if (efficiency > 0.8f || (conflict_rate < 0.05f && batch_success_rate > 0.6f)) {
        // performing well or read-only workload, can be more aggressive with batching
        emergency_threshold = std::max(emergency_threshold - 0.2f, 3.0f);
        timeout_threshold = std::max(timeout_threshold - 0.1f, 3.0f);
        batch_success_threshold = std::max(batch_success_threshold - 0.1f, 2.0f);
    } 
    else if (efficiency < 0.5f && conflict_rate > 0.15f) {
        // struggling with conflicts, be more conservative
        emergency_threshold = std::min(emergency_threshold + 0.2f, 15.0f);
        timeout_threshold = std::min(timeout_threshold + 0.2f, 15.0f);
        batch_success_threshold = std::min(batch_success_threshold + 0.1f, 8.0f);
    }

    manager->stats_conflicts *= 0.5;
    manager->stats_timeouts *= 0.5;
    manager->stats_batches *= 0.5;
}

float batch_decision_t::calculate_batch_score() const {
    float score = 0.0f;

    // Base score from batch success rate
    score += batch_success_rate * 100.0f;

    // Read-only workload bonus: if conflict rate is very low, give huge bonus to batching
    if (conflict_rate < 0.05f) { // Less than 5% conflict rate indicates read-only workload
        score += 100.0f; // Large bonus for read-only workloads
    }

    // Penalty for timeouts and conflicts
    score -= timeout_rate * 50.0f;
    score -= conflict_rate * 60.0f; // Increased penalty for conflicts to detect read-only vs read-write

    // Latency consideration - prefer batching if it's faster on average
    if (avg_batch_latency > 0 && avg_individual_latency > 0) {
        float latency_ratio = avg_individual_latency / avg_batch_latency;
        if (latency_ratio > 1.0f)
            score += (latency_ratio - 1.0f) * 30.0f; // Bonus if batch is faster
        else
            score -= (1.0f - latency_ratio) * 25.0f; // Penalty if batch is slower
    }
    
    // Bonus for network efficiency (batching reduces network overhead)
    score += 20.0f; // Base network efficiency bonus

    return std::max(0.0f, score);
}

float batch_decision_t::calculate_individual_score() const {
    float score = 50.0f; // Base score for individual sending
    
    // Bonus for reliability (no timeouts/conflicts in individual sends)
    score += (1.0f - timeout_rate) * 30.0f;
    score += (1.0f - conflict_rate) * 25.0f;

    // Latency consideration
    if (avg_individual_latency > 0 && avg_batch_latency > 0) {
        float latency_ratio = avg_batch_latency / avg_individual_latency;
        if (latency_ratio > 1.0f)
            score += (latency_ratio - 1.0f) * 35.0f; // Bonus if individual is faster
    }
    
    // Penalty for poor batch performance
    if (batch_success_rate < 0.5f)
        score += (0.5f - batch_success_rate) * 40.0f;

    return std::max(0.0f, score);
}