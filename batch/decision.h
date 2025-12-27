#pragma once
#include <algorithm>
#include <cstdint>
#include <random>
#include <array>
#include <chrono>

class batch_manager_t;

class batch_decision_t {
private:
    // Dynamic thresholds that adapt based on performance
    float emergency_threshold = 10.0;      
    float timeout_threshold = 10.0;       
    float batch_success_threshold = 5.0;   
    
    // EWMA parameters for tracking performance metrics
    static constexpr float ALPHA = 0.1;  // Learning rate for EWMA
    static constexpr float DECAY_FACTOR = 0.9;  // Decay for older observations

    // Performance tracking with EWMA
    float avg_batch_latency = 0.0;
    float avg_individual_latency = 0.0;
    float batch_success_rate = 0.9;  // Start at 50%
    float timeout_rate = 0.1;        // Start at 10%
    float conflict_rate = 0.1;       // Start at 10%
    
    // Multi-armed bandit exploration
    std::uniform_real_distribution<float> rand_dist;
    std::mt19937 rand_gen;
    float exploration_rate = 0.1;  // Epsilon for epsilon-greedy

    // Adaptive wait time parameters
    // uint32_t min_wait_time = 10000; // 1us minimum
    // uint32_t max_wait_time = 100000; // 10us maximum
    // uint32_t current_wait_time = 10000; // Start with 10us
    uint32_t min_wait_time = 1000; // 1us minimum
    uint32_t max_wait_time = 50000; // 10us maximum
    uint32_t current_wait_time = 10000; // Start with 10us
    // uint32_t min_wait_time = 1000; // 1us minimum
    // uint32_t max_wait_time = 10000; // 10us maximum
    // uint32_t current_wait_time = 10000; // Start with 10us

    // Context tracking for better decisions
    uint64_t last_decision_time = 0;
    uint32_t current_load_estimate = 0;
public:
    batch_decision_t(): rand_dist(0.0, 1.0), rand_gen(std::random_device{}()) { }
    ~batch_decision_t() { }

    enum decision_t {
        SEND_IMMEDIATELY, // don't wait, send right away
        WAIT_FOR_BATCH,   // wait for batching
    };
    
    decision_t should_wait_for_batch(batch_manager_t* manager); 
    uint32_t calculate_wait_time(batch_manager_t* manager);
    
    // Performance feedback methods for learning
    void report_batch_outcome(uint64_t latency, bool success);
    void report_individual_outcome(uint64_t latency);
    void report_timeout();
    void report_conflict();
    
    // Adaptive parameter updates
    void update_parameters(batch_manager_t* manager);
    float calculate_batch_score() const;
    float calculate_individual_score() const;
};