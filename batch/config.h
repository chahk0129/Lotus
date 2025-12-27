#pragma once

/*
 * Adaptive Batching Configuration
 * 
 * This file contains tunable parameters for the dynamic batching system.
 * Adjust these values based on your specific workload characteristics.
 */

namespace batch_config {

// Learning parameters
constexpr double LEARNING_RATE = 0.1;        // How quickly to adapt to new information
constexpr double DECAY_FACTOR = 0.95;        // How quickly to forget old information
constexpr double EXPLORATION_RATE = 0.1;     // Epsilon for epsilon-greedy exploration

// Performance thresholds
constexpr double TARGET_BATCH_SUCCESS_RATE = 0.8;   // Target success rate for batching
constexpr double MIN_BATCH_SUCCESS_RATE = 0.3;      // Minimum acceptable success rate
constexpr double MAX_TIMEOUT_RATE = 0.15;           // Maximum acceptable timeout rate
constexpr double MAX_CONFLICT_RATE = 0.2;           // Maximum acceptable conflict rate

// Wait time bounds (in CPU cycles)
constexpr uint32_t MIN_WAIT_TIME = 5000;      // 5ms minimum wait
constexpr uint32_t MAX_WAIT_TIME = 100000;    // 100ms maximum wait
constexpr uint32_t DEFAULT_WAIT_TIME = 30000; // 30ms default

// Adaptive threshold bounds
constexpr double MIN_EMERGENCY_THRESHOLD = 5.0;
constexpr double MAX_EMERGENCY_THRESHOLD = 20.0;
constexpr double MIN_TIMEOUT_THRESHOLD = 3.0;
constexpr double MAX_TIMEOUT_THRESHOLD = 15.0;

// Context sensitivity
constexpr uint64_t HIGH_FREQUENCY_THRESHOLD = 1000;  // CPU cycles between requests
constexpr double HIGH_FREQUENCY_BATCH_BONUS = 1.2;   // Multiplier for batch score

// Score weights
constexpr double SUCCESS_RATE_WEIGHT = 100.0;
constexpr double TIMEOUT_PENALTY_WEIGHT = 50.0;
constexpr double CONFLICT_PENALTY_WEIGHT = 40.0;
constexpr double LATENCY_WEIGHT = 30.0;
constexpr double NETWORK_EFFICIENCY_BONUS = 20.0;

// System efficiency thresholds
constexpr double HIGH_EFFICIENCY_THRESHOLD = 0.8;
constexpr double LOW_EFFICIENCY_THRESHOLD = 0.5;

// Workload-specific optimizations
enum class WorkloadType {
    LATENCY_SENSITIVE,    // Prioritize low latency
    THROUGHPUT_ORIENTED,  // Prioritize high throughput
    BALANCED,            // Balance latency and throughput
    ADAPTIVE             // Automatically detect workload type
};

// Default workload type
constexpr WorkloadType DEFAULT_WORKLOAD = WorkloadType::ADAPTIVE;

} // namespace batch_config
