#pragma once

#include <cstdint>

// Sizing constants for inter-transaction batching (batch/combiner.h).
namespace batch_limits {
// Each batch buffer (send and receive, per server and batch slot) holds this many
// max-size messages.
constexpr uint32_t BUFFER_MSGS = 8;
// Number of buckets in the batch-size histogram (stats).
constexpr uint32_t HIST_SIZE = 64;
}
