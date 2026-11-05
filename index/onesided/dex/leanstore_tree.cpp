#include "leanstore_tree.h"

namespace cachepush {
// Global variable definitions moved from header to avoid multiple definition errors
uint64_t push_down_counter = 0;
uint64_t cache_miss_counter = 0;
uint64_t admission_counter = 0;
uint64_t cache_miss_array[8] = {
    0, 0, 0, 0, 0, 0, 0, 0}; // Collect the cache miss in bottom 5 levels

// Thread-local variable definitions
thread_local uint64_t total_nanoseconds = 0;
thread_local uint64_t total_sample_times = 0;
}
