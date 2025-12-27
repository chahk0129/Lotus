#include "leanstore_cache.h"

namespace cachepush {
// Global variable definitions moved from header to avoid multiple definition errors
int probing_length = 8;
int num_pages_to_sample = 2;

uint64_t sample_num = 0;
uint64_t total_sample_loop = 0;
uint64_t wrong_page_state = 0;
uint64_t wrong_iteration = 0;
uint64_t wrong_parent = 0;
uint64_t wrong_parent_lock = 0;
uint64_t wrong_parent_check = 0;
uint64_t wrong_evict = 0;
uint64_t one_state = 0;
uint64_t zero_state = 0;
uint64_t three_state = 0;
}
