#include "utils/cache_allocator.h"

namespace cachepush {
// Static member definition to avoid multiple definition errors
cache_allocator *cache_allocator::instance_ = nullptr;
}