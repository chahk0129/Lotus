#pragma once
#include "system/global.h"
#include "system/global_address.h"
#include <boost/crc.hpp>
#include <boost/coroutine/all.hpp>

#if !PARTITIONED
namespace deft {

using CoroYield = boost::coroutines::symmetric_coroutine<void>::yield_type;
using CoroCall = boost::coroutines::symmetric_coroutine<void>::call_type;

struct CoroContext {
  CoroYield *yield;
  CoroCall *master;
  int coro_id;
};

#define LATENCY_WINDOWS 1000000
#define STRUCT_OFFSET(type, field) \
  (char *)&((type *)(0))->field - (char *)((type *)(0))
#define ADD_ROUND(x, n) ((x) = ((x) + 1) % (n))

namespace define {

constexpr uint64_t MB = 1024ull * 1024;
constexpr uint64_t GB = 1024ull * MB;
constexpr uint16_t kCacheLineSize = 64;

// for remote allocate
constexpr uint64_t kChunkSize = 16 * MB;

// for store root pointer
constexpr uint64_t kRootPointerStoreOffest = kChunkSize / 2;
static_assert(kRootPointerStoreOffest % sizeof(uint64_t) == 0, "XX");

// lock on-chip memory
constexpr uint64_t kLockStartAddr = 0;
constexpr uint64_t kLockChipMemSize = DEVICE_MEMORY_SIZE; // 128 KB

// number of locks
// we do not use 16-bit locks, since 64-bit locks can provide enough concurrency.
// if you want to use 16-bit locks, call *cas_dm_mask*
constexpr uint64_t kLockSize = 16;
constexpr uint64_t kNumOfLock = kLockChipMemSize / kLockSize;

// level of tree
constexpr uint64_t kMaxLevelOfTree = 7;

constexpr uint16_t kMaxCoro = 8;
constexpr int64_t kPerThreadRdmaBuf  = 12 * MB;
constexpr int64_t kPerCoroRdmaBuf = kPerThreadRdmaBuf / kMaxCoro;

constexpr uint8_t kMaxHandOverTime = 0;

constexpr int kIndexCacheSize = 1024;  // MB
} // namespace define

using InternalKey = Key;
using Value = uint64_t;
constexpr Value kValueNull = 0;

#define KEY_SIZE 8
constexpr Key kKeyMin = std::numeric_limits<Key>::min();
constexpr Key kKeyMax = std::numeric_limits<Key>::max();
// fixed for variable length key
constexpr size_t kHeaderRawSize = 30 + 2 * sizeof(InternalKey) + 16;
constexpr size_t kHeaderSize = (kHeaderRawSize + 63) / 64 * 64;
constexpr uint32_t kPageSize =
    (kHeaderSize + 60 * (sizeof(InternalKey) + sizeof(uint64_t)) + 63) / 64 *
    64;
constexpr uint32_t kInternalPageSize = kPageSize;
constexpr uint32_t kLeafPageSize = kPageSize;
constexpr int kMaxInternalGroup = 4;


enum internal_granularity { gran_full, gran_half, gran_quarter };

}
#endif