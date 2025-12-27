#pragma once
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>

#include <atomic>
#include <bitset>
#include <limits>
#include <queue>
#include <utility>
#include "system/global.h"
#include "system/global_address.h"
#include <boost/crc.hpp>
#include <boost/coroutine/all.hpp>
#include "index/WRLock.h"

#if !PARTITIONED
namespace sherman {
// CONFIG_ENABLE_EMBEDDING_LOCK and CONFIG_ENABLE_CRC
// **cannot** be ON at the same time

// #define CONFIG_ENABLE_EMBEDDING_LOCK
// #define CONFIG_ENABLE_CRC

// #define LEARNED 1
// #define PAGE_OFFSET 1

#define LATENCY_WINDOWS 1000000

#define STRUCT_OFFSET(type, field)                                             \
  (char *)&((type *)(0))->field - (char *)((type *)(0))

#define UNUSED(x) (void)(x)

#define ADD_ROUND(x, n) ((x) = ((x) + 1) % (n))

#define MESSAGE_SIZE 96 // byte

#define DIR_MESSAGE_NR 512

// #define LOCK_VERSION 1

inline int bits_in(std::uint64_t u) {
  auto bs = std::bitset<64>(u);
  return bs.count();
}

using CoroYield = boost::coroutines::symmetric_coroutine<void>::yield_type;
using CoroCall = boost::coroutines::symmetric_coroutine<void>::call_type;

using CheckFunc = std::function<bool()>;
using CoroQueue = std::queue<std::pair<uint16_t, CheckFunc>>;
struct CoroContext {
  CoroYield *yield;
  CoroCall *master;
  CoroQueue *busy_waiting_queue;
  int coro_id;
};

namespace define {

constexpr uint64_t MB = 1024ull * 1024;
constexpr uint64_t GB = 1024ull * MB;
constexpr uint16_t kCacheLineSize = 64;

// for remote allocate
constexpr uint64_t dsmSize = 64; // GB  [CONFIG]
constexpr uint64_t kChunkSize = 32 * MB;

// RDMA buffer
constexpr uint64_t rdmaBufferSize = 2; // GB  [CONFIG]
constexpr int64_t aligned_cache = ~((1ULL << 6) - 1);
constexpr int64_t kPerThreadRdmaBuf =
    (rdmaBufferSize * define::GB / MAX_NUM_THREADS) & aligned_cache;
// constexpr int64_t kSmartPerCoroRdmaBuf =
    // (kPerThreadRdmaBuf / MAX_CORO_NUM) & aligned_cache;
constexpr int64_t kPerCoroRdmaBuf = 128 * 1024;

// for store root pointer
constexpr uint64_t kRootPointerStoreOffest = kChunkSize / 2;
static_assert(kRootPointerStoreOffest % sizeof(uint64_t) == 0, "XX");

// lock on-chip memory
constexpr uint64_t kLockStartAddr = 0;
constexpr uint64_t kLockChipMemSize = DEVICE_MEMORY_SIZE;
// constexpr uint64_t kLockChipMemSize = 128 * 1024;

// number of locks
// we do not use 16-bit locks, since 64-bit locks can provide enough
// concurrency. if you want to use 16-bit locks, call *cas_dm_mask*
#ifdef LOCK_VERSION
constexpr uint64_t kNumOfLock = kLockChipMemSize / (sizeof(uint64_t) * 2);
#else
constexpr uint64_t kNumOfLock = kLockChipMemSize / sizeof(uint64_t);
#endif

// For SMART
constexpr uint64_t kLocalLockNum =
    4 * MB; // tune to an appropriate value (as small as possible without affect
            // the performance)
constexpr uint64_t kOnChipLockNum = kLockChipMemSize * 8; // 1bit-lock

// level of tree
constexpr uint64_t kMaxLevelOfTree = 10;

// To align with MAX_CORO_NUM
constexpr uint16_t kMaxCoro = 8;
// constexpr int64_t kPerCoroRdmaBuf = 128 * 1024;

constexpr uint8_t kMaxHandOverTime = 8;

constexpr int kIndexCacheSize = 512; // MB
} // namespace define

static inline unsigned long long asm_rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

// For Tree
using Value = uint64_t;
#define KEY_SIZE 8
constexpr Key kKeyMin = std::numeric_limits<Key>::min();
constexpr Key kKeyMax = std::numeric_limits<Key>::max();
constexpr Value kValueNull = 0;
// constexpr uint32_t kInternalPageSize = 512;
// constexpr uint32_t kLeafPageSize = 512;
constexpr uint32_t kInternalPageSize = 1024;
constexpr uint32_t kLeafPageSize = 1024;
}
#endif