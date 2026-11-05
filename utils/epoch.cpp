#include "utils/epoch.h"
#include "utils/utils.h"

namespace cachepush {

// EpochManager implementations
EpochManager::EpochManager()
    : current_epoch_{1}, safe_to_reclaim_epoch_{0}, epoch_table_{nullptr} {}

EpochManager::~EpochManager() { 
    Uninitialize(); 
}

bool EpochManager::Initialize() {
  if (epoch_table_)
    return true;

  MinEpochTable *new_table = new MinEpochTable();

  if (new_table == nullptr)
    return false;

  auto rv = new_table->Initialize();
  if (!rv)
    return rv;

  current_epoch_ = 1;
  safe_to_reclaim_epoch_ = 0;
  epoch_table_ = new_table;

  return true;
}

bool EpochManager::Uninitialize() {
  if (!epoch_table_)
    return true;

  auto s = epoch_table_->Uninitialize();

  // Keep going anyway. Even if the inner table fails to completely
  // clean up we want to clean up as much as possible.
  delete epoch_table_;
  epoch_table_ = nullptr;
  current_epoch_ = 1;
  safe_to_reclaim_epoch_ = 0;

  return s;
}

void EpochManager::BumpCurrentEpoch() {
  Epoch newEpoch = current_epoch_.fetch_add(1, std::memory_order_seq_cst);
  ComputeNewSafeToReclaimEpoch(newEpoch);
}

void EpochManager::ComputeNewSafeToReclaimEpoch(Epoch currentEpoch) {
  safe_to_reclaim_epoch_.store(
      epoch_table_->ComputeNewSafeToReclaimEpoch(currentEpoch),
      std::memory_order_release);
}

// MinEpochTable implementations
EpochManager::MinEpochTable::MinEpochTable() : table_{nullptr}, size_{} {}

bool EpochManager::MinEpochTable::Initialize(uint64_t size) {
  if (table_)
    return true;

  if (!IS_POWER_OF_TWO(size))
    return false;

  Entry *new_table = new Entry[size];
  if (!new_table)
    return false;

  table_ = new_table;
  size_ = size;

  return true;
}

bool EpochManager::MinEpochTable::Uninitialize() {
  if (!table_)
    return true;

  size_ = 0;
  delete[] table_;
  table_ = nullptr;

  return true;
}

bool EpochManager::MinEpochTable::Protect(Epoch current_epoch) {
  Entry *entry = nullptr;
  if (!GetEntryForThread(&entry)) {
    return false;
  }

  entry->last_unprotected_epoch = 0;
#if 1
  entry->protected_epoch.store(current_epoch, std::memory_order_release);
  // TODO: For this to really make sense according to the spec we
  // need a (relaxed) load on entry->protected_epoch. What we want to
  // ensure is that loads "above" this point in this code don't leak down
  // and access data structures before it is safe.
  // Consistent with http://preshing.com/20130922/acquire-and-release-fences/
  // but less clear whether it is consistent with stdc++.
  std::atomic_thread_fence(std::memory_order_acquire);
#else
  entry->m_protectedEpoch.exchange(currentEpoch, std::memory_order_acq_rel);
#endif
  return true;
}

bool EpochManager::MinEpochTable::Unprotect(Epoch currentEpoch) {
  Entry *entry = nullptr;
  if (!GetEntryForThread(&entry)) {
    return false;
  }

  entry->last_unprotected_epoch = currentEpoch;
  std::atomic_thread_fence(std::memory_order_release);
  // 0 means not protected
  entry->protected_epoch.store(0, std::memory_order_relaxed);
  return true;
}

Epoch EpochManager::MinEpochTable::ComputeNewSafeToReclaimEpoch(
    Epoch current_epoch) {
  Epoch oldest_call = current_epoch;
  for (uint64_t i = 0; i < size_; ++i) {
    Entry &entry = table_[i];
    // If any other thread has flushed a protected epoch to the cache
    // hierarchy we're guaranteed to see it even with relaxed access.
    Epoch entryEpoch = entry.protected_epoch.load(std::memory_order_acquire);
    if (entryEpoch != 0 && entryEpoch < oldest_call) {
      oldest_call = entryEpoch;
    }
  }
  // The latest safe epoch is the one just before the earlier unsafe one.
  return oldest_call - 1;
}

bool EpochManager::MinEpochTable::GetEntryForThread(Entry **entry) {
  thread_local Entry *tls = nullptr;
  if (tls) {
    *entry = tls;
    return true;
  }

  // No entry index was found in TLS, so we need to reserve a new entry
  // and record its index in TLS
  Entry *reserved = ReserveEntryForThread();
  tls = *entry = reserved;

  Thread::RegisterTls((uint64_t *)&tls, (uint64_t) nullptr);

  return true;
}

uint32_t Murmur3(uint32_t h) {
  h ^= h >> 16;
  h *= 0x85ebca6b;
  h ^= h >> 13;
  h *= 0xc2b2ae35;
  h ^= h >> 16;
  return h;
}

EpochManager::MinEpochTable::Entry *
EpochManager::MinEpochTable::ReserveEntryForThread() {
  uint64_t current_thread_id = pthread_self();
  uint64_t startIndex = Murmur3_64(current_thread_id);
  return ReserveEntry(startIndex, current_thread_id);
}

EpochManager::MinEpochTable::Entry *
EpochManager::MinEpochTable::ReserveEntry(uint64_t start_index,
                                          uint64_t thread_id) {
  for (;;) {
    // Reserve an entry in the table.
    for (uint64_t i = 0; i < size_; ++i) {
      uint64_t indexToTest = (start_index + i) & (size_ - 1);
      Entry &entry = table_[indexToTest];
      if (entry.thread_id == 0) {
        uint64_t expected = 0;
        // Atomically grab a slot. No memory barriers needed.
        // Once the threadId is in place the slot is locked.
        bool success = entry.thread_id.compare_exchange_strong(
            expected, thread_id, std::memory_order_relaxed);
        if (success) {
          return &table_[indexToTest];
        }
        // Ignore the CAS failure since the entry must be populated,
        // just move on to the next entry.
      }
    }
    ReclaimOldEntries();
  }
}

bool EpochManager::MinEpochTable::IsProtected() {
  Entry *entry = nullptr;
#ifdef TEST_BUILD
  auto s = GetEntryForThread(&entry);
  CHECK_EQ(s, true);
#else
  GetEntryForThread(&entry);
#endif
  // It's myself checking my own protected_epoch, safe to use relaxed
  return entry->protected_epoch.load(std::memory_order_relaxed) != 0;
}

void EpochManager::MinEpochTable::ReleaseEntryForThread() {}

void EpochManager::MinEpochTable::ReclaimOldEntries() {}

// Entry operator new/delete implementations
void *EpochManager::MinEpochTable::Entry::operator new(uint64_t count) {
  void *mem = nullptr;
  auto ret = posix_memalign(&mem, CACHELINE_SIZE, count);
  ((void)ret);
  return mem;
}

void EpochManager::MinEpochTable::Entry::operator delete(void *p) {
  free(p);
}

} // namespace cachepush
