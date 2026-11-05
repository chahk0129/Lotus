#pragma once

#include "utils/tls_thread.h"
#include "utils/utils.h"
#include <atomic>
#include <cstdint>
#include <list>
#include <mutex>
#include <thread>

namespace cachepush {

typedef uint64_t Epoch;

class EpochManager {
public:
  // static uint64_t global_epoch_;

  EpochManager();
  ~EpochManager();

  bool Initialize();
  bool Uninitialize();

  bool Protect() {
    return epoch_table_->Protect(
        current_epoch_.load(std::memory_order_relaxed));
  }

  bool Unprotect() {
    return epoch_table_->Unprotect(
        current_epoch_.load(std::memory_order_relaxed));
  }

  Epoch GetCurrentEpoch() {
    return current_epoch_.load(std::memory_order_seq_cst);
  }

  bool IsSafeToReclaim(Epoch epoch) {
    return epoch <= safe_to_reclaim_epoch_.load(std::memory_order_relaxed);
  }

  Epoch GetReclaimEpoch() {
    return safe_to_reclaim_epoch_.load(std::memory_order_seq_cst);
  }

  bool IsProtected() { return epoch_table_->IsProtected(); }

  void BumpCurrentEpoch();

public:
  void ComputeNewSafeToReclaimEpoch(Epoch currentEpoch);

  class MinEpochTable {
  public:
    enum { CACHELINE_SIZE = 64 };

    static const uint64_t kDefaultSize = 128;

    MinEpochTable();
    bool Initialize(uint64_t size = MinEpochTable::kDefaultSize);
    bool Uninitialize();
    bool Protect(Epoch currentEpoch);
    bool Unprotect(Epoch currentEpoch);

    Epoch ComputeNewSafeToReclaimEpoch(Epoch currentEpoch);

    struct Entry {
      /// Construct an Entry in an unlocked and ready to use state.
      Entry() : protected_epoch{0}, last_unprotected_epoch{0}, thread_id{0} {}

      std::atomic<Epoch> protected_epoch; // 8 bytes

      // BT(FIXME): usage of this parameter?
      Epoch last_unprotected_epoch; //  8 bytes

      std::atomic<uint64_t> thread_id; //  8 bytes

      char ___padding[40];

      void *operator new[](uint64_t count) {
        void *mem = nullptr;
        auto ret = posix_memalign(&mem, CACHELINE_SIZE, count);
        ((void)ret);
        return mem;
      }

      void operator delete[](void *p) { free(p); }

      void *operator new(uint64_t count);

      void operator delete(void *p);
    };
    static_assert(sizeof(Entry) == CACHELINE_SIZE,
                  "Unexpected table entry size");

  public:
    bool GetEntryForThread(Entry **entry);
    Entry *ReserveEntry(uint64_t startIndex, uint64_t threadId);
    Entry *ReserveEntryForThread();
    void ReleaseEntryForThread();
    void ReclaimOldEntries();
    bool IsProtected();

  private:
    Entry *table_;
    uint64_t size_;
  };
  // FIXME(BT): when should the current_epoch be bumped?
  // Is it computed periodically when the garbage size
  // reaches to a threshold? But it is inappliable in
  // my case
  std::atomic<Epoch> current_epoch_;
  // FIXME(BT): usage of this epoch
  // When should following parameter be bumped?
  std::atomic<Epoch> safe_to_reclaim_epoch_;
  MinEpochTable *epoch_table_;

  EpochManager(const EpochManager &) = delete;
  EpochManager(EpochManager &&) = delete;
  EpochManager &operator=(EpochManager &&) = delete;
  EpochManager &operator=(const EpochManager &) = delete;
};

/// Enters an epoch on construction and exits it on destruction. Makes it
/// easy to ensure epoch protection boundaries tightly adhere to stack life
/// time even with complex control flow.
class EpochGuard {
public:
  explicit EpochGuard(EpochManager *epoch_manager)
      : epoch_manager_{epoch_manager}, unprotect_at_exit_(true) {
    epoch_manager_->Protect();
  }

  /// Offer the option of having protext called on \a epoch_manager.
  /// When protect = false this implies "attach" semantics and the caller should
  /// have already called Protect. Behavior is undefined otherwise.
  explicit EpochGuard(EpochManager *epoch_manager, bool protect)
      : epoch_manager_{epoch_manager}, unprotect_at_exit_(protect) {
    if (protect) {
      epoch_manager_->Protect();
    }
  }

  ~EpochGuard() {
    if (unprotect_at_exit_ && epoch_manager_) {
      epoch_manager_->Unprotect();
    }
  }

  /// Release the current epoch manger. It is up to the caller to manually
  /// Unprotect the epoch returned. Unprotect will not be called upon EpochGuard
  /// desruction.
  EpochManager *Release() {
    EpochManager *ret = epoch_manager_;
    epoch_manager_ = nullptr;
    return ret;
  }

private:
  /// The epoch manager responsible for protect/unprotect.
  EpochManager *epoch_manager_;

  /// Whether the guard should call unprotect when going out of scope.
  bool unprotect_at_exit_;
};

} // namespace cachepush
