#include "utils/tls_thread.h"

// Static member definitions - moved from header to avoid multiple definition errors
std::unordered_map<std::thread::id, Thread::TlsList *> Thread::registry_;
std::mutex Thread::registryMutex_;

void Thread::RegisterTls(uint64_t *ptr, uint64_t val) {
  auto id = std::this_thread::get_id();
  std::unique_lock<std::mutex> lock(registryMutex_);
  if (registry_.find(id) == registry_.end()) {
    registry_.emplace(id, new TlsList);
  }
  registry_[id]->emplace_back(ptr, val);
}

void Thread::ClearTls(bool destroy) {
  std::unique_lock<std::mutex> lock(registryMutex_);
  auto id = std::this_thread::get_id();
  if (registry_.find(id) == registry_.end()) {
    return;
  }

  auto *list = registry_[id];
  for (auto &item : *list) {
    *(item.first) = item.second;
  }

  if (destroy) {
    delete list;
    registry_.erase(id);
  } else {
    list->clear();
  }
}

void Thread::ClearRegistry(bool destroy) {
  std::unique_lock<std::mutex> lock(registryMutex_);
  for (auto &entry : registry_) {
    auto *list = entry.second;
    if (destroy) {
      delete list;
    } else {
      for (auto &item : *list) {
        *(item.first) = item.second;
      }
      list->clear();
    }
  }
  if (destroy) {
    registry_.clear();
  }
}
