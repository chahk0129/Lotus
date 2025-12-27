#pragma once
#include "index/idx_wrapper.h"
#include "index/onesided/sherman/Common.h"
#include "system/global.h"
#include "system/global_address.h"
#include "utils/packetize.h"
#include <atomic>
#include <city.h>
#include <functional>
#include <iostream>

#if !PARTITIONED
namespace sherman {

class IndexCache;

struct LocalLockNode {
  std::atomic<uint64_t> ticket_lock;
  bool hand_over;
  uint8_t hand_time;
};

struct Request {
  bool is_search;
  Key k;
  Value v;
};

class RequstGen {
public:
  RequstGen() = default;
  virtual Request next() { return Request{}; }
};

struct SearchResult {
  bool is_leaf;
  uint8_t level;
  global_addr_t slibing;
  global_addr_t next_level;
  Value val;
};

class InternalPage;
class LeafPage;
class Tree : public idx_wrapper_t<value_t> {

public:
  Tree(uint32_t tree_id, uint64_t index_cache_size = 0);

  // overriding functions
  void insert(Key k, value_t v) { insert(k, v.val); }
  bool update(Key k, value_t v) { insert(k, v.val); return true; }
  bool lookup(Key k, value_t &v) { 
    Value val;
    bool ret = search(k, val);
    v.val = val;
    return ret; }
  bool remove(Key k) { del(k); return true; }
  int scan(Key k, int range, value_t*& buffer) { return 0; } // TODO

  // dummy functions
  int search_cache(Key k, UnstructuredBuffer& buffer) { return 0; }
  void invalidate(char* entry) {}
  void add_to_cache(Key k, char* page) {}
  // void add_to_cache(char* page) {}


  // original functions
  void insert(const Key &k, const Value &v, CoroContext *cxt = nullptr,
              int coro_id = 0);
  bool search(const Key &k, Value &v, CoroContext *cxt = nullptr,
              int coro_id = 0);
  void del(const Key &k, CoroContext *cxt = nullptr, int coro_id = 0);

  uint64_t range_query(const Key &from, const Key &to, Value *buffer,
                       CoroContext *cxt = nullptr, int coro_id = 0);

  void print_and_check_tree(CoroContext *cxt = nullptr, int coro_id = 0);

  // void run_coroutine(CoroFunc func, int id, int coro_cnt);

  // void lock_bench(const Key &k, CoroContext *cxt = nullptr, int coro_id = 0);

  void index_cache_statistics();
  void clear_statistics();

private:
  uint32_t tree_id;
  uint64_t index_cache_size;
  global_addr_t root_ptr_ptr; // the address which stores root pointer;

  // static thread_local int coro_id;
  static thread_local CoroCall worker[define::kMaxCoro];
  static thread_local CoroCall master;

  LocalLockNode **local_locks;

  IndexCache *index_cache;

  void print_verbose();
  void initializeRoot();
  void before_operation(CoroContext *cxt, int coro_id);

  global_addr_t get_root_ptr_ptr();
  global_addr_t get_root_ptr(CoroContext *cxt, int coro_id);

  void coro_worker(CoroYield &yield, RequstGen *gen, int coro_id);
  void coro_master(CoroYield &yield, int coro_cnt);

  void broadcast_new_root(global_addr_t new_root_addr, int root_level);
  bool update_new_root(global_addr_t left, const Key &k, global_addr_t right,
                       int level, global_addr_t old_root, CoroContext *cxt,
                       int coro_id);

  void insert_internal(const Key &k, global_addr_t v, CoroContext *cxt,
                       int coro_id, int level);

  bool try_lock_addr(global_addr_t lock_addr, uint64_t tag, uint64_t *buf,
                     CoroContext *cxt, int coro_id);
  void unlock_addr(global_addr_t lock_addr, uint64_t tag, uint64_t *buf,
                   CoroContext *cxt, int coro_id, bool async);
  void write_page_and_unlock(char *page_buffer, global_addr_t page_addr,
                             int page_size, uint64_t *cas_buffer,
                             global_addr_t lock_addr, uint64_t tag,
                             CoroContext *cxt, int coro_id, bool async);
  void lock_and_read_page(char *page_buffer, global_addr_t page_addr,
                          int page_size, uint64_t *cas_buffer,
                          global_addr_t lock_addr, uint64_t tag,
                          CoroContext *cxt, int coro_id);

  bool page_search(global_addr_t page_addr, const Key &k, SearchResult &result,
                   CoroContext *cxt, int coro_id, bool from_cache = false);
  void internal_page_search(InternalPage *page, const Key &k,
                            SearchResult &result);
  void leaf_page_search(LeafPage *page, const Key &k, SearchResult &result);

  void internal_page_store(global_addr_t page_addr, const Key &k,
                           global_addr_t value, global_addr_t root, int level,
                           CoroContext *cxt, int coro_id);
  bool leaf_page_store(global_addr_t page_addr, const Key &k, const Value &v,
                       global_addr_t root, int level, CoroContext *cxt,
                       int coro_id, bool from_cache = false);
  bool leaf_page_del(global_addr_t page_addr, const Key &k, int level,
                     CoroContext *cxt, int coro_id, bool from_cache = false);

  bool acquire_local_lock(global_addr_t lock_addr, CoroContext *cxt,
                          int coro_id);
  bool can_hand_over(global_addr_t lock_addr);
  void releases_local_lock(global_addr_t lock_addr);

  void debug_check_root(const char* location);
};

class Header {
private:
  global_addr_t leftmost_ptr;
  global_addr_t sibling_ptr;
  uint8_t level;
  int16_t last_index;
  Key lowest;
  Key highest;

  friend class InternalPage;
  friend class LeafPage;
  friend class Tree;
  friend class IndexCache;

public:
  Header() {
    leftmost_ptr = global_addr_t::null();
    sibling_ptr = global_addr_t::null();
    last_index = -1;
    lowest = kKeyMin;
    highest = kKeyMax;
  }

  void debug() const {
  //   std::cout << "leftmost=" << leftmost_ptr << ", "
  //             << "sibling=" << sibling_ptr << ", "
  //             << "level=" << (int)level << ","
  //             << "cnt=" << last_index + 1 << ","
  //             << "range=[" << lowest << " - " << highest << "]";
  }
} __attribute__((packed));
;

class InternalEntry {
public:
  Key key;
  global_addr_t ptr;

  InternalEntry() {
    ptr = global_addr_t::null();
    key = 0;
  }
} __attribute__((packed));

class LeafEntry {
public:
  uint8_t f_version : 4;
  Key key;
  Value value;
  uint8_t r_version : 4;

  LeafEntry() {
    f_version = 0;
    r_version = 0;
    value = kValueNull;
    key = 0;
  }
} __attribute__((packed));

constexpr int kInternalCardinality = (kInternalPageSize - sizeof(Header) -
                                      sizeof(uint8_t) * 2 - sizeof(uint64_t)) /
                                     sizeof(InternalEntry);
constexpr int kLeafCardinality =
    (kLeafPageSize - sizeof(Header) - sizeof(uint8_t) * 2 - sizeof(uint64_t)) /
    sizeof(LeafEntry);

class InternalPage {
private:
  union {
    uint32_t crc;
    uint64_t embedding_lock;
    uint64_t index_cache_freq;
  };

  uint8_t front_version;
  Header hdr;
  InternalEntry records[kInternalCardinality];

  uint8_t rear_version;

  friend class Tree;
  friend class IndexCache;

public:
  // this is called when tree grows
  InternalPage(global_addr_t left, const Key &key, global_addr_t right,
               uint32_t level = 0): hdr() {
    hdr.leftmost_ptr = left;
    hdr.level = level;
    records[0].key = key;
    records[0].ptr = right;
    records[1].ptr = global_addr_t::null();

    hdr.last_index = 0;

    front_version = 0;
    rear_version = 0;

    embedding_lock = 0;
  }

  InternalPage(uint32_t level = 0): hdr() {
    hdr.level = level;
    records[0].ptr = global_addr_t::null();

    front_version = 0;
    rear_version = 0;

    embedding_lock = 0;
  }

  void set_consistent() {
    front_version++;
    rear_version = front_version;
#ifdef CONFIG_ENABLE_CRC
    this->crc =
        CityHash32((char *)&front_version, (&rear_version) - (&front_version));
#endif
  }

  bool check_consistent() const {

    bool succ = true;
#ifdef CONFIG_ENABLE_CRC
    auto cal_crc =
        CityHash32((char *)&front_version, (&rear_version) - (&front_version));
    succ = cal_crc == this->crc;
#endif
    succ = succ && (rear_version == front_version);

    return succ;
  }

  void debug() const {
    std::cout << "InternalPage@ ";
    hdr.debug();
    std::cout << "version: [" << (int)front_version << ", " << (int)rear_version
              << "]" << std::endl;
  }

  void verbose_debug() const {
    this->debug();
    for (int i = 0; i < this->hdr.last_index + 1; ++i) {
      printf("[%lu %lu] ", this->records[i].key, this->records[i].ptr.val);
    }
    printf("\n");
  }

} __attribute__((packed));

class LeafPage {
private:
  union {
    uint32_t crc;
    uint64_t embedding_lock;
  };
  uint8_t front_version;
  Header hdr;
  LeafEntry records[kLeafCardinality];

  // uint8_t padding[1];
  uint8_t rear_version;

  friend class Tree;

public:
  LeafPage(uint32_t level = 0): hdr() {
    hdr.level = level;
    records[0].value = kValueNull;

    front_version = 0;
    rear_version = 0;

    embedding_lock = 0;
  }

  void set_consistent() {
    front_version++;
    rear_version = front_version;
#ifdef CONFIG_ENABLE_CRC
    this->crc =
        CityHash32((char *)&front_version, (&rear_version) - (&front_version));
#endif
  }

  bool check_consistent() const {

    bool succ = true;
#ifdef CONFIG_ENABLE_CRC
    auto cal_crc =
        CityHash32((char *)&front_version, (&rear_version) - (&front_version));
    succ = cal_crc == this->crc;
#endif

    succ = succ && (rear_version == front_version);

    return succ;
  }

  void debug() const {
    std::cout << "LeafPage@ ";
    hdr.debug();
    std::cout << "version: [" << (int)front_version << ", " << (int)rear_version
              << "]" << std::endl;
  }

} __attribute__((packed));

} // namespace sherman
#endif