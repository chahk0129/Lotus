#include "index/onesided/sherman/Tree.h"
#include "index/onesided/sherman/IndexCache.h"
#include "index/onesided/sherman/Timer.h"
#include "client/transport.h"
#include "system/workload.h"
#include "utils/helper.h"

#include <algorithm>
#include <city.h>
#include <iostream>
#include <queue>
#include <utility>
#include <vector>

#if !PARTITIONED
namespace sherman {

uint64_t cache_miss[MAX_NUM_THREADS][8];
uint64_t cache_hit[MAX_NUM_THREADS][8];
uint64_t latency[MAX_NUM_THREADS][LATENCY_WINDOWS];

thread_local CoroCall Tree::worker[define::kMaxCoro];
thread_local CoroCall Tree::master;
thread_local global_addr_t path_stack[define::kMaxCoro]
                                     [define::kMaxLevelOfTree];

thread_local Timer timer;
thread_local std::queue<uint16_t> hot_wait_queue;

void Tree::debug_check_root(const char* location) {
  char* buffer = transport->get_buffer() + 4096;
  transport->read(buffer, root_ptr_ptr, sizeof(global_addr_t));
  global_addr_t root_ptr = global_addr_t::null();
  memcpy(&root_ptr, buffer, sizeof(global_addr_t));
  if (root_ptr == global_addr_t::null()) {
    printf("ALERT: Root is 0 at %s\n", location);
    assert(false);
  }
}

Tree::Tree(uint32_t tree_id, uint64_t index_cache_size) : tree_id(tree_id), index_cache_size(index_cache_size) {
  local_locks = new LocalLockNode*[g_num_server_nodes];
  for (int i = 0; i < g_num_server_nodes; ++i) {
    local_locks[i] = new LocalLockNode[define::kNumOfLock];
    for (size_t k = 0; k < define::kNumOfLock; ++k) {
      auto &n = local_locks[i][k];
      n.ticket_lock.store(0);
      n.hand_over = false;
      n.hand_time = 0;
    }
  }

  index_cache = nullptr;
  if (index_cache_size != 0 ) {
    index_cache = new IndexCache(tree_id, index_cache_size);
  }
  print_verbose();

  root_ptr_ptr = get_root_ptr_ptr();
  if (g_node_id == 0) // master compute server initializes root
    initializeRoot();
  else {
    global_addr_t root_addr = global_addr_t::null();
    while (root_addr == global_addr_t::null()) {
      root_addr = get_root_ptr(nullptr, 0);
      PAUSE
    }
    printf("Tree %u get root addr %lu\n", tree_id, root_addr.addr);
  }
}

void Tree::initializeRoot() {
  auto page_buffer = transport->get_buffer();
  assert(root_ptr_ptr != global_addr_t::null());
  global_addr_t root_addr = GET_WORKLOAD->rpc_alloc(kLeafPageSize);
  auto root_page = new (page_buffer) LeafPage();
  root_page->set_consistent();
  transport->write(page_buffer, root_addr, kLeafPageSize);
  assert(root_addr != root_ptr_ptr);

  auto root_ptr = reinterpret_cast<global_addr_t*>(page_buffer);
  memcpy(root_ptr, &root_addr, sizeof(global_addr_t));
  transport->write(page_buffer, root_ptr_ptr, sizeof(global_addr_t));
  printf("Tree %u init root addr %lu\n", tree_id, root_addr.addr);
}

void Tree::print_verbose() {

  int kLeafHdrOffset = STRUCT_OFFSET(LeafPage, hdr);
  int kInternalHdrOffset = STRUCT_OFFSET(InternalPage, hdr);
  if (kLeafHdrOffset != kInternalHdrOffset) {
    std::cerr << "format error" << std::endl;
  }

  if (g_node_id == 0) {
    std::cout << "Header size: " << sizeof(Header) << std::endl;
    std::cout << "Internal Page size: " << sizeof(InternalPage) << " ["
              << kInternalPageSize << "]" << std::endl;
    std::cout << "Internal per Page: " << kInternalCardinality << std::endl;
    std::cout << "Leaf Page size: " << sizeof(LeafPage) << " [" << kLeafPageSize
              << "]" << std::endl;
    std::cout << "Leaf per Page: " << kLeafCardinality << std::endl;
    std::cout << "LeafEntry size: " << sizeof(LeafEntry) << std::endl;
    std::cout << "InternalEntry size: " << sizeof(InternalEntry) << std::endl;
  }
}

inline void Tree::before_operation(CoroContext *cxt, int coro_id) {
  for (size_t i = 0; i < define::kMaxLevelOfTree; ++i) {
    path_stack[coro_id][i] = global_addr_t::null();
  }
}

global_addr_t Tree::get_root_ptr_ptr() {
  return GET_WORKLOAD->get_root(tree_id, 0);
}

// extern global_addr_t g_root_ptr;
// extern int g_root_level;
// extern bool enable_cache;
global_addr_t Tree::get_root_ptr(CoroContext *cxt, int coro_id) {

  // if (g_root_ptr == global_addr_t::null()) {
    char* page_buffer = transport->get_buffer();
    // debug_check_root("get_root_ptr before");
    transport->read(page_buffer, root_ptr_ptr, sizeof(global_addr_t));
    // debug_check_root("get_root_ptr after");
    global_addr_t root_ptr = global_addr_t::null();
    memcpy(&root_ptr, page_buffer, sizeof(global_addr_t));
    // assert(root_ptr != global_addr_t::null());
    return root_ptr;
  // } else {
  //   return g_root_ptr;
  // }

  // std::cout << "root ptr " << root_ptr << std::endl;
}

// void Tree::broadcast_new_root(global_addr_t new_root_addr, int root_level) {
//   RawMessage m;
//   m.type = RpcType::NEW_ROOT;
//   m.addr = new_root_addr;
//   m.level = root_level;
//   for (int i = 0; i < dsm->getClusterSize(); ++i) {
//     dsm->rpc_call_dir(m, i);
//   }
// }

bool Tree::update_new_root(global_addr_t left, const Key &k,
                           global_addr_t right, int level,
                           global_addr_t old_root, CoroContext *cxt,
                           int coro_id) {

    // debug_check_root("update_new_root before rpc_alloc");
  auto new_root_addr = GET_WORKLOAD->rpc_alloc(kInternalPageSize);
    // debug_check_root("update_new_root after rpc_alloc");
    assert(new_root_addr != root_ptr_ptr);
  assert(new_root_addr != global_addr_t::null());
  // auto new_root_addr = dsm->alloc(kInternalPageSize);
  char* page_buffer = transport->get_buffer();
  char* cas_buffer = page_buffer + kInternalPageSize;
  // auto page_buffer = dsm->get_rbuf(coro_id).get_page_buffer();
  // auto cas_buffer = dsm->get_rbuf(coro_id).get_cas_buffer();
  auto new_root = new (page_buffer) InternalPage(left, k, right, level);


  printf("update_new_root level %d addr %lu old %lu\n", level, new_root_addr.addr,
         old_root.addr);
  new_root->set_consistent();
    // debug_check_root("update_new_root before write");
    assert(new_root_addr != root_ptr_ptr);
  transport->write(page_buffer, new_root_addr, kInternalPageSize);
    // debug_check_root("update_new_root after write");

  // dsm->write_sync(page_buffer, new_root_addr, kInternalPageSize, cxt);
    // debug_check_root("update_new_root before cas");
  if (transport->cas(cas_buffer, root_ptr_ptr, old_root, new_root_addr, sizeof(global_addr_t))) {
  // if (dsm->cas_sync(root_ptr_ptr, old_root, new_root_addr, cas_buffer, cxt)) {
    // broadcast_new_root(new_root_addr, level);
    std::cout << "new root level " << level << " " << new_root_addr
              << std::endl;
    // debug_check_root("update_new_root after cas");
    return true;
  } else {
    std::cout << "cas root fail " << std::endl;
  }

    // debug_check_root("update_new_root after cas fail");
  return false;
}

void Tree::print_and_check_tree(CoroContext *cxt, int coro_id) {
//   // assert(dsm->is_register());

//   auto root = get_root_ptr(cxt, coro_id);
//   // SearchResult result;

//   global_addr_t p = root;
//   global_addr_t levels[define::kMaxLevelOfTree];
//   int level_cnt = 0;
//   auto page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
//   global_addr_t leaf_head;

// next_level:

//   dsm->read_sync(page_buffer, p, kLeafPageSize);
//   auto header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
//   levels[level_cnt++] = p;
//   if (header->level != 0) {
//     p = header->leftmost_ptr;
//     goto next_level;
//   } else {
//     leaf_head = p;
//   }

// next:
//   dsm->read_sync(page_buffer, leaf_head, kLeafPageSize);
//   auto page = (LeafPage *)page_buffer;
//   for (int i = 0; i < kLeafCardinality; ++i) {
//     if (page->records[i].value != kValueNull) {
//     }
//   }
//   while (page->hdr.sibling_ptr != global_addr_t::null()) {
//     leaf_head = page->hdr.sibling_ptr;
//     goto next;
//   }

//   // for (int i = 0; i < level_cnt; ++i) {
//   //   dsm->read_sync(page_buffer, levels[i], kLeafPageSize);
//   //   auto header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
//   //   // std::cout << "addr: " << levels[i] << " ";
//   //   // header->debug();
//   //   // std::cout << " | ";
//   //   while (header->sibling_ptr != global_addr_t::null()) {
//   //     dsm->read_sync(page_buffer, header->sibling_ptr, kLeafPageSize);
//   //     header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
//   //     // std::cout << "addr: " << header->sibling_ptr << " ";
//   //     // header->debug();
//   //     // std::cout << " | ";
//   //   }
//   //   // std::cout << "\n------------------------------------" << std::endl;
//   //   // std::cout << "------------------------------------" << std::endl;
//   // }
}

inline bool Tree::try_lock_addr(global_addr_t lock_addr, uint64_t tag,
                                uint64_t *buf, CoroContext *cxt, int coro_id) {

  bool hand_over = acquire_local_lock(lock_addr, cxt, coro_id);
  if (hand_over) {
    return true;
  }

  {

    uint64_t retry_cnt = 0;
    uint64_t pre_tag = 0;
    uint64_t conflict_tag = 0;
  retry:
    retry_cnt++;
    if (retry_cnt > 1000000) {
      std::cout << "Deadlock " << lock_addr << std::endl;

      std::cout << g_node_id << ", " << GET_THD_ID
                << " locked by " << (conflict_tag >> 32) << ", "
                << (conflict_tag << 32 >> 32) << std::endl;
      assert(false);
    }

    // debug_check_root("try_lock_addr before cas_dm");
    assert(root_ptr_ptr != lock_addr);
    *buf = tag;
  #ifdef CONFIG_ENABLE_EMBEDDING_LOCK
    bool res = transport->cas((char*)buf, lock_addr, 0, tag, 8);
  #else
    assert(lock_addr.addr < define::kLockChipMemSize);
    bool res = transport->cas_dm((char*)buf, lock_addr, 0, tag, 8);
  #endif
    // bool res = dsm->cas_dm_sync(lock_addr, 0, tag, buf, cxt);
    // debug_check_root("try_lock_addr after cas_dm");

    if (!res) {
      conflict_tag = *buf - 1;
      if (conflict_tag != pre_tag) {
        retry_cnt = 0;
        pre_tag = conflict_tag;
      }
      goto retry;
    }
  }

  return true;
}

inline void Tree::unlock_addr(global_addr_t lock_addr, uint64_t tag,
                              uint64_t *buf, CoroContext *cxt, int coro_id,
                              bool async) {

  bool hand_over_other = can_hand_over(lock_addr);
  if (hand_over_other) {
    releases_local_lock(lock_addr);
    return;
  }

  // auto cas_buf = (uint64_t*)(transport->get_buffer() + kLeafPageSize);
  // auto cas_buf = dsm->get_rbuf(coro_id).get_cas_buffer();
  *buf = 0;

  // *cas_buf = 0;
    // debug_check_root("unlock_addr before write_dm");
    assert(root_ptr_ptr != lock_addr);
  if (async) {
  #ifdef CONFIG_ENABLE_EMBEDDING_LOCK
    transport->write((char *)buf, lock_addr, sizeof(uint64_t), false);
  #else
    assert(lock_addr.addr < define::kLockChipMemSize);
    transport->write_dm((char *)buf, lock_addr, sizeof(uint64_t));
  #endif
    // dsm->write_dm((char *)cas_buf, lock_addr, sizeof(uint64_t), false);
  } else {
  #ifdef CONFIG_ENABLE_EMBEDDING_LOCK
    transport->write((char *)buf, lock_addr, sizeof(uint64_t));
  #else
    assert(lock_addr.addr < define::kLockChipMemSize);
    transport->write_dm((char *)buf, lock_addr, sizeof(uint64_t));
  #endif
    // dsm->write_dm_sync((char *)cas_buf, lock_addr, sizeof(uint64_t), cxt);
  }
    // debug_check_root("unlock_addr after write_dm");

  releases_local_lock(lock_addr);
}

void Tree::write_page_and_unlock(char *page_buffer, global_addr_t page_addr,
                                 int page_size, uint64_t *cas_buffer,
                                 global_addr_t lock_addr, uint64_t tag,
                                 CoroContext *cxt, int coro_id, bool async) {

  bool hand_over_other = can_hand_over(lock_addr);
  if (hand_over_other) {
    // debug_check_root("write_page_and_unlock before write");
    transport->write(page_buffer, page_addr, page_size);
    // dsm->write_sync(page_buffer, page_addr, page_size, cxt);
    // debug_check_root("write_page_and_unlock after write");
    releases_local_lock(lock_addr);
    return;
  }

  // RdmaOpRegion rs[2];
  // rs[0].source = (uint64_t)page_buffer;
  // rs[0].dest = page_addr;
  // rs[0].size = page_size;
  // rs[0].is_on_chip = false;

  // rs[1].source = (uint64_t)dsm->get_rbuf(coro_id).get_cas_buffer();
  // rs[1].dest = lock_addr;
  // rs[1].size = sizeof(uint64_t);

  // rs[1].is_on_chip = true;

  // *(uint64_t *)rs[1].source = 0;
    assert(root_ptr_ptr != page_addr);
    assert(root_ptr_ptr != lock_addr);
  if (async) {
    // debug_check_root("write_page_and_unlock before write_batch write");
    transport->write(page_buffer, page_addr, page_size, false);
    // debug_check_root("write_page_and_unlock after write_batch write");
    // transport->write(page_buffer, page_addr, page_size, false);
  *cas_buffer = 0;
  #ifdef CONFIG_ENABLE_EMBEDDING_LOCK
    transport->write((char*)cas_buffer, lock_addr, sizeof(uint64_t));
  #else
    assert(lock_addr.addr < define::kLockChipMemSize);
    transport->write_dm((char*)cas_buffer, lock_addr, sizeof(uint64_t));
  #endif
    // debug_check_root("write_page_and_unlock after write_batch write_dm");
    // transport->write_dm((char*)cas_buffer, lock_addr, sizeof(uint64_t), false);
    // dsm->write_batch(rs, 2, false);
  } else {
    // debug_check_root("write_page_and_unlock before write_batch write");
    transport->write(page_buffer, page_addr, page_size, false);
    // debug_check_root("write_page_and_unlock after write_batch write");
    // transport->write(page_buffer, page_addr, page_size, false);
  *cas_buffer = 0;
  #ifdef CONFIG_ENABLE_EMBEDDING_LOCK
    transport->write((char*)cas_buffer, lock_addr, sizeof(uint64_t));
  #else
    assert(lock_addr.addr < define::kLockChipMemSize);
    transport->write_dm((char*)cas_buffer, lock_addr, sizeof(uint64_t));
  #endif
    // debug_check_root("write_page_and_unlock after write_batch write_dm");
    // dsm->write_batch_sync(rs, 2, cxt);
  }

  releases_local_lock(lock_addr);
}

void Tree::lock_and_read_page(char *page_buffer, global_addr_t page_addr,
                              int page_size, uint64_t *cas_buffer,
                              global_addr_t lock_addr, uint64_t tag,
                              CoroContext *cxt, int coro_id) {

    assert(root_ptr_ptr != page_addr);
  try_lock_addr(lock_addr, tag, cas_buffer, cxt, coro_id);
    // debug_check_root("lock_and_read_page before read");
  transport->read(page_buffer, page_addr, page_size);
    // debug_check_root("lock_and_read_page after read");
  // dsm->read_sync(page_buffer, page_addr, page_size, cxt);
}

// void Tree::lock_bench(const Key &k, CoroContext *cxt, int coro_id) {
//   uint64_t lock_index = CityHash64((char *)&k, sizeof(k)) % define::kNumOfLock;

//   global_addr_t lock_addr;
//   lock_addr.node_id = 0;
//   lock_addr.addr = lock_index * sizeof(uint64_t);
//   auto cas_buffer = dsm->get_rbuf(coro_id).get_cas_buffer();

//   // bool res = dsm->cas_sync(lock_addr, 0, 1, cas_buffer, cxt);
//   try_lock_addr(lock_addr, 1, cas_buffer, cxt, coro_id);
//   unlock_addr(lock_addr, 1, cas_buffer, cxt, coro_id, true);
// }

void Tree::insert_internal(const Key &k, global_addr_t v, CoroContext *cxt,
                           int coro_id, int level) {
  auto root = get_root_ptr(cxt, coro_id);
  SearchResult result;

  global_addr_t p = root;

next:

  if (!page_search(p, k, result, cxt, coro_id)) {
    std::cout << "SEARCH WARNING insert" << std::endl;
    p = get_root_ptr(cxt, coro_id);
    sleep(1);
    goto next;
  }

  // assert(result.level != 0);
  if (result.slibing != global_addr_t::null()) {
    p = result.slibing;
    goto next;
  }

  if (result.level == level) { 
    internal_page_store(p, k, v, root, level, cxt, coro_id);
  }
  else {
    p = result.next_level;
    if (result.level != level + 1) { 
      goto next;
    }

    internal_page_store(p, k, v, root, level, cxt, coro_id);
  }
}

void Tree::insert(const Key &k, const Value &v, CoroContext *cxt, int coro_id) {
  // assert(dsm->is_register());

  before_operation(cxt, coro_id);

  // avoid index cache upon index building
  /*
  if (index_cache) {
    global_addr_t cache_addr;
    auto entry = index_cache->search_from_cache(k, &cache_addr,
                                                GET_THD_ID == 0);
    if (entry) { // cache hit
      auto root = get_root_ptr(cxt, coro_id);
      if (leaf_page_store(cache_addr, k, v, root, 0, cxt, coro_id, true)) {

        cache_hit[GET_THD_ID][0]++;
        return;
      }
      // cache stale, from root,
      index_cache->invalidate(entry);
    }
    cache_miss[GET_THD_ID][0]++;
  }
  */

  auto root = get_root_ptr(cxt, coro_id);
  SearchResult result;
  memset(&result, 0, sizeof(result));

  global_addr_t p = root;

next:

  if (!page_search(p, k, result, cxt, coro_id)) {
    std::cout << "SEARCH WARNING insert" << std::endl;
    p = get_root_ptr(cxt, coro_id);
    sleep(1);
    goto next;
  }

  if (!result.is_leaf) {
    assert(result.level != 0);
    if (result.slibing != global_addr_t::null()) {
      p = result.slibing;
      goto next;
    }

    p = result.next_level;
    if (result.level != 1) {
      goto next;
    }
  }

  leaf_page_store(p, k, v, root, 0, cxt, coro_id);
}

bool Tree::search(const Key &k, Value &v, CoroContext *cxt, int coro_id) {
  // assert(dsm->is_register());

  auto root = get_root_ptr(cxt, coro_id);
  SearchResult result;

  global_addr_t p = root;

  bool from_cache = false;
  const CacheEntry *entry = nullptr;
  if (index_cache) {
    global_addr_t cache_addr;
    entry = index_cache->search_from_cache(k, &cache_addr,
                                           GET_THD_ID == 0);
    if (entry) { // cache hit
      cache_hit[GET_THD_ID][0]++;
      from_cache = true;
      p = cache_addr;

    } else {
      cache_miss[GET_THD_ID][0]++;
    }
  }

next:
  if (!page_search(p, k, result, cxt, coro_id, from_cache)) {
    if (from_cache) { // cache stale
      index_cache->invalidate(entry);
      cache_hit[GET_THD_ID][0]--;
      cache_miss[GET_THD_ID][0]++;
      from_cache = false;

      p = root;
    } else {
      std::cout << "SEARCH WARNING search" << std::endl;
      sleep(1);
    }
    goto next;
  }
  if (result.is_leaf) {
    if (result.val != kValueNull) { // find
      v = result.val;
      return true;
    }
    if (result.slibing != global_addr_t::null()) { // turn right
      p = result.slibing;
      goto next;
    }
    return false; // not found
  } else {        // internal
    p = result.slibing != global_addr_t::null() ? result.slibing
                                                : result.next_level;
    goto next;
  }
}

uint64_t Tree::range_query(const Key &from, const Key &to, Value *value_buffer,
                           CoroContext *cxt, int coro_id) {

  const int kParaFetch = 32;
  thread_local std::vector<InternalPage *> result;
  thread_local std::vector<global_addr_t> leaves;

  result.clear();
  leaves.clear();
  index_cache->search_range_from_cache(from, to, result);
  
  // FIXME: here, we assume all innernal nodes are cached in compute node
  if (result.empty()) {
    return 0;
  }

  uint64_t counter = 0;
  for (auto page : result) {
    auto cnt = page->hdr.last_index + 1;
    auto addr = page->hdr.leftmost_ptr;

    // [from, to]
    // [lowest, page->records[0].key);
    bool no_fetch = from > page->records[0].key || to < page->hdr.lowest;
    if (!no_fetch) {
      leaves.push_back(addr);
    }
    for (int i = 1; i < cnt; ++i) {
      no_fetch = from > page->records[i].key || to < page->records[i - 1].key;
      if (!no_fetch) {
        leaves.push_back(page->records[i - 1].ptr);
      }
    }

    no_fetch = from > page->hdr.highest || to < page->records[cnt - 1].key;
    if (!no_fetch) {
      leaves.push_back(page->records[cnt - 1].ptr);
    }
  }

  int cq_cnt = 0;
  char *range_buffer = transport->get_buffer();
  // char *range_buffer = (dsm->get_rbuf(coro_id)).get_range_buffer();
  for (size_t i = 0; i < leaves.size(); ++i) {
    if (i > 0 && i % kParaFetch == 0) {
      // dsm->poll_rdma_cq(kParaFetch);
      cq_cnt -= kParaFetch;
      for (int k = 0; k < kParaFetch; ++k) {
        auto page = (LeafPage *)(range_buffer + k * kLeafPageSize);
        for (int i = 0; i < kLeafCardinality; ++i) {
          auto &r = page->records[i];
          if (r.value != kValueNull && r.f_version == r.r_version) {
            if (r.key >= from && r.key <= to) {
              value_buffer[counter++] = r.value;
            }
          }
        }
      }
    }
    transport->read(range_buffer + kLeafPageSize * (i % kParaFetch), leaves[i],
                    kLeafPageSize);
    // dsm->read(range_buffer + kLeafPageSize * (i % kParaFetch), leaves[i],
    //           kLeafPageSize, true);
    cq_cnt++;
  }

  if (cq_cnt != 0) {
    // dsm->poll_rdma_cq(cq_cnt);
    for (int k = 0; k < cq_cnt; ++k) {
      auto page = (LeafPage *)(range_buffer + k * kLeafPageSize);
      for (int i = 0; i < kLeafCardinality; ++i) {
        auto &r = page->records[i];
        if (r.value != kValueNull && r.f_version == r.r_version) {
          if (r.key >= from && r.key <= to) {
            value_buffer[counter++] = r.value;
          }
        }
      }
    }
  }

  return counter;
}

void Tree::del(const Key &k, CoroContext *cxt, int coro_id) {
  // assert(dsm->is_register());

  before_operation(cxt, coro_id);

  if (index_cache) {
    global_addr_t cache_addr;
    auto entry = index_cache->search_from_cache(k, &cache_addr,
                                                GET_THD_ID == 0);
    if (entry) { // cache hit
      if (leaf_page_del(cache_addr, k, 0, cxt, coro_id, true)) {

        cache_hit[GET_THD_ID][0]++;
        return;
      }
      // cache stale, from root,
      index_cache->invalidate(entry);
    }
    cache_miss[GET_THD_ID][0]++;
  }

  auto root = get_root_ptr(cxt, coro_id);
  SearchResult result;

  global_addr_t p = root;

next:

  if (!page_search(p, k, result, cxt, coro_id)) {
    std::cout << "SEARCH WARNING del" << std::endl;
    p = get_root_ptr(cxt, coro_id);
    sleep(1);
    goto next;
  }

  if (!result.is_leaf) {
    assert(result.level != 0);
    if (result.slibing != global_addr_t::null()) {
      p = result.slibing;
      goto next;
    }

    p = result.next_level;
    if (result.level != 1) {
      goto next;
    }
  }

  leaf_page_del(p, k, 0, cxt, coro_id);
}

bool Tree::page_search(global_addr_t page_addr, const Key &k,
                       SearchResult &result, CoroContext *cxt, int coro_id,
                       bool from_cache) {
  auto page_buffer = transport->get_buffer();
  // auto page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
  auto header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));

    assert(root_ptr_ptr != page_addr);
  assert(page_addr.node_id < g_num_server_nodes);
  if (page_addr == global_addr_t::null()) {
    std::cout << "ERROR: page search get null page addr" << std::endl;
    return false;
  }
  assert(page_addr.addr != 0);
  int counter = 0;
re_read:
  if (++counter > 100) {
    printf("re read too many times\n");
    sleep(1);
  }
    // debug_check_root("page_search before read1");
  transport->read(page_buffer, page_addr, 16);
    // debug_check_root("page_search after read1");

  uint64_t embedding_lock = *reinterpret_cast<uint64_t*>(page_buffer);
  if (embedding_lock != 0) {
    // printf("embedding lock not 0 (%lu)\n", embedding_lock);
    goto re_read;
  }
  uint8_t version_start = *reinterpret_cast<uint8_t*>(page_buffer + 8);

    // debug_check_root("page_search before read2");
  transport->read(page_buffer, page_addr, kLeafPageSize);
    // debug_check_root("page_search after read2");

    // debug_check_root("page_search before read3");
  transport->read(page_buffer, page_addr, 16);
    // debug_check_root("page_search after read3");
  embedding_lock = *reinterpret_cast<uint64_t*>(page_buffer);
  uint8_t version_end = *reinterpret_cast<uint8_t*>(page_buffer + 8);
  // if (version_start != version_end) {
  //   printf("version not match %u, %u\n", version_start, version_end);
  //   goto re_read;
  // }
  if ((version_start != version_end) || (embedding_lock != 0)) {
    // printf("v_start %u, v_end %u, embedding lock (%lu)\n", version_start, version_end, embedding_lock);
    goto re_read;
  }
  // dsm->read_sync(page_buffer, page_addr, kLeafPageSize, cxt);

  memset(&result, 0, sizeof(result));
  result.is_leaf = header->leftmost_ptr == global_addr_t::null();
  result.level = header->level;
  path_stack[coro_id][result.level] = page_addr;
  // std::cout << "level " << (int)result.level << " " << page_addr <<
  // std::endl;

  if (result.is_leaf) {
    auto page = (LeafPage *)page_buffer;
    if (!page->check_consistent()) {
      goto re_read;
    }

    if (from_cache &&
        (k < page->hdr.lowest || k >= page->hdr.highest)) { // cache is stale
    // if (k < page->hdr.lowest || k >= page->hdr.highest) { // cache is stale
      // printf("key %lu error in leaf level (lowest %lu, highest %lu)\n", k, page->hdr.lowest, page->hdr.highest);
      return false;
    }

    assert(result.level == 0);
    if (k >= page->hdr.highest) { // should turn right
      result.slibing = page->hdr.sibling_ptr;
      return true;
    }
    if (k < page->hdr.lowest) {
      assert(false);
      return false;
    }
    leaf_page_search(page, k, result);
  } else {
    assert(result.level != 0);
    assert(!from_cache);
    auto page = (InternalPage *)page_buffer;

    if (!page->check_consistent()) {
      goto re_read;
    }

    if (result.level == 1 && index_cache) {
      index_cache->add_to_cache(page);
    }

    if (k >= page->hdr.highest) { // should turn right
      result.slibing = page->hdr.sibling_ptr;
      return true;
    }
    if (k < page->hdr.lowest) {
      printf("key %ld error in level %d\n", k, page->hdr.level);
      sleep(10);
      print_and_check_tree();
      assert(false);
      return false;
    }
    internal_page_search(page, k, result);
  }

  return true;
}

void Tree::internal_page_search(InternalPage *page, const Key &k,
                                SearchResult &result) {

  assert(k >= page->hdr.lowest);
  assert(k < page->hdr.highest);

  auto cnt = page->hdr.last_index + 1;
  // page->debug();
  if (k < page->records[0].key) {
    result.next_level = page->hdr.leftmost_ptr;
    return;
  }

  for (int i = 1; i < cnt; ++i) {
    if (k < page->records[i].key) {
      result.next_level = page->records[i - 1].ptr;
      return;
    }
  }
  result.next_level = page->records[cnt - 1].ptr;
}

void Tree::leaf_page_search(LeafPage *page, const Key &k,
                            SearchResult &result) {

  for (int i = 0; i < kLeafCardinality; ++i) {
    auto &r = page->records[i];
    if (r.key == k && r.value != kValueNull && r.f_version == r.r_version) {
      result.val = r.value;
      break;
    }
  }
}

void Tree::internal_page_store(global_addr_t page_addr, const Key &k,
                               global_addr_t v, global_addr_t root, int level,
                               CoroContext *cxt, int coro_id) {
  uint64_t lock_index =
      CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;

  global_addr_t lock_addr;
#ifdef CONFIG_ENABLE_EMBEDDING_LOCK
  lock_addr = page_addr;
#else
  lock_addr.node_id = page_addr.node_id;
  lock_addr.addr = (lock_index * sizeof(uint64_t)) % define::kLockChipMemSize;
#endif

  char* page_buffer = transport->get_buffer();
#ifdef CONFIG_ENABLE_EMBEDDING_LOCK
  uint64_t* cas_buffer = reinterpret_cast<uint64_t*>(page_buffer);
#else
  uint64_t *cas_buffer = reinterpret_cast<uint64_t*>(page_buffer + kInternalPageSize);
#endif
  // auto &rbuf = dsm->get_rbuf(coro_id);
  // uint64_t *cas_buffer = rbuf.get_cas_buffer();
  // auto page_buffer = rbuf.get_page_buffer();

  auto tag = static_cast<uint64_t>(GET_THD_ID + 1);
  // auto tag = (static_cast<uint64_t>(g_node_id) << 32) | (GET_THD_ID + 1);
  // auto tag = dsm->getThreadTag();
  assert(tag != 0);

  lock_and_read_page(page_buffer, page_addr, kInternalPageSize, cas_buffer,
                     lock_addr, tag, cxt, coro_id);

  auto page = (InternalPage *)page_buffer;

  assert(page->hdr.level == level);
  assert(page->check_consistent());
  if (k >= page->hdr.highest) {

    this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, true);

    assert(page->hdr.sibling_ptr != global_addr_t::null());

    this->internal_page_store(page->hdr.sibling_ptr, k, v, root, level, cxt,
                              coro_id);

    return;
  }
  assert(k >= page->hdr.lowest);

  auto cnt = page->hdr.last_index + 1;

  bool is_update = false;
  uint16_t insert_index = 0;
  for (int i = cnt - 1; i >= 0; --i) {
    if (page->records[i].key == k) { // find and update
      page->records[i].ptr = v;
      // assert(false);
      is_update = true;
      break;
    }
    if (page->records[i].key < k) {
      insert_index = i + 1;
      break;
    }
  }

  assert(cnt != kInternalCardinality);

  if (!is_update) { // insert and shift
    for (int i = cnt; i > insert_index; --i) {
      page->records[i].key = page->records[i - 1].key;
      page->records[i].ptr = page->records[i - 1].ptr;
    }
    page->records[insert_index].key = k;
    page->records[insert_index].ptr = v;

    page->hdr.last_index++;
  }

  cnt = page->hdr.last_index + 1;
  bool need_split = cnt == kInternalCardinality;
  Key split_key;
  global_addr_t sibling_addr = global_addr_t::null();
  if (need_split) { // need split
    // debug_check_root("internal_page_store before rpc_alloc");
    sibling_addr = GET_WORKLOAD->rpc_alloc(kInternalPageSize);
    // debug_check_root("internal_page_store after rpc_alloc");
    assert(sibling_addr != root_ptr_ptr);
    assert(sibling_addr != global_addr_t::null());
    // sibling_addr = dsm->alloc(kInternalPageSize);
  #ifdef CONFIG_ENABLE_EMBEDDING_LOCK
    auto sibling_buf = page_buffer + kInternalPageSize;
  #else
    auto sibling_buf = (char*)cas_buffer + 8;
  #endif
    // auto sibling_buf = rbuf.get_sibling_buffer();

    auto sibling = new (sibling_buf) InternalPage(page->hdr.level);

    //    std::cout << "addr " <<  sibling_addr << " | level " <<
    //    (int)(page->hdr.level) << std::endl;

    int m = cnt / 2;
    split_key = page->records[m].key;
    assert(split_key > page->hdr.lowest);
    assert(split_key < page->hdr.highest);
    for (int i = m + 1; i < cnt; ++i) { // move
      sibling->records[i - m - 1].key = page->records[i].key;
      sibling->records[i - m - 1].ptr = page->records[i].ptr;
    }
    page->hdr.last_index -= (cnt - m);
    sibling->hdr.last_index += (cnt - m - 1);

    sibling->hdr.leftmost_ptr = page->records[m].ptr;
    sibling->hdr.lowest = page->records[m].key;
    sibling->hdr.highest = page->hdr.highest;
    page->hdr.highest = page->records[m].key;

    // link
    sibling->hdr.sibling_ptr = page->hdr.sibling_ptr;
    page->hdr.sibling_ptr = sibling_addr;

    assert(sibling_addr != root_ptr_ptr);
    sibling->set_consistent();
    // debug_check_root("internal_page_store before write sibling");
    transport->write(sibling_buf, sibling_addr, kInternalPageSize);
    // dsm->write_sync(sibling_buf, sibling_addr, kInternalPageSize, cxt);
    // debug_check_root("internal_page_store after write sibling");
  }

  page->set_consistent();
  write_page_and_unlock(page_buffer, page_addr, kInternalPageSize, cas_buffer,
                        lock_addr, tag, cxt, coro_id, need_split);

  if (!need_split)
    return;

  if (root == page_addr) { // update root

    if (update_new_root(page_addr, split_key, sibling_addr, level + 1, root,
                        cxt, coro_id)) {
      return;
    }
  }

  auto up_level = path_stack[coro_id][level + 1];

  if (up_level != global_addr_t::null()) {
    internal_page_store(up_level, split_key, sibling_addr, root, level + 1, cxt,
                        coro_id);
  } else {
    insert_internal(split_key, sibling_addr, cxt, coro_id, level + 1);
    // assert(false);
  }
}

bool Tree::leaf_page_store(global_addr_t page_addr, const Key &k,
                           const Value &v, global_addr_t root, int level,
                           CoroContext *cxt, int coro_id, bool from_cache) {

  uint64_t lock_index =
      CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;

  global_addr_t lock_addr;

#ifdef CONFIG_ENABLE_EMBEDDING_LOCK
  lock_addr = page_addr;
#else
  lock_addr.node_id = page_addr.node_id;
  lock_addr.addr = (lock_index * sizeof(uint64_t)) % define::kLockChipMemSize;
#endif

  char* page_buffer = transport->get_buffer();
#ifdef CONFIG_ENABLE_EMBEDDING_LOCK
  uint64_t* cas_buffer = reinterpret_cast<uint64_t*>(page_buffer);
#else
  uint64_t *cas_buffer = reinterpret_cast<uint64_t*>(page_buffer + kLeafPageSize);
#endif
  //
  // auto &rbuf = dsm->get_rbuf(coro_id);
  // uint64_t *cas_buffer = rbuf.get_cas_buffer();
  // auto page_buffer = rbuf.get_page_buffer();

  auto tag = (static_cast<uint64_t>(g_node_id) << 32) | (GET_THD_ID + 1);
  // auto tag = dsm->getThreadTag();
  assert(tag != 0);

  lock_and_read_page(page_buffer, page_addr, kLeafPageSize, cas_buffer,
                     lock_addr, tag, cxt, coro_id);

  auto page = (LeafPage *)page_buffer;
  auto page_level = page->hdr.level;

  assert(page_level == level);
  assert(page->check_consistent());

  if (from_cache &&
      (k < page->hdr.lowest || k >= page->hdr.highest)) { // cache is stale
    this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, true);
    return false;
  }

  if (k >= page->hdr.highest) {

    this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, true);
    assert(page->hdr.sibling_ptr != global_addr_t::null());
    this->leaf_page_store(page->hdr.sibling_ptr, k, v, root, level, cxt,
                          coro_id);
    return true;
  }
  assert(k >= page->hdr.lowest);

  int cnt = 0;
  int empty_index = -1;
  char *update_addr = nullptr;
  for (int i = 0; i < kLeafCardinality; ++i) {

    auto &r = page->records[i];
    if (r.value != kValueNull) {
      cnt++;
      if (r.key == k) {
        r.value = v;
        r.f_version++;
        r.r_version = r.f_version;
        update_addr = (char *)&r;
        break;
      }
    } else if (empty_index == -1) {
      empty_index = i;
    }
  }

  assert(cnt != kLeafCardinality);

  if (update_addr == nullptr) { // insert new item
    if (empty_index == -1) {
      printf("%d cnt\n", cnt);
      assert(false);
    }

    auto &r = page->records[empty_index];
    r.key = k;
    r.value = v;
    r.f_version++;
    r.r_version = r.f_version;

    update_addr = (char *)&r;

    cnt++;
  }

  bool need_split = cnt == kLeafCardinality;
  if (!need_split) {
    assert(update_addr);
    write_page_and_unlock(
        update_addr, GADD(page_addr, (update_addr - (char *)page)),
        sizeof(LeafEntry), cas_buffer, lock_addr, tag, cxt, coro_id, false);

    return true;
  } else {
    std::sort(
        page->records, page->records + kLeafCardinality,
        [](const LeafEntry &a, const LeafEntry &b) { return a.key < b.key; });
  }

  Key split_key;
  global_addr_t sibling_addr = global_addr_t::null();
  if (need_split) { // need split
    // debug_check_root("leaf_page_store before rpc_alloc");
    auto new_addr = GET_WORKLOAD->rpc_alloc(kLeafPageSize);
    sibling_addr = new_addr;
    // sibling_addr = GET_WORKLOAD->rpc_alloc(kLeafPageSize);
    // debug_check_root("leaf_page_store after rpc_alloc");
    assert(sibling_addr != global_addr_t::null());
    assert(sibling_addr != root_ptr_ptr);
    // sibling_addr = dsm->alloc(kLeafPageSize);
    // auto sibling_buf = rbuf.get_sibling_buffer();
  #ifdef CONFIG_ENABLE_EMBEDDING_LOCK
    auto sibling_buf = page_buffer + kLeafPageSize;
  #else
    auto sibling_buf = (char*)cas_buffer + 8;
  #endif

    auto sibling = new (sibling_buf) LeafPage(page->hdr.level);

    // std::cout << "addr " <<  sibling_addr << " | level " <<
    // (int)(page->hdr.level) << std::endl;

    int m = cnt / 2;
    split_key = page->records[m].key;
    assert(split_key > page->hdr.lowest);
    assert(split_key < page->hdr.highest);

    for (int i = m; i < cnt; ++i) { // move
      sibling->records[i - m].key = page->records[i].key;
      sibling->records[i - m].value = page->records[i].value;
      page->records[i].key = 0;
      page->records[i].value = kValueNull;
    }
    page->hdr.last_index -= (cnt - m);
    sibling->hdr.last_index += (cnt - m);

    sibling->hdr.lowest = split_key;
    sibling->hdr.highest = page->hdr.highest;
    page->hdr.highest = split_key;

    // link
    sibling->hdr.sibling_ptr = page->hdr.sibling_ptr;
    page->hdr.sibling_ptr = sibling_addr;

    sibling->set_consistent();
    // debug_check_root("leaf_page_store before write sibling");
    assert(sibling_addr != root_ptr_ptr);
    transport->write(sibling_buf, sibling_addr, kLeafPageSize);
    // dsm->write_sync(sibling_buf, sibling_addr, kLeafPageSize, cxt);
    // debug_check_root("leaf_page_store after write sibling");
  }

  page->set_consistent();

  write_page_and_unlock(page_buffer, page_addr, kLeafPageSize, cas_buffer,
                        lock_addr, tag, cxt, coro_id, need_split);

  if (!need_split)
    return true;

  if (root == page_addr) { // update root
    if (update_new_root(page_addr, split_key, sibling_addr, level + 1, root,
                        cxt, coro_id)) {
      return true;
    }
  }

  auto up_level = path_stack[coro_id][level + 1];

  if (up_level != global_addr_t::null()) {
    internal_page_store(up_level, split_key, sibling_addr, root, level + 1, cxt,
                        coro_id);
  } else {
    // assert(from_cache);
    insert_internal(split_key, sibling_addr, cxt, coro_id, level + 1);
  }

  return true;
}

bool Tree::leaf_page_del(global_addr_t page_addr, const Key &k, int level,
                         CoroContext *cxt, int coro_id, bool from_cache) {
  uint64_t lock_index =
      CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;

  global_addr_t lock_addr;

#ifdef CONFIG_ENABLE_EMBEDDING_LOCK
  lock_addr = page_addr;
#else
  lock_addr.node_id = page_addr.node_id;
  lock_addr.addr = (lock_index * sizeof(uint64_t)) % define::kLockChipMemSize;
#endif

  char* page_buffer = transport->get_buffer();
#ifdef CONFIG_ENABLE_EMBEDDING_LOCK
  uint64_t* cas_buffer = reinterpret_cast<uint64_t*>(page_buffer);
#else
  uint64_t *cas_buffer = reinterpret_cast<uint64_t*>(page_buffer + kLeafPageSize);
#endif
  //
  // auto &rbuf = dsm->get_rbuf(coro_id);
  // uint64_t *cas_buffer = rbuf.get_cas_buffer();
  // auto page_buffer = rbuf.get_page_buffer();

  auto tag = (static_cast<uint64_t>(g_node_id) << 32) | (GET_THD_ID + 1);
  // auto tag = dsm->getThreadTag();
  assert(tag != 0);

  lock_and_read_page(page_buffer, page_addr, kLeafPageSize, cas_buffer,
                     lock_addr, tag, cxt, coro_id);

  auto page = (LeafPage *)page_buffer;

  assert(page->hdr.level == level);
  assert(page->check_consistent());

  if (from_cache &&
      (k < page->hdr.lowest || k >= page->hdr.highest)) { // cache is stale
    this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, true);
    return false;
  }

  if (k >= page->hdr.highest) {
    this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, true);
    assert(page->hdr.sibling_ptr != global_addr_t::null());
    this->leaf_page_del(page->hdr.sibling_ptr, k, level, cxt, coro_id);
    return true;
  }

  assert(k >= page->hdr.lowest);

  char *update_addr = nullptr;
  for (int i = 0; i < kLeafCardinality; ++i) {
    auto &r = page->records[i];
    if (r.key == k && r.value != kValueNull) {
      r.value = kValueNull;
      r.f_version++;
      r.r_version = r.f_version;
      update_addr = (char *)&r;
      break;
    }
  }

  if (update_addr) {
    write_page_and_unlock(
        update_addr, GADD(page_addr, (update_addr - (char *)page)),
        sizeof(LeafEntry), cas_buffer, lock_addr, tag, cxt, coro_id, false);
  } else {
    this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, false);
  }
  return true;
}

// void Tree::run_coroutine(CoroFunc func, int id, int coro_cnt) {

//   using namespace std::placeholders;

//   assert(coro_cnt <= define::kMaxCoro);
//   for (int i = 0; i < coro_cnt; ++i) {
//     auto gen = func(i, dsm, id);
//     worker[i] = CoroCall(std::bind(&Tree::coro_worker, this, _1, gen, i));
//   }

//   master = CoroCall(std::bind(&Tree::coro_master, this, _1, coro_cnt));

//   master();
// }

// void Tree::coro_worker(CoroYield &yield, RequstGen *gen, int coro_id) {
//   CoroContext ctx;
//   ctx.coro_id = coro_id;
//   ctx.master = &master;
//   ctx.yield = &yield;

//   Timer coro_timer;
//   auto thread_id = dsm->getMyThreadID();

//   while (true) {

//     auto r = gen->next();

//     coro_timer.begin();
//     if (r.is_search) {
//       Value v;
//       this->search(r.k, v, &ctx, coro_id);
//     } else {
//       this->insert(r.k, r.v, &ctx, coro_id);
//     }
//     auto us_10 = coro_timer.end() / 100;
//     if (us_10 >= LATENCY_WINDOWS) {
//       us_10 = LATENCY_WINDOWS - 1;
//     }
//     latency[thread_id][us_10]++;
//   }
// }

// void Tree::coro_master(CoroYield &yield, int coro_cnt) {

//   for (int i = 0; i < coro_cnt; ++i) {
//     yield(worker[i]);
//   }

//   while (true) {

//     uint64_t next_coro_id;

//     if (dsm->poll_rdma_cq_once(next_coro_id)) {
//       yield(worker[next_coro_id]);
//     }

//     if (!hot_wait_queue.empty()) {
//       next_coro_id = hot_wait_queue.front();
//       hot_wait_queue.pop();
//       yield(worker[next_coro_id]);
//     }
//   }
// }

// Local Locks
inline bool Tree::acquire_local_lock(global_addr_t lock_addr, CoroContext *cxt,
                                     int coro_id) {
  auto &node = local_locks[lock_addr.node_id][(lock_addr.addr / 8) % define::kNumOfLock];

  uint64_t lock_val = node.ticket_lock.fetch_add(1);

  uint32_t ticket = lock_val << 32 >> 32;
  uint32_t current = lock_val >> 32;

  while (ticket != current) { // lock failed

    if (cxt != nullptr) {
      hot_wait_queue.push(coro_id);
      (*cxt->yield)(*cxt->master);
    }

    current = node.ticket_lock.load(std::memory_order_relaxed) >> 32;
  }

  node.hand_time++;

  return node.hand_over;
}

inline bool Tree::can_hand_over(global_addr_t lock_addr) {
  auto &node = local_locks[lock_addr.node_id][(lock_addr.addr / 8) % define::kNumOfLock];
  uint64_t lock_val = node.ticket_lock.load(std::memory_order_relaxed);

  uint32_t ticket = lock_val << 32 >> 32;
  uint32_t current = lock_val >> 32;

  if (ticket <= current + 1) { // no pending locks
    node.hand_over = false;
  } else {
    node.hand_over = node.hand_time < define::kMaxHandOverTime;
  }
  if (!node.hand_over) {
    node.hand_time = 0;
  }

  return node.hand_over;
}

inline void Tree::releases_local_lock(global_addr_t lock_addr) {
  auto &node = local_locks[lock_addr.node_id][(lock_addr.addr / 8) % define::kNumOfLock];

  node.ticket_lock.fetch_add((1ull << 32));
}

void Tree::index_cache_statistics() {
  index_cache->statistics();
  index_cache->bench();
}

void Tree::clear_statistics() {
  for (int i = 0; i < MAX_NUM_THREADS; ++i) {
    cache_hit[i][0] = 0;
    cache_miss[i][0] = 0;
  }
}
}
#endif