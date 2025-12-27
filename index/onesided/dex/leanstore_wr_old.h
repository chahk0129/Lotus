#pragma once
#include "system/global.h"
#include "system/global_address.h"
#include "system/manager.h"
#include "utils/helper.h"
#include "utils/packetize.h"
#include "transport/message.h"
#include "transport/transport.h"
#include "client/transport.h"
#include "index/onesided/dex/leanstore_node.h"

namespace cachepush {
/*----------------------------------*/
/***** One-sided node operation *****/
/*----------------------------------*/

template <class Value>
int rpc_insert(global_addr_t start_node, Key k, Value v, global_addr_t& leaf_addr){
   auto send_ptr = transport->get_buffer();
   auto send_msg = new (send_ptr) message_t(message_t::type_t::REQUEST_DEX_INSERT);
   uint32_t base_size = sizeof(message_t) - sizeof(char*);

   UnstructuredBuffer send_buffer(send_msg->get_data());
   send_buffer.put(&k);
   send_buffer.put(&v);
   send_buffer.put(&start_node);
   uint32_t data_size = send_buffer.size();
   send_msg->set_data_size(data_size);
   transport->send(send_ptr, data_size + base_size, start_node.node_id);

   auto recv_ptr = transport->get_recv_buffer(start_node.node_id);
   auto recv_msg = new (recv_ptr) message_t();
   transport->recv(recv_ptr, MAX_MESSAGE_SIZE, start_node.node_id);
   if(recv_msg->get_type() != message_t::type_t::ACK)
       debug::notify_error("[rpc_insert] failed (key %lu, value %lu)", k, v);
   UnstructuredBuffer recv_buffer(recv_msg->get_data());
   int level = -1;
   recv_buffer.get(&level);
   assert(level != -1);
   recv_buffer.get(&leaf_addr);
   assert(recv_msg->get_data_size() == recv_buffer.size());
   return level;
}

template <class Value>
int rpc_remove(global_addr_t start_node, Key k, global_addr_t& leaf_addr){
   auto send_ptr = transport->get_buffer();
   auto send_msg = new (send_ptr) message_t(message_t::type_t::REQUEST_DEX_DELETE);
   UnstructuredBuffer send_buffer(send_msg->get_data());
   uint32_t base_size = sizeof(message_t) - sizeof(char*);
   send_buffer.put(&k);
   send_buffer.put(&start_node);
   uint32_t data_size = send_buffer.size();
   send_msg->set_data_size(data_size);
   transport->send(send_ptr, data_size + base_size, start_node.node_id);

   auto recv_ptr = transport->get_recv_buffer(start_node.node_id);
   auto recv_msg = new (recv_ptr) message_t();
   transport->recv(recv_ptr, MAX_MESSAGE_SIZE, start_node.node_id);
   if(recv_msg->get_type() != message_t::type_t::ACK)
       debug::notify_error("[rpc_remove] failed (key %lu)", k);
   UnstructuredBuffer recv_buffer(recv_msg->get_data());
   int level = -1;
   recv_buffer.get(&level);
   assert(level != -1);
   recv_buffer.get(&leaf_addr);
   assert(recv_msg->get_data_size() == recv_buffer.size());
   return level;
}

template <typename Value>
int rpc_update(global_addr_t start_node, Key k, Value v, global_addr_t& leaf_addr){
   auto send_ptr = transport->get_buffer();
   auto send_msg = new (send_ptr) message_t(message_t::type_t::REQUEST_DEX_UPDATE);
   UnstructuredBuffer send_buffer(send_msg->get_data());
   uint32_t base_size = sizeof(message_t) - sizeof(char*);
   send_buffer.put(&k);
   send_buffer.put(&v);
   send_buffer.put(&start_node);
   uint32_t data_size = send_buffer.size();
   send_msg->set_data_size(data_size);
   transport->send(send_ptr, data_size + base_size, start_node.node_id);

   auto recv_ptr = transport->get_recv_buffer(start_node.node_id);
   auto recv_msg = new (recv_ptr) message_t();
   transport->recv(recv_ptr, MAX_MESSAGE_SIZE, start_node.node_id);
   if(recv_msg->get_type() != message_t::type_t::ACK)
       debug::notify_error("[rpc_remove] failed (key %lu)", k);
   UnstructuredBuffer recv_buffer(recv_msg->get_data());
   int level = -1;
   recv_buffer.get(&level);
   assert(level != -1);
   recv_buffer.get(&leaf_addr);
   assert(recv_msg->get_data_size() == recv_buffer.size());
   return level;
}

template <typename Value>
int rpc_lookup(global_addr_t start_node, Key k, Value& v){
   auto send_ptr = transport->get_buffer();
   auto send_msg = new (send_ptr) message_t(message_t::type_t::REQUEST_DEX_LOOKUP);
   UnstructuredBuffer send_buffer(send_msg->get_data());
   uint32_t base_size = sizeof(message_t) - sizeof(char*);
   send_buffer.put(&k);
   send_buffer.put(&start_node);
   uint32_t data_size = send_buffer.size();
   send_msg->set_data_size(data_size);
   transport->send(send_ptr, data_size + base_size, start_node.node_id);

   auto recv_ptr = transport->get_recv_buffer(start_node.node_id);
   auto recv_msg = new (recv_ptr) message_t();
   transport->recv(recv_ptr, MAX_MESSAGE_SIZE, start_node.node_id);
   if(recv_msg->get_type() != message_t::type_t::ACK)
       debug::notify_error("[rpc_lookup] failed (key %lu)", k);
   UnstructuredBuffer recv_buffer(recv_msg->get_data());

   int level = -1;
   recv_buffer.get(&level);
   if (level >= 1)
       recv_buffer.get(&v);
   assert(recv_msg->get_data_size() == recv_buffer.size());
   return level;
}

// Return nullptr means there is global conflict in remote side
NodeBase *opt_remote_read(global_addr_t global_node);

// Consistent read using optimistic locking (From Carsten's SIGMOD 23')
NodeBase *remote_consistent_read(global_addr_t global_node);

char *raw_remote_read(global_addr_t global_node, size_t read_size);

NodeBase *raw_remote_read(global_addr_t global_node);

NodeBase *raw_remote_read_by_sibling(global_addr_t global_node);

void remote_write(global_addr_t global_node, NodeBase *mem_node, 
                  bool clear_lock = true, bool without_copy = false);

NodeBase *opt_remote_read_by_sibling(global_addr_t global_node);

// Consistent read using optimistic locking (From Carsten's SIGMOD 23')
NodeBase *remote_consistent_read_by_sibling(global_addr_t global_node);

void remote_write_by_sibling(global_addr_t global_node, NodeBase *mem_node, bool clear_lock = true);

uint64_t murmur64(uint64_t k);

void update_all_parent_ptr(NodeBase *mem_node);

// Without Bitmap
NodeBase *recursive_iterate_without_bitmap(NodeBase *mem_node,
                                           int &idx_in_parent);
  // First test whether this is a mininode and randomly select which node to
  // choose
  bool needRestart = false;
  uint64_t versionNode = mem_node->readLockOrRestart(needRestart);
  if (needRestart)
    return nullptr;

  BTreeInner *parent = nullptr;
  uint64_t versionParent = 0;

  while (mem_node->type == PageType::BTreeInner) {
    auto inner = static_cast<BTreeInner *>(mem_node);
    if (parent) {
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart)
        return nullptr;
    }

    parent = inner;
    versionParent = versionNode;

    uint64_t hash_k = murmur64(reinterpret_cast<uint64_t>(mem_node));
    auto inner_count = inner->count;
    int start_idx = hash_k % (inner_count + 1);
    int next_child_idx;
    for (int i = 0; i < inner_count + 1; i++) {
      next_child_idx = (start_idx + i) % (inner_count + 1);
      auto node_val = inner->children[next_child_idx].val;
      if (node_val & swizzle_tag) {
        idx_in_parent = next_child_idx;
        mem_node = reinterpret_cast<NodeBase *>(node_val & swizzle_hide);
        break;
      }
    }

    if (mem_node == reinterpret_cast<NodeBase *>(parent)) {
      break;
    }

    versionNode = mem_node->readLockOrRestart(needRestart);
    if (needRestart)
      return nullptr;
  }

  mem_node->upgradeToWriteLockOrRestart(versionNode, needRestart);
  if (needRestart)
    return nullptr;

  if (mem_node->pos_state != 2) {
    mem_node->writeUnlock();
    return nullptr;
  }

  if (mem_node->type == PageType::BTreeInner) {
    auto inner = static_cast<BTreeInner *>(mem_node);
    for (int i = 0; i < inner->count + 1; ++i) {
      if (inner->children[i].val & swizzle_tag) {
        inner->writeUnlock();
        return nullptr;
      }
    }
  }

  return mem_node;
}

//- 1 means we don't know which idx points to this child NodeBase *
// This iteration is not protected by epoch
inline NodeBase *recursive_iterate(NodeBase *mem_node, int &idx_in_parent) {
  // First test whether this is a mininode and randomly select which node to
  // choose
  bool needRestart = false;
  uint64_t versionNode = mem_node->readLockOrRestart(needRestart);
  if (needRestart)
    return nullptr;

  BTreeInner *parent = nullptr;
  uint64_t versionParent = 0;

  while (mem_node->bitmap != 0) {
    auto inner = static_cast<BTreeInner *>(mem_node);
    if (parent) {
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart)
        return nullptr;
    }

    parent = inner;
    versionParent = versionNode;

    uint64_t hash_k = murmur64(reinterpret_cast<uint64_t>(mem_node));
    int idx = hash_k % (inner->count + 1);

    auto next_idx = inner->closest_set(idx);
    if (next_idx == -1)
      return nullptr;
    idx_in_parent = next_idx;

    auto next_node = inner->children[next_idx].val;
    mem_node = reinterpret_cast<NodeBase *>(next_node & swizzle_hide);

    inner->checkOrRestart(versionNode, needRestart);
    if (needRestart)
      return nullptr;

    assert((next_node & swizzle_tag) != 0);
    versionNode = mem_node->readLockOrRestart(needRestart);
    if (needRestart)
      return nullptr;
  }

  mem_node->upgradeToWriteLockOrRestart(versionNode, needRestart);
  if (needRestart)
    return nullptr;

  if ((mem_node->bitmap != 0) || (mem_node->pos_state != 2)) {
    mem_node->writeUnlock();
    return nullptr;
  }

  return mem_node;
}

inline void fully_unswizzle(NodeBase *mem_node) {
  if (mem_node->type == PageType::BTreeLeaf)
    return;

  auto inner = reinterpret_cast<BTreeInner *>(mem_node);
  for (int i = 0; i < inner->count + 1; ++i) {
    if (inner->children[i].val & swizzle_tag) {
      assert(inner->bitmap & (1ULL << i));
      auto child =
          reinterpret_cast<NodeBase *>(inner->children[i].val & swizzle_hide);
      inner->children[i] = child->remote_address;
    }
  }
  inner->bitmap = 0;
}

// check whether the min/max limits match
inline bool check_limit_match(NodeBase *parent, NodeBase *child,
                       bool using_swizzle = false, bool verbose = false) {
  auto inner = reinterpret_cast<BTreeInner *>(parent);
  int idx = -1;
  if (using_swizzle) {
    idx = inner->findIdx(reinterpret_cast<uint64_t>(child) | swizzle_tag);
  } else {
    idx = inner->findIdx(child->remote_address.val);
  }
  if (verbose)
    std::cout << "The idx = " << idx << std::endl;
  if (idx == -1)
    return false;

  Key min_limit = (idx == 0) ? inner->min_limit_ : inner->keys[idx - 1];
  Key max_limit = (idx == inner->count) ? inner->max_limit_ : inner->keys[idx];
  if (verbose) {
    std::cout << "Parent min limit = " << parent->min_limit_
              << ", max limit = " << parent->max_limit_ << std::endl;
    std::cout << "Parent level = " << static_cast<int>(parent->level)
              << std::endl;
    std::cout << "Min limit in parent = " << min_limit
              << ", max_limit in parent = " << max_limit << std::endl;
    std::cout << "Child level = " << static_cast<int>(child->level)
              << std::endl;
    std::cout << "child min limit = " << child->min_limit_
              << ", max limit = " << child->max_limit_ << std::endl;
  }
  if (min_limit == child->min_limit_ && max_limit == child->max_limit_) {
    return true;
  }
  return false;
}

inline bool new_check_limit_match(NodeBase *parent, NodeBase *child, unsigned idx) {
  auto inner = reinterpret_cast<BTreeInner *>(parent);
  Key min_limit = (idx == 0) ? inner->min_limit_ : inner->keys[idx - 1];
  Key max_limit = (idx == inner->count) ? inner->max_limit_ : inner->keys[idx];
  if (min_limit == child->min_limit_ && max_limit == child->max_limit_) {
    return true;
  }
  return false;
}

inline bool check_parent_child_info(NodeBase *parent, NodeBase *child) {
  if (parent->level == 255)
    return true;
  bool verbose = false;
  auto parent_level = static_cast<int>(parent->level);
  auto child_level = static_cast<int>(child->level);
  if (parent_level != child_level + 1) {
    verbose = true;
  }

  if (child->min_limit_ < parent->min_limit_ ||
      child->max_limit_ > parent->max_limit_) {
    verbose = true;
  }

  if (!(check_limit_match(parent, child, true) ||
        check_limit_match(parent, child, false))) {
    verbose = true;
  }

  if (verbose) {
    if (child->parent_ptr == parent) {
      std::cout << "Parent is child's parent" << std::endl;
    } else {
      std::cout << "Parent is not child's parent" << std::endl;
    }

    std::cout << "Parent level = " << parent_level << std::endl;
    std::cout << "Child level = " << child_level << std::endl;

    std::cout << "parent min limit = " << parent->min_limit_
              << "; parent max limit = " << parent->max_limit_ << std::endl;
    std::cout << "child min limit = " << child->min_limit_
              << "; child max limit = " << child->max_limit_ << std::endl;

    if (parent->type == PageType::BTreeLeaf) {
      std::cout << "The parent is a leaf node." << std::endl;
    } else if (parent->type == PageType::BTreeInner) {
      std::cout << "The parent is a inner node." << std::endl;
    }

    if (child->type == PageType::BTreeLeaf) {
      std::cout << "The child is a leaf node." << std::endl;
    } else if (child->type == PageType::BTreeInner) {
      std::cout << "The child is a inner node." << std::endl;
    }
    std::cout << "1. --------------------------------" << std::endl;
    check_limit_match(parent, child, true, true);
    std::cout << "2. --------------------------------" << std::endl;
    check_limit_match(parent, child, false, true);
  }

   return !verbose;
}

inline void check_global_conflict(BTreeInner *remote_cur_node,
                           uint64_t org_version, bool &needRestart) {
  if (remote_cur_node->typeVersionLockObsolete != 0) {
    needRestart = true;
  }

  if (org_version != remote_cur_node->front_version) {
    needRestart = true;
  }
}

} // namespace cachepush
