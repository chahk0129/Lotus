#pragma once
#include "system/global.h"
#include "system/global_address.h"
#include "system/manager.h"
#include "utils/helper.h"
#include "utils/packetize.h"
#include "utils/debug.h"
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

   UnstructuredBuffer send_buffer(send_msg->get_data_ptr());
   send_buffer.put(&k);
   send_buffer.put(&v);
   send_buffer.put(&start_node);
   uint32_t data_size = send_buffer.size();
   send_msg->set_data_size(data_size);
   transport->send(send_ptr, start_node.node_id, data_size + base_size);

   auto recv_ptr = transport->get_recv_buffer(start_node.node_id);
   auto recv_msg = new (recv_ptr) message_t();
   transport->recv(recv_ptr, start_node.node_id, MAX_MESSAGE_SIZE);
   if(recv_msg->get_type() != message_t::type_t::ACK)
       debug::notify_error("[rpc_insert] failed (key %lu, value %lu)", k, v);
   UnstructuredBuffer recv_buffer(recv_msg->get_data_ptr());
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
   UnstructuredBuffer send_buffer(send_msg->get_data_ptr());
   uint32_t base_size = sizeof(message_t) - sizeof(char*);
   send_buffer.put(&k);
   send_buffer.put(&start_node);
   uint32_t data_size = send_buffer.size();
   send_msg->set_data_size(data_size);
   transport->send(send_ptr, start_node.node_id, data_size + base_size);

   auto recv_ptr = transport->get_recv_buffer(start_node.node_id);
   auto recv_msg = new (recv_ptr) message_t();
   transport->recv(recv_ptr, start_node.node_id, MAX_MESSAGE_SIZE);
   if(recv_msg->get_type() != message_t::type_t::ACK)
       debug::notify_error("[rpc_remove] failed (key %lu)", k);
   UnstructuredBuffer recv_buffer(recv_msg->get_data_ptr());
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
   UnstructuredBuffer send_buffer(send_msg->get_data_ptr());
   uint32_t base_size = sizeof(message_t) - sizeof(char*);
   send_buffer.put(&k);
   send_buffer.put(&v);
   send_buffer.put(&start_node);
   uint32_t data_size = send_buffer.size();
   send_msg->set_data_size(data_size);
   transport->send(send_ptr, start_node.node_id, data_size + base_size);

   auto recv_ptr = transport->get_recv_buffer(start_node.node_id);
   auto recv_msg = new (recv_ptr) message_t();
   transport->recv(recv_ptr, start_node.node_id, MAX_MESSAGE_SIZE);
   if(recv_msg->get_type() != message_t::type_t::ACK)
       debug::notify_error("[rpc_remove] failed (key %lu)", k);
   UnstructuredBuffer recv_buffer(recv_msg->get_data_ptr());
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
   UnstructuredBuffer send_buffer(send_msg->get_data_ptr());
   uint32_t base_size = sizeof(message_t) - sizeof(char*);
   send_buffer.put(&k);
   send_buffer.put(&start_node);
   uint32_t data_size = send_buffer.size();
   send_msg->set_data_size(data_size);
   transport->send(send_ptr, start_node.node_id, data_size + base_size);

   auto recv_ptr = transport->get_recv_buffer(start_node.node_id);
   auto recv_msg = new (recv_ptr) message_t();
   transport->recv(recv_ptr, start_node.node_id, MAX_MESSAGE_SIZE);
   if(recv_msg->get_type() != message_t::type_t::ACK)
       debug::notify_error("[rpc_lookup] failed (key %lu)", k);
   UnstructuredBuffer recv_buffer(recv_msg->get_data_ptr());

   int level = -1;
   recv_buffer.get(&level);
   if (level >= 1)
       recv_buffer.get(&v);
   assert(recv_msg->get_data_size() == recv_buffer.size());
   return level;
}

// Function declarations (implementations moved to leanstore_wr.cpp)
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
NodeBase *recursive_iterate_without_bitmap(NodeBase *mem_node, int &idx_in_parent);

//- 1 means we don't know which idx points to this child NodeBase *
// This iteration is not protected by epoch
NodeBase *recursive_iterate(NodeBase *mem_node, int &idx_in_parent);

void fully_unswizzle(NodeBase *mem_node);

// check whether the min/max limits match
bool check_limit_match(NodeBase *parent, NodeBase *child,
                       bool using_swizzle = false, bool verbose = false);

bool new_check_limit_match(NodeBase *parent, NodeBase *child, unsigned idx);

bool check_parent_child_info(NodeBase *parent, NodeBase *child);

void check_global_conflict(BTreeInner *remote_cur_node,
                           uint64_t org_version, bool &needRestart);

} // namespace cachepush
