#pragma once
#include <cstdint>

struct txn_id_t {
    uint32_t node_id;
    uint32_t tid;

    txn_id_t(uint32_t node_id, uint32_t tid): node_id(node_id), tid(tid) { }
    txn_id_t(const txn_id_t& other): node_id(other.node_id), tid(other.tid) { }
    
    bool operator==(const txn_id_t& other) const {
        return node_id == other.node_id && tid == other.tid;
    }

    bool operator!=(const txn_id_t& other) const {
        return !(*this == other);
    }

    bool operator==(const uint64_t& other) const {
        return to_uint64() == other;
    }

    bool operator!=(const uint64_t& other) const {
        return !(*this == other);
    }

    uint32_t get_node_id() const { return node_id; }
    uint32_t get_qp_id() const   { return tid; }
    uint64_t to_uint64() const { return (static_cast<uint64_t>(node_id) << 32) | tid; }
};