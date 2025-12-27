#pragma once
#include "system/cc_manager.h"

#if PARTITIONED

class idx_manager_t: public cc_manager_t {
public:
    idx_manager_t(txn_t* txn);
    ~idx_manager_t() { }

    void       register_access(uint32_t node_id, access_t type, uint64_t key, uint32_t table_id, uint32_t index_id, uint64_t value=0);
    void       register_access(access_t type, uint64_t key, uint32_t table_id, uint32_t index_id, uint64_t cache, uint64_t value=0, uint64_t evict_key=0, uint64_t evict_value=0);

    void       get_resp_data_last(char*& data, uint32_t& size);
    void       get_resp_data(uint32_t from, uint32_t to, char*& data, uint32_t& size);
    void       get_resp_data(uint32_t access_idx);
    void       get_resp_data();

    uint32_t   get_row_cnt() { return _access_set.size(); }
    RC         get_row(uint32_t access_idx) { return RCOK; }
    RC         get_row(cc_manager_t::RowAccess* access) { return RCOK; }
    RC         get_remote_row(uint32_t access_idx);
    RC         get_remote_row(cc_manager_t::RowAccess* access);

    void       process_cache(cc_manager_t::RowAccess* access);
    row_t*     process_index(cc_manager_t::RowAccess* access);
    RC         process_prepare(char* data, uint32_t size) { return RCOK; }
    RC         process_commit() { return COMMIT; }
    void       cleanup(RC rc);
    void       abort(uint32_t abort_access_idx) { assert(false); }
    void       abort(cc_manager_t::RowAccess* access) { assert(false); }
    void       clear();

    char*      get_data(uint64_t key, uint32_t table_id);
    void       serialize_commit(uint32_t node_id, char*& data, uint32_t& size) { assert(false); }
    void       serialize(uint32_t node_id, std::vector<cc_manager_t::RowAccess*>& access_set, char*& data, uint32_t& size);
    void       deserialize(uint32_t node_id, std::vector<cc_manager_t::RowAccess*>& access_set, char* data, uint32_t size);
    void       serialize_last(uint32_t node_id, char*& data, uint32_t& size);
    void       deserialize_last(uint32_t node_id, char* data, uint32_t size);
    
    uint32_t   get_last_node_involved();
    uint32_t   get_nodes_involved(std::vector<uint32_t>& nodes);
    uint32_t   get_commit_nodes(std::vector<uint32_t>& nodes);
    void       clear_nodes_involved() { }
    void       clear_nodes_involved(std::vector<uint32_t>& nodes_aborted) {
                   for (auto node_id: nodes_aborted) 
                       _nodes_involved.erase(node_id);
               }
    void       abort() { assert(false); }

    cc_manager_t::RowAccess* get_last_access();
    int        get_last_access_idx();

private:
    cc_manager_t::RowAccess* get_access(uint64_t key, uint32_t table_id);
    cc_manager_t::RowAccess* get_write_access(uint32_t& idx_writes);
    uint32_t   get_access_idx(uint64_t key, uint32_t table_id);
    void       commit_insdel();

    std::set<uint32_t>       _nodes_involved;
    std::vector<IndexAccess> _index_access_set;
    std::map<uint64_t, value_t> _cache_set;
    std::vector<cc_manager_t::RowAccess>   _access_set;
    cc_manager_t::RowAccess*               _last_access;
    int _last_access_idx;
};

#endif