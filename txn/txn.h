#pragma once

#include "system/global.h"
#include "txn/id.h"
#include <atomic>

class base_query_t;
class cc_manager_t;
class message_t;

class txn_t {
public:
    txn_t(txn_id_t txn_id);
    txn_t(base_query_t* query);
    virtual ~txn_t();
    virtual void init();

    txn_id_t      get_id()           { return _txn_id; }
    base_query_t* get_query()        { return _query; }
    void          set_query(base_query_t* query);
    cc_manager_t* get_cc_manager()   { return _cc_manager; }


    enum state_t { RUNNING, WAITING, COMMITTING, ABORTING };

    state_t       get_state()                                     { return _state.load(); }
    void          set_state(state_t state)                        { _state.store(state); }
    bool          update_state(state_t expected, state_t desired) { return _state.compare_exchange_strong(expected, desired); }

    void          set_ts(uint64_t ts) { _timestamp = ts; }
    uint64_t      get_ts()            { return _timestamp; }

    virtual RC    execute() = 0;
    virtual RC    process_request() = 0;

    void          parse_request(message_t* msg);
    RC            process_request(char*& resp_data, uint32_t& resp_size);
    RC            process_commit();
    RC            process_single_commit(std::vector<uint32_t>& nodes_involved);
    RC            process_2pc_prepare(std::vector<uint32_t>& nodes_involved);
    void          process_2pc_commit(std::vector<uint32_t>& nodes_involved);
    void          process_abort_nodes();

    RC            process_prepare(char* data, uint32_t size);
    RC            process_commit(char* data, uint32_t size);
    void          process_abort();
    void          process_abort(uint32_t abort_access_idx); 

    void          force_abort();
protected:
    bool          handle_batching_leader(std::vector<uint32_t>& nodes_involved, uint32_t* wait_time);
    bool          handle_batching_member(std::vector<uint32_t>& nodes_involved, uint32_t* wait_time);
    void          wait_for_responses(uint32_t num_responses);

    txn_id_t       _txn_id;
    cc_manager_t*  _cc_manager; // concurrency control manager
    base_query_t*  _query;
    uint64_t       _timestamp;
    uint32_t       _prev_offset;
    uint32_t       _curr_offset;

    // TXN state for concurrency control (for 2PC)
    std::atomic<state_t> _state;
};