#pragma once
#include "txn/id.h"
#include "txn/txn.h"
#include "transport/message.h"
#include <vector>

class base_query_t;

class interactive_t: public txn_t {
public:
    interactive_t(txn_id_t txn_id);
    interactive_t(base_query_t* query);
    virtual ~interactive_t() = default;

    virtual RC execute() = 0;

    virtual void init();

    message_t::type_t get_txn_type() { return message_t::type_t::REQUEST_INTERACTIVE; }
    uint32_t get_nodes_involved(std::vector<uint32_t>& nodes);
    RC       process_request();
};