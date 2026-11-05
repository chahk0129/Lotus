#pragma once
#include "txn/id.h"
#include "txn/txn.h"
#include "transport/message.h"
#include <vector>

class message_t;
class base_query_t;

class stored_procedure_t : public txn_t {
public:
    stored_procedure_t(txn_id_t txn_id);
    stored_procedure_t(base_query_t* query);
    virtual ~stored_procedure_t() = default;

    virtual RC execute() = 0;

    virtual void init();

    message_t::type_t get_txn_type() { return message_t::type_t::REQUEST_STORED_PROCEDURE; }
    uint32_t get_nodes_involved(std::vector<uint32_t>& nodes);
    RC       process_request();
};
