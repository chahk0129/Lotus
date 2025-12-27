#include "system/global.h"
#include "transport/message.h"
#include "utils/helper.h"
#include "system/manager.h"

message_t::message_t(): _type(ACK),_node_id(g_node_id), _qp_id(GET_THD_ID), _data_size(0) { }
message_t::message_t(type_t type): _type(type), _node_id(g_node_id), _qp_id(GET_THD_ID), _data_size(0) { }
message_t::message_t(type_t type, uint32_t data_size): _type(type), _node_id(g_node_id), _qp_id(GET_THD_ID), _data_size(data_size) { }
message_t::message_t(type_t type, uint32_t qp_id, uint32_t data_size): _type(type), _node_id(g_node_id), _qp_id(qp_id), _data_size(data_size) { }
message_t::message_t(type_t type, uint32_t qp_id, uint32_t data_size, char* data): _type(type), _node_id(g_node_id), _qp_id(qp_id), _data_size(data_size) { 
    _data = new char[_data_size];
    memcpy(_data, data, _data_size);
}

message_t::message_t(message_t* msg) {
    memcpy(this, msg, sizeof(message_t));
    _data = new char[_data_size];
    memcpy(_data, msg->get_data(), _data_size);
}

message_t::message_t(char* packet) {
    memcpy(this, packet, sizeof(message_t));
    if(_data_size > 0) {
        _data = new char[_data_size];
        memcpy(_data, packet + sizeof(message_t), _data_size);
    }
    else
        _data = nullptr;
}

message_t::~message_t() {
    if(_data) {
        delete[] _data;
        _data = nullptr;
    }
}

std::string message_t::get_name(type_t type) {
    switch (type) {
        // for txn
        case REQUEST_STORED_PROCEDURE:      return "REQUEST_STORED_PROCEDURE";
        case REQUEST_INTERACTIVE:           return "REQUEST_INTERACTIVE";
        case REQUEST_BATCHED_STORED_PROCEDURE: return "REQUEST_BATCHED_STORED_PROCEDURE";
        case REQUEST_PREPARE:               return "REQUEST_PREPARE";
        case REQUEST_COMMIT:                return "REQUEST_COMMIT";
        case REQUEST_ABORT:                 return "REQUEST_ABORT";
        case REQUEST_RPC_ALLOC:             return "REQUEST_RPC_ALLOC";
        case RESPONSE_WAIT:                 return "RESPONSE_WAIT";
        case RESPONSE_PREPARE:              return "RESPONSE_PREPARE";
        case RESPONSE_COMMIT:               return "RESPONSE_COMMIT";
        case RESPONSE_ABORT:                return "RESPONSE_ABORT";

        // for global synchronization
        case QP_SETUP:                      return "QP_SETUP";
        case TERMINATE:                     return "TERMINATE";
        case SYNC:                          return "SYNC";
        case ACK:                           return "ACK";

        // for one-sided RDMA
        case REQUEST_IDX_UPDATE_ROOT:       return "REQUEST_IDX_UPDATE_ROOT";
        case REQUEST_IDX_GET_ROOT:          return "REQUEST_IDX_GET_ROOT";
        case REQUEST_DEX_INSERT:            return "REQUEST_DEX_INSERT";
        case REQUEST_DEX_UPDATE:            return "REQUEST_DEX_UPDATE";
        case REQUEST_DEX_DELETE:            return "REQUEST_DEX_DELETE";
        case REQUEST_DEX_LOOKUP:            return "REQUEST_DEX_LOOKUP";
        case REQUEST_DEX_SCAN:              return "REQUEST_DEX_SCAN";
        default:                            break;
    }
    assert(false);
    return "UNKNOWN_TYPE";
}