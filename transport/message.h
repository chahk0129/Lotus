#pragma once

#include "system/global.h"

class message_t{
    public:
		enum type_t{
			// for txn
			REQUEST_STORED_PROCEDURE,
			REQUEST_BATCHED_STORED_PROCEDURE,
			REQUEST_INTERACTIVE,

			// for 2PC
			REQUEST_PREPARE,
			REQUEST_COMMIT,
			REQUEST_ABORT,
	
			RESPONSE_PREPARE,
			RESPONSE_COMMIT,
			RESPONSE_ABORT,
			RESPONSE_WAIT,
			
			// for global synchronization
			QP_SETUP,
			SYNC,
			TERMINATE,
			ACK,

			// for one-sided RDMA
			REQUEST_RPC_ALLOC,
			REQUEST_IDX_UPDATE_ROOT,
			REQUEST_IDX_GET_ROOT,


			// dex
			REQUEST_DEX_INSERT,
			REQUEST_DEX_UPDATE,
			REQUEST_DEX_DELETE,
			REQUEST_DEX_LOOKUP,
			REQUEST_DEX_SCAN,
		};

		message_t();
		message_t(type_t type);
		message_t(type_t type, uint32_t data_size);
		message_t(type_t type, uint32_t qp_id, uint32_t data_size);
		message_t(type_t type, uint32_t qp_id, uint32_t data_size, char* data);
		message_t(message_t* msg);
		message_t(char* packet);
		~message_t();
		
		type_t get_type() 				       { return _type; }
		void   set_type(type_t type)		   { _type = type; }
		bool   is_system_txn()				   { return _type >= QP_SETUP; }
		bool   is_regular_txn()				   { return _type >= REQUEST_STORED_PROCEDURE && _type <= REQUEST_INTERACTIVE; }

		uint32_t get_node_id()			       { return _node_id; }
		void     set_node_id(uint32_t node_id) { _node_id = node_id; }
		uint32_t get_qp_id()			       { return _qp_id; }
		void     set_qp_id(uint32_t qp_id)     { _qp_id = qp_id; }

		uint32_t get_packet_len()		       { return sizeof(message_t) + _data_size; }
		uint32_t get_data_size()		       { return _data_size; }
		void     set_data(char* data)	       { _data = data; }
		void     set_data_size(uint32_t size)  { _data_size = size; }
		char*    get_data()    		           { return _data; }
		char*    get_data_ptr()			       { return reinterpret_cast<char*>(&_data); }

		void to_packet(char* packet){
			memcpy(packet, this, sizeof(message_t));
			if(_data_size > 0)
				memcpy(packet + sizeof(message_t), _data, _data_size);
		}

		static std::string get_name(type_t type);
		std::string get_name() { return get_name(get_type()); }

    private:
		type_t		_type;
		uint32_t	_node_id;
		uint32_t    _qp_id;
		uint32_t	_data_size;
		char* 		_data;
};

