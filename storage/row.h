#pragma once
#include "system/global.h"
#include "system/global_address.h"

class table_t;
class catalog_t;

class row_t{
public:
	row_t();
	~row_t();

	const char* get_table_name(table_t* table);
	table_t* 	get_table();
	catalog_t* 	get_schema();
	int 		get_field_cnt(catalog_t* schema);
	int 		get_tuple_size(catalog_t* schema);

	void 		copy(catalog_t* schema, row_t* src);
	void 		copy(catalog_t* schema, row_t* src, int idx);
	void 		copy(catalog_t* schema, char* data);

	int 		get_table_idx();

	void 		set_value(catalog_t* schema, int id, int val);
	void 		set_value(catalog_t* schema, int id, void* ptr);
	void 		set_value_plain(catalog_t* schema, int id, void* ptr);
	void 		set_value(catalog_t* schema, int id, void* ptr, int size);
	void 		set_value(catalog_t* schema, const char* col_name, void* ptr);

	void 		get_value(catalog_t* schema, int id, void* ptr);
	char* 		get_value(catalog_t* schema, int id);
	char* 		get_value(catalog_t* schema, char* col_name);

	static char* get_value_v(catalog_t* schema, int id, char* data);
	static void  set_value_v(catalog_t* schema, int id, char* data, char* value);

	void 		set_data(char* data, int size);
	char* 		get_data();

	void 	  	init_manager();

	ROW_MAN*    manager;
	char        padding[8]; // for one-sided RDMA timestamp alignment
	char        data[DEFAULT_ROW_SIZE];
};
