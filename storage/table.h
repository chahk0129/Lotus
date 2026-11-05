#pragma once
#include "system/global.h"

class catalog_t;
class row_t;
class table_t{
public:
	table_t(catalog_t* schema);

	RC get_new_row(row_t*& row);
	uint64_t get_tuple_size();
	uint64_t get_field_cnt();
	catalog_t* get_schema();

	uint64_t get_table_size() { return cur_tab_size; }
	const char* get_table_name() { return table_name; }
	uint32_t get_table_id() { return table_id; }
	void set_table_id(uint32_t id) { table_id = id; }

	catalog_t* schema;

private:
	char* table_name;
	uint64_t cur_tab_size;
	uint32_t table_id;
};
