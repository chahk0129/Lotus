#include "storage/table.h"
#include "storage/catalog.h"
#include "storage/row.h"

table_t::table_t(catalog_t* schema){
    this->schema = schema;
    table_name = schema->table_name;
}

uint64_t table_t::get_tuple_size(){
    return schema->get_tuple_size();
}

uint64_t table_t::get_field_cnt(){
    return schema->field_cnt;
}

catalog_t* table_t::get_schema(){
    return schema;
}

RC table_t::get_new_row(row_t *& row) {
    row = new row_t();
    return RCOK;
}
