#include "system/global.h"
#include "storage/table.h"
#include "storage/catalog.h"
#include "storage/row.h"
#include "utils/helper.h"
#include "concurrency/twosided/nowait.h"
#include "concurrency/twosided/waitdie.h"
#include "concurrency/twosided/woundwait.h"

row_t::row_t() { 
    init_manager();
}

row_t::~row_t() {
#if TRANSPORT != ONE_SIDED
    if (manager) {
        delete manager;
        manager = nullptr;
    }
#endif
}

void row_t::init_manager() {
#if TRANSPORT == ONE_SIDED
    memset(&manager, 0, 16);
#else // TWO_SIDED
    manager = new ROW_MAN ();
#endif
}

int row_t::get_tuple_size(catalog_t* schema){
    return schema->get_tuple_size();
}

int row_t::get_field_cnt(catalog_t* schema){
    return schema->field_cnt;
}

void row_t::set_value(catalog_t* schema, int id, int val){
    int data_size = schema->get_field_size(id);
    int pos = schema->get_field_index(id);
    memcpy(&data[pos], &val, data_size);
}

void row_t::set_value(catalog_t* schema, int id, void* ptr){
    int data_size = schema->get_field_size(id);
    int pos = schema->get_field_index(id);
    memcpy(&data[pos], ptr, data_size);
}

void row_t::set_value(catalog_t* schema, int id, void* ptr, int size){
    int pos = schema->get_field_index(id);
    memcpy(&data[pos], ptr, size);
}

void row_t::set_value(catalog_t* schema, const char* col_name, void* ptr){
    auto id = schema->get_field_id(col_name);
    set_value(schema, id, ptr);
}

void row_t::get_value(catalog_t* schema, int id, void* ptr){
    int data_size = schema->get_field_size(id);
    int pos = schema->get_field_index(id);
    memcpy(ptr, &data[pos], data_size);
}

char* row_t::get_value(catalog_t* schema, int id){
    int pos = schema->get_field_index(id);
    return &data[pos];
}

char* row_t::get_value(catalog_t* schema, char* col_name){
    int pos = schema->get_field_index(col_name);
    return &data[pos];
}

char* row_t::get_value_v(catalog_t* schema, int id, char* data){
    return &data[schema->get_field_index(id)];
}

void row_t::set_value_v(catalog_t* schema, int id, char* data, char* value) {
    memcpy(&data[schema->get_field_index(id)], value, schema->get_field_size(id));
}

char* row_t::get_data(){
    return data;
}

void row_t::set_data(char* data_to_copy, int size){
    memcpy(data, data_to_copy, size);
}

void row_t::copy(catalog_t* schema, char* data_to_copy) {
    set_data(data_to_copy, get_tuple_size(schema));
}

void row_t::copy(catalog_t* schema, row_t* src, int idx){
    char* ptr = src->get_value(schema, idx);
    set_value(schema, idx, ptr);
}

void row_t::copy(catalog_t* schema, row_t* src){
    set_data(src->get_data(), src->get_tuple_size(schema));
}