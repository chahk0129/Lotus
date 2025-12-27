#include "batch/group.h"
#include "batch/table.h"
#include "system/global.h"

batch_table_t::batch_table_t() {
    _num_groups = g_num_client_threads / MAX_GROUP_SIZE;
    if (g_num_client_threads % MAX_GROUP_SIZE != 0) _num_groups++;
    _groups = new batch_group_t*[_num_groups];
    for (uint32_t i=0; i<_num_groups; i++)
        _groups[i] = new batch_group_t(i);
}

batch_table_t::~batch_table_t() { 
    for (uint32_t i=0; i<_num_groups; i++)
        delete _groups[i];
    delete[] _groups;
}