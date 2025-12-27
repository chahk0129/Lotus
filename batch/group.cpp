#include "batch/group.h"
#include "batch/table.h"
#include "system/global.h"

batch_group_t::batch_group_t(uint32_t group_id) {
    _leader.store(-1);
    _active_members.store(0);

    _entries = new std::pair<uint32_t, std::atomic<int>*>[batch_table_t::MAX_GROUP_SIZE];
    for (uint32_t i=0; i<batch_table_t::MAX_GROUP_SIZE; i++) {
        _entries[i].first = group_id * batch_table_t::MAX_GROUP_SIZE + i;
        _entries[i].second = new std::atomic<int>[g_num_server_nodes];
        for (uint32_t j=0; j<g_num_server_nodes; j++)
            _entries[i].second[j].store(0);
    }
}