#pragma once

#include <cstdint>
#include <atomic>
#include "system/global.h"

class batch_group_t;

// batch table managing multiple batch groups
//// [ batch table ] 
//// -- [ batch group ] * g_num_server_nodes 
//// ---- [ batch entry ] * MAX_GROUP_SIZE * g_num_batch_threads
class batch_table_t {
public:
    batch_table_t(); 
    ~batch_table_t();

    static constexpr uint32_t MAX_GROUP_SIZE = 8;
    static constexpr uint32_t MAX_GROUP_NUM = (MAX_NUM_THREADS - NUM_SERVER_THREADS) / MAX_GROUP_SIZE;
    // static constexpr uint32_t MAX_GROUP_SIZE = 4;
    // static constexpr uint32_t MAX_GROUP_SIZE = 2;

    batch_group_t* get_group(uint32_t idx) { return _groups[idx]; }
    uint32_t       get_num_groups() { return _num_groups; }
    

private:
    uint32_t        _num_groups;
    batch_group_t** _groups;
};