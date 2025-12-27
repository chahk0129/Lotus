#include "system/workload.h"
#include "system/cache_allocator.h"
#include "storage/row.h"
#include "storage/table.h"
#include "storage/catalog.h"
#include "client/transport.h"
#include "index/idx_wrapper.h"
#include "index/onesided/dex/leanstore_tree.h"
#include "index/onesided/sherman/Tree.h"
#include "index/twosided/partitioned/idx.h"
#include "index/twosided/non_partitioned/idx.h"
#include "index/twosided/partitioned/cache_manager.h"
#include "index/twosided/non_partitioned/cache_manager.h"

RC workload_t::init() {
    return RCOK;
}

catalog_t* workload_t::init_schema(std::string path) {
    std::string line;
    std::ifstream ifs(path);
    assert(ifs.is_open());

    catalog_t* schema = nullptr;
    uint32_t num_indexes = 0;
    uint32_t num_tables = 0;
    while (getline(ifs, line)) {
        if (line.compare(0, 6, "TABLE=") == 0) {
            std::string table_name = &line[6];
            getline(ifs, line);
            schema = new catalog_t();
            int cnt_columns = 0;
            // Read all fields for this table
            std::vector<std::string> lines;
            while (line.length() > 1) {
                lines.push_back(line);
                getline(ifs, line);
            }

            schema->init(table_name.c_str(), lines.size());
            for(int i=0; i<lines.size(); i++) {
                int pos = 0;
                int num_elements = 0;
                int size = 0;
                std::string l = lines[i];
                std::string type, name, token;
                while(l.length() != 0) {
                    pos = l.find(",");
                    if(pos == std::string::npos)
                        pos = l.length();
                    token = l.substr(0, pos);
                    l.erase(0, pos + 1);
                    switch(num_elements) {
                        case 0: size = atoi(token.c_str()); break;
                        case 1: type = token; break;
                        case 2: name = token; break;
                        default: assert(false);
                    }
                    num_elements++;
                }
                assert(num_elements == 3);
                schema->add_col(name.c_str(), size, type.c_str());
                cnt_columns++;
            }

            auto tab = new table_t(schema);
            tab->set_table_id(num_tables);
            _tables.push_back(tab);
            num_tables++;
        }
        else if(line.compare(0, 6, "INDEX=") == 0) {
            std::string index_name = &line[6];
            getline(ifs, line);

            std::vector<std::string> items;
            std::string token;
            int pos;
            while(line.length() != 0) {
                pos = line.find(",");
                if(pos == std::string::npos)
                    pos = line.length();
                token = line.substr(0, pos);
                items.push_back(token);
                line.erase(0, pos + 1);
            }

            std::string table_name(items[0]);
            auto index = create_index(num_indexes);
            _indexes.push_back(index);
            num_indexes++;
        }
    }

    _partition_size = std::vector<uint64_t>(num_tables, 0);
    _partitions = std::vector<std::vector<uint64_t>>(num_tables);

    ifs.close();
    return schema;
}

INDEX* workload_t::create_index(uint32_t index_id) {
    INDEX* index = nullptr;
    if (g_is_server) {
    #if PARTITIONED
        if (TRANSPORT == TWO_SIDED)
            index = new partitioned::idx_t<value_t>(index_id);
        // else, server does not maintain index for one-sided RDMA
    #else // NON_PARTITIONED
        if (TRANSPORT == TWO_SIDED)
            index = new nonpartitioned::idx_t<value_t>(index_id);
        // else, server does not maintain index for one-sided RDMA
    #endif // end of PARTITIONED
    }
    else { // client
    #if PARTITIONED
        assert(WORKLOAD == YCSB); // support YCSB only for now
        if (TRANSPORT == TWO_SIDED)
            index = new partitioned::cache_manager_t<value_t>(index_id, g_cache_size);
        else { // DEX
            std::vector<uint64_t> sharding;
            sharding.push_back(std::numeric_limits<uint64_t>::min());
            for (uint32_t i=0; i<g_num_nodes-1; i++)
                sharding.push_back((g_synth_table_size / g_num_nodes) + sharding[i]);
            sharding.push_back(std::numeric_limits<uint64_t>::max());
            index = new cachepush::BTree<value_t>(index_id, g_cache_size, g_rpc_rate, g_admission_rate, sharding, g_num_nodes);
        }
    #else // NON_PARTITIONED
        if (TRANSPORT == TWO_SIDED)
            index = new nonpartitioned::cache_manager_t<value_t>(index_id, g_cache_size);
        else { // Sherman
            index = new sherman::Tree(index_id, g_cache_size);
            // sherman has some correctness issues when building with multiple clients (nodes, threads)
            // initializing it with some dummy values resolve some of them ...
            global_manager->set_tid(0);
            if (g_node_id == 0) {
                for (uint64_t i = 1; i <= 10240; i++) {
                    value_t value;
                    value.val = 0;
                    index->insert(i, value);
                }
            }
            else sleep(5);
        }
    #endif
    }
    return index;
}

uint64_t workload_t::rpc_alloc_chunk(uint32_t node_id, uint64_t size) {
    assert(TRANSPORT == ONE_SIDED);
    assert(size > 0 && g_is_server == false);

    uint32_t base_size = sizeof(message_t) - sizeof(char*);
    char* send_buffer = transport->get_buffer() + 1024 * 4;
    auto send_msg = new (send_buffer) message_t(message_t::type_t::REQUEST_RPC_ALLOC, sizeof(uint64_t));
    memcpy(send_msg->get_data_ptr(), &size, sizeof(uint64_t));
    uint32_t data_size = sizeof(uint64_t);
    assert(send_msg->get_data_size() == data_size);
    transport->send(send_buffer, node_id, base_size + data_size);

    char* recv_buffer = transport->get_recv_buffer(node_id);
    transport->recv(recv_buffer, node_id, MAX_MESSAGE_SIZE);
    auto recv_msg = reinterpret_cast<message_t*>(recv_buffer);
    // struct ibv_wc wc;
    // uint32_t num = transport->poll(&wc, 1);
    // assert(num == 1);
    assert(recv_msg->get_type() == message_t::type_t::ACK);
    assert(recv_msg->get_data_size() == sizeof(uint64_t));
    uint64_t addr = *(reinterpret_cast<uint64_t*>(recv_msg->get_data_ptr()));
    // transport->post_recv(recv_buffer, node_id, MAX_MESSAGE_SIZE);
    return addr;
}

global_addr_t workload_t::rpc_alloc(uint32_t node_id, uint64_t size) {
RETRY:
    assert(TRANSPORT == ONE_SIDED);
    assert(node_id < g_num_server_nodes);
    assert(size > 0 && g_is_server == false);
    auto allocator = g_cache_allocators[node_id];
    uint64_t offset = allocator->alloc(size);
    if (offset != 0)
        return global_addr_t(node_id, offset);

    // need to fetch a new chunk from the server
    uint64_t chunk_size = (uint64_t)1024 * 1024 * 128; // 128 MB
    offset = rpc_alloc_chunk(node_id, chunk_size);
    assert(offset != 0);
    // uint64_t remain_offset = offset + size;
    // uint64_t remain_size = chunk_size - size;
    // allocator->add(remain_offset, remain_size);
    // return global_addr_t(node_id, offset);
    allocator->add(offset, chunk_size);
    goto RETRY;
    return global_addr_t::null(); // should not reach here
}

global_addr_t workload_t::rpc_alloc(uint64_t size) {
    thread_local static uint32_t last_node = 0;
    uint32_t node_id = last_node++ % g_num_server_nodes;
    return rpc_alloc(node_id, size);
}

global_addr_t workload_t::get_root(uint32_t index_id, uint32_t node_id) {
    uint32_t base_size = sizeof(message_t) - sizeof(char*);
    auto transport = GET_TRANSPORT;
    char* send_buffer = transport->get_buffer() + 1024 * 2;
    auto send_msg = new (send_buffer) message_t(message_t::type_t::REQUEST_IDX_GET_ROOT, sizeof(uint32_t));
    memcpy(send_msg->get_data_ptr(), &index_id, sizeof(uint32_t));
    uint32_t data_size = sizeof(uint32_t);
    assert(send_msg->get_data_size() == data_size);
    transport->send(send_buffer, node_id, base_size + data_size);

    uint32_t recv_size = sizeof(message_t);
    char* recv_buffer = transport->get_recv_buffer(node_id);
    auto recv_msg = reinterpret_cast<message_t*>(recv_buffer);
    transport->recv(recv_buffer, node_id, MAX_MESSAGE_SIZE);

    assert(recv_msg->get_type() == message_t::type_t::ACK);
    assert(recv_msg->get_data_size() == sizeof(uint64_t));
    global_addr_t addr = *(reinterpret_cast<global_addr_t*>(recv_msg->get_data_ptr()));
    return addr;
    // uint64_t addr = *(reinterpret_cast<uint64_t*>(recv_msg->get_data_ptr()));
    // printf("get_root: index %u, node %u, addr %lu\n", index_id, node_id, addr);
    // return global_addr_t(node_id, *(reinterpret_cast<uint64_t*>(recv_msg->get_data_ptr())));
    //return global_addr_t(node_id, *(reinterpret_cast<uint64_t*>(recv_msg->get_data_ptr())));
}

void workload_t::set_root(global_addr_t addr, uint32_t index_id) {
    uint32_t base_size = sizeof(message_t) - sizeof(char*);
    char* send_buffer = transport->get_buffer() + 1024 * 2;
    uint32_t data_size = sizeof(global_addr_t) + sizeof(uint32_t);
    auto send_msg = new (send_buffer) message_t(message_t::type_t::REQUEST_IDX_UPDATE_ROOT, data_size);
    char* data = send_msg->get_data_ptr();
    memcpy(data, &index_id, sizeof(uint32_t));
    memcpy(data + sizeof(uint32_t), &addr, sizeof(global_addr_t));
    assert(send_msg->get_data_size() == data_size);
    transport->send(send_buffer, addr.node_id, base_size + data_size);

    char* recv_buffer = transport->get_recv_buffer(addr.node_id);
    auto recv_msg = reinterpret_cast<message_t*>(recv_buffer);
    transport->recv(recv_buffer, addr.node_id, MAX_MESSAGE_SIZE);

    assert(recv_msg->get_type() == message_t::type_t::ACK);
    assert(recv_msg->get_data_size() == 0);
}

void workload_t::set_index_root(uint64_t addr, uint32_t index_id) {
    _index_roots.insert({index_id, addr});
}

uint64_t workload_t::get_index_root(uint32_t index_id) {
    assert(_index_roots.find(index_id) != _index_roots.end());
    return _index_roots[index_id];
}

uint32_t workload_t::get_partition_by_key(uint64_t key, uint32_t table_id) {
    auto& partition = _partitions[table_id];
    uint32_t low = 0;
    uint32_t high = partition.size();
    do {
        uint32_t mid = ((high - low) / 2) + low;
        if (key < partition[mid])
            high = mid;
        else if (key > partition[mid])
            low = mid + 1;
        else
            return mid - 1;
    } while (low < high);
    return low - 1;
}

uint32_t workload_t::get_partition_by_node(uint32_t node_id, uint32_t table_id) {
    auto& partition = _partitions[table_id];
    assert(node_id < partition.size());
    return partition[node_id];
}

void workload_t::index_insert(INDEX* index, uint64_t key, value_t value) {
#if TRANSPORT == ONE_SIDED
    index->insert(key, value);
#else // TWO_SIDED
    index->insert(key, value.row);
#endif
}