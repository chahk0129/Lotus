#include "system/global.h"
#include "benchmarks/ycsb/query.h"
#include "benchmarks/ycsb/workload.h"
#include "storage/table.h"
#include "system/manager.h"
#include <unordered_set>

uint64_t ycsb_query_t::the_n = 0;
double ycsb_query_t::denom   = 0;
double ycsb_query_t::zeta_2_theta;
std::vector<uint64_t> ycsb_query_t::partitions;
uint64_t ycsb_query_t::partition_size = 0;

void ycsb_query_t::preprocess() {
    assert(the_n == 0);
    uint64_t table_size = g_synth_table_size;
    the_n = table_size;
    denom = zeta(the_n, g_zipf_theta);
    zeta_2_theta = zeta(2, g_zipf_theta);

    partition_size = table_size / g_num_nodes;
    for (uint32_t i = 0; i < g_num_nodes; i++)
        partitions.push_back(i * partition_size);
}

// The following algorithm comes from the paper:
// Quickly generating billion-record synthetic databases
// However, it seems there is a small bug.
// The original paper says zeta(theta, 2.0). But I guess it should be
// zeta(2.0, theta).
double ycsb_query_t::zeta(uint64_t n, double theta) {
    double sum = 0;
    for (uint64_t i=1; i<=n; i++)
        sum += pow(1.0 / i, theta);
    return sum;
}

uint64_t ycsb_query_t::zipf(uint64_t n, double theta) {
    assert(this->the_n == n);
    assert(theta == g_zipf_theta);
    double alpha = 1 / (1 - theta);
    double zetan = denom;
    double eta = (1 - pow(2.0 / n, 1 - theta)) / (1 - zeta_2_theta / zetan);
    double u = global_manager->rand_double();
    double uz = u * zetan;
    if (uz < 1) return 1;
    if (uz < 1 + pow(0.5, theta)) return 1;
    return (uint64_t)(n * pow(eta*u -eta + 1, alpha));
}


ycsb_query_t::ycsb_query_t()
    : base_query_t() {
    _request_cnt = g_req_per_query;
    _requests = new request_t[_request_cnt];
}


ycsb_query_t::ycsb_query_t(request_t* requests, uint32_t num_requests) {
    _request_cnt = num_requests;
    _requests = new request_t[_request_cnt];
    memcpy(_requests, requests, sizeof(request_t) * _request_cnt);
}

ycsb_query_t::~ycsb_query_t() {
    delete[] _requests;
}

access_t ycsb_query_t::gen_type(request_t* req) {
    double r = global_manager->rand_double();
    if (r < g_read_perc)
        return RD;
    else if (r < g_read_perc + g_update_perc + g_insert_perc)
        return WR;
    else {
        auto scan_num = global_manager->rand_uint64(5, 10);
        req->value = scan_num;
        return SCAN;
    }
}

void ycsb_query_t::gen_requests() {
    uint32_t request_cnt = 0;
    uint64_t all_keys[_request_cnt];
    uint64_t table_size = g_synth_table_size;
    std::unordered_set<uint64_t> key_set;
    while (request_cnt < _request_cnt) {
        request_t* req = &_requests[request_cnt];
        #if PARTITIONED
        uint32_t node_id = g_node_id;
        #else
        uint32_t node_id = (g_node_id + global_manager->rand_uint64(0, g_num_nodes - 1)) % g_num_nodes;
        #endif
    RETRY:
        uint64_t row_id = zipf(table_size, g_zipf_theta);
        uint64_t primary_key = ((row_id % partition_size) + 1) + partitions[node_id];
        req->key = primary_key;
        req->value = primary_key;
        req->rtype = gen_type(req);
        bool duplicate = false;
        if (req->rtype == SCAN) {
            for (uint32_t i=0; i<req->value; i++) {
                uint64_t scan_key = req->key + i;
                auto it = key_set.find(scan_key);
                if (it != key_set.end()) {
                    duplicate = true;
                    break;
                }
            }

            if (!duplicate) {
                for (uint32_t i=0; i<req->value; i++) {
                    uint64_t scan_key = req->key + i;
                    key_set.insert(scan_key);
                }
            }
        }
        else { 
            auto it = key_set.find(req->key);
            if (it == key_set.end()) 
                key_set.insert(req->key);
            else
                duplicate = true;
        }

        if (duplicate) goto RETRY;
        else request_cnt++;
    }
    // Sort the requests in key order.
    if (g_key_order) {
        for (uint32_t i=request_cnt-1; i>0; i--) {
            for (uint32_t j=0; j<i; j++) { 
                if (_requests[j].key > _requests[j + 1].key) {
                    request_t tmp = _requests[j];
                    _requests[j] = _requests[j + 1];
                    _requests[j + 1] = tmp;
                }
            }
        }
        for (uint32_t i=0; i<request_cnt-1; i++)
            assert(_requests[i].key < _requests[i + 1].key);
    }
    _request_cnt = request_cnt;
}