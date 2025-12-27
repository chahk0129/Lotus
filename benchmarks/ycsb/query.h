#pragma once

#include "system/query.h"
#include <vector>
class ycsb_query_t : public base_query_t {
public:
    struct request_t {
        uint64_t key;
        access_t rtype;
        uint64_t value;
    };

    static void preprocess();

    ycsb_query_t();
    ycsb_query_t(request_t* requests, uint32_t num_requests);
    ~ycsb_query_t();

    uint64_t get_request_count() { return _request_cnt; }
    request_t* get_requests() { return _requests; }
    void gen_requests();

private:
    request_t* _requests;
    uint32_t  _request_cnt;

    access_t gen_type(request_t* req);

    // for Zipfian distribution
    uint64_t zipf(uint64_t n, double theta);
    static double zeta(uint64_t n, double theta);
    static uint64_t the_n;
    static double denom;
    static double zeta_2_theta;
    static std::vector<uint64_t> partitions;
    static uint64_t partition_size;
};