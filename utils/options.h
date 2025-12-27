#pragma once
#include <string>
#include <iostream>
#include "utils/cxxopts.hpp"
#include "system/global.h"

struct options_t{    
#if WORKLOAD == YCSB
    uint64_t synth_table_size = g_synth_table_size;
    double zipf_theta = g_zipf_theta;
    uint32_t req_per_query = g_req_per_query;
    double read_perc = g_read_perc;
    double update_perc = g_update_perc;
    double insert_perc = g_insert_perc;
    double scan_perc = g_scan_perc;
#else // TPCC
    uint32_t num_wh = g_num_wh;
    double perc_new_order = g_perc_new_order;
    double perc_payment = g_perc_payment;
    double perc_order_status = g_perc_order_status;
    double perc_delivery = g_perc_delivery;
    double stock_level = g_perc_stock_level;
#endif

    double run_time = g_run_time;
    double warmup_time = g_warmup_time;

    bool partitioned = false;
    uint32_t num_server_threads = g_num_server_threads;
    uint32_t num_client_threads = g_num_client_threads;
    uint64_t cache_size = g_cache_size;
    double admission_rate = g_admission_rate; 
    double rpc_rate = g_rpc_rate; // dex
};

std::ostream& operator<<(std::ostream& os, const options_t& opt){
    os << "Benchmark Options:\n"
       << "\tRun time (sec):       " << g_run_time << "\n"
       << "\tWarmup time (sec):    " << g_warmup_time << "\n"
    #if WORKLOAD == YCSB
       << "\tTable size:           " << g_synth_table_size << "\n"
       << "\tZipfian Factor:       " << g_zipf_theta << "\n"
       << "\tRequest num:          " << g_req_per_query << "\n"
       << "\tRead rate:            " << g_read_perc << "\n"
       << "\tUpdate rate:          " << g_update_perc << "\n"
       << "\tInsert rate:          " << g_insert_perc << "\n"
       << "\tScan rate:            " << g_scan_perc << "\n"
    #else // TPCC
       << "\tNumber of warehouses: " << g_num_wh << "\n"
       << "\tNewOrder rate:        " << g_perc_new_order << "\n"
       << "\tPayment rate:         " << g_perc_payment << "\n"
       << "\tOrderStatus rate:     " << g_perc_order_status << "\n"
       << "\tDelivery rate:       " << g_perc_delivery << "\n"
       << "\tCompute partitioned:  " << (g_partitioned ? "true" : "false") << "\n"
	#endif
       << "\tIndex cache (MB):     " << g_cache_size << "\n"
       << "\tPushdown rate:        " << g_rpc_rate << "\n"
       << "\tCache admission rate: " << g_admission_rate << std::endl;
       if (g_is_server)
           os << "\tNumber of Threads:    " << g_num_server_threads << std::endl;
       else
           os << "\tNumber of Threads:    " << g_num_client_threads << std::endl;
    return os;
}

std::string to_lower(const std::string& str) {
    std::string ret = str;
    std::transform(ret.begin(), ret.end(), ret.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return ret;
}

bool validate(options_t& opt) {
    bool valid = true;
    if (g_is_server) { // server
        #if WORKLOAD == YCSB
        if (opt.synth_table_size == 0) {
            std::cout << "Synthetic table size must be specified for YCSB workload" << std::endl;
            valid = false;
        }
        #else // TPCC
        if (opt.num_wh == 0) {
            std::cout << "Number of warehouses must be specified for TPCC workload" << std::endl;
            valid = false;
        }
        #endif
        if (opt.num_server_threads == 0) {
            std::cout << "Number of server threads must be specified" << std::endl;
            valid = false;
        }
    }
    else { // client
        if (opt.run_time <= 0 || opt.warmup_time < 0) {
            std::cout << "Run time should be > 0 and warmup time should be >= 0: " 
                    << opt.run_time << ", " << opt.warmup_time << std::endl;
            valid = false;
        }

        #if WORKLOAD == YCSB
        double total = opt.read_perc + opt.update_perc + opt.insert_perc + opt.scan_perc;
        if (total > 1.0 || total < 0.99) {
            std::cout << "YCSB transaction rates should sum up to 1.0: " << total << std::endl;
            valid = false;
        }

        if (opt.synth_table_size == 0) {
            std::cout << "Table size should be larger than 0: " << opt.synth_table_size << std::endl;
            valid = false;
        }

        if (opt.zipf_theta <= 0.0) {
            std::cout << "Zipfian distribution should be larger than 0.0: " << opt.zipf_theta << std::endl;
            valid = false;
        }

        if (opt.req_per_query <= 0) {
            std::cout << "Number of requests per query should be larger than 0: " << opt.req_per_query << std::endl;
            valid = false;
        }
        #else // TPCC
        if (opt.num_wh <= 0) {
            std::cout << "Number of warehouses should be larger than 0: " << opt.num_wh << std::endl;
            valid = false;
        }
        double total = opt.perc_new_order + opt.perc_payment + opt.perc_order_status + opt.perc_delivery + opt.stock_level;
        if (total > 1.0 || total < 0.99) {
            std::cout << "TPCC transaction rates should sum up to 1.0: " << total << std::endl;
            valid = false;
        }
        #endif

        if (opt.num_client_threads <= 0) {
            std::cout << "Number of threads should be larger than 0: " << opt.num_client_threads  << std::endl;
            valid = false;
        }

        if (opt.admission_rate < 0 || opt.admission_rate > 1.0) {
            std::cout << "Cache admission rate should be between [0, 1]: " << opt.admission_rate << std::endl;
            valid = false;
        }

        if (opt.rpc_rate < 0 || opt.rpc_rate > 1.0) {
            std::cout << "Pushdown (RPC) rate should be between [0, 1]: " << opt.rpc_rate << std::endl;
            valid = false;
        }

        if (opt.cache_size < 0) {
            std::cout << "Index cache size should be non-negative: " << opt.cache_size << std::endl;
            valid = false;
        }
    }
    return valid;
}

void parse_args(int argc, char* argv[]){
    options_t opt;
    try{
	    cxxopts::Options options("DMDB-Benchmark (Disaggregated Memory Database).");
        options.add_options()
	    #if WORKLOAD == YCSB
            ("size", "Table size", cxxopts::value<uint64_t>()->default_value(std::to_string(opt.synth_table_size)))
            ("zipf", "Zipfian distribution of accesses", cxxopts::value<double>()->default_value(std::to_string(opt.zipf_theta)))
            ("req", "Number of requests per query", cxxopts::value<uint32_t>()->default_value(std::to_string(opt.req_per_query)))
            ("read", "Read rate", cxxopts::value<double>()->default_value(std::to_string(opt.read_perc)))
            ("update", "Update rate", cxxopts::value<double>()->default_value(std::to_string(opt.update_perc)))
            ("insert", "Insert rate", cxxopts::value<double>()->default_value(std::to_string(opt.insert_perc)))
            ("scan", "Scan rate", cxxopts::value<double>()->default_value(std::to_string(opt.scan_perc)))
	    #else // TPCC
            ("num_wh", "Number of warehouses", cxxopts::value<uint64_t>()->default_value(std::to_string(opt.num_wh)))
            ("neworder", "Rate of new-order transactions", cxxopts::value<double>()->default_value(std::to_string(opt.perc_new_order)))
            ("payment", "Rate of payment transactions", cxxopts::value<double>()->default_value(std::to_string(opt.perc_payment)))
            ("orderstatus", "Rate of order-status transactions", cxxopts::value<double>()->default_value(std::to_string(opt.perc_order_status)))
            ("delivery", "Rate of delivery transactions", cxxopts::value<double>()->default_value(std::to_string(opt.perc_delivery)))
	    #endif
            ("server_threads", "Number of server threads", cxxopts::value<uint32_t>()->default_value(std::to_string(opt.num_server_threads)))
            ("client_threads", "Number of client threads", cxxopts::value<uint32_t>()->default_value(std::to_string(opt.num_client_threads)))
            ("cache", "Index cache size (MB)", cxxopts::value<uint64_t>()->default_value(std::to_string(opt.cache_size)))
            ("admission", "Cache admission rate", cxxopts::value<double>()->default_value(std::to_string(opt.admission_rate)))
            ("rpc", "Index pushdown (RPC) rate", cxxopts::value<double>()->default_value(std::to_string(opt.rpc_rate)))
            ("help", "Print help")
            ;

        auto result = options.parse(argc, argv);
        if(result.count("help")){
            std::cout << options.help() << std::endl;
            exit(0);
        }

#if WORKLOAD == YCSB
        if(result.count("read"))
            opt.read_perc = result["read"].as<double>();
        if(result.count("update"))
            opt.update_perc = result["update"].as<double>();
        if(result.count("insert"))
            opt.insert_perc = result["insert"].as<double>();
        if(result.count("scan"))
            opt.scan_perc = result["scan"].as<double>();
        if(result.count("size"))
            opt.synth_table_size = result["size"].as<uint64_t>();
        if(result.count("zipf"))
            opt.zipf_theta = result["zipf"].as<double>();
        if(result.count("req"))
            opt.req_per_query = result["req"].as<uint32_t>();
#else // TPCC
        if(result.count("num_wh"))
            opt.num_wh = result["num_wh"].as<uint64_t>();
        if(result.count("neworder"))
            opt.perc_new_order = result["neworder"].as<double>();
        if(result.count("payment"))
            opt.perc_payment = result["payment"].as<double>();
        if(result.count("orderstatus"))
            opt.perc_order_status = result["orderstatus"].as<double>();
        if(result.count("delivery"))
            opt.perc_delivery = result["delivery"].as<double>();
#endif

        if (result.count("threads")) {
            if (g_is_server) opt.num_server_threads = result["threads"].as<uint32_t>();
            else opt.num_client_threads = result["threads"].as<uint32_t>();
        } 
        if (result.count("partition"))
            opt.partitioned = result["partition"].as<bool>();
        if (result.count("cache"))
            opt.cache_size = result["cache"].as<uint64_t>();
        if (result.count("admission"))
            opt.admission_rate = result["admission"].as<double>();
        if (result.count("rpc"))
            opt.rpc_rate = result["rpc"].as<double>();
        
        if (!validate(opt)) {
            std::cout << "Invalid options detected:" << std::endl;
            std::cout << options.help() << std::endl;
            exit(0);
        }
    } catch(const cxxopts::OptionException& e) {
        std::cout << "Error parsing options: " << e.what() << std::endl;
        exit(0);
    }

    
    // set global variables with the parsed options
    std::cerr << opt << std::endl;

    g_run_time = opt.run_time;
    g_warmup_time = opt.warmup_time;

#if WORKLOAD == YCSB
    g_synth_table_size = opt.synth_table_size;
    g_zipf_theta = opt.zipf_theta;
    g_req_per_query = opt.req_per_query;
    g_read_perc = opt.read_perc;
    g_update_perc = opt.update_perc;
    g_insert_perc = opt.insert_perc;
    g_scan_perc = opt.scan_perc;
#else // TPCC
    g_num_wh = opt.num_wh;
    g_perc_new_order = opt.perc_new_order;
    g_perc_payment = opt.perc_payment;
    g_perc_order_status = opt.perc_order_status;
    g_perc_delivery = opt.perc_delivery;
#endif

    g_cache_size = opt.cache_size;
    g_admission_rate = opt.admission_rate;
    g_rpc_rate = opt.rpc_rate;
    g_num_client_threads = opt.num_client_threads;
    g_num_server_threads = opt.num_server_threads;
    if (g_is_server) g_total_num_threads = opt.num_server_threads;
    else g_total_num_threads = opt.num_client_threads;
    g_init_parallelism = g_total_num_threads;

    if (g_is_server) {
        if (!PARTITIONED) {
            if (g_init_parallelism < 8) g_init_parallelism = 40; // for faster loading
        }
    }
}