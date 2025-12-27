#include "system/global.h"
#include "system/stats.h"
#include "utils/helper.h"
#include "batch/table.h"
#include "transport/message.h"
#include <algorithm>
#include <iomanip>

stats_thd_t::stats_thd_t() {
    _float_stats = new double [NUM_FLOAT_STATS];
    _int_stats = new uint64_t [NUM_INT_STATS];
    _batch_stats = new uint64_t [batch_table_t::MAX_GROUP_SIZE];
    clear();
}

void stats_thd_t::init(uint32_t tid) {
    clear();
}

void stats_thd_t::clear() {
    memset(_float_stats, 0, sizeof(double) * NUM_FLOAT_STATS);
    memset(_int_stats, 0, sizeof(uint64_t) * NUM_INT_STATS);
    memset(_batch_stats, 0, sizeof(uint64_t) * batch_table_t::MAX_GROUP_SIZE);
    all_latency.clear();
}

void stats_thd_t::copy_from(stats_thd_t* stats_thd) {
    memcpy(_float_stats, stats_thd->_float_stats, sizeof(double) * NUM_FLOAT_STATS);
    memcpy(_int_stats, stats_thd->_int_stats, sizeof(double) * NUM_INT_STATS);
    memcpy(_batch_stats, stats_thd->_batch_stats, sizeof(uint64_t) * batch_table_t::MAX_GROUP_SIZE);
}

////////////////////////////////////////////////
// class stats_t
////////////////////////////////////////////////
stats_t::stats_t() {
    _num_checkpoints = 0;
    _stats = new stats_thd_t* [g_total_num_threads];
    for (uint32_t i = 0; i < g_total_num_threads; i++)
        _stats[i] = new stats_thd_t();
}


void stats_t::init(uint32_t tid) {
    if (!STATS_ENABLE)
        return;
    _stats[tid]->init(tid);
}

void stats_t::clear(uint32_t tid) {
    if (STATS_ENABLE)
        _stats[tid]->clear();
}

void stats_t::output(std::ostream* os) {
    std::ostream &out = *os;

    uint64_t total_num_commits = 0;
    double total_run_time = 0;
    for (uint32_t i= 0; i<g_total_num_threads; i++) {
        total_num_commits += _stats[i]->_int_stats[STAT_num_commits];
        total_run_time += _stats[i]->_float_stats[STAT_run_time];
    }

    if (!g_is_server) assert(total_num_commits > 0);
    std::cout << "=Stats=" << std::endl;
    std::cout << "    " << std::setw(30) << std::left << "Throughput:"
              << BILLION * total_num_commits / total_run_time * g_total_num_threads << std::endl;
    std::cout << std::endl;
    
    // print floating point stats
    for (uint32_t i=0; i<NUM_FLOAT_STATS; i++) {
        if (i == STAT_txn_latency)
            continue;
        double total = 0;
        for (uint32_t tid=0; tid<g_total_num_threads; tid++)
            total += _stats[tid]->_float_stats[i];
        std::string suffix = "";
        if (i >= STAT_time_process_txn) {
            total = total / total_num_commits * 1000000; // in us.
            suffix = " (in us) ";
        }
        std::cout << "    " << std::setw(30) << std::left << stats_float_name[i] + suffix + ':' << total / BILLION;
        std::cout << " (";
        for (uint32_t tid=0; tid<g_total_num_threads; tid++)
            std::cout << _stats[tid]->_float_stats[i] / BILLION << ',';
        std::cout << ')' << std::endl;
    }

    std::cout << std::endl;

    // print integer stats
    for (uint32_t i=0; i<NUM_INT_STATS; i++) {
        double total = 0;
        for (uint32_t tid=0; tid<g_total_num_threads; tid++)
            total += _stats[tid]->_int_stats[i];
        std::string suffix = "";
        uint64_t divisor = 1;
        if (i >= STAT_bytes_sent && i <= STAT_bytes_faa) {
            suffix = " (in MB) ";
            divisor = 1024 * 1024;
            total = total / divisor;
        }
        std::cout << "    " << std::setw(30) << std::left << stats_int_name[i] + suffix + ':' << total;
        std::cout << " (";
        for (uint32_t tid=0; tid<g_total_num_threads; tid++)
            std::cout << _stats[tid]->_int_stats[i] / divisor << ',';
        std::cout << ')' << std::endl;
    }

    // print batch stats
    for (uint32_t i=0; i<batch_table_t::MAX_GROUP_SIZE; i++) {
        uint64_t total = 0;
        for (uint32_t tid=0; tid<g_total_num_threads; tid++)
            total += _stats[tid]->_batch_stats[i];
        std::cout << "    " << std::setw(30) << std::left << ("batch_num_" + std::to_string(i + 1) + ':') << total << std::endl;
    }

#if COLLECT_LATENCY
    if (!g_is_server) {
        double avg_latency = 0;
        uint64_t total_latency_count = _aggregate_latency.size();
        for (uint32_t tid=0; tid<g_total_num_threads; tid++)
            avg_latency += _stats[tid]->_float_stats[STAT_txn_latency];
        avg_latency /= total_latency_count;

        std::cout << "    " << std::setw(30) << std::left << "average_latency:" << avg_latency / BILLION << std::endl;
        // print latency distribution
        std::cout << "    " << std::setw(30) << std::left << "90%_latency:"
                << _aggregate_latency[(uint64_t)(total_latency_count * 0.90)] / BILLION << std::endl;
        std::cout << "    " << std::setw(30) << std::left << "95%_latency:"
                << _aggregate_latency[(uint64_t)(total_latency_count * 0.95)] / BILLION << std::endl;
        std::cout << "    " << std::setw(30) << std::left << "99%_latency:"
                << _aggregate_latency[(uint64_t)(total_latency_count * 0.99)] / BILLION << std::endl;
        std::cout << "    " << std::setw(30) << std::left << "max_latency:"
                << _aggregate_latency[total_latency_count - 1] / BILLION << std::endl;

        std::cout << std::endl;
    }
#endif

    // print the checkpoints
    if (_checkpoints.size() > 1) {
        std::cout << "\n=Check Points=\n" << std::endl;
        std::cout << "Metrics:\tthr,";
        for (uint32_t i=0; i<NUM_INT_STATS; i++)
            std::cout << stats_int_name[i] << ',';
        for (uint32_t i=0; i<NUM_FLOAT_STATS; i++)
            std::cout << stats_float_name[i] << ',';
        std::cout << std::endl;
    }

    for (uint32_t i=1; i<_checkpoints.size(); i++) {
        uint64_t num_commits = 0;
        for (uint32_t tid=0; tid<g_total_num_threads; tid++) {
            num_commits += _checkpoints[i]->_stats[tid]->_int_stats[STAT_num_commits];
            num_commits -= _checkpoints[i - 1]->_stats[tid]->_int_stats[STAT_num_commits];
        }

        double thr = 1.0 * num_commits / STATS_CP_INTERVAL * 1000;
        std::cout << "CP" << i << ':';
        std::cout << "\t" << thr << ',';

        for (uint32_t n=0; n<NUM_INT_STATS; n++) {
            uint64_t value = 0;
            for (uint32_t tid=0; tid<g_total_num_threads; tid++) {
                value += _checkpoints[i]->_stats[tid]->_int_stats[n];
                value -= _checkpoints[i - 1]->_stats[tid]->_int_stats[n];
            }
            std::cout << value << ',';
        }

        for (uint32_t n=0; n<NUM_FLOAT_STATS; n++) {
            double value = 0;
            for (uint32_t tid=0; tid<g_total_num_threads; tid++) {
                value += _checkpoints[i]->_stats[tid]->_float_stats[n];
                value -= _checkpoints[i - 1]->_stats[tid]->_float_stats[n];
            }
            std::cout << value / BILLION << ',';
        }
        std::cout << std::endl;
    }
}

void stats_t::print() {
    std::ofstream file;
    bool write_to_file = false;
    if (output_file != nullptr) {
        write_to_file = true;
        file.open(output_file);
    }
    // compute the latency distribution
#if COLLECT_LATENCY
    if (!g_is_server) {
        for (uint32_t tid=0; tid<g_total_num_threads; tid++) {
            _aggregate_latency.insert(_aggregate_latency.end(),
                                    _stats[tid]->all_latency.begin(),
                                    _stats[tid]->all_latency.end());
        }
        if (_aggregate_latency.size() > 1000000) {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::vector<double> sampled_latency;
            sampled_latency.reserve(1000000);
            std::sample(_aggregate_latency.begin(), _aggregate_latency.end(),
                        std::back_inserter(sampled_latency), 1000000, gen);
            _aggregate_latency = std::move(sampled_latency);
        }
        std::sort(_aggregate_latency.begin(), _aggregate_latency.end());
    }
#endif
    output(&cout);
    if (write_to_file) {
        std::ofstream fout (output_file);
        g_warmup_time = 0;
        output(&fout);
        fout.close();
    }

    output_cdf();
    return;
}

void stats_t::output_cdf() {
#if COLLECT_LATENCY
    if (!g_is_server && g_node_id == 0) {
        std::ofstream file;
        file.open ("cdf.txt");
        file << std::fixed << std::setprecision(6);
        for (uint32_t i=0; i<_aggregate_latency.size(); i++) // in us
            file << _aggregate_latency[i] / 1000 << std::endl;
        file.close();
    }
#endif
}

void stats_t::checkpoint() {
    stats_t* stats = new stats_t();
    stats->copy_from(this);
    _checkpoints.push_back(stats);
    COMPILER_BARRIER
    _num_checkpoints++;
}

void stats_t::copy_from(stats_t* stats) {
    for (uint32_t i=0; i<g_total_num_threads; i++)
        _stats[i]->copy_from(stats->_stats[i]);
}