#include "client/thread.h"
#include "client/manager.h"
#include "txn/txn.h"
#include "benchmarks/ycsb/workload.h"
#include "benchmarks/ycsb/query.h"
#include "system/query.h"
#include "system/workload.h"
#include "system/manager.h"
#include "system/stats.h"
#include "batch/manager.h"
#include "batch/table.h"
#include "batch/group.h"
#include "utils/utils.h"
#include "utils/helper.h"
#include "utils/debug.h"
#include <functional>
#include <random>

client_thread_t::client_thread_t(uint32_t tid) 
	: thread_t(tid, thread_t::type_t::CLIENT_THREAD) {
	_txn_time_start = 0;
	_txn_time_end = 0;
	_txn_time_restart = 0;
	_txn_cnt_abort = 0;
	_txn_cnt_commit = 0;
}

RC client_thread_t::run() {
	bind_core(get_tid());
	global_manager->init_rand(get_tid());
	global_manager->set_tid(get_tid());
	assert(global_manager->get_tid() == get_tid());

	#if BATCH
	auto batch_manager = new batch_manager_t();
	global_manager->set_batch_manager(batch_manager);
	uint32_t group_id = get_tid() / batch_table_t::MAX_GROUP_SIZE;
	auto batch_group = GET_BATCH_TABLE->get_group(group_id);
	batch_group->join();
	#endif

	pthread_barrier_wait(&global_barrier);

    auto txn_man = new client_txn_manager_t(this);
	txn_man->global_sync();
	if (get_tid() == 0) {
		printf("Begin!\n");
	}

    RC rc = RCOK;
    uint64_t cnt_commit = 0;
    uint64_t cnt_abort = 0;
    uint64_t time_txn_start = 0;
    uint64_t time_txn_end = 0;

	bool sim_done = false;
	bool warmup_done = false;
	uint64_t init_time = get_sys_clock();
	uint64_t warmup_time = 0;

    while (true) {
		if (rc == RCOK) {
			_txn_time_start = get_sys_clock();
			_txn_time_restart = _txn_time_start;
			cnt_abort = 0;
			txn_man->init(GET_WORKLOAD->gen_query());
		}
		else {
			// keep the aborted txn, re-execute it after backoff
			assert(rc == ABORT);
			#if BATCH
			batch_group->leave();
			batch_manager->report_conflict();
			batch_manager->resign_leader();
			#endif
			backoff(cnt_abort);
			cnt_abort++;
			_txn_time_restart = get_sys_clock();
			#if BATCH
			batch_group->join();
			#endif
		}

		rc = RCOK;
		rc = txn_man->run();

		_txn_time_end = get_sys_clock();
		if (rc == RCOK) {
			INC_INT_STATS(num_commits, 1);
			INC_FLOAT_STATS(txn_latency, _txn_time_end - _txn_time_start);
			#if COLLECT_LATENCY
			stats->_stats[get_tid()]->all_latency.push_back((_txn_time_end - _txn_time_start));
			#endif
			INC_FLOAT_STATS(time_process_txn, _txn_time_end - _txn_time_restart);
			INC_FLOAT_STATS(time_abort, _txn_time_restart - _txn_time_start);
			_txn_cnt_commit++;
			INC_INT_STATS(bytes_request, traffic_request);
			INC_INT_STATS(bytes_response, traffic_response);
		}
		else {
			assert(rc == ABORT);
			INC_INT_STATS(num_aborts, 1);
			INC_FLOAT_STATS(time_process_txn, _txn_time_end - _txn_time_restart);
			INC_FLOAT_STATS(time_abort, _txn_time_restart - _txn_time_start);
			_txn_cnt_abort++;
			INC_INT_STATS(bytes_abort, traffic_request);
			INC_INT_STATS(bytes_abort, traffic_response);
		}

		reset_traffic_stats();
		uint64_t cur_time = _txn_time_end;
		if (!g_warmup_done && (cur_time - init_time > g_warmup_time * BILLION)) {
			if (!warmup_done) {
				clear();
				warmup_done = true;
				global_manager->warmup_thread_done();
				warmup_time = get_sys_clock();
			}
			if (global_manager->is_warmup_done()) {
				g_warmup_done = true;
			}
		}
		else if (g_warmup_done && (cur_time - warmup_time > g_run_time * BILLION)) {
			assert(warmup_time - init_time > 0);
			if (!sim_done) {
				sim_done = true;
				global_manager->worker_thread_done();
			}
			if (global_manager->is_sim_done()) {
				INC_FLOAT_STATS(run_time, cur_time - warmup_time);
				break;
			}
		}
	}

	txn_man->clear_txn();
	pthread_barrier_wait(&global_barrier);
	if (get_tid() == 0) {
		txn_man->terminate();
		stats->checkpoint();
	}
	#if BATCH
	delete batch_manager;
	#endif
	delete txn_man;

	return FINISH;
}

void client_thread_t::backoff(uint32_t abort_cnt) {
	_txn_cnt_abort++;
	for (uint64_t i = 0; i < abort_cnt; i++)
		PAUSE100
}

void client_thread_t::clear() {
	_txn_time_start = 0;
	_txn_time_end = 0;
	_txn_time_restart = 0;
	_txn_cnt_abort = 0;
	_txn_cnt_commit = 0;
	CLEAR_STATS();
	
}