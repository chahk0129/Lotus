#pragma once
#include "system/thread.h"

class ntp_client_t;

class client_thread_t : public thread_t {
public:
	client_thread_t(uint32_t tid);

	RC run();
	
private:
	void backoff(uint32_t penalty);
	void clear();

	uint64_t _txn_time_start;
	uint64_t _txn_time_end;
	uint64_t _txn_time_restart;
	uint64_t _txn_cnt_abort;
	uint64_t _txn_cnt_commit;
};