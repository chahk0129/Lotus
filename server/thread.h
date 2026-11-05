#pragma once
#include "system/thread.h"

class server_thread_t : public thread_t {
public:
	server_thread_t(uint32_t tid);

	RC run();
};


