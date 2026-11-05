#pragma once 

#include <cstdlib>
#include <iostream>
#include <stdint.h>
#include "system/global.h"
#include "system/stats.h"
#include "system/manager.h"

#define BILLION 1000000000UL

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

/************************************************/
// atomic operations
/************************************************/
#define ATOM_ADD(dest, value) \
        __sync_fetch_and_add(&(dest), value)
#define ATOM_SUB(dest, value) \
        __sync_fetch_and_sub(&(dest), value)
// returns true if cas is successful
#define ATOM_CAS(dest, oldval, newval) \
        __sync_bool_compare_and_swap(&(dest), oldval, newval)
#define ATOM_ADD_FETCH(dest, value) \
        __sync_add_and_fetch(&(dest), value)
#define ATOM_FETCH_ADD(dest, value) \
        __sync_fetch_and_add(&(dest), value)
#define ATOM_SUB_FETCH(dest, value) \
        __sync_sub_and_fetch(&(dest), value)

#define COMPILER_BARRIER asm volatile("" ::: "memory");
// on draco, each pause takes approximately 3.7 ns.
#define PAUSE __asm__ ( "pause;" );
#define PAUSE10 \
    PAUSE PAUSE PAUSE PAUSE PAUSE \
    PAUSE PAUSE PAUSE PAUSE PAUSE

// about 370 ns.
#define PAUSE100 \
    PAUSE10    PAUSE10    PAUSE10    PAUSE10    PAUSE10 \
    PAUSE10    PAUSE10    PAUSE10    PAUSE10    PAUSE10


//////////////////////////////////////////////////
// Global Data Structure
//////////////////////////////////////////////////
#define GET_WORKLOAD global_manager->get_workload()
#define GET_THD_ID global_manager->get_tid()
#define GET_TRANSPORT global_manager->get_transport()


/************************************************/
// ASSERT Helper
/************************************************/
#define M_ASSERT(cond, ...) \
        if (!(cond)) {\
                printf("ASSERTION FAILURE [%s : %d] ", \
                __FILE__, __LINE__); \
                printf(__VA_ARGS__);\
                assert(false);\
        }

#define ASSERT(cond) assert(cond)

/************************************************/
// QUEUE helper (push & pop)
/************************************************/
#define RETURN_PUSH(head, entry) { \
        if (head == NULL) { head = entry; entry->next = NULL;} \
        else { entry->next = head; head = entry;} \
}

#define QUEUE_PUSH(head, tail, entry) { \
        entry->next = NULL;\
        if (head == NULL) {     head = entry;   tail = entry; }\
        else {  tail->next = entry;     tail = entry; } }

#define QUEUE_RM(head, tail, prev, en, cnt) { \
        if (prev != NULL) prev->next = en->next; \
        else if (head == en)    head = en->next; \
        if (tail == en) tail = prev;    cnt-- ;}

/************************************************/
// STACK helper (push & pop)
/************************************************/
#define STACK_POP(stack, top) { \
        if (stack == NULL) top = NULL; \
        else {  top = stack;    stack=stack->next; } }
#define STACK_PUSH(stack, entry) {\
        entry->next = stack; stack = entry; }

/************************************************/
// LIST helper (read from head & write to tail)
/************************************************/
#define LIST_GET_HEAD(lhead, ltail, en) {\
        en = lhead; \
        lhead = lhead->next; \
        if (lhead) lhead->prev = NULL; \
        else ltail = NULL; \
        en->next = NULL; }

#define LIST_PUT_TAIL(lhead, ltail, en) {\
        en->next = NULL; \
        en->prev = NULL; \
        if (ltail) { en->prev = ltail; ltail->next = en; ltail = en; } \
        else { lhead = en; ltail = en; }}

#define LIST_INSERT_BEFORE(entry, newentry) { \
        newentry->next = entry; \
        newentry->prev = entry->prev; \
        if (entry->prev) entry->prev->next = newentry; \
        entry->prev = newentry; }

#define LIST_INSERT_AFTER(entry, newentry) { \
        newentry->next = entry->next; \
        newentry->prev = entry; \
        if (entry->next) entry->next->prev = newentry; \
        entry->next = newentry; }

#define LIST_REMOVE(entry) { \
        if (entry->next) entry->next->prev = entry->prev; \
        if (entry->prev) entry->prev->next = entry->next; }

#define LIST_REMOVE_HT(entry, head, tail) { \
        if (entry->next) entry->next->prev = entry->prev; \
        else { assert(entry == tail); tail = entry->prev; } \
        if (entry->prev) entry->prev->next = entry->next; \
        else { assert(entry == head); head = entry->next; } \
}

#define LIST_RM(head, tail, en, cnt) { \
        if (en->next) en->next->prev = en->prev; \
        if (en->prev) en->prev->next = en->next; \
        else if (head == en) {  head = en->next; } \
        if (tail == en) { tail = en->prev; } \
        cnt--; } \

#define LIST_RM_SINCE(head, tail, en) { \
        if (en->prev) en->prev->next = NULL; \
        else if (head == en) {  head = NULL; } \
        tail = en->prev; }

#define LIST_INSERT_BEFORE_CH(lhead, entry, newentry) { \
        newentry->next = entry; \
        newentry->prev = entry->prev; \
        if (entry->prev) entry->prev->next = newentry; \
        entry->prev = newentry; \
        if (lhead == entry) lhead = newentry; \
}

/************************************************/
// STATS helper
/************************************************/
#define INC_FLOAT_STATS(name, value) \
    if (STATS_ENABLE) \
        stats->_stats[GET_THD_ID]->_float_stats[STAT_##name] += value;

#define INC_INT_STATS(name, value) \
    if (STATS_ENABLE) \
        stats->_stats[GET_THD_ID]->_int_stats[STAT_##name] += value;

#define CLEAR_STATS() \
    if (STATS_ENABLE) \
        stats->clear(GET_THD_ID);

#define INC_BATCH_STATS(batch_size, value) \
    if (STATS_ENABLE) \
        stats->_stats[GET_THD_ID]->_batch_stats[std::min<uint32_t>(batch_size, batch_limits::HIST_SIZE) - 1] += value;

/************************************************/
// Helper functions
/************************************************/
// TSC frequency in GHz, measured once at program start (system/global.cpp);
// falls back to CPU_FREQ if the measurement looks implausible
extern double g_tsc_ghz;

// returns nanoseconds
inline uint64_t get_sys_clock() {
    uint64_t ret;
#if defined(__i386__)
    __asm__ __volatile__("rdtsc" : "=A" (ret));
#elif defined(__x86_64__)
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    ret = ( (uint64_t)lo)|( ((uint64_t)hi)<<32 );
    ret = (uint64_t) ((double)ret / g_tsc_ghz);
#else
    timespec * tp = new timespec;
    clock_gettime(CLOCK_REALTIME, tp);
    ret = tp->tv_sec * 1000000000 + tp->tv_nsec;
#endif
    return ret;
}

inline uint64_t asm_rdtsc() {
    return get_sys_clock();
}

inline uint64_t get_relative_clock() {
    timespec tp;
    clock_gettime(CLOCK_REALTIME, &tp);
    uint64_t ret = tp.tv_sec * 1000000000 + tp.tv_nsec;
    return ret;
}

// number of bits needed to represent ids in [0, n)
inline uint64_t id_bits_for(uint64_t n) {
    return (n <= 1) ? 0 : 64 - __builtin_clzll(n - 1);
}

// Globally unique, time-ordered priority for WAIT_DIE / WOUND_WAIT.
// The raw nanosecond clock is not unique across threads or nodes: two
// conflicting transactions with tied timestamps can each pass the strict
// "older than owner" check and wait on each other, deadlocking. The epoch
// clock in nanoseconds already uses ~61 of 64 bits, so shifting it left to
// make room for an id would overflow; instead the low bits are cleared and
// replaced with a globally unique (node_id, thd_id) tag, sized to just fit
// g_num_client_nodes and g_num_client_threads. Only the clock's sub-
// microsecond granularity is sacrificed, never its ordering.
// (node_id/thd_id are parameters rather than read via GET_THD_ID because
// helper.h is included by system/manager.h, where manager_t is incomplete)
inline uint64_t get_priority(uint64_t node_id, uint64_t thd_id) {
    timespec tp;
    clock_gettime(CLOCK_REALTIME, &tp);
    uint64_t ts = tp.tv_sec * 1000000000 + tp.tv_nsec;
    assert(!g_is_server && node_id < g_num_client_nodes && thd_id < g_num_client_threads);
    uint64_t thd_bits = id_bits_for(g_num_client_threads);
    uint64_t id_bits = id_bits_for(g_num_client_nodes) + thd_bits;
    return (ts & ~((1ULL << id_bits) - 1)) | (node_id << thd_bits) | thd_id;
}

static void bind_core(uint32_t core){
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if(rc != 0)
        std::cerr << "Failed to bind core at " << core << std::endl;
}

inline void compiler_barrier(){
    asm volatile("" ::: "memory");
}