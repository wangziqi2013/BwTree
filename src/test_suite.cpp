
/*
 * test_suite.cpp
 *
 * This files includes basic testing infrastructure
 *
 * by Ziqi Wang
 */

#include "test_suite.h"


/*
 * PrintStat() - Print the current statical information on stdout
 */
void PrintStat(TreeType *t) {
  printf("Insert op = %lu; abort = %lu; abort rate = %lf\n",
         t->insert_op_count.load(),
         t->insert_abort_count.load(),
         (double)t->insert_abort_count.load() / (double)t->insert_op_count.load());

  printf("Delete op = %lu; abort = %lu; abort rate = %lf\n",
         t->delete_op_count.load(),
         t->delete_abort_count.load(),
         (double)t->delete_abort_count.load() / (double)t->delete_op_count.load());

  return;
}

/*
 * PinToCore() - Pin the current calling thread to a particular core
 */
void PinToCore(size_t core_id) {
  cpu_set_t cpu_set;
  CPU_ZERO(&cpu_set);
  CPU_SET(core_id, &cpu_set);

  int ret = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set), &cpu_set);

  printf("pthread_setaffinity_np() returns %d\n", ret);

  return;
}
