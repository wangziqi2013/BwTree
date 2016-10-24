
/*
 * benchmark_bwtree_full.cpp - This file contains test suites for command
 *                             benchmark-bwtree-full
 */

#include "test_suite.h"

/*
 * TestBwTreeMultiThreadInsertPerformance() - As name suggests
 *
 * This should be called in a multithreaded environment
 */
void TestBwTreeMultiThreadInsertPerformance(TreeType *t, int key_num) {
  const int num_thread = 8;

  // This is used to record time taken for each individual thread
  double thread_time[num_thread];
  for(int i = 0;i < num_thread;i++) {
    thread_time[i] = 0.0;
  }

  auto func = [key_num, &thread_time](uint64_t thread_id, TreeType *t) {
    long int start_key = key_num / num_thread * (long)thread_id;
    long int end_key = start_key + key_num / num_thread;

    // Declare timer and start it immediately
    Timer timer{true};

    for(int i = start_key;i < end_key;i++) {
      t->Insert(i, i);
    }

    double duration = timer.Stop();

    std::cout << "[Thread " << thread_id << " Done] @ " \
              << (key_num / num_thread) / (1024.0 * 1024.0) / duration \
              << " million insert/sec" << "\n";

    thread_time[thread_id] = duration;

    return;
  };

  LaunchParallelTestID(num_thread, func, t);

  double elapsed_seconds = 0.0;
  for(int i = 0;i < num_thread;i++) {
    elapsed_seconds += thread_time[i];
  }

  std::cout << num_thread << " Threads BwTree: overall "
            << (key_num / (1024.0 * 1024.0) * num_thread) / elapsed_seconds
            << " million insert/sec" << "\n";
            
  return;
}

/*
 * TestBwTreeMultiThreadReadPerformance() - As name suggests
 *
 * This should be called in a multithreaded environment
 */
void TestBwTreeMultiThreadReadPerformance(TreeType *t, int key_num) {
  const int num_thread = 40;
  int iter = 1;
  
  // This is used to record time taken for each individual thread
  double thread_time[num_thread];
  for(int i = 0;i < num_thread;i++) {
    thread_time[i] = 0.0;
  }
  
  auto func = [key_num, iter, &thread_time](uint64_t thread_id, TreeType *t) {
    std::vector<long> v{};

    v.reserve(1);

    Timer timer{true};

    for(int j = 0;j < iter;j++) {
      for(int i = 0;i < key_num;i++) {
        t->GetValue(i, v);

        v.clear();
      }
    }

    double duration = timer.Stop();

    std::cout << "[Thread " << thread_id << " Done] @ " \
              << (iter * key_num / (1024.0 * 1024.0)) / duration \
              << " million read/sec" << "\n";
              
    thread_time[thread_id] = duration;

    return;
  };

  LaunchParallelTestID(num_thread, func, t);
  
  double elapsed_seconds = 0.0;
  for(int i = 0;i < num_thread;i++) {
    elapsed_seconds += thread_time[i];
  }

  std::cout << num_thread << " Threads BwTree: overall "
            << (iter * key_num / (1024.0 * 1024.0) * num_thread * num_thread) / elapsed_seconds
            << " million read/sec" << "\n";
            
  ///////////////////////////////////////////////////////////////////
  // Multithread random read performance
  ///////////////////////////////////////////////////////////////////
  
  for(int i = 0;i < num_thread;i++) {
    thread_time[i] = 0.0;
  }
  
  auto func2 = [key_num, iter, &thread_time](uint64_t thread_id, TreeType *t) {
    std::vector<long> v{};

    v.reserve(1);
    
    // This is the random number generator we use
    SimpleInt64Random<0, 30 * 1024 * 1024> h{};

    Timer timer{true};

    for(int j = 0;j < iter;j++) {
      for(int i = 0;i < key_num;i++) {
        //int key = uniform_dist(e1);
        long int key = (long int)h((uint64_t)i, thread_id);

        t->GetValue(key, v);

        v.clear();
      }
    }

    double duration = timer.Stop();

    std::cout << "[Thread " << thread_id << " Done] @ " \
              << (iter * key_num / (1024.0 * 1024.0)) / duration \
              << " million read (random)/sec" << "\n";
              
    thread_time[thread_id] = duration;

    return;
  };

  LaunchParallelTestID(num_thread, func2, t);

  elapsed_seconds = 0.0;
  for(int i = 0;i < num_thread;i++) {
    elapsed_seconds += thread_time[i];
  }

  std::cout << num_thread << " Threads BwTree: overall "
            << (iter * key_num / (1024.0 * 1024.0) * num_thread * num_thread) / elapsed_seconds
            << " million read (random)/sec" << "\n";

  return;
}
