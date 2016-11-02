
/*
 * benchmark_art_full.cpp - Benchmarks adaptive radix tree
 *
 * Please note that the ART we use for benchmark only supports single threaded 
 * execution, so therefore it refuses to execute if the number of thread
 * is more than 1
 */ 

#include "test_suite.h"

/*
 * BenchmarkARTSeqInsert() - As name suggests
 */
void BenchmarkARTSeqInsert(ARTType *t, 
                           int key_num, 
                           int num_thread) {

  // Enforce this with explicit assertion that is still valid under 
  // release mode
  if(num_thread != 1) {
    throw "ART must be run under single threaded environment!"; 
  }

  // This is used to record time taken for each individual thread
  double thread_time[num_thread];
  for(int i = 0;i < num_thread;i++) {
    thread_time[i] = 0.0;
  }

  auto func = [key_num, 
               &thread_time, 
               num_thread](uint64_t thread_id, ARTType *t) {
    long int start_key = key_num / num_thread * (long)thread_id;
    long int end_key = start_key + key_num / num_thread;

    // Declare timer and start it immediately
    Timer timer{true};

    for(long int i = start_key;i < end_key;i++) {
      // 8 byte key, 8 byte payload (i.e. nullptr)
      // This is a little bit cheating - since we have only 1 payload and
      // in order to get the value we need another level of indirection which
      // is not demonstrated here
      art_insert(t, (unsigned char *)&i, sizeof(i), nullptr);
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

  std::cout << num_thread << " Threads ART: overall "
            << (key_num / (1024.0 * 1024.0) * num_thread) / elapsed_seconds
            << " million insert/sec" << "\n";
            
  return;
}


/*
 * BenchmarkARTSeqRead() - As name suggests
 */
void BenchmarkARTSeqRead(ARTType *t, 
                         int key_num,
                         int num_thread) {
  int iter = 1;
  
  // This is used to record time taken for each individual thread
  double thread_time[num_thread];
  for(int i = 0;i < num_thread;i++) {
    thread_time[i] = 0.0;
  }
  
  #ifndef USE_BOOST
  // Declear a spinlock protecting the data structure
  spinlock_t lock;
  rwlock_init(lock);
  #else
  shared_mutex lock;
  #endif
  
  auto func = [key_num, 
               iter, 
               &thread_time, 
               num_thread](uint64_t thread_id, ARTType *t) {
    std::vector<long> v{};

    v.reserve(1);

    Timer timer{true};

    for(int j = 0;j < iter;j++) {
      for(long int i = 0;i < key_num;i++) {
        #ifndef USE_BOOST
        read_lock(lock);
        #else
        lock.lock_shared();
        #endif
        
        auto it_pair = t->equal_range(i);

        // For multimap we need to write an external loop to
        // extract all keys inside the multimap
        // This is the place where btree_multimap is slower than
        // btree
        for(auto it = it_pair.first;it != it_pair.second;it++) {
          v.push_back(it->second);
        }
        
        #ifndef USE_BOOST
        read_unlock(lock);
        #else
        lock.unlock_shared();
        #endif
  
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

  std::cout << num_thread << " Threads BTree Multimap: overall "
            << (iter * key_num / (1024.0 * 1024.0) * num_thread * num_thread) / elapsed_seconds
            << " million read/sec" << "\n";

  return;
}
