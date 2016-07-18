
#include "test_suite.h"

constexpr int key_num = 128 * 1024;
constexpr int thread_num = 8;

std::atomic<size_t> tree_size;



void InsertTest1(uint64_t thread_id, TreeType *t) {
  for(int i = thread_id * key_num;i < (int)(thread_id + 1) * key_num;i++) {
    t->Insert(i, i + 1);
    t->Insert(i, i + 2);
    t->Insert(i, i + 3);
    t->Insert(i, i + 4);

    //tree_size_mutex.lock();
    //tree_size += 4;
    //tree_size_mutex.unlock();
  }

  return;
}

void DeleteTest1(uint64_t thread_id, TreeType *t) {
  for(int i = thread_id * key_num;i < (int)(thread_id + 1) * key_num;i++) {
    t->Delete(i, i + 1);
    t->Delete(i, i + 2);
    t->Delete(i, i + 3);
    t->Delete(i, i + 4);

    //tree_size_mutex.lock();
    //tree_size -= 4;
    //tree_size_mutex.unlock();

    //printf("Tree size = %lu\n", tree_size);
  }

  return;
}

void InsertTest2(uint64_t thread_id, TreeType *t) {
  tree_size = 0UL;

  for(int i = 0;i < key_num;i++) {
    int key = thread_num * i + thread_id;

    t->Insert(key, key + 1);
    t->Insert(key, key + 2);
    t->Insert(key, key + 3);
    t->Insert(key, key + 4);

    tree_size.fetch_add(4);

    size_t current_size = tree_size.load();
    if(current_size % 1000 == 0) {
      //printf("Tree size = %lu\n", current_size);
    }
  }

  return;
}

void DeleteTest2(uint64_t thread_id, TreeType *t) {
  for(int i = 0;i < key_num;i++) {
    int key = thread_num * i + thread_id;

    t->Delete(key, key + 1);
    t->Delete(key, key + 2);
    t->Delete(key, key + 3);
    t->Delete(key, key + 4);
  }

  return;
}

bool delete_get_value_print = false;
void DeleteGetValueTest(TreeType *t) {
  for(int i = 0;i < key_num * thread_num;i ++) {
    auto value_set = t->GetValue(i);

    if(delete_get_value_print) {
      printf("i = %d\n    Values = ", i);
    }

    for(auto it : value_set) {
      if(delete_get_value_print) {
        printf("%ld ", it);
      }
    }

    assert(value_set.size() == 0);

    if(delete_get_value_print) {
      putchar('\n');
    }
  }

  return;
}

bool insert_get_value_print = false;
void InsertGetValueTest(TreeType *t) {
  bwt_printf("GetValueTest()\n");

  for(int i = 0;i < key_num * thread_num;i++) {
    auto value_set = t->GetValue(i);

    if(insert_get_value_print) {
      printf("i = %d\n    Values = ", i);
    }

    for(auto it : value_set) {
      if(insert_get_value_print) {
        printf("%ld ", it);
      }
    }

    if(insert_get_value_print) {
      putchar('\n');
    }

    if(value_set.size() != 4) {
      assert(false);
    }
  }

  return;
}

std::atomic<size_t> insert_success;
std::atomic<size_t> delete_success;
std::atomic<size_t> delete_attempt;

void MixedTest1(uint64_t thread_id, TreeType *t) {
  if((thread_id % 2) == 0) {
    for(int i = 0;i < key_num;i++) {
      int key = thread_num * i + thread_id;

      if(t->Insert(key, key)) insert_success.fetch_add(1);
    }

    printf("Finish inserting\n");
  } else {
    for(int i = 0;i < key_num;i++) {
      int key = thread_num * i + thread_id - 1;

      while(t->Delete(key, key) == false) ;

      delete_success.fetch_add(1);
      delete_attempt.fetch_add(1UL);
    }
  }

  return;
}



void MixedGetValueTest(TreeType *t) {
  size_t value_count = 0UL;

  for(int i = 0;i < key_num * thread_num;i ++) {
    auto value_set = t->GetValue(i);

    value_count += value_set.size();
  }

  printf("Finished counting values: %lu\n", value_count);
  printf("    insert success = %lu; delete success = %lu\n",
         insert_success.load(),
         delete_success.load());
  printf("    delete attempt = %lu\n", delete_attempt.load());

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

void TestStdMapInsertReadPerformance() {
  std::chrono::time_point<std::chrono::system_clock> start, end;
  start = std::chrono::system_clock::now();

  // Insert 1 million keys into std::map
  std::map<long, long> test_map{};
  for(int i = 0;i < 1024 * 1024;i++) {
    test_map[i] = i;
  }

  end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed_seconds = end - start;

  std::cout << "std::map: " << 1.0 / elapsed_seconds.count()
            << " million insertion/sec" << "\n";

  ////////////////////////////////////////////
  // Test read
  std::vector<long> v{};
  v.reserve(100);

  start = std::chrono::system_clock::now();

  int iter = 10;
  for(int j = 0;j < iter;j++) {
    // Read 1 million keys from std::map
    for(int i = 0;i < 1024 * 1024;i++) {
      long t = test_map[i];

      v.push_back(t);
      v.clear();
    }
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  std::cout << "std::map: " << (1.0 * iter) / elapsed_seconds.count()
            << " million read/sec" << "\n";

  return;
}

void TestStdUnorderedMapInsertReadPerformance() {
  std::chrono::time_point<std::chrono::system_clock> start, end;
  start = std::chrono::system_clock::now();

  // Insert 1 million keys into std::map
  std::unordered_map<long, long> test_map{};
  for(int i = 0;i < 1024 * 1024;i++) {
    test_map[i] = i;
  }

  end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed_seconds = end - start;

  std::cout << "std::unordered_map: " << 1.0 / elapsed_seconds.count()
            << " million insertion/sec" << "\n";

  ////////////////////////////////////////////
  // Test read
  std::vector<long> v{};
  v.reserve(100);

  start = std::chrono::system_clock::now();

  int iter = 10;
  for(int j = 0;j < iter;j++) {
    // Read 1 million keys from std::map
    for(int i = 0;i < 1024 * 1024;i++) {
      long t = test_map[i];

      v.push_back(t);
      v.clear();
    }
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  std::cout << "std::unordered_map: " << (1.0 * iter) / elapsed_seconds.count()
            << " million read/sec" << "\n";

  return;
}

void TestBTreeInsertReadPerformance() {
  std::chrono::time_point<std::chrono::system_clock> start, end;

  // Insert 1 million keys into stx::btree
  btree<long,
        long,
        std::pair<long, long>,
        KeyComparator> test_map{KeyComparator{1}};

  start = std::chrono::system_clock::now();

  for(long i = 0;i < 1024 * 1024;i++) {
    test_map.insert(i, i);
  }

  end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed_seconds = end - start;

  std::cout << "stx::btree: " << 1.0 / elapsed_seconds.count()
            << " million insertion/sec" << "\n";

  ////////////////////////////////////////////
  // Test read
  std::vector<long> v{};
  v.reserve(100);

  start = std::chrono::system_clock::now();

  int iter = 10;
  for(int j = 0;j < iter;j++) {
    // Read 1 million keys from stx::btree
    for(int i = 0;i < 1024 * 1024;i++) {
      auto it = test_map.find(i);

      v.push_back(it->second);
      v.clear();
    }
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  std::cout << "stx::btree " << (1.0 * iter) / elapsed_seconds.count()
            << " million read/sec" << "\n";

  return;
}

void TestBTreeMultimapInsertReadPerformance() {
  std::chrono::time_point<std::chrono::system_clock> start, end;

  // Initialize multimap with a key comparator that is not trivial
  btree_multimap<long, long, KeyComparator> test_map{KeyComparator{1}};

  start = std::chrono::system_clock::now();

  // Insert 1 million keys into stx::btree_multimap
  for(long i = 0;i < 1024 * 1024;i++) {
    test_map.insert(i, i);
  }

  end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed_seconds = end - start;

  std::cout << "stx::btree_multimap: " << 1.0 / elapsed_seconds.count()
            << " million insertion/sec" << "\n";

  ////////////////////////////////////////////
  // Test read
  std::vector<long> v{};
  v.reserve(100);

  start = std::chrono::system_clock::now();

  int iter = 10;
  for(int j = 0;j < iter;j++) {
    // Read 1 million keys from stx::btree
    for(int i = 0;i < 1024 * 1024;i++) {
      auto it_pair = test_map.equal_range(i);

      // For multimap we need to write an external loop to
      // extract all keys inside the multimap
      // This is the place where btree_multimap is slower than
      // btree
      for(auto it = it_pair.first;it != it_pair.second;it++) {
        v.push_back(it->second);
      }

      v.clear();
    }
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  std::cout << "stx::btree_multimap " << (1.0 * iter) / elapsed_seconds.count()
            << " million read/sec" << "\n";

  return;
}


void TestBwTreeInsertReadDeletePerformance(TreeType *t, int key_num) {
  std::chrono::time_point<std::chrono::system_clock> start, end;
  start = std::chrono::system_clock::now();

  for(int i = 0;i < key_num;i++) {
    t->Insert(i, i);
  }

  end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed_seconds = end - start;

  std::cout << "BwTree: " << (key_num / (1024.0 * 1024.0)) / elapsed_seconds.count()
            << " million insertion/sec" << "\n";

  // Then test read performance
  int iter = 10;
  std::vector<long> v{};

  v.reserve(100);

  start = std::chrono::system_clock::now();

  for(int j = 0;j < iter;j++) {
    for(int i = 0;i < key_num;i++) {
      t->GetValue(i, v);

      v.clear();
    }
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  std::cout << "BwTree: " << (iter * key_num / (1024.0 * 1024.0)) / elapsed_seconds.count()
            << " million read/sec" << "\n";

  ///////////////////////////////////////////////////////////////////
  // Test Iterator (single value)
  ///////////////////////////////////////////////////////////////////
  start = std::chrono::system_clock::now();
  {
    for(int j = 0;j < iter;j++) {
      auto it = t->Begin();
      while(it.IsEnd() == false) {
        v.push_back(it->second);

        v.clear();
        it++;
      }
    }

    end = std::chrono::system_clock::now();

    elapsed_seconds = end - start;
    std::cout << "BwTree: " << (iter * key_num / (1024.0 * 1024.0)) / elapsed_seconds.count()
              << " million iteration/sec" << "\n";
  }

  // Insert again
  start = std::chrono::system_clock::now();

  for(int i = key_num - 1;i >= 0;i--) {
    t->Insert(i, i + 1);
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;

  std::cout << "BwTree: " << (key_num / (1024.0 * 1024.0)) / elapsed_seconds.count()
            << " million insertion (reverse order)/sec" << "\n";

  // Read again

  start = std::chrono::system_clock::now();

  for(int j = 0;j < iter;j++) {
    for(int i = 0;i < key_num;i++) {
      t->GetValue(i, v);

      v.clear();
    }
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  std::cout << "BwTree: " << (iter * key_num / (1024.0 * 1024.0)) / elapsed_seconds.count()
            << " million read (2 values)/sec" << "\n";

  // Verify reads

  for(int i = 0;i < key_num;i++) {
    t->GetValue(i, v);

    assert(v.size() == 2);
    if(v[0] == (i)) {
      assert(v[1] == (i + 1));
    } else if(v[0] == (i + 1)) {
      assert(v[1] == (i));
    } else {
      assert(false);
    }

    v.clear();
  }

  std::cout << "    All values are correct!\n";

  // Finally remove values

  start = std::chrono::system_clock::now();

  for(int i = 0;i < key_num;i++) {
    t->Delete(i, i);
  }

  for(int i = key_num - 1;i >= 0;i--) {
    t->Delete(i, i + 1);
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  std::cout << "BwTree: " << (key_num * 2 / (1024.0 * 1024.0)) / elapsed_seconds.count()
            << " million remove/sec" << "\n";

  for(int i = 0;i < key_num;i++) {
    t->GetValue(i, v);

    assert(v.size() == 0);
  }

  std::cout << "    All values have been removed!\n";

  return;
}

void TestBwTreeInsertReadPerformance(TreeType *t, int key_num) {
  std::chrono::time_point<std::chrono::system_clock> start, end;
  start = std::chrono::system_clock::now();

  for(int i = 0;i < key_num;i++) {
    t->Insert(i, i);
  }

  end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed_seconds = end - start;

  std::cout << "BwTree: " << (key_num / (1024.0 * 1024.0)) / elapsed_seconds.count()
            << " million insertion/sec" << "\n";

  // Then test read performance

  int iter = 10;
  std::vector<long> v{};

  v.reserve(100);

  start = std::chrono::system_clock::now();

  for(int j = 0;j < iter;j++) {
    for(int i = 0;i < key_num;i++) {
      t->GetValue(i, v);

      v.clear();
    }
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  std::cout << "BwTree: " << (iter * key_num / (1024.0 * 1024.0)) / elapsed_seconds.count()
            << " million read/sec" << "\n";

  return;
}

void TestBwTreeMultiThreadReadPerformance(TreeType *t, int key_num) {
  const int num_thread = 8;
  int iter = 10;

  std::chrono::time_point<std::chrono::system_clock> start, end;

  auto func = [key_num, iter](uint64_t thread_id, TreeType *t) {
    // First pin the thread to a core
    //PinToCore(thread_id);

    std::vector<long> v{};

    v.reserve(100);

    std::chrono::time_point<std::chrono::system_clock> start, end;

    start = std::chrono::system_clock::now();

    for(int j = 0;j < iter;j++) {
      for(int i = 0;i < key_num;i++) {
        t->GetValue(i, v);

        v.clear();
      }
    }

    end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;

    std::cout << "[Thread " << thread_id << " Done] @ "
              << (iter * key_num / (1024.0 * 1024.0)) / elapsed_seconds.count()
              << " million read/sec" << "\n";

    return;
  };

  start = std::chrono::system_clock::now();
  LaunchParallelTestID(num_thread, func, t);
  end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed_seconds = end - start;
  std::cout << num_thread << " Threads BwTree: overall "
            << (iter * key_num / (1024.0 * 1024.0) * num_thread) / elapsed_seconds.count()
            << " million read/sec" << "\n";

  return;
}

void StressTest(uint64_t thread_id, TreeType *t) {
  static std::atomic<size_t> tree_size;
  static std::atomic<size_t> insert_success;
  static std::atomic<size_t> delete_success;
  static std::atomic<size_t> total_op;

  const size_t thread_num = 8;

  int max_key = 1024 * 1024;

  std::random_device r;

  // Choose a random mean between 0 and max key value
  std::default_random_engine e1(r());
  std::uniform_int_distribution<int> uniform_dist(0, max_key - 1);

  while(1) {
    int key = uniform_dist(e1);

    if((thread_id % 2) == 0) {
      if(t->Insert(key, key)) {
        tree_size.fetch_add(1);
        insert_success.fetch_add(1);
      }
    } else {
      if(t->Delete(key, key)) {
        tree_size.fetch_sub(1);
        delete_success.fetch_add(1);
      }
    }

    size_t op = total_op.fetch_add(1);

    if(op % max_key == 0) {
      PrintStat(t);
      printf("Total operation = %lu; tree size = %lu\n",
             op,
             tree_size.load());
      printf("    insert success = %lu; delete success = %lu\n",
             insert_success.load(),
             delete_success.load());
    }

    size_t remainder = (op % (1024UL * 1024UL * 10UL));

    if(remainder < thread_num) {
      std::vector<long> v{};
      int iter = 10;

      v.reserve(100);

      std::chrono::time_point<std::chrono::system_clock> start, end;

      start = std::chrono::system_clock::now();

      for(int j = 0;j < iter;j++) {
        for(int i = 0;i < max_key;i++) {
          t->GetValue(i, v);

          v.clear();
        }
      }

      end = std::chrono::system_clock::now();
      std::chrono::duration<double> elapsed_seconds = end - start;

      std::cout << " Stress Test BwTree: "
                << (iter * max_key / (1024.0 * 1024.0)) / elapsed_seconds.count()
                << " million read/sec" << "\n";
    }
  }

  return;
}

/*
 * IteratorTest() - Tests iterator functionalities
 */
void IteratorTest(TreeType *t) {
  const long key_num = 1024 * 1024;

  // First insert from 0 to 1 million
  for(long int i = 0;i < key_num;i++) {
    t->Insert(i, i);
  }

  auto it = t->Begin();

  long i = 0;
  while(it.IsEnd() == false) {
    assert(it->first == it->second);
    assert(it->first == i);

    i++;
    it++;
  }

  assert(i == (key_num));

  auto it2 = t->Begin(key_num - 1);
  auto it3 = it2;

  it2++;
  assert(it2.IsEnd() == true);

  assert(it3->first == (key_num - 1));

  auto it4 = t->Begin(key_num + 1);
  assert(it4.IsEnd() == true);

  return;
}

void TestEpochManager(TreeType *t) {
  std::atomic<int> thread_finished;

  thread_finished = 1;

  auto func = [t, &thread_finished](uint64_t thread_id, int iter) {
    for(int i = 0;i < iter;i++) {
      auto node = t->epoch_manager.JoinEpoch();

      // Copied from stack overflow:
      // http://stackoverflow.com/questions/7577452/random-time-delay

      std::mt19937_64 eng{std::random_device{}()};  // or seed however you want
      std::uniform_int_distribution<> dist{1, 100};
      std::this_thread::sleep_for(std::chrono::milliseconds{dist(eng) +
                                                            thread_id});

      t->epoch_manager.LeaveEpoch(node);
    }

    printf("Thread finished: %d        \r", thread_finished.fetch_add(1));

    return;
  };

  LaunchParallelTestID(2, func, 10000);

  putchar('\n');

  return;
}

#define END_TEST do{ \
                  print_flag = true; \
                  delete t1; \
                  \
                  return 0; \
                 }while(0);



int main(int argc, char **argv) {
  bool run_benchmark_all = false;
  bool run_test = false;
  bool run_benchmark_bwtree = false;
  bool run_benchmark_bwtree_full = false;
  bool run_stress = false;
  bool run_epoch_test = false;

  int opt_index = 1;
  while(opt_index < argc) {
    char *opt_p = argv[opt_index];

    if(strcmp(opt_p, "--benchmark-all") == 0) {
      run_benchmark_all = true;
    } else if(strcmp(opt_p, "--test") == 0) {
      run_test = true;
    } else if(strcmp(opt_p, "--benchmark-bwtree") == 0) {
      run_benchmark_bwtree = true;
    } else if(strcmp(opt_p, "--benchmark-bwtree-full") == 0) {
      run_benchmark_bwtree_full = true;
    } else if(strcmp(opt_p, "--stress-test") == 0) {
      run_stress = true;
    } else if(strcmp(opt_p, "--epoch-test") == 0) {
      run_epoch_test = true;
    } else {
      printf("ERROR: Unknown option: %s\n", opt_p);

      return 0;
    }

    opt_index++;
  }

  bwt_printf("RUN_BENCHMARK = %d\n", run_benchmark_all);
  bwt_printf("RUN_BENCHMARK_BWTREE = %d\n", run_benchmark_bwtree);
  bwt_printf("RUN_TEST = %d\n", run_test);
  bwt_printf("RUN_STRESS = %d\n", run_stress);
  bwt_printf("RUN_EPOCH_TEST = %d\n", run_epoch_test);
  bwt_printf("======================================\n");

  //////////////////////////////////////////////////////
  // Next start running test cases
  //////////////////////////////////////////////////////

  TreeType *t1 = nullptr;
  tree_size = 0;

  if(run_epoch_test == true) {
    print_flag = true;
    t1 = new TreeType{KeyComparator{1},
                      KeyEqualityChecker{1}};
    print_flag = false;

    TestEpochManager(t1);

    print_flag = true;
    delete t1;
    print_flag = false;
  }

  if(run_benchmark_bwtree == true ||
     run_benchmark_bwtree_full == true) {
    print_flag = true;
    t1 = new TreeType{KeyComparator{1},
                      KeyEqualityChecker{1}};

    int key_num = 3 * 1024 * 1024;

    if(run_benchmark_bwtree_full == true) {
      key_num *= 10;
    }

    bwt_printf("Using key size = %d (%f million)\n",
               key_num,
               key_num / (1024.0 * 1024.0));

    print_flag = false;

    if(run_benchmark_bwtree_full == true) {
      // First we rely on this test to fill bwtree with 30 million keys
      TestBwTreeInsertReadPerformance(t1, key_num);

      // And then do a multithreaded read
      TestBwTreeMultiThreadReadPerformance(t1, key_num);
    } else {
      // This function will delete all keys at the end, so the tree
      // is empty after it returns
      TestBwTreeInsertReadDeletePerformance(t1, key_num);
      
      delete t1;
      t1 = new TreeType{KeyComparator{1},
                        KeyEqualityChecker{1}};
      
      // Tests random insert using one thread
      RandomInsertSpeedTest(t1, key_num);
      
      // Use stree_multimap as a reference
      RandomBtreeMultimapInsertSpeedTest(key_num);
    }

    print_flag = true;
    delete t1;
    print_flag = false;
  }

  if(run_benchmark_all == true) {
    print_flag = true;
    t1 = new TreeType{KeyComparator{1},
                      KeyEqualityChecker{1}};

    int key_num = 1024 * 1024;
    bwt_printf("Using key size = %d (%f million)\n",
               key_num,
               key_num / (1024.0 * 1024.0));

    print_flag = false;

    TestStdMapInsertReadPerformance();
    TestStdUnorderedMapInsertReadPerformance();
    TestBTreeInsertReadPerformance();
    TestBTreeMultimapInsertReadPerformance();
    TestBwTreeInsertReadPerformance(t1, key_num);

    print_flag = true;
    delete t1;
    print_flag = false;
  }

  if(run_test == true) {
    print_flag = true;
    t1 = new TreeType{KeyComparator{1},
                      KeyEqualityChecker{1}};
    print_flag = false;

    //////////////
    // Test iterator
    //////////////

    printf("Testing iterator...\n");

    IteratorTest(t1);
    PrintStat(t1);

    printf("Finised tetsing iterator\n");

    //////////////////////
    // Test random insert
    //////////////////////

    printf("Testing random insert...\n");

    // Do not print here otherwise we could not see result
    delete t1;
    t1 = new TreeType{KeyComparator{1},
                      KeyEqualityChecker{1}};

    LaunchParallelTestID(8, RandomInsertTest, t1);
    RandomInsertVerify(t1);
    
    printf("Finished random insert testing. Delete the tree.\n");
    
    delete t1;
    t1 = new TreeType{KeyComparator{1},
                      KeyEqualityChecker{1}};

    ////////////////////////////
    // Test mixed insert/delete
    ////////////////////////////

    LaunchParallelTestID(thread_num, MixedTest1, t1);
    printf("Finished mixed testing\n");

    PrintStat(t1);

    MixedGetValueTest(t1);

    LaunchParallelTestID(thread_num, InsertTest2, t1);
    printf("Finished inserting all keys\n");

    PrintStat(t1);

    InsertGetValueTest(t1);
    printf("Finished verifying all inserted values\n");

    LaunchParallelTestID(thread_num, DeleteTest1, t1);
    printf("Finished deleting all keys\n");

    PrintStat(t1);

    DeleteGetValueTest(t1);
    printf("Finished verifying all deleted values\n");

    LaunchParallelTestID(thread_num, InsertTest1, t1);
    printf("Finished inserting all keys\n");

    PrintStat(t1);

    InsertGetValueTest(t1);
    printf("Finished verifying all inserted values\n");

    LaunchParallelTestID(thread_num, DeleteTest2, t1);
    printf("Finished deleting all keys\n");

    PrintStat(t1);

    DeleteGetValueTest(t1);
    printf("Finished verifying all deleted values\n");

    LaunchParallelTestID(thread_num, InsertTest1, t1);
    printf("Finished inserting all keys\n");

    PrintStat(t1);

    InsertGetValueTest(t1);
    printf("Finished verifying all inserted values\n");

    LaunchParallelTestID(thread_num, DeleteTest1, t1);
    printf("Finished deleting all keys\n");

    PrintStat(t1);

    DeleteGetValueTest(t1);
    printf("Finished verifying all deleted values\n");

    LaunchParallelTestID(thread_num, InsertTest2, t1);
    printf("Finished inserting all keys\n");

    PrintStat(t1);

    InsertGetValueTest(t1);
    printf("Finished verifying all inserted values\n");

    LaunchParallelTestID(thread_num, DeleteTest2, t1);
    printf("Finished deleting all keys\n");

    PrintStat(t1);

    DeleteGetValueTest(t1);
    printf("Finished verifying all deleted values\n");

    print_flag = true;
    delete t1;
    print_flag = false;
  }

  if(run_stress == true) {
    print_flag = true;
    t1 = new TreeType{KeyComparator{1},
                      KeyEqualityChecker{1}};
    print_flag = false;

    LaunchParallelTestID(8, StressTest, t1);

    print_flag = true;
    delete t1;
    print_flag = false;
  }

  return 0;
}

