
#include "test_suite.h"


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

  bwt_printf("RUN_BENCHMARK_ALL = %d\n", run_benchmark_all);
  bwt_printf("RUN_BENCHMARK_BWTREE = %d\n", run_benchmark_bwtree);
  bwt_printf("RUN_BENCHMARK_BWTREE_FULL = %d\n", run_benchmark_bwtree_full);
  bwt_printf("RUN_TEST = %d\n", run_test);
  bwt_printf("RUN_STRESS = %d\n", run_stress);
  bwt_printf("RUN_EPOCH_TEST = %d\n", run_epoch_test);
  bwt_printf("======================================\n");

  //////////////////////////////////////////////////////
  // Next start running test cases
  //////////////////////////////////////////////////////

  TreeType *t1 = nullptr;

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

    LaunchParallelTestID(basic_test_thread_num, MixedTest1, t1);
    printf("Finished mixed testing\n");

    PrintStat(t1);

    MixedGetValueTest(t1);

    LaunchParallelTestID(basic_test_thread_num, InsertTest2, t1);
    printf("Finished inserting all keys\n");

    PrintStat(t1);

    InsertGetValueTest(t1);
    printf("Finished verifying all inserted values\n");

    LaunchParallelTestID(basic_test_thread_num, DeleteTest1, t1);
    printf("Finished deleting all keys\n");

    PrintStat(t1);

    DeleteGetValueTest(t1);
    printf("Finished verifying all deleted values\n");

    LaunchParallelTestID(basic_test_thread_num, InsertTest1, t1);
    printf("Finished inserting all keys\n");

    PrintStat(t1);

    InsertGetValueTest(t1);
    printf("Finished verifying all inserted values\n");

    LaunchParallelTestID(basic_test_thread_num, DeleteTest2, t1);
    printf("Finished deleting all keys\n");

    PrintStat(t1);

    DeleteGetValueTest(t1);
    printf("Finished verifying all deleted values\n");

    LaunchParallelTestID(basic_test_thread_num, InsertTest1, t1);
    printf("Finished inserting all keys\n");

    PrintStat(t1);

    InsertGetValueTest(t1);
    printf("Finished verifying all inserted values\n");

    LaunchParallelTestID(basic_test_thread_num, DeleteTest1, t1);
    printf("Finished deleting all keys\n");

    PrintStat(t1);

    DeleteGetValueTest(t1);
    printf("Finished verifying all deleted values\n");

    LaunchParallelTestID(basic_test_thread_num, InsertTest2, t1);
    printf("Finished inserting all keys\n");

    PrintStat(t1);

    InsertGetValueTest(t1);
    printf("Finished verifying all inserted values\n");

    LaunchParallelTestID(basic_test_thread_num, DeleteTest2, t1);
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

