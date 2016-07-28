
/*
 * performance_test.cpp
 *
 * This includes performance test for bwtree and other similat index
 * structures such as std::map, std::unordered_map, stx::btree
 * and stx::btree_multimap
 */

#include "test_suite.h"

/*
 * TestStdMapInsertReadPerformance() - As name suggests
 */
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

/*
 * TestStdUnorderedMapInsertReadPerformance() - As name suggests
 */
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

/*
 * TestBTreeInsertReadPerformance() - As name suggests
 */
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

/*
 * TestBTreeMultimapInsertReadPerformance() - As name suggests
 *
 * This function tests btree_multimap in a sense that retrieving values
 * are not considered complete until values have all been pushed
 * into the vector. This requires getting the iterator pair first, and
 * then iterate on the interval, pushing values into the vector
 */
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

/*
 * TestBwTreeInsertReadDeletePerformance() - As name suggests
 *
 * This function runs the following tests:
 *
 * 1. Sequential insert (key, key)
 * 2. Sequential read
 * 3. Sequential iterate
 * 4. Reverse order insert (key, key + 1)
 * 5. Reverse order read
 * 6. Remove all values
 */
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

/*
 * TestBwTreeInsertReadPerformance() - As name suggests
 */
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

/*
 * TestBwTreeMultiThreadReadPerformance() - As name suggests
 *
 * This should be called in a multithreaded environment
 */
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
            
  ///////////////////////////////////////////////////////////////////
  // Multithread random read performance
  ///////////////////////////////////////////////////////////////////
  
  // Since it might be slow, we make it as 1
  iter = 1;
  
  auto func2 = [key_num, iter](uint64_t thread_id, TreeType *t) {
    std::vector<long> v{};

    v.reserve(100);

    std::chrono::time_point<std::chrono::system_clock> start, end;
    
    std::random_device r{};
    std::default_random_engine e1(r());
    std::uniform_int_distribution<int> uniform_dist(0, key_num - 1);

    start = std::chrono::system_clock::now();

    for(int j = 0;j < iter;j++) {
      for(int i = 0;i < key_num;i++) {
        int key = uniform_dist(e1);
        
        t->GetValue(key, v);

        v.clear();
      }
    }

    end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;

    std::cout << "[Thread " << thread_id << " Done] @ "
              << (iter * key_num / (1024.0 * 1024.0)) / elapsed_seconds.count()
              << " million read (random)/sec" << "\n";

    return;
  };

  start = std::chrono::system_clock::now();
  LaunchParallelTestID(num_thread, func2, t);
  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  std::cout << num_thread << " Threads BwTree: overall "
            << (iter * key_num / (1024.0 * 1024.0) * num_thread) / elapsed_seconds.count()
            << " million read (random)/sec" << "\n";

  return;
}

/*
 * TestBwTreeEmailInsertPerformance() - Tests insert performance on string
 *                                      workload (email)
 *
 * This function requires a special email file that is not distributed
 * publicly
 */
void TestBwTreeEmailInsertPerformance(BwTree<std::string, long int> *t,
                                      std::string filename) {
  std::ifstream email_file{filename};
  
  std::vector<std::string> string_list{};
  
  // If unable to open file
  if(email_file.good() == false) {
    std::cout << "Unable to open file: " << filename << std::endl;
    
    return;
  }
  
  int counter = 0;
  std::string s{};
  
  // Then load the line until reaches EOF
  while(std::getline(email_file, s).good() == true) {
    string_list.push_back(s);
    
    counter++;
  }
    
  printf("Successfully loaded %d entries\n", counter);
  
  ///////////////////////////////////////////////////////////////////
  // After this point we continue with insertion
  ///////////////////////////////////////////////////////////////////
  
  int key_num = counter;
  
  std::chrono::time_point<std::chrono::system_clock> start, end;
  start = std::chrono::system_clock::now();

  for(int i = 0;i < key_num;i++) {
    t->Insert(string_list[i], i);
  }

  end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed_seconds = end - start;

  std::cout << "BwTree: " << (key_num / (1024.0 * 1024.0)) / elapsed_seconds.count()
            << " million email insertion/sec" << "\n";
            
  print_flag = true;
  delete t;
  print_flag = false;
            
  ///////////////////////////////////////////////////////////////////
  // Then test stx::btree_multimap
  ///////////////////////////////////////////////////////////////////

  stx::btree_multimap<std::string, long int> bt{};

  start = std::chrono::system_clock::now();

  for(int i = 0;i < key_num;i++) {
    bt.insert(string_list[i], i);
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;

  std::cout << "stx::btree_multimap: " << (key_num / (1024.0 * 1024.0)) / elapsed_seconds.count()
            << " million email insertion/sec" << "\n";
            
  return;
}
