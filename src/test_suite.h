
/*
 * test_suite.cpp
 *
 * This files includes basic testing infrastructure and function declarations
 *
 * by Ziqi Wang
 */

#include <cstring>
#include <string>
#include <unordered_map>
#include <random>
#include <map>
#include <fstream>
#include <iostream>

#include <pthread.h>

#include "bwtree.h"
#include "../benchmark/stx_btree/btree.h"
#include "../benchmark/stx_btree/btree_multimap.h"
#include "../benchmark/libcuckoo/cuckoohash_map.hh"

#ifdef BWTREE_PELOTON
using namespace peloton::index;
#endif

using namespace stx;

/*
 * class KeyComparator - Test whether BwTree supports context
 *                       sensitive key comparator
 *
 * If a context-sensitive KeyComparator object is being used
 * then it should follow rules like:
 *   1. There could be no default constructor
 *   2. There MUST be a copy constructor
 *   3. operator() must be const
 *
 */
class KeyComparator {
 public:
  inline bool operator()(const long int k1, const long int k2) const {
    return k1 < k2;
  }

  KeyComparator(int dummy) {
    (void)dummy;

    return;
  }

  KeyComparator() = delete;
  //KeyComparator(const KeyComparator &p_key_cmp_obj) = delete;
};

/*
 * class KeyEqualityChecker - Tests context sensitive key equality
 *                            checker inside BwTree
 *
 * NOTE: This class is only used in KeyEqual() function, and is not
 * used as STL template argument, it is not necessary to provide
 * the object everytime a container is initialized
 */
class KeyEqualityChecker {
 public:
  inline bool operator()(const long int k1, const long int k2) const {
    return k1 == k2;
  }

  KeyEqualityChecker(int dummy) {
    (void)dummy;

    return;
  }

  KeyEqualityChecker() = delete;
  //KeyEqualityChecker(const KeyEqualityChecker &p_key_eq_obj) = delete;
};

using TreeType = BwTree<long int,
                        long int,
                        KeyComparator,
                        KeyEqualityChecker>;
using LeafRemoveNode = typename TreeType::LeafRemoveNode;
using LeafInsertNode = typename TreeType::LeafInsertNode;
using LeafDeleteNode = typename TreeType::LeafDeleteNode;
using LeafSplitNode = typename TreeType::LeafSplitNode;
using LeafMergeNode = typename TreeType::LeafMergeNode;
using LeafNode = typename TreeType::LeafNode;

using InnerRemoveNode = typename TreeType::InnerRemoveNode;
using InnerInsertNode = typename TreeType::InnerInsertNode;
using InnerDeleteNode = typename TreeType::InnerDeleteNode;
using InnerSplitNode = typename TreeType::InnerSplitNode;
using InnerMergeNode = typename TreeType::InnerMergeNode;
using InnerNode = typename TreeType::InnerNode;

using DeltaNode = typename TreeType::DeltaNode;

using NodeType = typename TreeType::NodeType;
using ValueSet = typename TreeType::ValueSet;
using NodeSnapshot = typename TreeType::NodeSnapshot;
using BaseNode = typename TreeType::BaseNode;

using Context = typename TreeType::Context;

/*
 * Common Infrastructure
 */
 
#define END_TEST do{ \
                print_flag = true; \
                delete t1; \
                \
                return 0; \
               }while(0);
 
/*
 * LaunchParallelTestID() - Starts threads on a common procedure
 *
 * This function is coded to be accepting variable arguments
 *
 * NOTE: Template function could only be defined in the header
 */
template <typename Fn, typename... Args>
void LaunchParallelTestID(uint64_t num_threads, Fn&& fn, Args &&... args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(fn, thread_itr, args...));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

void PrintStat(TreeType *t);
void PinToCore(size_t core_id);

/*
 * Basic test suite
 */
void InsertTest1(uint64_t thread_id, TreeType *t);
void InsertTest2(uint64_t thread_id, TreeType *t);
void DeleteTest1(uint64_t thread_id, TreeType *t);
void DeleteTest2(uint64_t thread_id, TreeType *t);

void InsertGetValueTest(TreeType *t);
void DeleteGetValueTest(TreeType *t);

extern int basic_test_key_num;
extern int basic_test_thread_num;

/*
 * Mixed test suite
 */
void MixedTest1(uint64_t thread_id, TreeType *t);
void MixedGetValueTest(TreeType *t);

extern std::atomic<size_t> mixed_insert_success;
extern std::atomic<size_t> mixed_delete_success;
extern std::atomic<size_t> mixed_delete_attempt;

extern int mixed_thread_num;
extern int mixed_key_num;

/*
 * Performance test suite
 */
void TestStdMapInsertReadPerformance(int key_size);
void TestStdUnorderedMapInsertReadPerformance(int key_size);
void TestBTreeInsertReadPerformance(int key_size);
void TestBTreeMultimapInsertReadPerformance(int key_size);
void TestCuckooHashTableInsertReadPerformance(int key_size);
void TestBwTreeInsertReadDeletePerformance(TreeType *t, int key_num);
void TestBwTreeInsertReadPerformance(TreeType *t, int key_num);
void TestBwTreeMultiThreadInsertPerformance(TreeType *t, int key_num);
void TestBwTreeMultiThreadReadPerformance(TreeType *t, int key_num);
void TestBwTreeEmailInsertPerformance(BwTree<std::string, long int> *t, std::string filename);

/*
 * Stress test suite
 */
void StressTest(uint64_t thread_id, TreeType *t);

/*
 * Iterator test suite
 */
void IteratorTest(TreeType *t);

/*
 * Random test suite
 */
void RandomBtreeMultimapInsertSpeedTest(size_t key_num);
void RandomCuckooHashMapInsertSpeedTest(size_t key_num);
void RandomInsertSpeedTest(TreeType *t, size_t key_num);
void RandomInsertSeqReadSpeedTest(TreeType *t, size_t key_num);
void SeqInsertRandomReadSpeedTest(TreeType *t, size_t key_num);
void InfiniteRandomInsertTest(TreeType *t);
void RandomInsertTest(uint64_t thread_id, TreeType *t);
void RandomInsertVerify(TreeType *t);

/*
 * Misc test suite
 */
void TestEpochManager(TreeType *t);

