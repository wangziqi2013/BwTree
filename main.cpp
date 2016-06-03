
#include "bwtree.h"

#ifdef BWTREE_PELOTON
using namespace peloton::index;
#endif

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
  bool operator()(const int k1, const int k2) const {
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
  bool operator()(const int k1, const int k2) const {
    return k1 == k2;
  }
  
  KeyEqualityChecker(int dummy) {
    (void)dummy;
    
    return;
  }
  
  KeyEqualityChecker() = delete;
  //KeyEqualityChecker(const KeyEqualityChecker &p_key_eq_obj) = delete;
};

using TreeType = BwTree<int,
                        double,
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
using DataItem = typename TreeType::DataItem;
using ValueSet = typename TreeType::ValueSet;
using KeyValueSet = typename TreeType::KeyValueSet;
using KeyType = typename TreeType::KeyType;
using LogicalLeafNode = typename TreeType::LogicalLeafNode;
using NodeSnapshot = typename TreeType::NodeSnapshot;
using LogicalInnerNode = typename TreeType::LogicalInnerNode;
using BaseNode = typename TreeType::BaseNode;

using Context = typename TreeType::Context;
using OpState = typename TreeType::OpState;

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

void GetNextNodeIDTestThread(uint64_t thread_id, BwTree<int, double> *tree_p) {
  bwt_printf("ID = %lu\n", tree_p->GetNextNodeID());
  return;
}

void GetNextNodeIDTest(BwTree<int, double> *tree_p) {
  LaunchParallelTestID(100, GetNextNodeIDTestThread, tree_p);
  return;
}

/////////////////////////////////////////////////////////////////////
// Prepare test structures
/////////////////////////////////////////////////////////////////////
BaseNode *PrepareSplitMergeLeaf(TreeType *t) {
  LeafNode *leaf_node_1_p = \
    t->DebugGetLeafNode(1, 10, INVALID_NODE_ID, {1, 2, 4, 6},
                        {{1.11},
                         {2.22},
                         {4.44},
                         {6.66}});

  LeafInsertNode *insert_node_1_p = \
    new LeafInsertNode{5, 5.555, 0, leaf_node_1_p};

  LeafInsertNode *insert_node_2_p = \
    new LeafInsertNode{3, 3.333, 0, insert_node_1_p};

  LeafSplitNode *split_node_1_p = \
    new LeafSplitNode{5, 1001, 0, insert_node_2_p};

  LeafNode *leaf_node_2_p = \
    t->DebugGetLeafNode(5, 10, 1002, {5, 6},
                        {{5.555},
                         {6.66}});

  LeafInsertNode *insert_node_3_p = \
    new LeafInsertNode{9, 9.999, 0, leaf_node_2_p};

  LeafDeleteNode *delete_node_1_p = \
    new LeafDeleteNode{5, 5.555, 0, insert_node_3_p};

  LeafMergeNode *merge_node_1_p = \
    new LeafMergeNode{5, delete_node_1_p, 0, split_node_1_p};

  LeafDeleteNode *delete_node_2_p = \
    new LeafDeleteNode{6, 6.66, 0, merge_node_1_p};

  t->InstallNewNode(1000, delete_node_2_p);
  t->InstallNewNode(1001, delete_node_1_p);

  return delete_node_2_p;
}

BaseNode *PrepareSplitMergeInner(TreeType *t) {
  InnerNode *inner_node_1_p = \
    t->DebugGetInnerNode(1, 10, 1002, {1, 2, 4, 6},
                         {101, 102, 104, 106});
  InnerInsertNode *insert_node_1_p = \
    new InnerInsertNode{5, 6, 205, 0, inner_node_1_p};

  InnerInsertNode *insert_node_2_p = \
    new InnerInsertNode{3, 4, 203, 0, insert_node_1_p};

  InnerNode *inner_node_2_p = \
    t->DebugGetInnerNode(5, 10, 1002, {5, 6},
                         {205, 106});

  InnerInsertNode *insert_node_3_p = \
    new InnerInsertNode{9, 10, 209, 0, inner_node_2_p};

  InnerDeleteNode *delete_node_1_p = \
    new InnerDeleteNode{6, 9, 5, 205, 0, insert_node_3_p};

  InnerSplitNode *split_node_1_p = \
    new InnerSplitNode{5, 1001, 0, insert_node_2_p};

  InnerMergeNode *merge_node_1_p = \
      new InnerMergeNode{5, delete_node_1_p, 0, split_node_1_p};

  InnerDeleteNode *delete_node_2_p = \
    new InnerDeleteNode{5, 9, 4, 104, 0, merge_node_1_p};

  t->InstallNewNode(1000, delete_node_2_p);
  t->InstallNewNode(1001, delete_node_1_p);

  return delete_node_2_p;
}

/*
void LocateLeftSiblingTest(TreeType *t) {
  LogicalInnerNode lin{{0, nullptr}};

  KeyType ubound{50};
  KeyType lbound{1};

  lin.ubound_p = &ubound;
  lin.lbound_p = &lbound;


  //lin.key_value_map[KeyType{1}] = 1;
  //lin.key_value_map[KeyType{10}] = 2;
  //lin.key_value_map[KeyType{20}] = 3;
  //lin.key_value_map[KeyType{30}] = 4;
  //lin.key_value_map[KeyType{40}] = 5;


  lin.key_value_map[KeyType{1}] = 1;
  lin.key_value_map[KeyType{10}] = 2;

  //KeyType search_key{1};
  //KeyType search_key{12};
  //KeyType search_key{30};
  KeyType search_key{11};

  NodeID node_id = t->LocateLeftSiblingByKey(search_key, &lin);

  bwt_printf("Left sib NodeId for key %d is %lu\n", search_key.key, node_id);

  return;
}
*/

/*
void CollectNewNodeSinceLastSnapshotTest(TreeType *t) {
  LeafNode *leaf_1_p = new LeafNode{0, 0, INVALID_NODE_ID};
  LeafRemoveNode *remove_1_p = new LeafRemoveNode{1, leaf_1_p};
  LeafRemoveNode *remove_2_p = new LeafRemoveNode{2, remove_1_p};
  LeafRemoveNode *remove_3_p = new LeafRemoveNode{3, remove_2_p};
  LeafRemoveNode *remove_4_p = new LeafRemoveNode{4, remove_3_p};
  LeafRemoveNode *remove_5_p = new LeafRemoveNode{5, remove_4_p};
  LeafRemoveNode *remove_6_p = new LeafRemoveNode{6, remove_5_p};

  LeafNode *leaf_2_p = new LeafNode{1, 1, INVALID_NODE_ID};
  LeafRemoveNode *remove_7_p = new LeafRemoveNode{1, leaf_2_p};
  LeafRemoveNode *remove_8_p = new LeafRemoveNode{2, remove_7_p};
  LeafRemoveNode *remove_9_p = new LeafRemoveNode{3, remove_8_p};

  bwt_printf("remove_1_p = %p\n", remove_1_p);
  bwt_printf("remove_2_p = %p\n", remove_2_p);
  bwt_printf("remove_3_p = %p\n", remove_3_p);
  bwt_printf("remove_4_p = %p\n", remove_4_p);
  bwt_printf("remove_5_p = %p\n", remove_5_p);
  bwt_printf("remove_6_p = %p\n", remove_6_p);
  bwt_printf("remove_7_p = %p\n", remove_7_p);
  bwt_printf("remove_8_p = %p\n", remove_8_p);
  bwt_printf("remove_9_p = %p\n", remove_9_p);

  ConstNodePointerList node_list{};

  bool ret = \
    t->CollectNewNodesSinceLastSnapshot(remove_3_p, remove_9_p, &node_list);

  bwt_printf("ret = %d\n", ret);
  for(auto p : node_list) {
    bwt_printf("ptr = %p\n", p);
  }

  return;
}
*/

void TestCollectAllValuesOnLeaf(TreeType *t) {
  BaseNode *node_p = PrepareSplitMergeLeaf(t);
  NodeSnapshot *snapshot_p = new NodeSnapshot{true, t};
  snapshot_p->node_id = 1000;
  snapshot_p->node_p = node_p;

  //Context *context_p = t->DebugGetContext(nullptr, snapshot_p);
  //snapshot_p = t->GetLatestNodeSnapshot(context_p);

  t->CollectAllValuesOnLeaf(snapshot_p);

  bwt_printf("========== Test CollectAllValuesOnLeaf ==========\n");

  for(auto &item : snapshot_p->GetLogicalLeafNode()->GetContainer()) {
    bwt_printf("Key = %d\n", item.first.key);
    for(auto value : item.second) {
      bwt_printf("      Value = %lf \n", value);
    }
  }

  bwt_printf("Low key = %d; High key = %d\n",
             snapshot_p->GetLogicalLeafNode()->lbound_p->key,
             snapshot_p->GetLogicalLeafNode()->ubound_p->key);

  bwt_printf("Next Node Id = %lu\n",
             snapshot_p->GetLogicalLeafNode()->next_node_id);

  bwt_printf("========== Test CollectMetaDataOnLeaf ==========\n");

  snapshot_p = new NodeSnapshot{true, t};
  snapshot_p->node_id = 1000;
  snapshot_p->node_p = node_p;

  //context_p = t->DebugGetContext(nullptr, snapshot_p);
  //snapshot_p = t->GetLatestNodeSnapshot(context_p);

  t->CollectMetadataOnLeaf(snapshot_p);

  for(auto &item : snapshot_p->GetLogicalLeafNode()->GetContainer()) {
    bwt_printf("Key = %d\n", item.first.key);
    for(auto value : item.second) {
      bwt_printf("      Value = %lf \n", value);
    }
  }

  bwt_printf("Low key = %d; High key = %d\n",
             snapshot_p->GetLogicalLeafNode()->lbound_p->key,
             snapshot_p->GetLogicalLeafNode()->ubound_p->key);

  bwt_printf("Next Node Id = %lu\n",
             snapshot_p->GetLogicalLeafNode()->next_node_id);

  t->DebugUninstallNode(1000);
  t->DebugUninstallNode(1001);

  return;
}

/*
void TestNavigateLeafNode(TreeType *t) {
  BaseNode *node_p = PrepareSplitMergeLeaf(t);

  bwt_printf("========== Test NavigateLeafNode ==========\n");

  // NOTE: CANNOT USE 10 SINCE 10 IS OUT OF BOUND
  for(auto i : std::vector<int>{1, 2, 3, 4, 5, 6, 7, 8, 9}) {
    NodeSnapshot *snapshot_p = new NodeSnapshot{true, t};
    snapshot_p->node_id = 1000;
    snapshot_p->node_p = node_p;

    t->NavigateLeafNode(i, snapshot_p);

    bwt_printf(">> Current testing: key = %d\n", i);
    for(auto &item : snapshot_p->GetLogicalLeafNode()->GetContainer()) {
      bwt_printf(">> Key = %d\n", item.first.key);
      for(auto value : item.second) {
        bwt_printf(">>      Value = %lf \n", value);
      }
    }

    bwt_printf("is sibling node = %d; NodeID = %lu\n",
               snapshot_p->is_split_sibling,
               snapshot_p->node_id);

    delete snapshot_p;
  }

  /////////////////////////////////////////////

  bwt_printf("========== Test NavigateLeafNode ==========\n");
  bwt_printf("              With split delta             \n");

  for(auto i : std::vector<int>{1, 2, 3, 4, 5, 6, 7, 8, 9}) {
    NodeSnapshot *snapshot_p = new NodeSnapshot{true, t};
    snapshot_p->node_id = 1000;
    // Points to split node
    snapshot_p->node_p = \
      ((DeltaNode *)(((DeltaNode *)node_p)->child_node_p))->child_node_p;

    t->NavigateLeafNode(i, snapshot_p);

    bwt_printf(">> Current testing: key = %d\n", i);
    for(auto &item : snapshot_p->GetLogicalLeafNode()->GetContainer()) {
      bwt_printf(">> Key = %d\n", item.first.key);
      for(auto value : item.second) {
        bwt_printf(">>      Value = %lf \n", value);
      }
    }

    bwt_printf("is sibling node = %d; NodeID = %lu\n",
               snapshot_p->is_split_sibling,
               snapshot_p->node_id);

    delete snapshot_p;
  }

  t->DebugUninstallNode(1000);
  t->DebugUninstallNode(1001);

  return;
}
*/

void TestCollectAllSepsOnInner(TreeType *t) {
  BaseNode *node_p = PrepareSplitMergeInner(t);

  NodeSnapshot *snapshot_p = new NodeSnapshot{false, t};
  snapshot_p->node_id = 1000;
  snapshot_p->node_p = node_p;

  t->CollectAllSepsOnInner(snapshot_p);

  bwt_printf("========== Test CollectAllSepsOnInner ==========\n");

  for(auto &item : snapshot_p->GetLogicalInnerNode()->GetContainer()) {
    bwt_printf("Key = %d\n", item.first.key);
    bwt_printf("      Value = %lu \n", item.second);
  }

  bwt_printf("Low key = %d; High key = %d\n",
             snapshot_p->GetLogicalInnerNode()->lbound_p->key,
             snapshot_p->GetLogicalInnerNode()->ubound_p->key);

  bwt_printf("Next Node Id = %lu\n",
             snapshot_p->GetLogicalInnerNode()->next_node_id);

  delete snapshot_p;

  bwt_printf("========== Test CollectMetadataOnInner ==========\n");

  snapshot_p = new NodeSnapshot{false, t};
  snapshot_p->node_id = 1000;
  snapshot_p->node_p = node_p;

  t->CollectMetadataOnInner(snapshot_p);

  for(auto &item : snapshot_p->GetLogicalInnerNode()->GetContainer()) {
    bwt_printf("Key = %d\n", item.first.key);
    bwt_printf("      Value = %lu \n", item.second);
  }

  bwt_printf("Low key = %d; High key = %d\n",
             snapshot_p->GetLogicalInnerNode()->lbound_p->key,
             snapshot_p->GetLogicalInnerNode()->ubound_p->key);

  bwt_printf("Next Node Id = %lu\n",
             snapshot_p->GetLogicalInnerNode()->next_node_id);

  t->DebugUninstallNode(1000);
  t->DebugUninstallNode(1001);

  return;
}

/*
void TestNavigateInnerNode(TreeType *t) {
  BaseNode *node_p = PrepareSplitMergeInner(t);

  bwt_printf("========== Test NavigateInnerNode ==========\n");

  // NOTE: CANNOT USE 10 SINCE 10 IS OUT OF BOUND
  for(auto i : std::vector<int>{1, 2, 3, 4, 5, 6, 7, 8, 9}) {
    NodeSnapshot *snapshot_p = new NodeSnapshot{false, t};
    snapshot_p->node_id = 1000;
    snapshot_p->node_p = node_p;

    NodeID node_id = t->NavigateInnerNode(i, snapshot_p);

    bwt_printf(">> Current testing: key = %d\n", i);
    if(node_id == INVALID_NODE_ID) {
      bwt_printf(">>      Value = INVALID\n");
    } else {
      bwt_printf(">>      Value = %lu \n", node_id);
      bwt_printf(">> is sibling node = %d; NodeID = %lu\n",
               snapshot_p->is_split_sibling,
               snapshot_p->node_id);
    }

    delete snapshot_p;
  }

  bwt_printf("========== Test NavigateInnerNode ==========\n");
  bwt_printf("               With Split Node              \n");

  // NOTE: CANNOT USE 10 SINCE 10 IS OUT OF BOUND
  for(auto i : std::vector<int>{1, 2, 3, 4, 5, 6, 7, 8, 9}) {
    NodeSnapshot *snapshot_p = new NodeSnapshot{false, t};
    snapshot_p->node_id = 1000;
    snapshot_p->node_p = \
      ((DeltaNode *)(((DeltaNode *)node_p)->child_node_p))->child_node_p;

    NodeID node_id = t->NavigateInnerNode(i, snapshot_p);

    bwt_printf(">> Current testing: key = %d\n", i);
    if(node_id == INVALID_NODE_ID) {
      bwt_printf(">>      Value = INVALID\n");
    } else {
      bwt_printf(">>      Value = %lu \n", node_id);
      bwt_printf(">> is sibling node = %d; NodeID = %lu\n",
               snapshot_p->is_split_sibling,
               snapshot_p->node_id);
    }

    delete snapshot_p;
  }
}
*/

constexpr int key_num = 65536;
constexpr int thread_num = 4;

std::mutex tree_size_mutex;
size_t tree_size = 0;

void InsertTest1(uint64_t thread_id, TreeType *t) {
  for(int i = thread_id * key_num;i < (int)(thread_id + 1) * key_num;i++) {
    t->Insert(i, 1.11L * i);
    t->Insert(i, 1.111L * i);
    t->Insert(i, 1.1111L * i);
    t->Insert(i, 1.11111L * i);

    //tree_size_mutex.lock();
    //tree_size += 4;
    //tree_size_mutex.unlock();
  }

  return;
}

void DeleteTest1(uint64_t thread_id, TreeType *t) {
  for(int i = thread_id * key_num;i < (int)(thread_id + 1) * key_num;i++) {
    t->Delete(i, 1.11L * i);
    t->Delete(i, 1.111L * i);
    t->Delete(i, 1.1111L * i);
    t->Delete(i, 1.11111L * i);

    //tree_size_mutex.lock();
    //tree_size -= 4;
    //tree_size_mutex.unlock();

    //printf("Tree size = %lu\n", tree_size);
  }

  return;
}

void InsertTest2(uint64_t thread_id, TreeType *t) {
  for(int i = 0;i < key_num;i++) {
    int key = thread_num * i + thread_id;

    t->Insert(key, 1.11L * key);
    t->Insert(key, 1.111L * key);
    t->Insert(key, 1.1111L * key);
    t->Insert(key, 1.11111L * key);

    //tree_size_mutex.lock();
    //tree_size += 4;
    //tree_size_mutex.unlock();

    //printf("Tree size = %lu\n", tree_size);
  }

  return;
}

void DeleteTest2(uint64_t thread_id, TreeType *t) {
  for(int i = 0;i < key_num;i++) {
    int key = thread_num * i + thread_id;

    t->Delete(key, 1.11L * key);
    t->Delete(key, 1.111L * key);
    t->Delete(key, 1.1111L * key);
    t->Delete(key, 1.11111L * key);

    //tree_size_mutex.lock();
    //tree_size += 4;
    //tree_size_mutex.unlock();

    //printf("Tree size = %lu\n", tree_size);
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
        printf("%lf ", it);
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

  auto value_set = t->GetValue(0);
  assert(value_set.size() == 1);

  for(int i = 1;i < key_num * thread_num;i++) {
    auto value_set = t->GetValue(i);

    if(insert_get_value_print) {
      printf("i = %d\n    Values = ", i);
    }

    for(auto it : value_set) {
      if(insert_get_value_print) {
        printf("%lf ", it);
      }
    }

    if(insert_get_value_print) {
      putchar('\n');
    }

    if(value_set.size() != 4) {
      //debug_stop_mutex.lock();
      //t->idb.Start();
      //debug_stop_mutex.unlock();

      assert(false);
    }
  }

  return;
}

void UpdateTest2(uint64_t thread_id, TreeType *t) {
  for(int i = 0;i < key_num;i++) {
    int key = thread_num * i + thread_id;

    t->Update(key, 1.11L * key, 2.22L * key);
    t->Update(key, 1.111L * key, 2.222L * key);
    t->Update(key, 1.1111L * key, 2.2222L * key);
    t->Update(key, 1.11111L * key, 2.22222L * key);
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

      if(t->Insert(key, 1.11L * key)) insert_success.fetch_add(1);
      //if(t->Insert(key, 1.111L * key)) insert_success.fetch_add(1);
      //if(t->Insert(key, 1.1111L * key)) insert_success.fetch_add(1);
      //if(t->Insert(key, 1.11111L * key)) insert_success.fetch_add(1);
    }
    
    printf("Finish inserting\n");
  } else {
    for(int i = 0;i < key_num;i++) {
      int key = thread_num * i + thread_id - 1;

      while(t->Delete(key, 1.11L * key) == false) ;
      delete_success.fetch_add(1);
      //while(t->Delete(key, 1.111L * key) == false) ;
      //while(t->Delete(key, 1.1111L * key) == false) ;
      //while(t->Delete(key, 1.11111L * key) == false) ;

      //if(t->Delete(key, 1.11L * key)) delete_success.fetch_add(1);
      //if(t->Delete(key, 1.111L * key)) delete_success.fetch_add(1);
      //if(t->Delete(key, 1.1111L * key)) delete_success.fetch_add(1);
      //if(t->Delete(key, 1.11111L * key)) delete_success.fetch_add(1);
      
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

#define END_TEST do{ \
                  print_flag = true; \
                  delete t1; \
                  \
                  return 0; \
                 }while(0);



int main() {
  TreeType *t1 = new TreeType{KeyComparator{1},
                              KeyEqualityChecker{1}};

  tree_size = 0;
  print_flag = false;
  
  // Test iterator
  //////////////
  
  LaunchParallelTestID(thread_num, InsertTest2, t1);
  printf("Finished inserting all keys\n");

  PrintStat(t1);
  
  auto it = t1->Begin();
  while(it.IsEnd() == false) {
    printf("%lf\n", *it);
  }
  
  //////////////
  
  insert_success = 0UL;
  delete_success = 0UL;
  delete_attempt = 0UL;
  
  LaunchParallelTestID(thread_num, MixedTest1, t1);
  printf("Finished mixed testing\n");
  
  PrintStat(t1);
  
  MixedGetValueTest(t1);

  LaunchParallelTestID(thread_num, InsertTest2, t1);
  printf("Finished inserting all keys\n");

  PrintStat(t1);

  //END_TEST

  //LaunchParallelTestID(thread_num, UpdateTest2, t1);
  //printf("Finished updating all keys\n");

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

  END_TEST

  return 0;
}
