
#include "bwtree.h"
#include <thread>

using namespace peloton::index;

//template class BwTree<int, double>;
//template class BwTree<long, double>;
//template class BwTree<std::string, double>;
//template class BwTree<std::string, std::string>;

using TreeType = BwTree<int, double>;
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
using InnerNode = typename TreeType::LeafNode;

using NodeType = typename TreeType::NodeType;
using DataItem = typename TreeType::DataItem;
//using NodeID = typename TreeType::NodeID;
using ValueSet = typename TreeType::ValueSet;
using KeyValueSet = typename TreeType::KeyValueSet;
using KeyType = typename TreeType::KeyType;
using LogicalLeafNode = typename TreeType::LogicalLeafNode;
using TreeSnapshot = typename TreeType::TreeSnapshot;
using LogicalInnerNode = typename TreeType::LogicalInnerNode;
using BaseNode = typename TreeType::BaseNode;
using ConstNodePointerList = typename TreeType::ConstNodePointerList;

//NodeID INVALID_NODE_ID = TreeType::INVALID_NODE_ID;

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

void CollectDeltaPoniterTest1(TreeType *t) {
  LeafNode *leaf1 = t->DebugGetLeafNode(1, 6, 101,
                                        {1, 2, 4},
                                        {{1.11},
                                         {2.22, 2.222},
                                         {4.44, 4.4444}}
                                       );

  LeafInsertNode *insert1 = new LeafInsertNode{3, 3.33, 1, leaf1};

  LeafNode *leaf2 = t->DebugGetLeafNode(6, 11, 102,
                                        {6, 7, 8, 15},
                                        {{6.66},
                                         {7.77},
                                         {8.188},
                                         {15.15}}
                                        );

  LeafInsertNode *insert2 = new LeafInsertNode{8, 8.288, 1, leaf2};

/*
  LeafNode *leaf3 = new LeafNode{11, 16, INVALID_NODE_ID};
  leaf3->data_list.push_back({12, {12.12}});
  leaf3->data_list.push_back({14, {14.14}});
*/

  LeafNode *leaf3 = t->DebugGetLeafNode(11, 16, INVALID_NODE_ID,
                                        {12, 14},
                                        {{12.12, 12.1212},
                                         {14.14, 14.1414}}
                                       );
  LeafInsertNode *insert3 = new LeafInsertNode{11, 11.11, 1, leaf3};
  LeafSplitNode *split1 = new LeafSplitNode{10, 102, 2, insert2};
  LeafDeleteNode *delete1 = new LeafDeleteNode{8, 8.188, 3, split1};

  // NOTE: Merge needs also to keep an upperbound
  LeafMergeNode *merge1 = new LeafMergeNode{6, 11, delete1, 2, insert1};
  LeafRemoveNode *remove1 = new LeafRemoveNode{4, delete1};

  t->InstallNewNode(100, merge1);
  t->InstallNewNode(101, remove1);
  t->InstallNewNode(102, insert3);

  ValueSet value_set{};
  t->ReplayLogOnLeafByKey(14, merge1, &value_set);

  for(auto it: value_set) {
    bwt_printf("Values = %lf\n", it);
  }

  LogicalLeafNode logical_leaf{{100, merge1}};
  LogicalLeafNode logical_leaf_2{{100, merge1}};

  t->CollectAllValuesOnLeaf(&logical_leaf);

  for(auto &it : logical_leaf.key_value_set) {
    bwt_printf("key = %d\n", it.first.key);

    for(double it2 : logical_leaf.key_value_set[it.first]) {
      bwt_printf("    Value = %lf\n", it2);
    }
  }

  bwt_printf("Upperbound = %d, lowerbound = %d\n,",
             logical_leaf.ubound_p->key,
             logical_leaf.lbound_p->key);

  t->CollectMetadataOnLeaf(&logical_leaf_2);

  return;
}

void LocateLeftSiblingTest(TreeType *t) {
  LogicalInnerNode lin{{0, nullptr}};

  KeyType ubound{50};
  KeyType lbound{1};

  lin.ubound_p = &ubound;
  lin.lbound_p = &lbound;

  /*
  lin.key_value_map[KeyType{1}] = 1;
  lin.key_value_map[KeyType{10}] = 2;
  lin.key_value_map[KeyType{20}] = 3;
  lin.key_value_map[KeyType{30}] = 4;
  lin.key_value_map[KeyType{40}] = 5;
  */

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

int main() {
  TreeType *t1 = new BwTree<int, double>{};
  //BwTree<long, double> *t2 = new BwTree<long, double>{};

  BwTree<int, double>::KeyType k1 = t1->GetWrappedKey(3);
  BwTree<int, double>::KeyType k2 = t1->GetWrappedKey(2);
  BwTree<int, double>::KeyType k3 = t1->GetNegInfKey();

  bwt_printf("KeyComp: %d\n", t1->KeyCmpLess(k2, k3));
  bwt_printf("sizeof(class BwTree) = %lu\n", sizeof(BwTree<long double, long double>));

  //GetNextNodeIDTest(t1);
  BwTree<int, double>::PathHistory ph{};
  t1->TraverseDownInnerNode(k1, &ph);

  CollectDeltaPoniterTest1(t1);
  LocateLeftSiblingTest(t1);
  CollectNewNodeSinceLastSnapshotTest(t1);

  t1->CollectAllSepsOnInnerRecursive(nullptr, nullptr, false, false, false);

  return 0;
}
