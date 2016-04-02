

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
using NodeType = typename TreeType::NodeType;
using DataItem = typename TreeType::DataItem;
using NodeID = typename TreeType::NodeID;
using ValueSet = typename TreeType::ValueSet;
using KeyType = typename TreeType::KeyType;

NodeID INVALID_NODE_ID = TreeType::INVALID_NODE_ID;

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
  LeafNode *leaf1 = new LeafNode{1, 6, 101};
  //DataItem dt1;
  //DataItem dt2;
  //DataItem dt4;
  leaf1->data_list.push_back({1, {1.11}});
  leaf1->data_list.push_back({2, {2.22}});
  leaf1->data_list.push_back({4, {4.44}});

  LeafInsertNode *insert1 = new LeafInsertNode{3, 3.33, 1, leaf1};

  LeafNode *leaf2 = new LeafNode{6, 11, 102};
  //DataItem dt6{6, {6.66}};
  //DataItem dt7{7, {7.77}};
  //DataItem dt8{8, {8.88}};
  leaf2->data_list.push_back({6, {6.66}});
  leaf2->data_list.push_back({7, {7.77}});
  leaf2->data_list.push_back({8, {8.188}});

  LeafInsertNode *insert2 = new LeafInsertNode{8, 8.288, 1, leaf2};

  LeafNode *leaf3 = new LeafNode{11, 16, INVALID_NODE_ID};
  //DataItem dt12{12, {12.12}};
  //DataItem dt14{14, {14.14}};
  leaf3->data_list.push_back({12, {12.12}});
  leaf3->data_list.push_back({14, {14.14}});

  LeafInsertNode *insert3 = new LeafInsertNode{11, 11.11, 1, leaf3};
  LeafSplitNode *split1 = new LeafSplitNode{10, 102, 2, insert2};
  LeafDeleteNode *delete1 = new LeafDeleteNode{8, 8.288, 3, split1};

  LeafMergeNode *merge1 = new LeafMergeNode{6, delete1, 2, insert1};
  LeafRemoveNode *remove1 = new LeafRemoveNode{100, 4, delete1};

  t->InstallNewNode(100, merge1);
  t->InstallNewNode(101, remove1);
  t->InstallNewNode(102, insert3);

  ValueSet value_set{};
  t->ReplayLogOnLeafByKey(14, remove1, &value_set);

  for(auto it: value_set) {
    bwt_printf("Values = %lf\n", it);
  }

  return;
}


int main() {
  BwTree<int, double> *t1 = new BwTree<int, double>{};
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

  return 0;
}
