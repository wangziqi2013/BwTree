
#include "bwtree.h"
#include <thread>

using namespace peloton::index;

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

/*
void TestNavigateInnerNode(TreeType *t) {
  // node NodeID = 1000
  InnerNode *inner_node_p_1 = \
    t->DebugGetInnerNode(1, 10, INVALID_NODE_ID,
                         {1, 2, 4, 5, 8, 9},
                         {100, 102, 104, 105, 1088, 109});

  InnerNode *inner_node_p_2 = \
    t->DebugGetInnerNode(6, 10, INVALID_NODE_ID,
                         {6, 7, 8, 9},
                         {106, 107, 10888, 109});

  InnerDeleteNode *delete_node_p = \
    new InnerDeleteNode{8, 9, 7, 107, 0, inner_node_p_2};

  InnerInsertNode *insert_node_p_1 = \
    new InnerInsertNode{6, 8, 106, 0, inner_node_p_1};

  InnerInsertNode *insert_node_p_2 = \
    new InnerInsertNode{7, 8, 107, 0, insert_node_p_1};

  InnerSplitNode *split_node_p = \
    new InnerSplitNode{6, 1001, 0, insert_node_p_2};

  InnerInsertNode *insert_node_p_3 = \
    new InnerInsertNode{3, 4, 103, 0, split_node_p};

  InnerMergeNode *merge_node_p = \
    new InnerMergeNode{6, delete_node_p, 0, insert_node_p_3};

  t->InstallNewNode(1000, merge_node_p);
  t->InstallNewNode(1001, delete_node_p);

  LogicalInnerNode logical_node{TreeSnapshot{}};

  t->CollectAllSepsOnInnerRecursive(merge_node_p, &logical_node, true, true, true);

  for(auto item : logical_node.key_value_map) {
    bwt_printf("key = %d, value = %lu\n", item.first.key, item.second);
  }

  bwt_printf("High key = %d, low key = %d, next_id = %lu\n",
             logical_node.ubound_p->key,
             logical_node.lbound_p->key,
             logical_node.next_node_id);

  TreeSnapshot ts{};
  NodeID node_id = t->NavigateInnerNode(6, merge_node_p, &ts);

  bwt_printf("Node id = %lu; ts.id = %lu;\n", node_id, ts.first);

  return;
}
*/

void TestCollectAllValuesOnLeaf(TreeType *t) {
  BaseNode *node_p = PrepareSplitMergeLeaf(t);
  NodeSnapshot *snapshot_p = new NodeSnapshot{true};
  snapshot_p->node_id = 1000;
  snapshot_p->node_p = node_p;

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

  snapshot_p = new NodeSnapshot{true};
  snapshot_p->node_id = 1000;
  snapshot_p->node_p = node_p;

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

void TestNavigateLeafNode(TreeType *t) {
  BaseNode *node_p = PrepareSplitMergeLeaf(t);

  bwt_printf("========== Test NavigateLeafNode ==========\n");

  // NOTE: CANNOT USE 10 SINCE 10 IS OUT OF BOUND
  for(auto i : std::vector<int>{1, 2, 3, 4, 5, 6, 7, 8, 9}) {
    NodeSnapshot *snapshot_p = new NodeSnapshot{true};
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
    NodeSnapshot *snapshot_p = new NodeSnapshot{true};
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
}

int main() {
  TreeType *t1 = new BwTree<int, double>{};
  //BwTree<long, double> *t2 = new BwTree<long, double>{};

  //BwTree<int, double>::KeyType k1 = t1->GetWrappedKey(3);
  //BwTree<int, double>::KeyType k2 = t1->GetWrappedKey(2);
  //BwTree<int, double>::KeyType k3 = t1->GetNegInfKey();

  //bwt_printf("KeyComp: %d\n", t1->KeyCmpLess(k2, k3));
  //bwt_printf("sizeof(class BwTree) = %lu\n", sizeof(BwTree<long double, long double>));

  //GetNextNodeIDTest(t1);
  //BwTree<int, double>::PathHistory ph{};
  //t1->TraverseDownInnerNode(k1, &ph);

  //LocateLeftSiblingTest(t1);
  //CollectNewNodeSinceLastSnapshotTest(t1);

  //TestNavigateInnerNode(t1);
  TestCollectAllValuesOnLeaf(t1);
  TestNavigateLeafNode(t1);

  return 0;
}
