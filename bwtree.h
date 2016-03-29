//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// BWTree.h
//
// Identification: src/backend/index/BWTree.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include <limits>
#include <vector>
#include <atomic>
#include <algorithm>
#include <cassert>
#include <stack>
#include <unordered_map>
#include <mutex>
#include <string>
#include <iostream>
#include <cassert>
#include <set>

#define BWTREE_DEBUG
//#define INTERACTIVE_DEBUG
#define ALL_PUBLIC

namespace peloton {
namespace index {

#ifdef INTERACTIVE_DEBUG

#define idb_assert(cond)                                                \
  do {                                                                  \
    if (!(cond)) {                                                      \
      debug_stop_mutex.lock();                                          \
      printf("assert, %-24s, line %d \033[0m", __FUNCTION__, __LINE__); \
      idb.start();                                                      \
      debug_stop_mutex.unlock();                                        \
    }                                                                   \
  } while (0);

#define idb_assert_key(key, cond)                                       \
  do {                                                                  \
    if (!(cond)) {                                                      \
      debug_stop_mutex.lock();                                          \
      printf("assert, %-24s, line %d \033[0m", __FUNCTION__, __LINE__); \
      idb.key_list.push_back(&key);                                     \
      idb.start();                                                      \
      debug_stop_mutex.unlock();                                        \
    }                                                                   \
  } while (0);

#else

#define idb_assert(cond) \
  do {                   \
    assert(cond);        \
  } while (0);

#define idb_assert_key(key, cond) \
  do {                            \
    assert(cond);                 \
  } while (0);

#endif

#ifdef BWTREE_DEBUG

#define bwt_printf(fmt, ...)                              \
  do {                                                    \
    printf("%-24s(): " fmt, __FUNCTION__, ##__VA_ARGS__); \
    fflush(stdout);                                       \
  } while (0);

#else

#define bwt_printf(fmt, ...)   \
  do {                         \
    dummy(fmt, ##__VA_ARGS__); \
  } while (0);

static void dummy(const char*, ...) {}

#endif

#ifdef INTERACTIVE_DEBUG
// In multi-threaded testing, if we want to halt all threads when an error
// happens
// then we lock this mutex
// Since every other thread will try to lock this at the beginning of
// findLeafPage() which is called for every operation, they will all stop
static std::mutex debug_stop_mutex;
#endif

/*
 * class BwTree() - Lock-free BwTree index implementation
 */
template <typename RawKeyType,
          typename ValueType,
          typename KeyComparator = std::less<RawKeyType>,
          typename KeyEqualityChecker = std::equal_to<RawKeyType>,
          typename ValueEqualityChecker = std::equal_to<ValueType>>
class BwTree {
 /*
  * Private & Public declaration (no definition)
  */
#ifndef ALL_PUBLIC
 private:
#else
 public:
#endif
  class InteractiveDebugger;
  friend InteractiveDebugger;

 public:
  class BaseNode;

 /*
  * private: Basic type definition
  */
#ifndef ALL_PUBLIC
 private:
#else
 public:
#endif

  using NodeID = uint64_t;
  // We use this type to represent the path we traverse down the tree
  using TreeSnapshot = std::pair<NodeID, BaseNode *>;
  using PathHistory = std::vector<TreeSnapshot>;

  // The maximum number of nodes we could map in this index
  constexpr static NodeID MAPPING_TABLE_SIZE = 1 << 24;

  // We use uint64_t(-1) as invalid node ID
  constexpr static NodeID INVALID_NODE_ID = std::numeric_limits<NodeID>::max();

  // Debug constant: The maximum number of iterations we could do
  // It prevents dead loop hopefully
  constexpr static int ITER_MAX = 99999;

  /*
   * enum class NodeType - Bw-Tree node type
   */
  enum class NodeType {
    // Data page type
    LeafType,
    InnerType,

    // Only valid for leaf
    LeafInsertType,
    LeafSplitType,
    LeafDeleteType,
    LeafRemoveType,
    LeafMergeType,

    // Only valid for inner
    InnerInsertType,
    InnerSplitType,
    InnerDeleteType,
    InnerRemoveType,
    InnerMergeType,
  };

  /*
   * enum class ExtendedKeyValue - We wrap the raw key with an extra
   * of indirection in order to identify +/-Inf
   */
  enum class ExtendedKeyValue {
    RawKey,
    PosInf,
    NegInf,
  };

  /*
   * struct KeyType - Wrapper class for RawKeyType which supports +/-Inf
   * for arbitrary key types
   *
   * NOTE: Comparison between +Inf and +Inf, comparison between
   * -Inf and -Inf, and equality check between them are not defined
   * If they occur at run time, then just fail
   */
  class KeyType {
   public:
    // If type is +/-Inf then we use this key to compare
    RawKeyType key;

    // Denote whether the key value is +/-Inf or not
    ExtendedKeyValue type;

    /*
     * IsNegInf() - Whether the key value is -Inf
     */
    bool IsNegInf() {
      return type == ExtendedKeyValue::NegInf;
    }

    /*
     * IsPosInf() - Whether the key value is +Inf
     */
    bool IsPosInf() {
      return type == ExtendedKeyValue::PosInf;
    }
  };

  /*
   * KeyCmpLess() - Compare two keys for "less than" relation
   *
   * If key1 < key2 return true
   * If key1 >= key2 return false
   * If comparison not defined assertion would fail
   */
  bool KeyCmpLess(KeyType &key1, KeyType &key2) const {
    // As long as the second operand is not -Inf then
    // we return true
    if(key1.IsNegInf()) {
      if(key2.IsNegInf()) {
        bwt_printf("ERROR: Compare negInf and negInf\n");
        assert(false);
      }

      return true;
    }

    // We already know key1 would not be negInf
    if(key2.IsNegInf()) {
      return false;
    }

    // As long as second operand is not
    if(key2.IsPosInf()) {
      if(key1.IsPosInf()) {
        bwt_printf("ERROR: Compare posInf and posInf\n");
        assert(false);
      }

      return true;
    }

    // We already know key2.type is not posInf
    if(key1.IsPosInf()) {
      return false;
    }

    // Then we need to compare real key
    return key_comp_obj(key1.key, key2.key);
  }

  /*
   * KeyCmpEqual() - Compare a pair of keys for equality
   *
   * If any of the key is +/-Inf then assertion fails
   * Otherwise use key_eq_obj to compare (fast comparison)
   *
   * NOTE: This property does not affect <= and >= since these
   * two are implemented using > and < respectively
   */
  bool KeyCmpEqual(KeyType &key1, KeyType &key2) const {
    if(key1.IsPosInf() || \
       key1.IsNegInf() || \
       key2.IsPosInf() || \
       key2.IsNegInf()) {
      bwt_printf("ERROR: Equality comparison involving Inf\n");
      bwt_printf("       [key1.type, key2.type] = [%d, %d]\n",
                 key1.type, key2.type);
      assert(false);
    }

    return key_eq_obj(key1.key, key2.key);
  }

  /*
   * KeyCmpNotEqual() - Comapre a pair of keys for inequality
   *
   * It negates result of keyCmpEqual()
   */
  inline bool KeyCmpNotEqual(KeyType &key1, KeyType &key2) const {
    return !KeyCmpEqual(key1, key2);
  }

  /*
   * KeyCmpGreaterEqual() - Compare a pair of keys for >= relation
   *
   * It negates result of keyCmpLess()
   */
  inline bool KeyCmpGreaterEqual(KeyType &key1, KeyType &key2) const {
    return !KeyCmpLess(key1, key2);
  }

  /*
   * KeyCmpGreater() - Compare a pair of keys for > relation
   *
   * It flips input for keyCmpLess()
   */
  inline bool KeyCmpGreater(KeyType &key1, KeyType &key2) const {
    return KeyCmpLess(key2, key1);
  }

  /*
   * KeyCmpLessEqual() - Compare a pair of keys for <= relation
   */
  inline bool KeyCmpLessEqual(KeyType &key1, KeyType &key2) const {
    return !KeyCmpGreater(key1, key2);
  }

  /*
   * struct DataItem - Actual data stored inside leaf of bw-tree
   *
   * We choose to define our own data container rather than using
   * std::pair because we anticipate to add more control structure
   * inside this class
   */
  struct DataItem {
    KeyType key;

    // Each key could be mapped to multiple values
    // These values could be repeated, be unique by value
    // or be unique by key. No matter which rule it obeys
    // we always use a vector to hold them and transfer the
    // logic to identify situation to data manipulation routines
    std::vector<ValueType> value_list;

    /*
     * Move Constructor - We move value list to save space
     */
    DataItem(DataItem &&di) {
      key = di.key;
      value_list = std::move(di.value_list);

      return;
    }

    /*
     * Move Assignment - Fast assignment
     */
    DataItem &operator=(DataItem &&di) {
      key = di.key;
      value_list = std::move(value_list);

      return *this;
    }
  };

  /*
   * struct SepItem() - Separator item for inner nodes
   *
   * We choose not to use std::pair bacause we probably need to
   * add information into this structure
   */
  struct SepItem {
    KeyType key;
    NodeID node;
  };

  /*
   * class BWNode - Generic node class; inherited by leaf, inner
   *                and delta node
   */
  class BaseNode {
   public:
    NodeType type;

    /*
     * Destructor - This must be virtual in order to properly destroy
     * the object only given a base type key
     */
    virtual ~BaseNode() {}

    /*
     * GetType() - Return the type of node
     *
     * This method does not allow overridding
     */
    virtual NodeType GetType() final {
      return type;
    }
  };

  /*
   * class LeafNode - Leaf node that holds data
   *
   * There are 5 types of delta nodes that could be appended
   * to a leaf node. 3 of them are SMOs, and 2 of them are data operation
   */
  class LeafNode : public BaseNode {
   public:
    // We always hold data within a vector of vectors
    std::vector<DataItem> data_list;
    KeyType lbound;
    KeyType ubound;

    // If this is INVALID_NODE_ID then we know it is the
    // last node in the leaf chain
    NodeID next_node_id;
  };

  /*
   * class LeafInsertNode - Insert record into a leaf node
   */
  class LeafInsertNode : public BaseNode {
    KeyType key;
    ValueType value;

    // This records the current length of the delta chain
    // which facilitates consolidation
    int depth;

    BaseNode *child_node_p;
  };

  /*
   * class LeafDeleteNode - Delete record from a leaf node
   *
   * In multi-value mode, it takes a value to identify which value
   * to delete. In single value mode, value is redundant but what we
   * could use for sanity check
   */
  class LeafDeleteNode {
    KeyType key;
    ValueType value;

    BaseNode *child_node_p;
  };

  /*
   * class LeafSplitNode - Split node for leaf
   *
   * It includes a separator key to direct search to a correct direction
   * and a physical pointer to find the current next node in delta chain
   */
  class LeafSplitNode : public BaseNode {
   public:
    KeyType sep_key;
    NodeID split_sibling;

    // This records whether the SMO has completed (since split is
    // the first step of the SMO
    std::atomic<int> counter;

    BaseNode *child_node_p;
  };

  /*
   * class InnerNode - Inner node that holds separators
   */
  class InnerNode : public BaseNode {
   public:
    std::vector<SepItem> sep_list;
    KeyType lbound;
    KeyType ubound;
  };

  /*
   * class InnerInsertNode - Insert node for inner nodes
   *
   * It has two keys in order to make decisions upon seeing this
   * node when traversing down the delta chain of an inner node
   * If the search key lies in the range between sep_key and
   * next_key then we know we should go to new_node_id
   */
  class InnerInsertNode : public BaseNode {
    KeyType sep_key;
    KeyType next_key;
    NodeID new_node_id;

    BaseNode *child_node_p;
  };

  /*
   * class InnerSplitNode - Split inner nodes into two
   *
   * It has the same layout as leaf split node except for
   * the base class type variable. We make such distinguishment
   * to facilitate identifying current delta chain type
   */
  class InnerSplitNode : public BaseNode {
    KeyType sep_key;
    NodeID split_sibling;

    std::atomic<int> counter;

    BaseNode *child_node_p;
  };


 /*
  * Interface Method Implementation
  */
 public:
  /*
   * Constructor - Set up initial environment for BwTree
   *
   * Any tree instance must start with an intermediate node as root, together
   * with an empty leaf node as child
   */
  BwTree(KeyComparator p_key_cmp_obj = std::less<RawKeyType>{},
         KeyEqualityChecker p_key_eq_obj = std::equal_to<RawKeyType>{},
         ValueEqualityChecker p_value_eq_obj = std::equal_to<ValueType>{},
         bool p_key_dup = false) :
      key_comp_obj{p_key_cmp_obj},
      key_eq_obj{p_key_eq_obj},
      value_eq_obj{p_value_eq_obj},
      key_dup{p_key_dup},
      next_unused_node_id{0} {
    bwt_printf("Bw-Tree Constructor called. "
               "Setting up execution environment...\n");

    InitMappingTable();
    InitNodeLayout();

    return;
  }

  /*
   * Destructor - Destroy BwTree instance
   */
  ~BwTree() {
    return;
  }

  /*
   * InitNodeLayout() - Initialize the nodes required to start BwTree
   *
   * We need at least a root inner node and a leaf node in order
   * to guide the first operation to the right place
   */
  void InitNodeLayout() {
    bwt_printf("Initializing node layout for root and first page...\n");

    root_id = GetNextNodeID();
    assert(root_id == 0UL);

    first_node_id = GetNextNodeID();
    assert(first_node_id == 1UL);

    SepItem neg_si {GetNegInfKey(), first_node_id};
    SepItem pos_si {GetPosInfKey(), INVALID_NODE_ID};

    InnerNode *root_node_p = new InnerNode{};
    root_node_p->type = NodeType::InnerType;
    root_node_p->lbound = GetNegInfKey();
    root_node_p->ubound = GetPosInfKey();

    root_node_p->sep_list.push_back(neg_si);
    root_node_p->sep_list.push_back(pos_si);

    bwt_printf("root id = %lu; first leaf id = %lu\n",
               root_id.load(),
               first_node_id);
    bwt_printf("Plugging in new node\n");

    InstallNewNode(root_id, root_node_p);

    LeafNode *left_most_leaf = new LeafNode{};
    left_most_leaf->lbound = GetNegInfKey();
    left_most_leaf->ubound = GetPosInfKey();
    left_most_leaf->type = NodeType::LeafType;
    left_most_leaf->next_node_id = INVALID_NODE_ID;

    InstallNewNode(first_node_id, left_most_leaf);

    return;
  }

  /*
   * InitMappingTable() - Initialize the mapping table
   *
   * It initialize all elements to NULL in order to make
   * first CAS on the mapping table would succeed
   */
  void InitMappingTable() {
    bwt_printf("Initializing mapping table.... size = %lu\n",
               MAPPING_TABLE_SIZE);

    for(NodeID i = 0;i < MAPPING_TABLE_SIZE;i++) {
      mapping_table[i] = nullptr;
    }

    return;
  }

  /*
   * GetWrappedKey() - Return an internally wrapped key type used
   * to traverse the index
   */
  inline KeyType GetWrappedKey(RawKeyType key) {
    return KeyType {key, ExtendedKeyValue::RawKey};
  }

  /*
   * GetPosInfKey() - Get a positive infinite key
   *
   * Assumes there is a trivial constructor for RawKeyType
   */
  inline KeyType GetPosInfKey() {
    return KeyType {RawKeyType{}, ExtendedKeyValue::PosInf};
  }

  /*
   * GetNegInfKey() - Get a negative infinite key
   *
   * Assumes there is a trivial constructor for RawKeyType
   */
  inline KeyType GetNegInfKey() {
    return KeyType {RawKeyType{}, ExtendedKeyValue::NegInf};
  }

  /*
   * GetNextNodeID() - Thread-safe lock free method to get next node ID
   *
   * This function will not return until we have successfully obtained the
   * ID and increased counter by 1
   */
  NodeID GetNextNodeID() {
    bool ret = false;
    NodeID current_id, next_id;

    do {
      current_id = next_unused_node_id.load();
      next_id = current_id + 1;

      // Optimistic approach: If nobody has touched next_id then we are done
      // Otherwise CAS would fail, and we try again
      ret = next_unused_node_id.compare_exchange_strong(current_id, next_id);
    } while(ret == false);

    return current_id;
  }

  /*
   * InstallNodeToReplace() - Install a node to replace a previous one
   *
   * If installation fails because CAS returned false, then return false
   * This function does not retry
   */
  bool InstallNodeToReplace(NodeID node_id,
                            BaseNode *node_p,
                            BaseNode *prev_p) {
    // Make sure node id is valid and does not exceed maximum
    assert(node_id != INVALID_NODE_ID);
    assert(node_id < MAPPING_TABLE_SIZE);

    bool ret = mapping_table[node_id].compare_exchange_strong(prev_p, node_p);

    return ret;
  }

  /*
   * InstallNewNode() - Install a new node into the mapping table
   *
   * This function does not return any value since we assume new node
   * installation would always succeed
   */
  void InstallNewNode(NodeID node_id, BaseNode *node_p) {
    // We initialize the mapping table to always have 0 for
    // unused entries
    bool ret = InstallNodeToReplace(node_id, node_p, nullptr);

    // So using nullptr to CAS must succeed
    assert(ret == true);

    return;
  }

  /*
   * GetNode() - Return the pointer mapped by a node ID
   *
   * This function checks the validity of the node ID
   */
  BaseNode *GetNode(NodeID node_id) {
    assert(node_id != INVALID_NODE_ID);
    assert(node_id < MAPPING_TABLE_SIZE);

    return mapping_table[node_id].load();
  }

  /*
   * IsLeafDeltaChainType() - Returns whether type is on of the
   *                          possible types on a leaf delta chain
   *
   * NOTE: Since we distinguish between leaf and inner node on all
   * SMOs it is easy to just judge underlying data node type using
   * the top of delta chain
   */
  bool IsLeafDeltaChainType(NodeType type) {
    return (type == NodeType::LeafDeleteType ||
            type == NodeType::LeafInsertType ||
            type == NodeType::LeafMergeType ||
            type == NodeType::LeafRemoveType ||
            type == NodeType::LeafSplitType ||
            type == NodeType::LeafType);
  }

  /*
   * LocateSeparatorForInnerNode() - Locate the child node for a key
   *
   * This functions works with any non-empty inner nodes. However
   * it fails assertion with empty inner node
   */
  NodeID LocateSeparatorForInnerNode(InnerNode *inner_node_p,
                                     KeyType search_key) {
    std::vector<SepItem> *sep_list_p = &inner_node_p->sep_list;

    // We do not know what to do for an empty inner node
    assert(sep_list_p->size() != 0UL);

    auto iter1 = sep_list_p->begin();
    auto iter2 = iter1 + 1;

    // NOTE: If there is only one element then we would
    // not be able to go into while() loop
    // and in that case we just check for upper bound
    //assert(iter2 != sep_list_p->end());

    // TODO: Replace this with binary search
    while(iter2 != sep_list_p->end()) {
      if(KeyCmpGreaterEqual(search_key, iter1->key) && \
         KeyCmpLess(search_key, iter2->key)) {
        return iter1->node;
      }

      iter1++;
      iter2++;
    }

    // This could only happen if we hit +Inf as separator
    assert(iter1->node != INVALID_NODE_ID);
    // If search key >= upper bound then we have hit the wrong
    // inner node
    assert(KeyCmpLess(search_key, inner_node_p->ubound));

    return iter1->node;
  }

  /*
   * SwitchToNewID() - Short hand helper function to update
   * current node and current head node
   *
   * NOTE: This also saves the new ID and new node pointer into
   * a stack which could be used to retrieve path history
   */
  inline void SwitchToNewID(NodeID new_id,
                            BaseNode **current_node_pp,
                            NodeType *current_node_type_p,
                            BaseNode **current_head_node_pp,
                            NodeType *current_head_node_type_p,
                            PathHistory *path_list_p) {
    *current_node_pp = GetNode(new_id);
    *current_node_type_p = (*current_node_pp)->GetType();

    *current_head_node_pp = *current_node_pp;
    *current_head_node_type_p = *current_node_type_p;

    // Save history for the new ID and new node pointer
    path_list_p->push_back(std::make_pair(new_id, *current_node_pp));

    return;
  }

  /*
   * TraverseDownInnerNode() - Find the leaf page given a key
   *
   * Append pairs of NodeID and BaseNode * type to path list since
   * these two fixed a view for the leaf page at the time we return it
   * And after we have finishing the job, we need to validate whether we
   * are still working on the latest snapshot, by CAS NodeID with the pointer
   */
  void TraverseDownInnerNode(KeyType &search_key,
                             PathHistory *path_list_p,
                             NodeID start_id = INVALID_NODE_ID) {

    // There is a slight difference: If start ID is a valid one
    // then we just use it; otherwise we need to fetch the root ID
    // from an atomic variable
    if(start_id == INVALID_NODE_ID)
    {
      start_id = root_id.load();
    }

    NodeID current_node_id = root_id.load();

    // Whether or not this has changed depends on the order of
    // this line and the CAS on the root (if there is one)
    BaseNode *current_node_p;
    // We need to update this while traversing down the delta chain
    NodeType current_node_type;

    // This always points to the head of delta chain
    BaseNode *current_head_node_p;
    NodeType current_head_node_type;

    SwitchToNewID(current_node_id,
                  &current_node_p,
                  &current_node_type,
                  &current_head_node_p,
                  &current_head_node_type,
                  path_list_p);

    while(1) {
      if(current_node_type == NodeType::InnerType) {
        bwt_printf("InnerType node\n");

        InnerNode *inner_node_p = \
          static_cast<InnerNode *>(current_node_p);
        NodeID subtree_id = \
          LocateSeparatorForInnerNode(inner_node_p, search_key);

        current_node_id = subtree_id;
        SwitchToNewID(current_node_id,
                      &current_node_p,
                      &current_node_type,
                      &current_head_node_p,
                      &current_head_node_type,
                      path_list_p);
        continue;
      } // If current node type == InnerType
      else if(IsLeafDeltaChainType(current_head_node_type)) {
        bwt_printf("Leaf delta chain type (%d)\n", current_head_node_type);

        // Current node and head pointer has been already pushed into
        // the stack
        return;
      } // If IsLeafDeltaChainType(current_head_node_type)
      else {
        bwt_printf("ERROR: Unknown node type = %d\n", current_node_type);
        bwt_printf("       node id = %lu\n", current_node_id);

        assert(false);
      }
    }


    return;
  }

  /*
   * IsKeyPresent() - Check whether the key is present in the tree
   *
   * This routine does not modify any of the tree structure, and
   * therefore it does not need to keep any snapshot of the tree
   * even if it might jump from one ID to another
   */
  bool IsKeyPresent(KeyType &search_key,
                    BaseNode *leaf_head_node_p) const {
    NodeType leaf_head_node_type = leaf_head_node_p->GetType();

    while(1) {
      switch(leaf_head_node_type) {
        case NodeType::LeafType: {

          break;
        }
        default: {
          bwt_printf("ERROR: Unknown leaf (delta) node type: %d\n",
                     leaf_head_node_type);
          assert(false);
        }
      }
    }

    return false;
  }

  bool Insert(RawKeyType &raw_key, ValueType &value) {
    PathHistory ph{};
    KeyType search_key = GetWrappedKey(raw_key);

    TraverseDownInnerNode(search_key, &ph);
    assert(ph.size() > 1UL);

    // Since it returns on seeing a leaf delta chain head
    // We use reference here to avoid copy
    const TreeSnapshot &ts = ph.back();
    NodeID leaf_head_id = ts.first;
    BaseNode *leaf_head_p = ts.second;

    if(key_dup == false) {

    }
  }

 /*
  * Private Method Implementation
  */
#ifndef ALL_PUBLIC
 private:
#else
 public:
#endif

 /*
  * Data Member Definition
  */
#ifndef ALL_PUBLIC
 private:
#else
 public:
#endif
  // Compare key in a less than manner
  KeyComparator key_comp_obj;
  // It is faster to compare equality using this than using two <
  KeyEqualityChecker key_eq_obj;
  // Check whether values are equivalent
  ValueEqualityChecker value_eq_obj;

  // Whether we should allow keys to have multiple values
  // This does not affect data layout, and will introduce extra overhead
  // for a given key. But it simplifies coding for duplicated values
  bool key_dup;

  std::atomic<NodeID> root_id;
  NodeID first_node_id;
  std::atomic<NodeID> next_unused_node_id;
  std::array<std::atomic<BaseNode *>, MAPPING_TABLE_SIZE> mapping_table;

 /*
  * Utility class definition
  */
#ifndef ALL_PUBLIC
 private:
#else
 public:
#endif

};

}  // End index namespace
}  // End peloton namespace


