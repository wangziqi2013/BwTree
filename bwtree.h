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
#include <unordered_map>
#include <mutex>
#include <string>
#include <iostream>
#include <cassert>
#include <set>
#include <unordered_set>

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
          typename ValueEqualityChecker = std::equal_to<ValueType>,
          typename ValueHashFunc = std::hash<ValueType>>
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
  using ValueSet = std::unordered_set<ValueType, ValueHashFunc, ValueEqualityChecker>;

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
     * Constructor  - Use RawKeyType object to initialize
     */
    KeyType(const RawKeyType &p_key) : key{p_key} {}

    /*
     * Constructor - Use value type only (must not be raw value type)
     */
    KeyType(ExtendedKeyValue p_type) :
      key{},
      type{p_type} {
      // If it is raw type then we are having an empty key
      assert(p_type != ExtendedKeyValue::RawKey);
      return;
    }

    /*
     * IsNegInf() - Whether the key value is -Inf
     */
    bool IsNegInf() const {
      return type == ExtendedKeyValue::NegInf;
    }

    /*
     * IsPosInf() - Whether the key value is +Inf
     */
    bool IsPosInf() const {
      return type == ExtendedKeyValue::PosInf;
    }
  };

  /*
   * RawKeyCmpLess() - Compare raw key for < relation
   *
   * Directly uses the comparison object
   */
  inline bool RawKeyCmpLess(const RawKeyType &key1, const RawKeyType &key2) {
    return key_cmp_obj(key1, key2);
  }

  /*
   * RawKeyCmpEqual() - Compare raw key for == relation
   *
   * We use the fast comparison object rather than traditional < && >
   * approach to avoid performance penalty
   */
  inline bool RawKeyCmpEqual(const RawKeyType &key1, const RawKeyType &key2) {
    return key_eq_obj(key1, key2);
  }

  /*
   * RawKeyCmpNotEqual() - Compare raw key for != relation
   *
   * It negates result of RawKeyCmpEqual()
   */
  inline bool RawKeyCmpNotEqual(const RawKeyType &key1, const RawKeyType &key2) {
    return !RawKeyCmpEqual(key1, key2);
  }

  /*
   * RawKeyCmpGreaterEqual() - Compare raw key for >= relation
   *
   * It negates result of RawKeyCmpLess()
   */
  inline bool RawKeyCmpGreaterEqual(const RawKeyType &key1, const RawKeyType &key2) {
    return !RawKeyCmpLess(key1, key2);
  }

  /*
   * RawKeyCmpGreater() - Compare raw key for > relation
   *
   * It inverts input of RawKeyCmpLess()
   */
  inline bool RawKeyCmpGreater(const RawKeyType &key1, const RawKeyType &key2) {
    return RawKeyCmpLess(key2, key1);
  }

  /*
   * RawKeyCmpLessEqual() - Cmpare raw key for <= relation
   *
   * It negates result of RawKeyCmpGreater()
   */
  bool RawKeyCmpLessEqual(const RawKeyType &key1, const RawKeyType &key2) {
    return !RawKeyCmpGreater(key1, key2);
  }

  /*
   * KeyCmpLess() - Compare two keys for "less than" relation
   *
   * If key1 < key2 return true
   * If key1 >= key2 return false
   * If comparison not defined assertion would fail
   */
  bool KeyCmpLess(const KeyType &key1, const KeyType &key2) const {
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
    return key_cmp_obj(key1.key, key2.key);
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
  bool KeyCmpEqual(const KeyType &key1, const KeyType &key2) const {
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
  inline bool KeyCmpNotEqual(const KeyType &key1, const KeyType &key2) const {
    return !KeyCmpEqual(key1, key2);
  }

  /*
   * KeyCmpGreaterEqual() - Compare a pair of keys for >= relation
   *
   * It negates result of keyCmpLess()
   */
  inline bool KeyCmpGreaterEqual(const KeyType &key1, const KeyType &key2) const {
    return !KeyCmpLess(key1, key2);
  }

  /*
   * KeyCmpGreater() - Compare a pair of keys for > relation
   *
   * It flips input for keyCmpLess()
   */
  inline bool KeyCmpGreater(const KeyType &key1, const KeyType &key2) const {
    return KeyCmpLess(key2, key1);
  }

  /*
   * KeyCmpLessEqual() - Compare a pair of keys for <= relation
   */
  inline bool KeyCmpLessEqual(const KeyType &key1, const KeyType &key2) const {
    return !KeyCmpGreater(key1, key2);
  }

  /*
   * ValueCmpEqual() - Compare value for equality relation
   *
   * It directly wraps value comparatpr object
   */
  inline bool ValueCmpEuqal(const ValueType &val1, const ValueType &val2) const {
    return value_eq_obj(val1, val2);
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

    DataItem(const KeyType &p_key, const std::vector<ValueType> &p_value_list) :
      key{p_key},
      value_list{p_value_list} {}

    /*
     * Move Constructor - We move value list to save space
     */
    DataItem(DataItem &&di) :
      key{di.key},
      value_list{std::move(di.value_list)} {}

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
   * class DataItemComparator - Data item comparator function object
   *
   * NOTE: Since we could not instanciate object comparator so in order
   * to construct this object we need to pass in the object
   */
  class DataItemComparator {
   public:
    KeyComparator &key_cmp_obj;

    DataItemComparator(const KeyComparator &p_key_cmp_obj) :
      key_cmp_obj{p_key_cmp_obj} {}

    bool operator()(const DataItem &d1, const DataItem &d2) const {
      return key_cmp_obj(d1.key, d2.key);
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
    const NodeType type;

    /*
     * Constructor - Initialize type
     */
    BaseNode(NodeType p_type) : type{p_type} {}

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
    virtual NodeType GetType() const final {
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
    const KeyType lbound;
    const KeyType ubound;

    // If this is INVALID_NODE_ID then we know it is the
    // last node in the leaf chain
    const NodeID next_node_id;

    /*
     * Constructor - Initialize bounds and next node ID
     */
    LeafNode(const KeyType &p_lbound,
             const KeyType &p_ubound,
             NodeID p_next_node_id) :
      BaseNode{NodeType::LeafType},
      lbound{p_lbound},
      ubound{p_ubound},
      next_node_id{p_next_node_id}
      {}
  };

  /*
   * class LeafInsertNode - Insert record into a leaf node
   */
  class LeafInsertNode : public BaseNode {
   public:
    const KeyType insert_key;
    const ValueType value;

    // This records the current length of the delta chain
    // which facilitates consolidation
    const int depth;

    const BaseNode *child_node_p;

    /*
     * Constructor
     */
    LeafInsertNode(const KeyType &p_insert_key,
                   const ValueType &p_value,
                   int p_depth,
                   const BaseNode *p_child_node_p) :
    BaseNode{NodeType::LeafInsertType},
    insert_key{p_insert_key},
    value{p_value},
    depth{p_depth},
    child_node_p{p_child_node_p}
    {}
  };

  /*
   * class LeafDeleteNode - Delete record from a leaf node
   *
   * In multi-value mode, it takes a value to identify which value
   * to delete. In single value mode, value is redundant but what we
   * could use for sanity check
   */
  class LeafDeleteNode : public BaseNode {
   public:
    KeyType delete_key;
    ValueType value;

    const int depth;

    const BaseNode *child_node_p;

    /*
     * Constructor
     */
    LeafDeleteNode(const KeyType &p_delete_key,
                   const ValueType &p_value,
                   int p_depth,
                   const BaseNode *p_child_node_p) :
    BaseNode{NodeType::LeafDeleteType},
    delete_key{p_delete_key},
    value{p_value},
    depth{p_depth},
    child_node_p{p_child_node_p}
    {}
  };

  /*
   * class LeafSplitNode - Split node for leaf
   *
   * It includes a separator key to direct search to a correct direction
   * and a physical pointer to find the current next node in delta chain
   */
  class LeafSplitNode : public BaseNode {
   public:
    KeyType split_key;
    NodeID split_sibling;

    int depth;

    const BaseNode *child_node_p;

    /*
     * Constructor
     */
    LeafSplitNode(const KeyType &p_split_key,
                  NodeID p_split_sibling,
                  int p_depth,
                  const BaseNode *p_child_node_p) :
    BaseNode{NodeType::LeafSplitType},
    split_key{p_split_key},
    split_sibling{p_split_sibling},
    depth{p_depth},
    child_node_p{p_child_node_p}
    {}
  };

  /*
   * class LeafRemoveNode - Remove all physical children and redirect
   *                        all access to its logical left sibling
   *
   * It does not contain data and acts as merely a redirection flag
   */
  class LeafRemoveNode : public BaseNode {
   public:
    NodeID remove_sibling;

    int depth;

    const BaseNode *child_node_p;

    /*
     * Constructor
     */
    LeafRemoveNode(NodeID p_remove_sibling,
                   int p_depth,
                   const BaseNode *p_child_node_p) :
    BaseNode{NodeType::LeafRemoveType},
    remove_sibling{p_remove_sibling},
    depth{p_depth},
    child_node_p{p_child_node_p}
    {}
  };

  /*
   * class LeafMergeNode - Merge two delta chian structure into one node
   *
   * This structure uses two physical pointers to indicate that the right
   * half has become part of the current node and there is no other way
   * to access it
   */
  class LeafMergeNode : public BaseNode {
   public:
    KeyType merge_key;

    // For merge nodes we use actual physical pointer
    // to indicate that the right half is already part
    // of the logical node
    const BaseNode *right_merge_p;
    int depth;

    const BaseNode *child_node_p;

    /*
     * Constructor
     */
    LeafMergeNode(const KeyType &p_merge_key,
                  const BaseNode *p_right_merge_p,
                  int p_depth,
                  const BaseNode *p_child_node_p) :
    BaseNode{NodeType::LeafMergeType},
    merge_key{p_merge_key},
    right_merge_p{p_right_merge_p},
    depth{p_depth},
    child_node_p{p_child_node_p}
    {}
  };

  /*
   * class InnerNode - Inner node that holds separators
   */
  class InnerNode : public BaseNode {
   public:
    std::vector<SepItem> sep_list;
    KeyType lbound;
    KeyType ubound;

    // Used even in inner node to prevent early consolidation
    NodeID next_node_id;

    /*
     * Constructor
     */
    InnerNode(const KeyType &p_lbound,
              const KeyType &p_ubound,
              NodeID p_next_node_id) :
    BaseNode{NodeType::InnerType},
    lbound{p_lbound},
    ubound{p_ubound},
    next_node_id{p_next_node_id}
    {}
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
   public:
    KeyType insert_key;
    KeyType next_key;
    NodeID new_node_id;

    int depth;

    const BaseNode *child_node_p;

    /*
     * Constructor
     */
    InnerInsertNode(const KeyType &p_insert_key,
                    const KeyType &p_next_key,
                    int p_depth,
                    const BaseNode *p_child_node_p) :
    BaseNode{NodeType::InnerInsertType},
    insert_key{p_insert_key},
    next_key{p_next_key},
    depth{p_depth},
    child_node_p{p_child_node_p}
    {}
  };

  /*
   * class InnerSplitNode - Split inner nodes into two
   *
   * It has the same layout as leaf split node except for
   * the base class type variable. We make such distinguishment
   * to facilitate identifying current delta chain type
   */
  class InnerSplitNode : public BaseNode {
   public:
    KeyType split_key;
    NodeID split_sibling;

    int depth;

    const BaseNode *child_node_p;

    /*
     * Constructor
     */
    InnerSplitNode(const KeyType &p_split_key,
                   NodeID p_split_sibling,
                   int p_depth,
                   const BaseNode *p_child_node_p) :
    BaseNode{NodeType::InnerSplitType},
    split_key{p_split_key},
    split_sibling{p_split_sibling},
    depth{p_depth},
    child_node_p{p_child_node_p}
    {}
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
         ValueHashFunc p_value_hash_obj = std::hash<ValueType>{},
         bool p_key_dup = false) :
      key_cmp_obj{p_key_cmp_obj},
      key_eq_obj{p_key_eq_obj},
      value_eq_obj{p_value_eq_obj},
      value_hash_obj{p_value_hash_obj},
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

    InnerNode *root_node_p = \
      new InnerNode{GetNegInfKey(), GetPosInfKey(), INVALID_NODE_ID};

    root_node_p->sep_list.push_back(neg_si);
    root_node_p->sep_list.push_back(pos_si);

    bwt_printf("root id = %lu; first leaf id = %lu\n",
               root_id.load(),
               first_node_id);
    bwt_printf("Plugging in new node\n");

    InstallNewNode(root_id, root_node_p);

    LeafNode *left_most_leaf = \
      new LeafNode{GetNegInfKey(), GetPosInfKey(), INVALID_NODE_ID};

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
   *                   to traverse the index
   */
  inline KeyType GetWrappedKey(RawKeyType key) {
    return KeyType {key};
  }

  /*
   * GetPosInfKey() - Get a positive infinite key
   *
   * Assumes there is a trivial constructor for RawKeyType
   */
  inline KeyType GetPosInfKey() {
    return KeyType {ExtendedKeyValue::PosInf};
  }

  /*
   * GetNegInfKey() - Get a negative infinite key
   *
   * Assumes there is a trivial constructor for RawKeyType
   */
  inline KeyType GetNegInfKey() {
    return KeyType {ExtendedKeyValue::NegInf};
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
  BaseNode *GetNode(const NodeID node_id) const {
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
  bool IsLeafDeltaChainType(const NodeType type) const {
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
  NodeID LocateSeparatorForInnerNode(const KeyType &search_key,
                                     InnerNode *inner_node_p) const {
    const std::vector<SepItem> *sep_list_p = &inner_node_p->sep_list;

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
                            PathHistory *path_list_p) const {
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
  void TraverseDownInnerNode(const KeyType &search_key,
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
          LocateSeparatorForInnerNode(search_key, inner_node_p);

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
   * CollectDeltaPointer() - Collect delta node pointer for insert and delete
   *
   * This function correctly deals with merge, split and remove, starting on
   * the topmost node of a delta chain
   *
   * It pushes pointers of nodes into a vector, and stops at the leaf node.
   * The pointer of leaf node is not pushed into the vector, and it is
   * returned as the return value.
   *
   * Besides that this function takes an output argument which reports the current
   * NodeID and BaseNode pointer in case that a LeafSplitNode or LeafRemoveNode
   * redirects the current NodeID (for appending this is crucial). This argument
   * is merely for output, and if no change on NodeID then the NodeID field
   * is set to INVALID_NODE_ID and the pointer is set to nullptr.
   *
   * If real_tree is not needed (as for read-only routines) then it will not be
   * updated.
   *
   * This function is read-only, so it does not need to validate any structure
   * change.
   */
  const BaseNode *CollectDeltaPointer(const KeyType &search_key,
                                      const BaseNode *leaf_node_p,
                                      std::vector<const BaseNode *> *pointer_list_p,
                                      TreeSnapshot *real_tree) const {
    if(real_tree != nullptr) {
      // If the NodeID does not change then these two are retnrned to
      // signal the caller
      real_tree->first = INVALID_NODE_ID;
      real_tree->second = nullptr;
    }

    // This is used to test whether a remove node is valid
    // since it could only be the first node on a delta chain
    bool first_node = true;

    while(1) {
      NodeType type = leaf_node_p->GetType();
      switch(type) {
        case NodeType::LeafType: {
          // Make sure we are on the correct page
          // NOTE: Even under a b-link design, we could still accurately locate
          // a leaf node since split delta is actually the side pointer
          const LeafNode *leaf_p = static_cast<const LeafNode *>(leaf_node_p);

          // Even if we have seen merge and split this always hold
          // since merge and split would direct to the correct page by sep key
          assert(KeyCmpGreaterEqual(search_key, leaf_p->lbound) && \
                 KeyCmpLess(search_key, leaf_p->ubound));

          return leaf_node_p;
        }
        case NodeType::LeafInsertType: {
          const LeafInsertNode *insert_node_p = \
            static_cast<const LeafInsertNode *>(leaf_node_p);

          // If key is not specified, then blindly push the delta
          // If key is specified and there is a match then push the delta
          if(KeyCmpEqual(search_key, insert_node_p->insert_key)) {
            bwt_printf("Push insert delta\n");

            pointer_list_p->push_back(leaf_node_p);
          }

          leaf_node_p = insert_node_p->child_node_p;
          first_node = false;

          break;
        } // case LeafInsertType
        case NodeType::LeafDeleteType: {
          const LeafDeleteNode *delete_node_p = \
            static_cast<const LeafDeleteNode *>(leaf_node_p);

          if(KeyCmpEqual(search_key, delete_node_p->delete_key)) {
            bwt_printf("Push delete delta\n");

            pointer_list_p->push_back(leaf_node_p);
          }

          leaf_node_p = delete_node_p->child_node_p;
          first_node = false;

          break;
        } // case LeafDeleteType
        case NodeType::LeafRemoveType: {
          bwt_printf("Observed a remove node on leaf delta chain\n");
          assert(first_node == true);

          const LeafRemoveNode *leaf_remove_node_p = \
            static_cast<const LeafRemoveNode *>(leaf_node_p);

          // Remove node just acts as a redirection flag
          // goto its left sibling node by NodeID and continue
          NodeID left_node_id = leaf_remove_node_p->remove_sibling;
          leaf_node_p = GetNode(left_node_id);

          if(real_tree != nullptr) {
            // Since the NodeID has changed, we need to update path information
            real_tree->first = left_node_id;
            real_tree->second = const_cast<BaseNode *>(leaf_node_p);
          }

          // We do not set first_node to false here since we switched to
          // another new NodeID
          first_node = true;

          break;
        } // case LeafRemoveType
        case NodeType::LeafMergeType: {
          bwt_printf("Observed a merge node on leaf delta chain\n");

          const LeafMergeNode *merge_node_p = \
            static_cast<const LeafMergeNode *>(leaf_node_p);

          // Decide which side we should choose
          // Using >= for separator key
          if(KeyCmpGreaterEqual(search_key, merge_node_p->merge_key)) {
            bwt_printf("Take leaf merge right branch\n");

            leaf_node_p = merge_node_p->right_merge_p;
          } else {
            bwt_printf("Take leaf merge left branch\n");

            leaf_node_p = merge_node_p->child_node_p;
          }

          first_node = false;

          break;
        } // case LeafMergeType
        case NodeType::LeafSplitType: {
          bwt_printf("Observed a split node on leaf delta chain\n");

          const LeafSplitNode *split_node_p = \
            static_cast<const LeafSplitNode *>(leaf_node_p);

          if(KeyCmpGreaterEqual(search_key, split_node_p->split_key)) {
            bwt_printf("Take leaf split right (NodeID branch)\n");

            NodeID split_sibling_id = split_node_p->split_sibling;
            leaf_node_p = GetNode(split_sibling_id);

            if(real_tree != nullptr) {
              // Same as that in RemoveNode since the NodeID has changed
              real_tree->first = split_sibling_id;
              real_tree->second = const_cast<BaseNode *>(leaf_node_p);
            }

            // Since we are on the branch side of a split node
            // there should not be any record with search key in
            // the chain from where we come since otherwise these
            // records are misplaced
            assert(pointer_list_p->size() == 0);

            // Since we have switched to a new NodeID
            first_node = true;
          } else {
            leaf_node_p = split_node_p->child_node_p;

            first_node = false;
          }

          break;
        } // case LeafSplitType
        default: {
          bwt_printf("ERROR: Unknown leaf delta node type: %d\n",
                     leaf_node_p->GetType());

          assert(false);
        } // default
      } // switch
    } // while

    assert(false);
    return nullptr;
  } // function body

  /*
   * ReplayLogOnLeafByKey() - Replay the log for a certain key given leaf node
   *
   * This function traverses starting from the given pointer which is assumed
   * to point to a leaf-related node. It traverses down the delta chain until
   * a leaf node is encountered, pushing insert and delete deltas into a stack.
   *
   * Remove, split and merge are delat with correctly.
   *
   * Return value is stored in the output argument which is an unordered set
   * of ValueType
   */
  void ReplayLogOnLeafByKey(const KeyType &search_key,
                            const BaseNode *leaf_head_node_p,
                            ValueSet *value_set_p) const {
    std::vector<const BaseNode *> delta_node_list{};

    // We specify a key for the rouine to collect
    const BaseNode *ret = \
      CollectDeltaPointer(search_key, leaf_head_node_p, &delta_node_list, nullptr);
    assert(ret->GetType() == NodeType::LeafType);

    const LeafNode *leaf_base_p = static_cast<const LeafNode *>(ret);
    // Lambda is implemented with function object? Just wondering...
    // Find whether the leaf contains the key, and if yes then load
    // all values into the value set
    auto it = std::find_if(leaf_base_p->data_list.begin(),
                           leaf_base_p->data_list.end(),
                           [&search_key, this](const DataItem &di) {
                             return this->KeyCmpEqual(search_key, di.key);
                           });

    if(it != leaf_base_p->data_list.end()) {
      assert(KeyCmpEqual(search_key, it->key));

      for(const ValueType &value : it->value_list) {
        value_set_p->insert(value);
      }
    }

    // We use reverse iterator to traverse delta record
    // and do log replay
    for(auto rit = delta_node_list.rbegin();
        rit != delta_node_list.rend();
        rit++) {
      NodeType type = (*rit)->GetType();

      switch(type) {
        case NodeType::LeafInsertType: {
          const LeafInsertNode *insert_node_p = \
            static_cast<const LeafInsertNode *>(*rit);

          value_set_p->insert(insert_node_p->value);

          break;
        }
        case NodeType::LeafDeleteType: {
          const LeafDeleteNode *delete_node_p = \
            static_cast<const LeafDeleteNode *>(*rit);

          value_set_p->erase(delete_node_p->value);

          break;
        }
        default: {
          bwt_printf("ERROR: Unknown delta type: %d\n", type);

          assert(false);
        }
      } // switch(type)
    } // for(rit...)

    return;
  }

  /*
   * CollectAllValuesOnLeaf() - Collect all values given a pointer
   *
   * It does not need NodeID to collect values since only read-only
   * routine calls this one, so no validation is ever needed even in
   * its caller.
   *
   *
   */
  void CollectAllValuesOnLeaf(const BaseNode *leaf_node_p,
                              ValueSet *value_set_p) const {

  }

  /*
   * IsKeyPresent() - Check whether the key is present in the tree
   *
   * This routine does not modify any of the tree structure, and
   * therefore it does not need to keep any snapshot of the tree
   * even if it might jump from one ID to another
   *
   * Based on the same reason above this routine either does not
   * require a NodeID
   */
  bool IsKeyPresent(const KeyType &search_key,
                    const BaseNode *leaf_head_node_p) const {
    // Just to pass compilation
    ReplayLogOnLeafByKey(search_key, nullptr, nullptr);

    return false;
  }

  bool Insert(const RawKeyType &raw_key, ValueType &value) {
    bwt_printf("Insert called\n");

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
      //IsKeyPresent(search_key, leaf_head_p);
    }

    return false;
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
  const KeyComparator key_cmp_obj;
  // It is faster to compare equality using this than using two <
  const KeyEqualityChecker key_eq_obj;
  // Check whether values are equivalent
  const ValueEqualityChecker value_eq_obj;
  // Hash ValueType into a size_t
  const ValueHashFunc value_hash_obj;

  // Whether we should allow keys to have multiple values
  // This does not affect data layout, and will introduce extra overhead
  // for a given key. But it simplifies coding for duplicated values
  const bool key_dup;

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


