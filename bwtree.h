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
#include <vector>
#include <atomic>
#include <algorithm>
#include <cassert>
#include <unordered_map>
#include <mutex>
#include <string>
#include <iostream>
#include <set>
#include <unordered_set>
#include <map>

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

using NodeID = uint64_t;
// We use uint64_t(-1) as invalid node ID
constexpr NodeID INVALID_NODE_ID = NodeID(-1);

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
  class KeyType;
  class WrappedKeyComparator;
  class BaseLogicalNode;
  class NodeSnapshot;

 /*
  * private: Basic type definition
  */
#ifndef ALL_PUBLIC
 private:
#else
 public:
#endif

  // This is used to hold values in a set
  using ValueSet = std::unordered_set<ValueType, ValueHashFunc, ValueEqualityChecker>;
  // This is used to hold mapping from key to a set of values
  using KeyValueSet = std::map<KeyType, ValueSet, WrappedKeyComparator>;

  // This is used to hold key and NodeID ordered mapping relation
  using KeyNodeIDMap = std::map<KeyType, NodeID, WrappedKeyComparator>;

  // The maximum number of nodes we could map in this index
  constexpr static NodeID MAPPING_TABLE_SIZE = 1 << 24;

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
   * struct NodeSnapshot - Describes the states in a tree when we see them
   *
   * node_id and node_p are pairs that represents the state when we traverse
   * the node and use GetNode() to resolve the node ID.
   *
   * logical_node_p points to a consolidated view of the node, which might or
   * might not contain actual data (indicated by the boolean member), or even
   * might not be valid (if the pointer is nullptr).
   *
   * Also we need to distinguish between leaf snapshot and inner node snapshots
   * which is achieved by
   */

  class NodeSnapshot {
    NodeID node_id;
    BaseNode *node_p;
    BaseLogicalNode *logical_node_p;

    // Whether there is data or only metadata
    bool has_data;
    // Whether logical node pointer points to a logical leaf node
    // or logical inner node
    bool is_leaf;

    // Whether we have traversed through a sibling pointer
    // from a split delta node because of a half split state
    bool is_split_sibling;

    /*
     * Constructor - Initialize every member to invalid state
     */
    NodeSnapshot() :
      node_id{INVALID_NODE_ID},
      node_p{nullptr},
      logical_node_p{nullptr},
      has_data{false},
      is_leaf{false},
      is_split_sibling{false}
    {}
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
    KeyType(const RawKeyType &p_key) :
      key{p_key},
      type{ExtendedKeyValue::RawKey} // DO NOT FORGET THIS!
    {}

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
   * class WrappedKeyComparator - Compares wrapped key, using raw key
   *                              comparator in template argument
   */
  class WrappedKeyComparator {
   public:
    bool operator()(const KeyType &key1, const KeyType &key2) {
      return KeyComparator{}(key1.key, key2.key);
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

    /*
     * Constructor - Construction from a list of data values
     *
     * This one is mainly used for debugging purposes
     */
    DataItem(const KeyType p_key,
             const std::vector<ValueType> &p_value_list) :
      key{p_key},
      value_list{p_value_list}
    {}

    /*
     * Constructor - Use a value set to construct
     *
     * It bulk loads the value vector with an unordered set's begin()
     * and end() iterator
     */
    DataItem(const KeyType &p_key, const ValueSet &p_value_set) :
      key{p_key},
      value_list{p_value_set.begin(), p_value_set.end()}
    {}

    /*
     * Copy Constructor - Copy construct key and value vector
     *
     * We must declare this function since we defined a move constructor
     * and move assignment, copy constructor is deleted by default
     */
    DataItem(const DataItem &di) :
      key{di.key},
      value_list{di.value_list}
    {}

    /*
     * Move Constructor - We move value list to save space
     */
    DataItem(DataItem &&di) :
      key{di.key},
      value_list{std::move(di.value_list)}
    {}

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

    SepItem(const KeyType &p_key, NodeID p_node) :
      key{p_key},
      node{p_node}
    {}
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

    /*
     * IsLeafRemoveNode() - Return true if it is
     *
     * This function is specially defined since we want to test
     * for remove node as a special case
     */
    virtual bool IsLeafRemoveNode() const final {
      return type == NodeType::LeafRemoveType;
    }

    /*
     * IsInnerRemoveNode() - Return true if it is
     *
     * Same reason as above
     */
    virtual bool IsInnerRemoveNode() const final {
      return type == NodeType::InnerRemoveType;
    }

    /*
     * IsDeltaNode() - Return whether a node is delta node
     *
     * All nodes that are neither inner nor leaf type are of
     * delta node type
     */
    virtual bool IsDeltaNode() const final {
      if(type == NodeType::InnerType || \
         type == NodeType::LeafType) {
        return false;
      } else {
        return true;
      }
    }
  };

  /*
   * class DeltaNode - Common element in a delta node
   *
   * Common elements include depth of the node and pointer to
   * children node
   */
  class DeltaNode : public BaseNode {
   public:
    const int depth;

    const BaseNode *child_node_p;

    /*
     * Constructor
     */
    DeltaNode(NodeType p_type,
              int p_depth,
              const BaseNode *p_child_node_p) :
      BaseNode{p_type},
      depth{p_depth},
      child_node_p{p_child_node_p}
    {}
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
  class LeafInsertNode : public DeltaNode {
   public:
    const KeyType insert_key;
    const ValueType value;

    /*
     * Constructor
     */
    LeafInsertNode(const KeyType &p_insert_key,
                   const ValueType &p_value,
                   int p_depth,
                   const BaseNode *p_child_node_p) :
    DeltaNode{NodeType::LeafInsertType, p_depth, p_child_node_p},
    insert_key{p_insert_key},
    value{p_value}
    {}
  };

  /*
   * class LeafDeleteNode - Delete record from a leaf node
   *
   * In multi-value mode, it takes a value to identify which value
   * to delete. In single value mode, value is redundant but what we
   * could use for sanity check
   */
  class LeafDeleteNode : public DeltaNode {
   public:
    KeyType delete_key;
    ValueType value;

    /*
     * Constructor
     */
    LeafDeleteNode(const KeyType &p_delete_key,
                   const ValueType &p_value,
                   int p_depth,
                   const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::LeafDeleteType, p_depth, p_child_node_p},
      delete_key{p_delete_key},
      value{p_value}
    {}
  };

  /*
   * class LeafSplitNode - Split node for leaf
   *
   * It includes a separator key to direct search to a correct direction
   * and a physical pointer to find the current next node in delta chain
   */
  class LeafSplitNode : public DeltaNode {
   public:
    KeyType split_key;
    NodeID split_sibling;

    /*
     * Constructor
     */
    LeafSplitNode(const KeyType &p_split_key,
                  NodeID p_split_sibling,
                  int p_depth,
                  const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::LeafSplitType, p_depth, p_child_node_p},
      split_key{p_split_key},
      split_sibling{p_split_sibling}
    {}
  };

  /*
   * class LeafRemoveNode - Remove all physical children and redirect
   *                        all access to its logical left sibling
   *
   * It does not contain data and acts as merely a redirection flag
   */
  class LeafRemoveNode : public DeltaNode {
   public:

    /*
     * Constructor
     */
    LeafRemoveNode(int p_depth,
                   const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::LeafRemoveType, p_depth, p_child_node_p}
    {}
  };

  /*
   * class LeafMergeNode - Merge two delta chian structure into one node
   *
   * This structure uses two physical pointers to indicate that the right
   * half has become part of the current node and there is no other way
   * to access it
   */
  class LeafMergeNode : public DeltaNode {
   public:
    KeyType merge_key;

    // For merge nodes we use actual physical pointer
    // to indicate that the right half is already part
    // of the logical node
    const BaseNode *right_merge_p;

    /*
     * Constructor
     */
    LeafMergeNode(const KeyType &p_merge_key,
                  const BaseNode *p_right_merge_p,
                  int p_depth,
                  const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::LeafMergeType, p_depth, p_child_node_p},
      merge_key{p_merge_key},
      right_merge_p{p_right_merge_p}
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
  class InnerInsertNode : public DeltaNode {
   public:
    KeyType insert_key;
    KeyType next_key;
    NodeID new_node_id;

    /*
     * Constructor
     */
    InnerInsertNode(const KeyType &p_insert_key,
                    const KeyType &p_next_key,
                    NodeID p_new_node_id,
                    int p_depth,
                    const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::InnerInsertType, p_depth, p_child_node_p},
      insert_key{p_insert_key},
      next_key{p_next_key},
      new_node_id{p_new_node_id}
    {}
  };

  /*
   * class InnerDeleteNode - Delete node
   *
   * NOTE: There are three keys associated with this node, two of them
   * defining the new range after deleting this node, the remaining one
   * describing the key being deleted
   */
  class InnerDeleteNode : public DeltaNode {
   public:
    KeyType delete_key;
    // These two defines a new range associated with this delete node
    KeyType next_key;
    KeyType prev_key;

    NodeID prev_node_id;

    /*
     * Constructor
     *
     * NOTE: We need to provide three keys, two for defining a new
     * range, and one for removing the index term from base node
     */
    InnerDeleteNode(const KeyType &p_delete_key,
                    const KeyType &p_next_key,
                    const KeyType &p_prev_key,
                    NodeID p_prev_node_id,
                    int p_depth,
                    const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::InnerDeleteType, p_depth, p_child_node_p},
      delete_key{p_delete_key},
      next_key{p_next_key},
      prev_key{p_prev_key},
      prev_node_id{p_prev_node_id}
    {}
  };

  /*
   * class InnerSplitNode - Split inner nodes into two
   *
   * It has the same layout as leaf split node except for
   * the base class type variable. We make such distinguishment
   * to facilitate identifying current delta chain type
   */
  class InnerSplitNode : public DeltaNode {
   public:
    KeyType split_key;
    NodeID split_sibling;

    /*
     * Constructor
     */
    InnerSplitNode(const KeyType &p_split_key,
                   NodeID p_split_sibling,
                   int p_depth,
                   const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::InnerSplitType, p_depth, p_child_node_p},
      split_key{p_split_key},
      split_sibling{p_split_sibling}
    {}
  };

  /*
   * class InnerMergeNode - Merge delta for inner nodes
   */
  class InnerMergeNode : public DeltaNode {
   public:
    KeyType merge_key;

    const BaseNode *right_merge_p;

    /*
     * Constructor
     */
    InnerMergeNode(const KeyType &p_merge_key,
                   const BaseNode *p_right_merge_p,
                   int p_depth,
                   const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::InnerMergeType, p_depth, p_child_node_p},
      merge_key{p_merge_key},
      right_merge_p{p_right_merge_p}
    {}
  };

  /*
   * class InnerRemoveNode
   */
  class InnerRemoveNode : public DeltaNode {
   public:

    /*
     * Constructor
     */
    InnerRemoveNode(int p_depth,
                    BaseNode *p_child_node_p) :
      DeltaNode{NodeType::InnerRemoveType, p_depth, p_child_node_p}
    {}
  };

  ///////////////////////////////////////////////////////////////////
  // Logical node definition
  ///////////////////////////////////////////////////////////////////

  /*
   * class BaseLogicalNode - Base class of logical node
   *
   * Both inner logical node and leaf logical node have a high key,
   * low key, and next node NodeID
   *
   * NOTE: This structure is used as part of the NodeSnapshot structure
   */
  class BaseLogicalNode {
   public:
    const KeyType *lbound_p;
    const KeyType *ubound_p;

    NodeID next_node_id;

    /*
     * Constructor - Initialize everything to initial invalid state
     */
    BaseLogicalNode() :
      lbound_p{nullptr},
      ubound_p{nullptr},
      next_node_id{INVALID_NODE_ID}
    {}

  };

  /*
   * LogicalLeafNode() - A logical representation of a logical node
   *                     which is physically one or more chains of
   *                     delta node, SMO node and base nodes.
   *
   * This structure is usually used as a container to hold intermediate
   * results for a whole page operation. The caller fills in ID and pointer
   * field, while callee would fill the other fields
   */
  class LogicalLeafNode : public BaseLogicalNode {
   public:
    // These fields are filled by callee
    KeyValueSet key_value_set;

    // This is used to temporarily hold results, and should be empty
    // after all deltas has been applied
    std::vector<const BaseNode *> pointer_list;

    /*
     * Constructor - Initialize logical ID and physical pointer
     *               as the tree snapshot
     */
    LogicalLeafNode() :
      BaseLogicalNode{},
      key_value_set{},
      pointer_list{}
    {}

    /*
     * BulkLoadValue() - Load a vector of ValueType into the given key
     *
     * Parameter is given by a DataItem reference which points to the
     * data field from some leaf base node
     */
    void BulkLoadValue(const DataItem &item) {
      auto ret = key_value_set.emplace(std::make_pair(item.key, ValueSet{}));
      // Make sure it does not already exist in key value set
      assert(ret.second == true);
      // This points to the newly constructed ValueSet
      auto it = ret.first;

      // Take advantange of the bulk load method in unordered_map
      it->second.insert(item.value_list.begin(), item.value_list.end());

      return;
    }

    /*
     * ReplayLog() - This function iterates through delta nodes
     *               in the reverse order that they are pushed
     *               and apply them to the key value set
     *
     * NOTE: This method does not check for empty keys, i.e. even if
     * the value set for a given key is empty, we still let it be
     * So don't rely on the key map size to determine space needed
     * to store the list; also when packing this into a leaf node
     * we need to check for emptiness and remove
     */
    void ReplayLog() {
      // For each insert/delete delta, replay the change on top
      // of base page values
      for(auto node_p_it = pointer_list.rbegin();
          node_p_it != pointer_list.rend();
          node_p_it++) {
        const BaseNode *node_p = *node_p_it;
        NodeType type = node_p->GetType();

        switch(type) {
          case NodeType::LeafInsertType: {
            const LeafInsertNode *insert_node_p = \
              static_cast<const LeafInsertNode *>(node_p);

            auto it = key_value_set.find(insert_node_p->insert_key);
            if(it == key_value_set.end()) {
              // If the key does not exist yet we create a new value set
              auto ret = key_value_set.emplace( \
                std::make_pair(insert_node_p->insert_key,
                               ValueSet{})
              );

              // Insertion always happen
              assert(ret.second == true);

              // Now assign the newly allocated slot for the new key to it
              // so that we could use it later
              it = ret.first;
            }

            // it is either newly allocated or already exists
            it->second.insert(insert_node_p->value);

            break;
          } // case LeafInsertType
          case NodeType::LeafDeleteType: {
            const LeafDeleteNode *delete_node_p = \
              static_cast<const LeafDeleteNode *>(node_p);

            auto it = key_value_set.find(delete_node_p->delete_key);
            if(it == key_value_set.end()) {
              bwt_printf("ERROR: Delete a value that does not exist\n");

              assert(false);
            }

            it->second.erase(delete_node_p->value);

            break;
          } // case LeafDeleteType
          default: {
            bwt_printf("ERROR: Unknown delta node type: %d\n", type);

            assert(false);
          } // default
        } // case type
      } // for node_p in node_list

      return;
    }

    /*
     * ToLeafNode() - Convert this logical node into a leaf node
     *
     * The most significant change would be to marshal the hash table
     * and map used to store keys and values into a two dimentional vector
     * of keys and values
     *
     * NOTE: This routine allocates memory for leaf page!!!!!!!!!!!!!
     */
    LeafNode *ToLeafNode() {
      assert(BaseLogicalNode::lbound_p != nullptr);
      assert(BaseLogicalNode::ubound_p != nullptr);
      assert(BaseLogicalNode::next_node_id != INVALID_NODE_ID);

      LeafNode *leaf_node_p = \
        new LeafNode(*BaseLogicalNode::lbound_p,
                     *BaseLogicalNode::ubound_p,
                     BaseLogicalNode::next_node_id);

      // The key is already ordered, we just need to check for value
      // emptiness
      for(auto &it : key_value_set) {
        if(it.second.size() == 0) {
          bwt_printf("Skip empty value set\n");

          continue;
        }

        // Construct a data item in-place
        // NOTE: The third parameter is just for disambiguity (we overloaded
        // the constructor to let it either receive an array of values or
        // <key, value-vector> initializers). Compiler could not disambiguate
        // without the extra dummy argument
        leaf_node_p->data_list.emplace({it.first, it->second}, true);
      }

      return leaf_node_p;
    }

  };

  /*
   * LogicalInnerNode - Logical representation of an inner node
   *
   * This corresponds to a single-key single-value version logical
   * page node with all delta changes applied to separator list
   *
   * The structure of this logical node is even more similar to
   * InnerNode than its counterart for leaf. The reason is that
   * for inner nodes, we do not have to worry about multi-value
   * single-key, and thus the reconstruction algorithm does not
   * require replaying the log.
   *
   * In order to do down-traversal we do not have to compress
   * delta chain into a single logical node since the inner delta
   * node is already optimized for doing traversal. However, when
   * we perform left sibling locating, it is crucial for us to
   * be able to sequentially access all separators
   */
  class LogicalInnerNode : public BaseLogicalNode {
   public:
    KeyNodeIDMap key_value_map;

    /*
     * Constructor - Accept an initial starting point and init others
     */
    LogicalInnerNode() :
      BaseLogicalNode{},
      key_value_map{}
    {}

    /*
     * ToInnerNode() - Convert to inner node object
     *
     * This function allocates a chunk of memory from the heap, which
     * should be freed by epoch manager.
     */
    InnerNode *ToInnerNode() {
      assert(BaseLogicalNode::lbound_p != nullptr);
      assert(BaseLogicalNode::ubound_p != nullptr);
      assert(BaseLogicalNode::next_node_id != INVALID_NODE_ID);

      // Memory allocated here should be freed by epoch
      InnerNode *inner_node_p = \
        new InnerNode{*BaseLogicalNode::lbound_p,
                      *BaseLogicalNode::ubound_p,
                      BaseLogicalNode::next_node_id};

      // Iterate through the ordered map and push separator items
      for(auto &it : key_value_map) {
        inner_node_p->sep_list.emplace({it.first, it.second});
      }

      return inner_node_p;
    }
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
   *
   * NOTE: This function fixes a snapshot; its counterpart using
   * CAS instruction to install a new node creates new snapshot
   * and the serialization order of these two atomic operations
   * determines actual serialization order
   *
   * If we want to keep the same snapshot then we should only
   * call GetNode() once and stick to that physical pointer
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
   * CollectNewNodesSinceLastSnapshot() - This function compares two different
   *                                      snapshots
   *
   * If CAS fails then we know the snaoshot is no longer most up-to-date
   * in which case we need to find out what happened between the time
   * we took the snapshot and the time we did CAS
   *
   * There are three possibilities:
   * (1) There are delta nodes appended onto the delta chain
   *     In this case we could find pointer after traversing down
   *     for some number of nodes
   *     All new nodes excluding the common one and below are
   *     pushed into node_list_p, and return false
   * (2) The old delta chain has been consolidated and replaced
   *     with a brand new one. There is no old pointers in the
   *     new delta chain (epoch manager guarantees they will not
   *     be recycled before the current thread exits)
   *     In this case we will traverse down to the base node
   *     and could not find a matching pointer
   *     In this case the entire delta chain is pushed (naturally
   *     because there is no common pointer) and return true
   *     NOTE: In this case the base page is also in the node list
   * (3) The NodeID has been deleted (so that new_pointer_p is nullptr)
   *     because the NodeID has been removed and merged into its left
   *     sibling.
   *     Return true and node_list_p is empty
   *
   */
  bool CollectNewNodesSinceLastSnapshot(
    const BaseNode *old_node_p,
    const BaseNode *new_node_p,
    std::vector<const BaseNode *> *node_list_p) const {
    // We only call this function if CAS fails, so these two pointers
    // must be different
    assert(new_node_p != old_node_p);

    // Return true means the entire delta chain has changed
    if(new_node_p == nullptr) {
      bwt_printf("The NodeID has been released permanently\n");

      return true;
    }

    while(1) {
      if(new_node_p != old_node_p) {
        node_list_p->push_back(new_node_p);
      } else {
        bwt_printf("Find common pointer! Delta chain append.\n")

        // We have found two equivalent pointers
        // which implies the delta chain is only prolonged
        // but not consolidated or removed
        return false;
      }

      if(!new_node_p->IsDeltaNode()) {
        bwt_printf("Did not find common pointer! Delta chian consolidated\n");

        // If we have reached the bottom and still do not
        // see equivalent pointers then the entire delta
        // chain has been consolidated
        return true;
      }

      // Try next one
      const DeltaNode *delta_node_p = \
        static_cast<const DeltaNode *>(new_node_p);

      new_node_p = delta_node_p->child_node_p;
    }

    assert(false);
    return false;
  }

  /*
   * LocateSeparatorByKey() - Locate the child node for a key
   *
   * This functions works with any non-empty inner nodes. However
   * it fails assertion with empty inner node
   *
   * NOTE: This function takes a pointer that points to a new high key
   * if we have met a split delta node before reaching the base node.
   * The new high key is used to test against the search key
   *
   * NOTE 2: This function will hit assertion failure if the key
   * range is not correct OR the node ID is invalid
   */
  NodeID LocateSeparatorByKey(const KeyType &search_key,
                              const InnerNode *inner_node_p,
                              const KeyType *ubound_p) const {
    // If the upper bound is not set because we have not met a
    // split delta node, then just set to the current upperbound
    if(ubound_p == nullptr) {
      ubound_p = &inner_node_p->ubound;
    }

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

    // This assertion failure could only happen if we
    // hit +Inf as separator
    assert(iter1->node != INVALID_NODE_ID);

    // If search key >= upper bound (natural or artificial) then
    // we have hit the wrong inner node
    assert(KeyCmpLess(search_key, *ubound_p));
    // Search key must be greater than or equal to the lower bound
    // which is assumed to be a constant associated with a NodeID
    assert(KeyCmpGreaterEqual(search_key, inner_node_p->lbound));

    return iter1->node;
  }

  /*
   * LocateLeftSiblingByKey() - Locate the left sibling given a key
   *
   * If the search key matches the left most separator then return
   * INVALID_NODE_ID to imply the left sibling is in another node of
   * the same level. For searching removed node's left sibling this
   * should be treated as an error; but if we are searching for the
   * real left sibling then it is a signal for us to jump to the left
   * sibling of the current node to search again
   *
   * If the key is not in the right range of the inner node (this is possible
   * if the key was once included in the node, but then node splited)
   * then assertion fail. This condition should be checked before calling
   * this routine
   */
  NodeID LocateLeftSiblingByKey(const KeyType &search_key,
                                const LogicalInnerNode *logical_inner_p)
    const {
    // This could happen when the inner node splits
    if(KeyCmpGreaterEqual(search_key, *logical_inner_p->ubound_p)) {
      bwt_printf("ERROR: Search key >= inner node upper bound!\n");

      assert(false);
    }

    // This is definitely an error no matter what happened
    if(KeyCmpLess(search_key, *logical_inner_p->lbound_p)) {
      bwt_printf("ERROR: Search key < inner node lower bound!\n");

      assert(false);
    }

    size_t node_size = logical_inner_p->key_value_map.size();

    if(node_size == 0) {
      bwt_printf("Logical inner node is empty\n");

      assert(false);
    } else if(node_size == 1) {
      bwt_printf("There is only 1 entry, implying jumping left\n");
      // We already guaranteed the key is in the range of the node
      // So as long as the node is consistent (bounds and actual seps)
      // then the key must match this sep
      // So we need to go left
      return INVALID_NODE_ID;
    }

    // From this line we know the number of seps is >= 2

    // Initialize two iterators from an ordered map
    // We are guaranteed that it1 and it2 are not null
    auto it1 = logical_inner_p->key_value_map.begin();

    // NOTE: std::map iterators are only bidirectional, which means it does
    // not support random + operation. Must use std::move or std::advance
    auto it2 = std::next(it1, 1);
    // Since we only restrict # of elements to be >= 2, it3 could be end()
    auto it3 = std::next(it2, 1);

    // We should not match first entry since there is no direct left
    // and in the situation of remove, we always do not remove
    // the left most child in any node since this would change
    // the lower bound of that node which is guaranteed not to change
    if(KeyCmpGreaterEqual(search_key, it1->first) && \
       KeyCmpLess(search_key, it2->first)) {
      bwt_printf("First entry is matched. Implying jumping left\n");

      return INVALID_NODE_ID;
    }

    while(1) {
      // If we already reached the last sep-ID pair
      // then only test the last sep
      if(it3 == logical_inner_p->key_value_map.end()) {
        if(KeyCmpGreaterEqual(search_key, it2->first)) {
          return it1->second;
        } else {
          // This should not happen since we already tested it
          // by upper bound
          assert(false);
        }
      }

      // If the second iterator matches the key (tested also with it3)
      // then we return the first iterator's node ID
      if(KeyCmpLess(search_key, it3->first) && \
         KeyCmpGreaterEqual(search_key, it2->first)) {
        return it1->second;
      }

      // This points to the left sib
      it1++;
      // This points to current node under testing
      it2++;
      // This points to next node as upperbound for current node
      it3++;
    }

    // After this line we already know that for every sep
    // the key is not matched

    bwt_printf("ERROR: Left sibling not found; Please check key before entering\n");

    // Loop will not break to here
    assert(false);
    return INVALID_NODE_ID;
  }

  /*
   * NavigateInnerNode() - Traverse down through the inner node delta chain
   *                       and probably horizontally to right sibling nodes
   *
   * This function does not have to always reach the base node in order to
   * find the target since we know for inner nodes it is always single key
   * single node mapping. Therefore there is neither need to keep a delta
   * pointer list to recover full key-value mapping, nor returning a base node
   * pointer to test low key and high key.
   *
   * However, if we have reached a base node, for debugging purposes we
   * need to test current search key against low key and high key
   *
   * NOTE: This function returns a NodeID, instead of NodeSnapshot since
   * its behaviour is not dependent on the actual content of the physical
   * pointer associated with the node ID, so we could choose to fix the
   * snapshot later
   *
   * NOTE: This function will jump to a sibling if the current node is on
   * a half split state. If this happens, then the flag inside snapshot_p
   * is set to true, and also the corresponding NodeId and BaseNode *
   * will be updated to reflect the newest sibling ID and pointer.
   * After returnrning of this function please remember to check the flag
   * and update path history.
   */
  NodeID NavigateInnerNode(const KeyType &search_key,
                           NodeSnapshot *snapshot_p) const {
    // Make sure the structure is valid
    assert(snapshot_p->is_split_sibling == false);
    assert(snapshot_p->is_leaf == false);
    assert(snapshot_p->node_p != nullptr);
    assert(snapshot_p->node_id != INVALID_NODE_ID);

    bool first_time = true;

    // Save some keystrokes
    const BaseNode *node_p = snapshot_p->node_p;

    // We track current artificial high key brought about by split node
    const KeyType *ubound_p = nullptr;

    while(1) {
      NodeType type = node_p->GetType();

      switch(type) {
        case NodeType::InnerType: {
          const InnerNode *inner_node_p = \
            static_cast<const InnerNode *>(node_p);

          NodeID target_id = \
            LocateSeparatorByKey(search_key, inner_node_p, ubound_p);

          bwt_printf("Found an inner node ID = %lu\n", target_id);

          return target_id;
        } // case InnerType
        case NodeType::InnerRemoveType: {
          assert(first_time == true);

          // TODO: Fix this to let it deal with remove node
          assert(false);
        } // case InnerRemoveType
        case NodeType::InnerInsertType: {
          const InnerInsertNode *insert_node_p = \
            static_cast<const InnerInsertNode *>(node_p);

          const KeyType &insert_low_key = insert_node_p->insert_key;
          const KeyType &insert_high_key = insert_node_p->next_key;
          NodeID target_id = insert_node_p->new_node_id;

          if(KeyCmpGreaterEqual(search_key, insert_low_key) && \
             KeyCmpLess(search_key, insert_high_key)) {
            bwt_printf("Find target ID = %lu in insert delta\n", target_id);

            return target_id;
          }

          node_p = insert_node_p->child_node_p;

          break;
        } // InnerInsertType
        case NodeType::InnerDeleteType: {
          const InnerDeleteNode *delete_node_p = \
            static_cast<const InnerDeleteNode *>(node_p);

          // For inner delete node, we record its left and right sep
          // as a fast path
          // The node ID stored inside inner delete node is the NodeID
          // of its left sibling before deletion
          const KeyType &delete_low_key = delete_node_p->prev_key;
          const KeyType &delete_high_key = delete_node_p->next_key;
          NodeID target_id = delete_node_p->prev_node_id;

          if(KeyCmpGreaterEqual(search_key, delete_low_key) && \
             KeyCmpLess(search_key, delete_high_key)) {
            bwt_printf("Find target ID = %lu in delete delta\n", target_id);

            return target_id;
          }

          node_p = delete_node_p->child_node_p;

          break;
        } // InnerDeleteType
        case NodeType::InnerSplitType: {
          const InnerSplitNode *split_node_p = \
            static_cast<const InnerSplitNode *>(node_p);

          const KeyType &split_key = split_node_p->split_key;
          // If current key is on the new node side,
          // we need to update tree snapshot to reflect the fact that we have
          // traversed to a new NodeID
          if(KeyCmpGreaterEqual(search_key, split_key)) {
            bwt_printf("Go to split branch\n");

            NodeID branch_id = split_node_p->split_sibling;
            // SERIALIZATION POINT!
            const BaseNode *branch_node_p = GetNode(branch_id);

            snapshot_p->node_id = branch_id;
            snapshot_p->node_p = branch_node_p;

            // Set this to true means we have already traversed through a
            // side sibling pointer because of a half split state
            snapshot_p->is_split_sibling = true;

            // Since we have jumped to a new NodeID, we could see a remove node
            first_time = true;

            node_p = branch_node_p;

            // Continue in the while loop to avoid setting first_time to false
            continue;
          } else {
            // If we do not take the branch, then the high key has changed
            // since the splited half takes some keys from the logical node
            // downside
            ubound_p = &split_key;

            node_p = split_node_p->child_node_p;
          }

          break;
        } // case InnerSplitType
        case NodeType::InnerMergeType: {
          const InnerMergeNode *merge_node_p = \
            static_cast<const InnerMergeNode *>(node_p);

          const KeyType &merge_key = merge_node_p->merge_key;

          if(KeyCmpGreaterEqual(search_key, merge_key)) {
            node_p = merge_node_p->right_merge_p;
          } else {
            node_p = merge_node_p->child_node_p;
          }

          break;
        } // InnerMergeType
        default: {
          bwt_printf("ERROR: Unknown node type = %d", type);

          assert(false);
        }
      } // switch type

      first_time = false;
    } // while 1

    // Should not reach here
    assert(false);
    return INVALID_NODE_ID;
  }

  /*
   * CollectAllSepsOnInner() - Collect all separators given a snapshot
   *
   * This function is just a wrapper for the recursive version, and the
   * difference is that this wrapper accepts snapshot pointer, and also
   * it collects all metadata (low key, high key and next node id)
   *
   * After this function returns, it is guaranteed that has_data flag
   * inside snapshot object is set to true
   */
  void CollectAllSepsOnInner(NodeSnapshot *snapshot_p) {
    CollectAllSepsOnInnerRecursive(snapshot_p->node_p,
                                   snapshot_p->logical_node_p,
                                   true,
                                   true,
                                   true);

    snapshot_p->has_data = true;

    return;
  }

  /*
   * CollectMetadataOnInner() - Collect metadata on a logical inner node
   *
   * This function collects high key, low key and next node ID for an inner
   * node and stores it in the logical node
   *
   * After this function returns, it is guarantted that has_data flag is set
   * to false
   */
  void CollectMetadataOnInner(NodeSnapshot *snapshot_p) {
    CollectAllSepsOnInnerRecursive(snapshot_p->node_p,
                                   snapshot_p->logical_node_p,
                                   true,
                                   true,
                                   false);

    snapshot_p->has_data = false;

    return;
  }

  /*
   * CollectAllSpesOnInnerRecursive() - This is the counterpart on inner node
   *
   * Please refer to the function on leaf node for details. These two have
   * almost the same logical flow
   */
  void
  CollectAllSepsOnInnerRecursive(const BaseNode *node_p,
                                 LogicalInnerNode *logical_node_p,
                                 bool collect_lbound,
                                 bool collect_ubound,
                                 bool collect_sep) const {
    // Validate remove node, if any
    bool first_time = true;

    // Used to restrict the upper bound in a local branch
    // If we are collecting upper bound, then this will finally
    // be assign to the logical node
    const KeyType *ubound_p = nullptr;

    while(1) {
      NodeType type = node_p->GetType();

      switch(type) {
        case NodeType::InnerType: {
          const InnerNode *inner_node_p = \
            static_cast<const InnerNode *>(node_p);

          // If the caller cares about the actual content
          if(collect_sep) {
            for(const SepItem &item : inner_node_p->sep_list) {
              // Since we are using INVALID_NODE_ID to mark deleted nodes
              // we check for other normal node id to avoid accidently
              // having a bug and deleting random keys
              assert(item.node != INVALID_NODE_ID);

              // If we observed an out of range key (brought about by split)
              // just ignore it (>= high key, if exists a high key)
              if(ubound_p != nullptr && \
                 KeyCmpGreaterEqual(item.key, *ubound_p)) {
                bwt_printf("Detected a out of range key in inner base node\n");

                continue;
              }

              // If the sep key has already been collected, then ignore
              logical_node_p->key_value_map.insert( \
                typename decltype(logical_node_p->key_value_map)::value_type( \
                  item.key, item.node));
            } // for item : sep_list
          } // if collect_sep

          // If we collect low key (i.e. on the left most branch
          // of a merge tree)
          if(collect_lbound == true) {
            assert(logical_node_p->lbound_p == nullptr);

            logical_node_p->lbound_p = &inner_node_p->lbound;
          }

          // A base node also defines the high key
          if(ubound_p == nullptr) {
            ubound_p = &inner_node_p->ubound;
          }

          // If we clooect high key, then it is set to the local branch
          if(collect_ubound == true) {
            assert(logical_node_p->ubound_p == nullptr);

            logical_node_p->ubound_p = ubound_p;
          }

          // If it is the rightmost node, and we have not seen
          // a split node that would change its next node ID
          if(collect_ubound == true && \
             logical_node_p->next_node_id == INVALID_NODE_ID) {
            logical_node_p->next_node_id = inner_node_p->next_node_id;
          }

          // Remove all keys with INVALID_NODE_ID as target node id
          // since they should have been deleted, and we just put them
          // into the map to block any further key operation
          for(auto &item : logical_node_p->key_value_map) {
            if(item.second == INVALID_NODE_ID) {
              logical_node_p->key_value_map.erase(item.first);
            }
          }

          return;
        } // case InnerType
        case NodeType::InnerRemoveType: {
          bwt_printf("ERROR: Observed an inner remove node\n");

          assert(first_time == true);
          assert(false);
          return;
        } // case InnerRemoveType
        case NodeType::InnerInsertType: {
          const InnerInsertNode *insert_node_p = \
            static_cast<const InnerInsertNode *>(node_p);

          const KeyType &insert_key = insert_node_p->insert_key;
          assert(insert_node_p->new_node_id != INVALID_NODE_ID);

          if(collect_sep == true) {
            // If there is a ubound and the key is inside the bound, or
            // there is no bound
            if((ubound_p != nullptr && \
                KeyCmpLess(insert_key, *ubound_p)) || \
                ubound_p == nullptr) {
              // This will insert if key does not exist yet
              logical_node_p->key_value_map.insert( \
                typename decltype(logical_node_p->key_value_map)::value_type( \
                  insert_key, insert_node_p->new_node_id));
            }
          }

          // Go to next node
          node_p = insert_node_p->child_node_p;

          break;
        } // case InnerInsertType
        case NodeType::InnerDeleteType: {
          const InnerDeleteNode *delete_node_p = \
            static_cast<const InnerDeleteNode *>(node_p);

          // For deleted keys, we first add it with INVALID ID
          // to block all further updates
          // In the last stage we just remove all keys with
          // INVALID_NODE_ID as node ID
          const KeyType &delete_key = delete_node_p->delete_key;

          if(collect_sep == true) {
            // Only delete key if it is in range (not in a split node)
            if((ubound_p != nullptr && \
                KeyCmpLess(delete_key, *ubound_p)) || \
                ubound_p == nullptr) {
              bwt_printf("Key deleted!\n");

              logical_node_p->key_value_map.insert( \
                typename decltype(logical_node_p->key_value_map)::value_type( \
                  delete_node_p->delete_key,
                  INVALID_NODE_ID));
            }
          }

          node_p = delete_node_p->child_node_p;

          break;
        } // case InnerDeleteType
        case NodeType::InnerSplitType: {
          const InnerSplitNode *split_node_p = \
            static_cast<const InnerSplitNode *>(node_p);

          // If the high key has not been set yet, just set it
          if(ubound_p == nullptr) {
            bwt_printf("Updating high key with split node\n");

            ubound_p = &split_node_p->split_key;
          }

          // If we are the right most branch, then also update next ID
          if(collect_ubound == true && \
             logical_node_p->next_node_id == INVALID_NODE_ID) {
            logical_node_p->next_node_id = split_node_p->split_sibling;
          }

          node_p = split_node_p->child_node_p;

          break;
        } // case InnerSplitType
        case NodeType::InnerMergeType: {
          const InnerMergeNode *merge_node_p = \
            static_cast<const InnerMergeNode *>(node_p);

          // Use different flags to collect on left and right branch
          CollectAllSepsOnInnerRecursive(merge_node_p->child_node_p,
                                         logical_node_p,
                                         collect_lbound, // Take care!
                                         false,
                                         collect_sep);

          CollectAllSepsOnInnerRecursive(merge_node_p->right_merge_p,
                                         logical_node_p,
                                         false,
                                         collect_ubound, // Take care!
                                         collect_sep);

          // There is no unvisited node
          return;
        } // case InnerMergeType
        default: {
          bwt_printf("ERROR: Unknown inner node type\n");

          return;
        }
      } // switch type

      first_time = false;
    } // while(1)

    // Should not get to here
    assert(false);
    return;
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
                            std::vector<NodeSnapshot> *path_list_p) const {
    *current_node_pp = GetNode(new_id);
    *current_node_type_p = (*current_node_pp)->GetType();

    *current_head_node_pp = *current_node_pp;
    *current_head_node_type_p = *current_node_type_p;

    // TODO: Save a snapshot object
    // Save history for the new ID and new node pointer
    //path_list_p->push_back(std::make_pair(new_id, *current_node_pp));

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
                             std::vector<NodeSnapshot> *path_list_p,
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
          LocateSeparatorByKey(search_key, inner_node_p, nullptr);

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
   * NavigateLeafNode() - Find search key on a logical leaf node
   *
   * This function correctly deals with merge and split, starting on
   * the topmost node of a delta chain
   *
   * It pushes pointers of nodes into a vector, and stops at the leaf node.
   * After that it bulk loads the leaf page's data item of the search key
   * into logical leaf node, and then replay log
   *
   * In order to reflect the fact that we might jump to a split sibling using
   * NodeID due to a half split state, the function will modify snapshot_p's
   * NodeID and BaseNode pointer if this happens, and furthermore it sets
   * is_sibling flag to true to notify the caller that path history needs to
   * be updated
   *
   * This function is read-only, so it does not need to validate any structure
   * change.
   */
  void NavigateLeafNode(const KeyType &search_key,
                        NodeSnapshot *snapshot_p) const {
    assert(snapshot_p->is_leaf == true);
    assert(snapshot_p->is_split_sibling == false);
    assert(snapshot_p->node_p != nullptr);
    assert(snapshot_p->logical_node_p != nullptr);
    assert(snapshot_p->node_id != INVALID_NODE_ID);
    assert(snapshot_p->has_data == false);

    const BaseNode *node_p = snapshot_p->node_p;

    // Since upperbound could be updated by split delta, we just
    // record the newest ubound using a pointer; If this is nullptr
    // at leaf page then just use the ubound in leaf page
    const KeyType *ubound_p = nullptr;
    const KeyType *lbound_p = nullptr;

    // This is used to test whether a remove node is valid
    // since it could only be the first node on a delta chain
    bool first_time = true;

    LogicalLeafNode *logical_node_p = \
      static_cast<LogicalLeafNode *>(snapshot_p->logical_node_p);

    while(1) {
      NodeType type = node_p->GetType();
      switch(type) {
        case NodeType::LeafType: {
          const LeafNode *leaf_node_p = \
            static_cast<const LeafNode *>(node_p);

          if(lbound_p == nullptr) {
            lbound_p = &leaf_node_p->lbound;
          }

          if(ubound_p == nullptr) {
            ubound_p = &leaf_node_p->ubound;
          }

          // Even if we have seen merge and split this always hold
          // since merge and split would direct to the correct page by sep key
          assert(KeyCmpGreaterEqual(search_key, *lbound_p) && \
                 KeyCmpLess(search_key, *ubound_p));

          // First bulk load data item for the search key, if exists
          for(DataItem &item : leaf_node_p->data_list) {
            if(KeyCmpEqual(item.key, search_key)) {
              logical_node_p->BulkLoadValue(item);

              break;
            }
          }

          // Then replay log
          logical_node_p->ReplayLog();

          return;
        }
        case NodeType::LeafInsertType: {
          const LeafInsertNode *insert_node_p = \
            static_cast<const LeafInsertNode *>(node_p);

          // If there is a match then push the delta
          if(KeyCmpEqual(search_key, insert_node_p->insert_key)) {
            bwt_printf("Push insert delta\n");

            logical_node_p->pointer_list.push_back(node_p);
          }

          node_p = insert_node_p->child_node_p;

          break;
        } // case LeafInsertType
        case NodeType::LeafDeleteType: {
          const LeafDeleteNode *delete_node_p = \
            static_cast<const LeafDeleteNode *>(node_p);

          if(KeyCmpEqual(search_key, delete_node_p->delete_key)) {
            bwt_printf("Push delete delta\n");

            logical_node_p->pointer_list.push_back(node_p);
          }

          node_p = delete_node_p->child_node_p;

          break;
        } // case LeafDeleteType
        case NodeType::LeafRemoveType: {
          bwt_printf("ERROR: Observed LeafRemoveNode in delta chain\n");

          assert(first_time == true);
          assert(false);
        } // case LeafRemoveType
        case NodeType::LeafMergeType: {
          bwt_printf("Observed a merge node on leaf delta chain\n");

          const LeafMergeNode *merge_node_p = \
            static_cast<const LeafMergeNode *>(node_p);

          // Decide which side we should choose
          // Using >= for separator key
          if(KeyCmpGreaterEqual(search_key, merge_node_p->merge_key)) {
            bwt_printf("Take leaf merge right branch\n");

            node_p = merge_node_p->right_merge_p;
          } else {
            bwt_printf("Take leaf merge left branch\n");

            node_p = merge_node_p->child_node_p;
          }

          break;
        } // case LeafMergeType
        case NodeType::LeafSplitType: {
          bwt_printf("Observed a split node on leaf delta chain\n");

          const LeafSplitNode *split_node_p = \
            static_cast<const LeafSplitNode *>(node_p);

          if(KeyCmpGreaterEqual(search_key, split_node_p->split_key)) {
            bwt_printf("Take leaf split right (NodeID branch)\n");

            NodeID split_sibling_id = split_node_p->split_sibling;
            // SERIALIZATION POINT!
            node_p = GetNode(split_sibling_id);

            // Update current NodeID and pointer
            // and also notify caller that the NodeId has changed
            snapshot_p->node_p = node_p;
            snapshot_p->node_id = split_sibling_id;
            snapshot_p->is_split_sibling = true;

            // Since we are on the branch side of a split node
            // there should not be any record with search key in
            // the chain from where we come since otherwise these
            // records are misplaced
            assert(logical_node_p->pointer_list.size() == 0);

            // Since we have switched to a new NodeID
            first_time = true;

            // Avoid setting first_time = false at the end
            continue;
          } else {
            // Since we follow the child physical pointer, it is necessary
            // to update upper bound to have a better bounds checking
            if(ubound_p == nullptr) {
              ubound_p = &split_node_p->split_key;
            }

            node_p = split_node_p->child_node_p;
          }

          break;
        } // case LeafSplitType
        default: {
          bwt_printf("ERROR: Unknown leaf delta node type: %d\n",
                     node_p->GetType());

          assert(false);
        } // default
      } // switch

      // After first loop most nodes will set this to false
      // If we continue inside the switch then we will not reach this
      // line
      first_time = false;
    } // while

    // We cannot reach here
    assert(false);
    return;
  }

  /*
   * CollectAllValuesOnLeafRecursive() - Collect all values given a
   *                                     pointer recursively
   *
   * It does not need NodeID to collect values since only read-only
   * routine calls this one, so no validation is ever needed even in
   * its caller.
   *
   * This function only travels using physical pointer, which implies
   * that it does not deal with LeafSplitNode and LeafRemoveNode
   * For LeafSplitNode it only collects value on child node
   * For LeafRemoveNode it fails assertion
   * If LeafRemoveNode is not the topmost node it also fails assertion
   *
   * NOTE: This function calls itself to collect values in a merge node
   * since logically speaking merge node consists of two delta chains
   * DO NOT CALL THIS DIRECTLY - Always use the wrapper (the one without
   * "Recursive" suffix)
   *
   * NOTE 2: This function tracks the upperbound of a logical
   * page using split node and merge node, since logically they are both
   * considered to be overwriting the uppberbound. All keys larger than
   * the upperbound will not be collected, since during a node split,
   * it is possible that there are obsolete keys left in the base leaf
   * page.
   *
   * NOTE 3: This function only collects delta node pointers, and
   * arrange key-values pairs ONLY in base page. What should be done in
   * the wrapper is to replay all deltas onto key_value_set_p
   * For merge delta nodes, it would serialize delta updates to its
   * two children but that does not matter, since delta updates in
   * the two branches do not have any key in common (if we do it correctly)
   *
   * NOTE 4: Despite the seemingly obsecure naming, this function actually
   * has an option that serves as an optimization when we only want metadata
   * (e.g. ubound, lbound, next ID) but not actual value. This saves some
   * computing resource. We need metadata only e.g. when we are going to verify
   * whether the upperbound of a page matches the lower bound of a given page
   * to confirm that we have found the left sibling of a node
   */
  void
  CollectAllValuesOnLeafRecursive(const BaseNode *node_p,
                                  LogicalLeafNode *logical_node_p,
                                  bool collect_lbound,
                                  bool collect_ubound,
                                  bool collect_value) const {
    bool first_time = true;
    // This is the high key for local branch
    // At the end of the loop if we are collecting high key then
    // this value will be set into logical node as its high key
    const KeyType *ubound_p = nullptr;

    while(1) {
      NodeType type = node_p->GetType();

      switch(type) {
        // When we see a leaf node, just copy all keys together with
        // all values into the value set
        case NodeType::LeafType: {
          const LeafNode *leaf_base_p = \
            static_cast<const LeafNode *>(node_p);

          // If we collect values into logical node pointer
          if(collect_value == true) {
            for(auto &data_item : leaf_base_p->data_list) {
              // If we find a key in the leaf page which is >= the latest
              // separator key of a split node (if there is one) then ignore
              // these key since they have been now stored in another leaf
              if(ubound_p != nullptr && \
                 KeyCmpGreaterEqual(data_item.key, *ubound_p)) {
                bwt_printf("Obsolete key has been detected\n");

                continue;
              }

              // Load all values in the vector using the given key
              logical_node_p->BulkLoadValue(data_item);
            } // for auto it : data_item ...
          } // if collect value == true

          // Then try to fill in ubound and lbound
          if(collect_lbound == true) {
            // Since lbound should not be changed by delta nodes
            // it must be not set by any other nodes
            assert(logical_node_p->lbound_p == nullptr);

            logical_node_p->lbound_p = &leaf_base_p->lbound;
          }

          // fill in next node id if it has not been changed by
          // some split delta way down the delta chain
          if(logical_node_p->next_node_id == INVALID_NODE_ID && \
             collect_ubound == true) {
            // This logically updates the next node pointer for a
            // logical node
            logical_node_p->next_node_id = leaf_base_p->next_node_id;
          }

          // We set ubound_p here to avoid having to compare keys above
          if(ubound_p == nullptr) {
            ubound_p = &leaf_base_p->ubound;
          }

          // If we collect high key, then local high key is set to be
          // logical node's high key
          if(collect_ubound == true) {
            logical_node_p->ubound_p = ubound_p;
          }

          // After setting up all bounds, replay the log with bounds
          // checking
          logical_node_p->ReplayLog();

          return;
        } // case LeafType
        case NodeType::LeafInsertType: {
          const LeafInsertNode *insert_node_p = \
            static_cast<const LeafInsertNode *>(node_p);

          // Only collect split delta if its key is in
          // the range
          if(ubound_p != nullptr && \
             KeyCmpGreaterEqual(insert_node_p->insert_key,
                                *ubound_p)) {
            bwt_printf("Insert key not in range (>= high key)\n");
          } else {
            logical_node_p->pointer_list.push_back(node_p);
          }

          node_p = insert_node_p->child_node_p;

          break;
        } // case LeafInsertType
        case NodeType::LeafDeleteType: {
          const LeafDeleteNode *delete_node_p = \
            static_cast<const LeafDeleteNode *>(node_p);

          // Only collect delete delta if it is in the range
          // i.e. < newest high key, since otherwise it will
          // be in splited nodes
          if(ubound_p != nullptr && \
             KeyCmpGreaterEqual(delete_node_p->delete_key,
                                *ubound_p)) {
            bwt_printf("Delete key not in range (>= high key)\n");
          } else {
            logical_node_p->pointer_list.push_back(node_p);
          }

          node_p = delete_node_p->child_node_p;

          break;
        } // case LeafDeleteType
        case NodeType::LeafRemoveType: {
          bwt_printf("ERROR: LeafRemoveNode not allowed\n");

          // If we see a remove node, then this node is removed
          // and in that case we just return silently
          assert(first_time == true);

          // These two are trivial but just put them here for safety
          assert(logical_node_p->key_value_set.size() == 0);
          assert(logical_node_p->pointer_list.size() == 0);

          // Fail here
          assert(false);
        } // case LeafRemoveType
        case NodeType::LeafSplitType: {
          const LeafSplitNode *split_node_p = \
            static_cast<const LeafSplitNode *>(node_p);

          // If we have not seen a split node, then this is the first one
          // and we need to remember the upperbound for the logical page
          // Since this is the latest change to its upperbound
          if(ubound_p == nullptr) {
            ubound_p = &split_node_p->split_key;
          }

          // Must test collect_ubound since we only collect
          // next node id for the right most node
          if(collect_ubound == true && \
             logical_node_p->next_node_id == INVALID_NODE_ID) {
            // This logically updates the next node pointer for a
            // logical node
            logical_node_p->next_node_id = split_node_p->split_sibling;
          }

          node_p = split_node_p->child_node_p;

          break;
        } // case LeafSplitType
        case NodeType::LeafMergeType: {
          bwt_printf("Observe LeafMergeNode; recursively collect nodes\n");

          const LeafMergeNode *merge_node_p = \
            static_cast<const LeafMergeNode *>(node_p);

          /**** RECURSIVE CALL ON LEFT AND RIGHT SUB-TREE ****/
          CollectAllValuesOnLeafRecursive(merge_node_p->child_node_p,
                                          logical_node_p,
                                          collect_lbound,
                                          false,  // Always not collect ubound
                                          collect_value);

          CollectAllValuesOnLeafRecursive(merge_node_p->right_merge_p,
                                          logical_node_p,
                                          false, // Always not collect lbound
                                          collect_ubound,
                                          collect_value);

          return;
        } // case LeafMergeType
        default: {
          bwt_printf("ERROR: Unknown node type: %d\n", type);

          assert(false);
        } // default
      }

      first_time = false;
    }

    return;
  }

  /*
   * CollectAllValuesOnLeaf() - Consolidate delta chain for a single logical
   *                            leaf node
   *
   * This function is the non-recursive wrapper of the resursive core function.
   * It calls the recursive version to collect all base leaf nodes, and then
   * it replays delta records on top of them.
   */
  void
  CollectAllValuesOnLeaf(NodeSnapshot *snapshot_p) {
    assert(snapshot_p->is_leaf == true);
    assert(snapshot_p->is_split_sibling == false);
    assert(snapshot_p->logical_node_p != nullptr);

    // We want to collect both ubound and lbound in this call
    // These two flags will be set to false for every node
    // that is neither a left not right most node
    CollectAllValuesOnLeafRecursive(snapshot_p->node_p,
                                    (LogicalLeafNode *)snapshot_p->logical_node_p,
                                    true,
                                    true,
                                    true);  // collect data

    snapshot_p->has_data = true;

    return;
  }


  /*
   * CollectMetadataOnLeaf() - Collect high key, low key and next node ID
   *
   * This function will not collect actual data, but only metadata including
   * high key, low key and next node ID in a logical leaf node.
   *
   * After this function returns snapshot has its has_data set to false
   */
  void
  CollectMetadataOnLeaf(NodeSnapshot *snapshot_p) {
    assert(snapshot_p->is_leaf == true);
    assert(snapshot_p->is_split_sibling == false);
    assert(snapshot_p->logical_node_p != nullptr);

    // We want to collect both ubound and lbound in this call
    // These two flags will be set to false for every node
    // that is neither a left not right most node
    CollectAllValuesOnLeafRecursive(snapshot_p->node_p,
                                    (LogicalLeafNode *)snapshot_p->logical_node_p,
                                    true,
                                    true,
                                    false); // Do not collect data

    snapshot_p->has_data = false;

    return;
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

    // Path history
    std::vector<NodeSnapshot> ph{};
    KeyType search_key = GetWrappedKey(raw_key);

    TraverseDownInnerNode(search_key, &ph);
    assert(ph.size() > 1UL);

    // Since it returns on seeing a leaf delta chain head
    // We use reference here to avoid copy
    const NodeSnapshot &ts = ph.back();
    NodeID leaf_head_id = ts.node_id;
    const BaseNode *leaf_head_p = ts.node_p;

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
  * Debug function definition
  */
 public:

  /*
   * DebugGetInnerNode() - Debug routine. Return inner node given two
   *                       lists of keys and values
   *
   * This is only used for debugging externally - please do not call
   * this function in side the class
   *
   * NOTE: Since this function does not actually require "this"
   * pointer, we could make it as static. However, making it static might
   * cause complex grammar, so we make it a member function
   */
  InnerNode *DebugGetInnerNode(const RawKeyType &p_lbound,
                               const RawKeyType &p_ubound,
                               NodeID p_next_node_id,
                               std::vector<RawKeyType> raw_key_list,
                               std::vector<NodeID> node_id_list) {
    InnerNode *temp_node_p = \
      new InnerNode{p_lbound, p_ubound, p_next_node_id};

    assert(raw_key_list.size() == node_id_list.size());

    // Push item into the node 1 by 1
    for(int i = 0;i < raw_key_list.size();i++) {
      SepItem item{KeyType{raw_key_list[i]}, node_id_list[i]};

      temp_node_p->sep_list.push_back(item);
    }

    return temp_node_p;
  }

  /*
   * DebugGetLeafNode() - Return a leaf node given a list of keys
   *                      and a list of list of values
   *
   * This function is only used for debugging.
   */
  LeafNode *DebugGetLeafNode(const RawKeyType &p_lbound,
                             const RawKeyType &p_ubound,
                             NodeID p_next_node_id,
                             std::vector<RawKeyType> raw_key_list,
                             std::vector<std::vector<ValueType>> value_list_list) {
    LeafNode *temp_node_p = \
      new LeafNode{p_lbound, p_ubound, p_next_node_id};

    assert(raw_key_list.size() == value_list_list.size());

    for(int i = 0;i < raw_key_list.size();i++) {
      // Construct the data item using a key and a list of values
      DataItem item{KeyType{raw_key_list[i]},
                    value_list_list[i]};

      temp_node_p->data_list.push_back(item);
    }

    return temp_node_p;
  }

};

}  // End index namespace
}  // End peloton namespace


