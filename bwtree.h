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
  using RawKeyValueSet = std::map<RawKeyType, ValueSet, KeyComparator>;
  using KeyValueSet = std::map<KeyType, ValueSet, WrappedKeyComparator>;
  using NodePointerList = std::vector<BaseNode *>;
  using ConstNodePointerList = std::vector<const BaseNode *>;

  // This is used to hold single key and single value ordered mapping relation
  using KeySingleValueMap = std::map<KeyType, ValueType, WrappedKeyComparator>;

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
     * Constructor - Use a value vector to construct
     *
     * This method is mainly called for debugging purpose
     */
    DataItem(const KeyType &p_key, const std::vector<ValueType> &p_value_list) :
      key{p_key},
      value_list{p_value_list}
    {}

    /*
     * Constructor - Use a value set to construct
     *
     * It bulk loads the value vector with an unordered set's begin()
     * and end() iterator
     */
    DataItem(const KeyType &p_key, const ValueSet &p_value_set, bool) :
      key{p_key},
      value_list{p_value_set.begin(), p_value_set.end()}
    {}

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

    // Since merge delta also changes the upper bound of a
    // logical node, we need to record this as an update
    // to "upperbound" field of a logical page, and
    KeyType merge_ubound;

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
                  const KeyType &p_merge_ubound,
                  const BaseNode *p_right_merge_p,
                  int p_depth,
                  const BaseNode *p_child_node_p) :
      BaseNode{NodeType::LeafMergeType},
      merge_key{p_merge_key},
      merge_ubound{p_merge_ubound},
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
   * LogicalLeafNode() - A logical representation of a logical node
   *                     which is physically one or more chains of
   *                     delta node, SMO node and base nodes.
   *
   * This structure is usually used as a container to hold intermediate
   * results for a whole page operation. The caller fills in ID and pointer
   * field, while callee would fill the other fields
   */
  class LogicalLeafNode {
   public:
    // These fields are filled by callee
    KeyValueSet key_value_set;
    // These two needs to be null to indicate whether
    // they are valid or not
    const KeyType *ubound_p;
    const KeyType *lbound_p;
    NodeID next_node_id;

    // This is used to temporarily hold results, and should be empty
    // after all deltas has been applied
    ConstNodePointerList pointer_list;

    // Two components are usually provided by the caller
    TreeSnapshot snapshot;

    /*
     * Constructor - Initialize logical ID and physical pointer
     *               as the tree snapshot
     */
    LogicalLeafNode(TreeSnapshot p_snapshot) :
      key_value_set{},
      ubound_p{nullptr},
      lbound_p{nullptr},
      next_node_id{INVALID_NODE_ID},
      pointer_list{},
      snapshot{p_snapshot}
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
      LeafNode *leaf_node_p = new LeafNode(*lbound_p, *ubound_p, next_node_id);

      // The key is already ordered, we just need to check for value
      // emptiness
      for(auto &it : key_value_set) {
        if(it.second.size() == 0) {
          bwt_printf("Skip empty value set\n");

          continue;
        }

        // Construct a data item in-place
        // NOTE: The third parameter is just for disambiguity
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
  class LogicalInnerNode {
    KeySingleValueMap key_value_map;

    const KeyType *lbound_p;
    const KeyType *ubound_p;

    NodeID next_node_id;

    TreeSnapshot snapshot;

    /*
     * Constructor - Accept an initial starting point and init others
     */
    LogicalInnerNode(TreeSnapshot p_snapshot) :
      key_value_map{},
      lbound_p{nullptr},
      ubound_p{nullptr},
      next_node_id{INVALID_NODE_ID},
      snapshot{p_snapshot}
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
   * LocateSeparatorByKey() - Locate the child node for a key
   *
   * This functions works with any non-empty inner nodes. However
   * it fails assertion with empty inner node
   */
  NodeID LocateSeparatorByKey(const KeyType &search_key,
                              const InnerNode *inner_node_p) const {
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
   * LocateLeftSiblingByKey() - Locate the left sibling given a key
   *
   * If the left sibling is not found (i.e. this is already the left most
   * sep-key pair) then return INVALID_NODE_ID (Since we do not allow
   * removing the left most node for each inner node)
   *
   * If the key is not in the range of the inner node (this is possible
   * if the key was once included in the node, but then node splited)
   * then assertion fail. This condition should be checked before calling
   * this routine
   */
  NodeID LocateLeftSiblingByKey(const KeyType &search_key,
                                const LogicalInnerNode *logical_inner_p) {
    // This could happen at runtime, so make it an error report
    if(KeyCmpLess(search_key, *logical_inner_p->ubound_p)) {
      bwt_printf("ERROR: Search key >= inner node upper bound!\n");

      assert(false);
    }

    // This could not happen, so make it an assert
    assert(KeyCmpGreaterEqual(search_key, *logical_inner_p->lbound_p));

    if(logical_inner_p->key_value_map.size() < 2) {
      bwt_printf("ERROR: There is only %lu entry\n",
                 logical_inner_p->key_value_map.size());

      assert(false);
    }

    // Initialize two iterators from an ordered map
    // We are guaranteed that it1 and it2 are not null
    auto it1 = logical_inner_p->key_value_map.begin();
    auto it2 = it1 + 1;
    auto it3 = it2 + 1;

    if(KeyCmpGreaterEqual(search_key, it1->first) && \
       KeyCmpLess(search_key, it2->first)) {
      bwt_printf("ERROR: First entry is matched\n");

      assert(false);
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

      it1++;
      it2++;
      it3++;
    }

    // Loop will not break to here
    assert(false);
    return INVALID_NODE_ID;
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
          LocateSeparatorByKey(search_key, inner_node_p);

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
                                      ConstNodePointerList *pointer_list_p,
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

          // If there is a match then push the delta
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

              // We must keep using the same pointer to ensure consistency:
              // GetNode() actually serializes access to a node's physical pointer
              // so that in 1 atomic operation we could have only 1 such
              // serialization point
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
    ConstNodePointerList delta_node_list{};

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
  CollectAllValuesOnLeafRecursive(const BaseNode *leaf_node_p,
                                  LogicalLeafNode *logical_node_p,
                                  bool collect_lbound,
                                  bool collect_ubound,
                                  bool collect_value) const {
    bool first_time = true;

    while(1) {
      NodeType type = leaf_node_p->GetType();

      switch(type) {
        // When we see a leaf node, just copy all keys together with
        // all values into the value set
        case NodeType::LeafType: {
          const LeafNode *leaf_base_p = \
            static_cast<const LeafNode *>(leaf_node_p);

          // If we collect values into logical node pointer
          if(collect_value == true) {
            for(auto &data_item : leaf_base_p->data_list) {
              // If we find a key in the leaf page which is >= the latest
              // separator key of a split node (if there is one) then ignore
              // these key since they have been now stored in another leaf
              if(logical_node_p->ubound_p != nullptr && \
                 KeyCmpGreaterEqual(data_item.key, *logical_node_p->ubound_p)) {
                bwt_printf("Obsolete key has already been placed"
                           " into split sibling\n");

                continue;
              }

              // Load all values in the vector using the given key
              logical_node_p->BulkLoadValue(data_item);
            } // for auto it : data_item ...
          }

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

          // If we want to collect upperbound and also the ubound
          // has not been set by delta nodes then just set it here
          // as the leaf's ubound
          if(collect_ubound == true && \
             logical_node_p->ubound_p == nullptr) {
            logical_node_p->ubound_p = &leaf_base_p->ubound;
          }

          return;
        } // case LeafType
        case NodeType::LeafInsertType: {
          const LeafInsertNode *insert_node_p = \
            static_cast<const LeafInsertNode *>(leaf_node_p);

          logical_node_p->pointer_list.push_back(insert_node_p);

          leaf_node_p = insert_node_p->child_node_p;

          break;
        } // case LeafInsertType
        case NodeType::LeafDeleteType: {
          const LeafDeleteNode *delete_node_p = \
            static_cast<const LeafDeleteNode *>(leaf_node_p);

          logical_node_p->pointer_list.push_back(delete_node_p);

          leaf_node_p = delete_node_p->child_node_p;

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
            static_cast<const LeafSplitNode *>(leaf_node_p);

          // If we have not seen a split node, then this is the first one
          // and we need to remember the upperbound for the logical page
          // Since this is the latest change to its upperbound
          if(logical_node_p->ubound_p == nullptr && \
             collect_ubound == true) {
            logical_node_p->ubound_p = &split_node_p->split_key;
          }

          // Must test collect_ubound since we only collect
          // next node id for the right most node
          if(logical_node_p->next_node_id == INVALID_NODE_ID && \
             collect_ubound == true) {
            // This logically updates the next node pointer for a
            // logical node
            logical_node_p->next_node_id = split_node_p->split_sibling;
          }

          leaf_node_p = split_node_p->child_node_p;

          break;
        } // case LeafSplitType
        case NodeType::LeafMergeType: {
          bwt_printf("Observe LeafMergeNode; recursively collect nodes\n");

          const LeafMergeNode *merge_node_p = \
            static_cast<const LeafMergeNode *>(leaf_node_p);

          if(logical_node_p->ubound_p == nullptr && \
             collect_ubound == true) {
            logical_node_p->ubound_p = &merge_node_p->merge_ubound;
          }

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
  CollectAllValuesOnLeaf(LogicalLeafNode *logical_node_p) {

    // We want to collect both ubound and lbound in this call
    // These two flags will be set to false for every node
    // that is neither a left not right most node
    CollectAllValuesOnLeafRecursive(logical_node_p->snapshot.second,
                                    logical_node_p,
                                    true,
                                    true,
                                    true);

    // Apply delta changes to the base key value set
    logical_node_p->ReplayLog();

    return;
  }

  /*
   * CollectMetadataOnLeaf() - Collects next ID, lbound and ubound on leaf
   *
   * This function wraps the recursive version of value collector, and
   * it sets the flag to disable value collection
   */
  void CollectMetadataOnLeaf(LogicalLeafNode *logical_node_p) {
    // We set collect_value flag to false to indicate that we are
    // only interested in metadata itself
    CollectAllValuesOnLeafRecursive(logical_node_p->snapshot.second,
                                    logical_node_p,
                                    true,
                                    true,
                                    false);

    // Assume metadata are at least not empty, and there is no value
    assert(logical_node_p->lbound_p != nullptr);
    assert(logical_node_p->ubound_p != nullptr);
    assert(logical_node_p->key_value_set.size() == 0);

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


