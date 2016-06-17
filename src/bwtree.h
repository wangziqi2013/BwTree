//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bwtree.h
//
// Identification: src/index/bwtree.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
//
// NOTE: If you encounter any bug, assertion failure, segment fault or
// other anomalies, please contact:
//
// Ziqi Wang
// ziqiw a.t. andrew.cmu.edu
//
// in order to get a quick response and diagnosis
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
#include <stack>
#include <thread>
#include <chrono>

/*
 * BWTREE_PELOTON - Specifies whether Peloton-specific features are
 *                  Compiled or not
 *                  We strive to make BwTree a standalone and independent
 *                  module that can be plugged-and-played in any situation
 */
//#define BWTREE_PELOTON

// Used for debugging
#include <mutex>

#ifdef BWTREE_PELOTON
// This contains IndexMetadata definition
// It uses peloton::index namespace
#include "index/index.h"
#endif

/*
 * BWTREE_DEBUG - This flag enables assertions that check for
 *                structural consistency
 *                DO NOT RECOMMEND REMOVING
 */
#define BWTREE_DEBUG

/*
 * INTERACTIVE_DEBUG - This flag enables interactive debugger
 *                     which serializes CAS operation using a
 *                     lock, halting all threads before starting
 *                     the interactive debugger when assertion fails
 *                     RECOMMEND DEACTIVATING WHEN RELASING
 */
//#define INTERACTIVE_DEBUG

/*
 * ALL_PUBLIC - This flag makes all private members become public
 *              to simplify debugging
 */
#define ALL_PUBLIC

/*
 * BWTREE_TEMPLATE_ARGUMENTS - Save some key strokes
 */
#define BWTREE_TEMPLATE_ARGUMENTS template <typename KeyType, \
                                            typename ValueType, \
                                            typename KeyComparator, \
                                            typename KeyEqualityChecker, \
                                            typename ValueEqualityChecker, \
                                            typename ValueHashFunc>

#ifdef BWTREE_PELOTON
namespace peloton {
namespace index {
#endif

// This needs to be always here
static void dummy(const char*, ...) {}

#ifdef INTERACTIVE_DEBUG

#define idb_assert(cond)                                                \
  do {                                                                  \
    if (!(cond)) {                                                      \
      debug_stop_mutex.lock();                                          \
      fprintf(stderr, "assert, %-24s, line %d\n", __FUNCTION__, __LINE__); \
      idb.Start();                                                      \
      debug_stop_mutex.unlock();                                        \
    }                                                                   \
  } while (0);

#define idb_assert_key(node_id, key, context_p, cond)                   \
  do {                                                                  \
    if (!(cond)) {                                                      \
      debug_stop_mutex.lock();                                          \
      fprintf(stderr, "assert, %-24s, line %d\n", __FUNCTION__, __LINE__); \
      idb.key_list.push_back(key);                                      \
      idb.node_id_list.push_back(node_id);                              \
      idb.context_p = context_p;                                        \
      idb.Start();                                                      \
      debug_stop_mutex.unlock();                                        \
    }                                                                   \
  } while (0);

#else

#define idb_assert(cond) \
  do {                   \
    assert(cond);        \
  } while (0);

#define idb_assert_key(key, id, context_p, cond) \
  do {                                           \
    assert(cond);                                \
  } while (0);

#endif

#ifdef BWTREE_DEBUG

#define bwt_printf(fmt, ...)                              \
  do {                                                    \
    if(print_flag == false) break;                        \
    fprintf(stderr, "%-24s(%8lX): " fmt, __FUNCTION__, std::hash<std::thread::id>()(std::this_thread::get_id()), ##__VA_ARGS__); \
    fflush(stdout);                                       \
  } while (0);

#else

#define bwt_printf(fmt, ...)   \
  do {                         \
    dummy(fmt, ##__VA_ARGS__); \
  } while (0);

#endif

#ifdef INTERACTIVE_DEBUG
// In multi-threaded testing, if we want to halt all threads when an error
// happens
// then we lock this mutex
// Since every other thread will try to lock this before SMO
// it guarantees no SMO would happen before everybody stops
static std::mutex debug_stop_mutex;
#endif

/*
 * class DummyOutObject - Mimics std::cout interface and avoids std::cout
 *                       appearing in the source
 *
 * It is a validation requirement that std::cout should not appear in the
 * source code, and all output should use logger function
 */
class DummyOutObject {
 public:

  /*
   * operator<<() - accepts any type and chauin them up
   *
   * The template argument can be automatically deducted from the actual
   * argument, which is done by the compiler
   */
  template <typename T>
  DummyOutObject &operator<<(const T &value) {
    (void)value;
    
    return *this;
  }
  
  /*
   * operator<<() - The following three are to support std::endl()
   */
  DummyOutObject &operator<<(std::ostream& (*f)(std::ostream &)) {
    (void)f;
    
    return *this;
  }

  DummyOutObject &operator<<(std::ostream& (*f)(std::ios &)) {
    (void)f;

    return *this;
  }

  DummyOutObject &operator<<(std::ostream& (*f)(std::ios_base &)) {
    (void)f;

    return *this;
  }
};

/*
static DummyOutObject dummy_out;
*/

#define dummy_out std::cout

using NodeID = uint64_t;

extern NodeID INVALID_NODE_ID;
extern bool print_flag;

/*
 * class BwTree - Lock-free BwTree index implementation
 *
 * Template Arguments:
 *
 * template <typename RawKeyType,
 *          typename ValueType,
 *          typename KeyComparator = std::less<RawKeyType>,
 *          typename KeyEqualityChecker = std::equal_to<RawKeyType>,
 *          typename KeyHashFunc = std::hash<KeyType>,
 *          typename ValueEqualityChecker = std::equal_to<ValueType>,
 *          typename ValueHashFunc = std::hash<ValueType>>
 *
 * Explanation:
 *
 *  - RawKeyType: Key type of the map
 *                *** DO NOT CONFUSE THIS WITH WRAPPED KEY TYPE
 *
 *  - ValueType: Value type of the map. Note that it is possible
 *               that a single key is mapped to multiple values
 *
 *  - KeyComparator: "less than" relation comparator for RawKeyType
 *                   Returns true if "less than" relation holds
 *                   *** NOTE: THIS OBJECT DO NOT NEED TO HAVE A DEFAULT
 *                   CONSTRUCTOR. THIS MODIFICATION WAS MADE TO FIT
 *                   INTO Peloton's ARCHITECTURE
 *                   Please refer to main.cpp, class KeyComparator for more
 *                   information on how to define a proper key comparator
 *
 *  - KeyEqualityChecker: Equality checker for RawKeyType
 *                        Returns true if two keys are equal
 *
 *  - KeyHashFunc: Hashes KeyType into size_t. This is used in unordered_set
 *
 *  - ValueEqualityChecker: Equality checker for value type
 *                          Returns true for ValueTypes that are equal
 *
 *  - ValueHashFunc: Hashes ValueType into a size_t
 *                   This is used in unordered_set
 *
 * If not specified, then by default all arguments except the first two will
 * be set as the standard operator in C++ (i.e. the operator for primitive types
 * AND/OR overloaded operators for derived types)
 */
template <typename RawKeyType,
          typename ValueType,
          typename KeyComparator = std::less<RawKeyType>,
          typename KeyEqualityChecker = std::equal_to<RawKeyType>,
          typename KeyHashFunc = std::hash<RawKeyType>,
          typename ValueComparator = std::less<ValueType>,
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

  // This does not have to be the friend class of BwTree
  class EpochManager;

 public:
  class BaseNode;
  class KeyType;
  class NodeSnapshot;
  
  class KeyNodeIDPairHashFunc;
  class KeyNodeIDPairEqualityChecker;
  class KeyValuePairHashFunc;
  class KeyValuePairEqualityChecker;

 /*
  * private: Basic type definition
  */
#ifndef ALL_PUBLIC
 private:
#else
 public:
#endif
  // KeyType-NodeID pair
  using KeyNodeIDPair = std::pair<KeyType, NodeID>;
  using KeyNodeIDPairSet = std::unordered_set<KeyNodeIDPair,
                                              KeyNodeIDPairHashFunc,
                                              KeyNodeIDPairEqualityChecker>;
  
  // KeyType-ValueType pair
  using KeyValuePair = std::pair<KeyType, ValueType>;
  using KeyValuePairSet = std::unordered_set<KeyValuePair,
                                             KeyValuePairHashFunc,
                                             KeyValuePairEqualityChecker>;
                                             
  using ValueSet = std::unordered_set<ValueType,
                                      ValueHashFunc,
                                      ValueEqualityChecker>;

  using EpochNode = typename EpochManager::EpochNode;

  // The maximum number of nodes we could map in this index
  constexpr static NodeID MAPPING_TABLE_SIZE = 1 << 18;

  // If the length of delta chain exceeds this then we consolidate the node
  constexpr static int DELTA_CHAIN_LENGTH_THRESHOLD = 8;
  // So maximum delta chain length on leaf is 12
  constexpr static int DELTA_CHAIN_LENGTH_THRESHOLD_LEAF_DIFF = -4;

  // If node size goes above this then we split it
  constexpr static size_t INNER_NODE_SIZE_UPPER_THRESHOLD = 32;
  constexpr static size_t LEAF_NODE_SIZE_UPPER_THRESHOLD = 16;

  constexpr static size_t INNER_NODE_SIZE_LOWER_THRESHOLD = 15;
  constexpr static size_t LEAF_NODE_SIZE_LOWER_THRESHOLD = 7;
  
  constexpr static int64_t max_thread_count = 888888;

  /*
   * enum class NodeType - Bw-Tree node type
   */
  enum class NodeType {
    // Data page type
    LeafType = 0,
    InnerType,

    // Only valid for leaf
    LeafInsertType,
    LeafSplitType,
    LeafDeleteType,
    LeafUpdateType,
    LeafRemoveType,
    LeafMergeType,

    // Only valid for inner
    InnerInsertType,
    InnerSplitType,
    InnerDeleteType,
    InnerRemoveType,
    InnerMergeType,
    InnerAbortType, // Unconditional abort
  };

  /*
   * enum class ExtendedKeyValue - We wrap the raw key with an extra
   * of indirection in order to identify +/-Inf
   *
   * NOTE: It must maintained that the numeric value obeys the following
   * relation:
   *
   *   NegInf < RawKey < PosInf
   *
   * This is because when we compare two wrapped keys, as long as their
   * types are not all of RawKey, we could directly compare using numeric
   * values of their type
   */
  enum class ExtendedKeyValue {
    NegInf = 0,
    RawKey = 1,
    PosInf = 2,
  };

  /*
   * struct KeyType - Wrapper class for RawKeyType which supports +/-Inf
   * for arbitrary key types
   *
   * NOTE: Comparison between +Inf and +Inf, comparison between
   * -Inf and -Inf, and equality check between them are also defined
   * because they will be needed in corner cases
   */
  class KeyType {
   public:
    // If type is +/-Inf then we use this key to compare
    RawKeyType key;

    // Denote whether the key value is +/-Inf or not
    ExtendedKeyValue type;

    /*
     * Constructor - This is required by class NodeMetaData
     *               since this object is initialized later
     */
    KeyType() {}

    /*
     * Constructor  - Use RawKeyType object to initialize
     */
    KeyType(const RawKeyType &p_key) :
      key(p_key),
      type{ExtendedKeyValue::RawKey} // DO NOT FORGET THIS!
    {}

    /*
     * Constructor - Use value type only (must not be raw value type)
     *
     * This function assumes RawKeyType being default constructible
     */
    KeyType(ExtendedKeyValue p_type) :
      key(),
      type{p_type} {
      // If it is raw type then we are having an empty key
      assert(p_type != ExtendedKeyValue::RawKey);
      return;
    }

    /*
     * Copy Constructoe - copy construct a KeyType with exactly the same
     *                    raw key and key type
     */
    KeyType(const KeyType &p_key) :
      key(p_key.key),
      type{p_key.type}
    {}

    /*
     * IsNegInf() - Whether the key value is -Inf
     */
    inline bool IsNegInf() const {
      return type == ExtendedKeyValue::NegInf;
    }

    /*
     * IsPosInf() - Whether the key value is +Inf
     */
    inline bool IsPosInf() const {
      return type == ExtendedKeyValue::PosInf;
    }
    
    /*
     * GetIntType() - Cast ExtendedKeyType as integer and return
     */
    inline int GetIntType() const {
      return static_cast<int>(type);
    }
  };
  
  ///////////////////////////////////////////////////////////////////
  // Comparator, eq cheker and hasher for KeyType
  ///////////////////////////////////////////////////////////////////
  
  /*
   * class WrappedKeyComparator - Compares wrapped key, using raw key
   *                              comparator in template argument
   */
  class WrappedKeyComparator {
   public:
    // A pointer to the "less than" comparator object that might be
    // context sensitive
    const KeyComparator *key_cmp_obj_p;

    /*
     * Constructor - Takes a pointer to raw key comparator object
     */
    WrappedKeyComparator(const KeyComparator *p_key_cmp_obj_p) :
      key_cmp_obj_p{p_key_cmp_obj_p}
    {}

    /*
     * Default Constructor - Deleted
     *
     * This is defined to emphasize the face that a key comparator
     * is always needed to construct this object
     */
    WrappedKeyComparator() = delete;

    /*
     * operator() - Compares two keys for < relation
     *
     * This functions first check whether two keys are both of raw key type
     * If it is the case then compare two keys using key comparator object
     * Otherwise, we compare numeric values of the type to determine
     * which one is smaller.
     */
    inline bool operator()(const KeyType &key1, const KeyType &key2) const {
      int key1_type = key1.GetIntType();
      int key2_type = key2.GetIntType();

      if((key1_type == 1) && (key2_type == 1)) {
        // Then we need to compare real key
        // NOTE: We use the comparator object passed as constructor argument
        // This might not be necessary if the comparator is trivially
        // constructible, but in the case of Peloton, tuple key comparison
        // requires knowledge of the tuple schema which cannot be achieved
        // without a comparator object that is not trivially constructible
        return (*key_cmp_obj_p)(key1.key, key2.key);
      }

      return key1_type < key2_type;
    }
  };
  
  /*
   * class WrappedRawKeyComparator - Compares wrapped keys by just comparing
   *                                 raw key
   *
   * This is used in leaf node sorting function since there could not be
   * +/-Inf used in key-value pair
   *
   * But for inner level sorting, we should always use WrappedKeyComparator
   * since -Inf could be used as a separator key
   */
  class WrappedRawKeyComparator {
   public:
    const KeyComparator *key_cmp_obj_p;
    
    /*
     * Default Constructor - deleted
     */
    WrappedRawKeyComparator() = delete;
    
    /*
     * Constructor - Initialize a wrapped raw key comparator
     */
    WrappedRawKeyComparator(const KeyComparator *p_key_cmp_obj_p) :
      key_cmp_obj_p{p_key_cmp_obj_p}
    {}
    
    /*
     * operator() - Compares two wrapped keys of raw key type
     *
     * We just ignore key type field to have slightly better comparison
     * performance
     */
    inline bool operator()(const KeyType &key1, const KeyType &key2) const {
      assert(key1.type == ExtendedKeyValue::RawKey);
      assert(key2.type == ExtendedKeyValue::RawKey);

      return (*key_cmp_obj_p)(key1.key, key2.key);
    }
  };
  
  /*
   * class WrappedKeyEqualityChecker - Checks equivalence relation between
   *                                   two wrapped keys
   *
   * NOTE: This class supports +/-Inf
   */
  class WrappedKeyEqualityChecker {
   public:
    const KeyEqualityChecker *key_eq_obj_p;

    /*
     * Constructor - deleted
     */
    WrappedKeyEqualityChecker() = delete;

    /*
     * Constructor - Initialize wrapped raw key comparator
     */
    WrappedKeyEqualityChecker(const KeyEqualityChecker *p_key_eq_obj_p) :
      key_eq_obj_p{p_key_eq_obj_p}
    {}

    /*
     * operator() - Compares two RawKey type keys
     *
     * If both keys are of RawKey type then compare keys
     * Otherwise compare their types
     */
    inline bool operator()(const KeyType &key1, const KeyType &key2) const {
      if(key1.type == ExtendedKeyValue::RawKey && \
         key2.type == ExtendedKeyValue::RawKey) {
        return (*key_eq_obj_p)(key1.key, key2.key);
      }
      
      return key1.type == key2.type;
  };
  
  /*
   * class WrappedRawKeyEqualityChecker - Checks equivalence relation between
   *                                      two wrapped keys
   *
   * NOTE: This class onlu supports comparing two raw key type keys
   * If there are comparison involving +/-Inf please do not use this class
   */
  class WrappedRawKeyEqualityChecker {
   public:
    const KeyEqualityChecker *key_eq_obj_p;
    
    /*
     * Constructor - deleted
     */
    WrappedRawKeyEqualityChecker() = delete;
    
    /*
     * Constructor - Initialize wrapped raw key comparator
     */
    WrappedRawKeyEqualityChecker(const KeyEqualityChecker *p_key_eq_obj_p) :
      key_eq_obj_p{p_key_eq_obj_p}
    {}
    
    /*
     * operator() - Compares two RawKey type keys
     */
    inline bool operator()(const KeyType &key1, const KeyType &key2) const {
      assert(key1.type == ExtendedKeyValue::RawKey);
      assert(key2.type == ExtendedKeyValue::RawKey);
      
      return (*key_eq_obj_p)(key1.key, key2.key);
    }
  };
  
  /*
   * class WrappedKeyHashFunc - Key hash function that returns size_t
   *
   * This is used to hash std::pair<KeyType, ValueType>
   */
  class WrappedKeyHashFunc {
   public:
    const KeyHashFunc *key_hash_obj_p;

    /*
     * Constructor - Initialize hash function using a hash object
     */
    WrappedKeyHashFunc(const KeyHashFunc *p_key_hash_obj_p) :
      key_hash_obj_p{p_key_hash_obj_p}
    {}

    /*
     * Default Constructor - deleted
     */
    WrappedKeyHashFunc() = delete;

    /*
     * operator() - Returns the hash using raw key
     *
     * If the key is of raw key type then hash the key.
     * Otherwise directly returns its type as a size_t
     */
    inline size_t operator()(const KeyType &key) const {
      if(key.type == ExtendedKeyValue::RawKey) {
        return (*key_hash_obj_p)(key.key);
      }
      
      return static_cast<size_t>(key.type);
    }
  };
  
  /*
   * class WrappedRawKeyHashFunc - Key hash function that returns size_t
   *
   * This is used to hash std::pair<KeyType, ValueType>
   * NOTE: This function only supports hashing raw key type. If there
   * if +/-Inf then assertion fails
   */
  class WrappedRawKeyHashFunc {
   public:
    const KeyHashFunc *key_hash_obj_p;
    
    /*
     * Constructor - Initialize hash function using a hash object
     */
    WrappedRawKeyHashFunc(const KeyHashFunc *p_key_hash_obj_p) :
      key_hash_obj_p{p_key_hash_obj_p}
    {}
    
    /*
     * Default Constructor - deleted
     */
    WrappedRawKeyHashFunc() = delete;
    
    /*
     * operator() - Returns the hash using raw key
     *
     * We only allow hashing raw key type. +Inf and -Inf do not have hash
     * value since they never appear as valid key-value pair
     */
    inline size_t operator()(const KeyType &key) const {
      assert(key.type == ExtendedKeyValue::RawKey);
      
      return (*key_hash_obj_p)(key.key);
    }
  };
  
  ///////////////////////////////////////////////////////////////////
  // Comparator, equality checker and hasher for key-NodeID pair
  ///////////////////////////////////////////////////////////////////
  
  /*
   * class KeyNodeIDPairComparator - Compares key-value pair for < relation
   *
   * Only key values are compares. However, we should use WrappedKeyComparator
   * instead of the raw one, since there could be -Inf involved in inner nodes
   */
  class KeyNodeIDPairComparator {
   public:
    const WrappedKeyComparator *wrapped_key_cmp_obj_p;

    /*
     * Default constructor - deleted
     */
    KeyNodeIDPairComparator() = delete;

    /*
     * Constructor - Initialize a key-NodeID pair comparator using
     *               wrapped key comparator
     */
    KeyNodeIDPairComparator(TreeType *p_tree_p) :
      wrapped_key_cmp_obj_p{&p_tree_p->wrapped_key_cmp_obj},
    {}

    /*
     * operator() - Compares whether a key NodeID pair is less than another
     *
     * We only compare keys since there should not be duplicated
     * keys inside an inner node
     */
    inline bool operator()(const KeyNodeIDPair &knp1,
                           const KeyNodeIDPair &knp2) const {
      // First compare keys for relation
      return (*wrapped_key_cmp_obj_p)(knp1.first, knp2.first);
    }
  };
  
  /*
   * class KeyNodeIDPairEqualityChecker - Checks KeyNodeIDPair equality
   *
   * Only keys are checked since there should not be duplicated keys inside
   * inner nodes. However we should always use wrapped key eq checker rather
   * than wrapped raw key eq checker
   */
  class KeyNodeIDPairEqualityChecker {
   public:
    const WrappedKeyEqualityChecker *wrapped_key_eq_obj_p;

    /*
     * Default constructor - deleted
     */
    KeyNodeIDPairEqualityChecker() = delete;

    /*
     * Constructor - Initialize a key node pair eq checker
     */
    KeyNodeIDPairEqualityChecker(TreeType *p_tree_p) :
      wrapped_key_eq_obj_p{&p_tree_p->wrapped_key_eq_obj},
    {}

    /*
     * operator() - Compares key-NodeID pair by comparing keys
     */
    inline bool operator()(const KeyNodeIDPair &knp1,
                           const KeyNodeIDPair &knp2) const {
      return (*wrapped_key_eq_obj_p)(knp1.first, knp2.first);
    }
  };
  
  /*
   * class KeyNodeIDPairHashFunc - Hashes a key-NodeID pair into size_t
   */
  class KeyNodeIDPairHashFunc {
   public:
    const WrappedKeyHashFunc *wrapped_key_hash_obj_p;

    /*
     * Default constructor - deleted
     */
    KeyNodeIDPairHashFunc() = delete;

    /*
     * Constructor - Initialize a key value pair hash function
     */
    KeyNodeIDPairHashFunc(TreeType *p_tree_p) :
      wrapped_key_hash_obj_p{&p_tree_p->wrapped_key_hash_obj},
    {}

    /*
     * operator() - Hashes a key-value pair by hashing each part and
     *              combine them into one size_t
     *
     * We use XOR to combine hashes of the key and value together into one
     * single hash value
     */
    inline size_t operator()(const KeyNodeIDPair &knp) const {
      return (*wrapped_key_hash_obj_p)(knp.first);
    }
  };

  ///////////////////////////////////////////////////////////////////
  // Comparator, equality checker and hasher for key-value pair
  ///////////////////////////////////////////////////////////////////
  
  /*
   * class KeyValuePairComparator - Compares key-value pair for < relation
   *
   * Only keys are compared since we only use this in std::lower_bound()
   * to find the lower bound given a key and an empty value
   */
  class KeyValuePairComparator {
   public:
    const WrappedRawKeyComparator *wrapped_raw_key_cmp_obj_p;
    
    /*
     * Default constructor - deleted
     */
    KeyValuePairComparator() = delete;
    
    /*
     * Constructor - Initialize a key-value pair comparator using
     *               wrapped key comparator and value comparator
     */
    KeyValuePairComparator(TreeType *p_tree_p) :
      wrapped_raw_key_cmp_obj_p{&p_tree_p->wrapped_raw_key_cmp_obj},
    {}
    
    /*
     * operator() - Compares whether a key value pair is less than another
     *
     * NOTE: We only compare keys, since all pairs with the same key are
     * considered as equal when searching in an array of key-value
     * pairs. We usually could not determine an order of the values
     * if value is not a primitive type
     */
    inline bool operator()(const KeyValuePair &kvp1,
                           const KeyValuePair &kvp2) const {
      return (*wrapped_raw_key_cmp_obj_p)(kvp1.first, kvp2.first);
    }
  };
  
  
  /*
   * class KeyValuePairEqualityChecker - Checks KeyValuePair equality
   */
  class KeyValuePairEqualityChecker {
   public:
    const WrappedRawKeyEqualityChecker *wrapped_raw_key_eq_obj_p;
    const ValueEqualityChecker *value_eq_obj_p;
    
    /*
     * Default constructor - deleted
     */
    KeyValuePairEqualityChecker() = delete;
    
    /*
     * Constructor - Initialize a key value pair equality checker with
     *               WrappedRawKeyEqualityChecker and ValueEqualityChecker
     */
    KeyValuePairEqualityChecker(TreeType *p_tree_p) :
      wrapped_raw_key_eq_obj_p{&p_tree_p->wrapped_raw_key_eq_obj},
      value_eq_obj_p{&p_tree_p->value_eq_obj}
    {}
    
    /*
     * operator() - Compares key-value pair by comparing each component
     *              of them
     *
     * NOTE: This function only compares keys with RawKey type. For +/-Inf
     * the wrapped raw key comparator will fail
     */
    inline bool operator()(const KeyValuePair &kvp1,
                           const KeyValuePair &kvp2) const {
      return ((*wrapped_raw_key_eq_obj_p)(kvp1.first, kvp2.first)) && \
             ((*value_eq_obj_p)(kvp1.second, kvp2.second));
    }
  };
  
  /*
   * class KeyValuePairHashFunc - Hashes a key-value pair into size_t
   *
   * This is used as the hash function of unordered_map
   */
  class KeyValuePairHashFunc {
   public:
    const WrappedRawKeyHashFunc *wrapped_raw_key_hash_obj_p;
    const ValueHashFunc *value_hash_obj_p;

    /*
     * Default constructor - deleted
     */
    KeyValuePairHashFunc() = delete;

    /*
     * Constructor - Initialize a key value pair hash function
     */
    KeyValuePairHashFunc(TreeType *p_tree_p) :
      wrapped_raw_key_hash_obj_p{&p_tree_p->wrapped_raw_key_hash_obj},
      value_hash_obj_p{&p_tree_p->value_hash_obj}
    {}

    /*
     * operator() - Hashes a key-value pair by hashing each part and
     *              combine them into one size_t
     *
     * We use XOR to combine hashes of the key and value together into one
     * single hash value
     */
    inline size_t operator()(const KeyValuePair &kvp) const {
      return ((*wrapped_raw_key_hash_obj_p)(kvp.first)) ^ \
             ((*value_hash_obj_p)(kvp.second));
    }
  };
  
  ///////////////////////////////////////////////////////////////////
  // Key Comparison Member Functions
  ///////////////////////////////////////////////////////////////////
  
  /*
   * KeyCmpLess() - Compare two keys for "less than" relation
   *
   * If key1 < key2 return true
   * If key1 >= key2 return false
   *
   * NOTE: All comparisons are defined, since +/- Inf themselves
   * could be used as separator/high key
   */
  bool KeyCmpLess(const KeyType &key1, const KeyType &key2) const {
    // The wrapped key comparator object is defined as a object member
    // since we need it to be passed in std::map as the comparator
    return wrapped_key_cmp_obj(key1, key2);
  }

  /*
   * KeyCmpEqual() - Compare a pair of keys for equality
   *
   * This function tests whether two keys are equal or not. It correctly
   * deals with +Inf and -Inf, in the sense that +Inf == +Inf and
   * -Inf == -Inf.
   *
   * If two keys are both of raw key type then their raw keys are
   * compared using the object. Otherwise their types are compared
   * directly by numeric values.
   */
  bool KeyCmpEqual(const KeyType &key1, const KeyType &key2) const {
    int key1_type = key1.GetIntType();
    int key2_type = key2.GetIntType();

    if((key1_type == 1) && (key2_type == 1)) {
      return key_eq_obj(key1.key, key2.key);
    }

    return key1_type == key2_type;
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
  
  ///////////////////////////////////////////////////////////////////
  // Value Comparison Member
  ///////////////////////////////////////////////////////////////////
  
  /*
   * ValueCmpEqual() - Compares whether two values are equal
   */
  inline bool ValueCmpEqual(const ValueType &v1, const ValueType &v2) {
    return value_eq_obj(v1, v2);
  }

  /*
   * enum class OpState - Current state of the state machine
   *
   * Init - We need to load root ID and start the traverse
   *        After loading root ID should switch to Inner state
   *        since we know there must be an inner node
   * Inner - We are currently on an inner node, and want to go one
   *         level down
   * Abort - We just saw abort flag, and will reset everything back to Init
   */
  enum class OpState {
    Init,
    Inner,
    Leaf,
    Abort
  };

  /*
   * class Context - Stores per thread context data that is used during
   *                 tree traversal
   *
   * NOTE: For each thread there could be only 1 instance of this object
   * so we forbid copy construction and assignment and move
   */
  class Context {
   public:
    // This is cannot be changed once initialized
    const KeyType search_key;
    
    // It is used in the finite state machine that drives the traversal
    // process down to a leaf node
    OpState current_state;

    // Whether to abort current traversal, and start a new one
    // after seeing this flag, all function should return without
    // any further action, and let the main driver to perform abort
    // and restart
    // NOTE: Only the state machine driver could abort
    // and other functions just return on seeing this flag
    bool abort_flag;
    
    // Saves NodeSnapshot object inside an array for snapshot maintenance
    std::vector<NodeSnapshot> path_list;

    // Counts abort in one traversal
    size_t abort_counter;
    
    // Current level (root = 0)
    int current_level;

    /*
     * Constructor - Initialize a context object into initial state
     */
    Context(const KeyType p_search_key) :
      search_key{p_search_key},
      current_state{OpState::Init},
      abort_flag{false},
      path_list{},
      abort_counter{0},
      current_level{0}
    {}

    /*
     * Destructor - Cleanup
     */
    ~Context() {}

    /*
     * Copy constructor - deleted
     * Assignment operator - deleted
     * Move constructor - deleted
     * Move assignment - deleted
     */
    Context(const Context &p_context) = delete;
    Context &operator=(const Context &p_context) = delete;
    Context(Context &&p_context) = delete;
    Context &operator=(Context &&p_context) = delete;
    
    /*
     * HasParentNode() - Returns whether the current node (top of path list)
     *                   has a parent node
     */
    inline bool HasParentNode() const {
      int path_list_length = static_cast<int>(path_list.size());
      
      // If currently the length of the path > 1 then there is a parent node
      return path_list_length >= 2;
    }
  };

  /*
   * class NodeMetaData - Holds node metadata in an object
   *
   * Node metadata includes low key (conventionally called "lower bound"),
   * high key (called "upper bound") and next node's NodeID
   *
   * Since we need to query for high key and low key in every step of
   * traversal down the tree (i.e. on each level we need to verify we
   * are on the correct node). It would be wasteful if we traverse down the
   * delta chain down to the bottom everytime to collect these metadata
   * therefore as an optimization we store them inside each delta node
   * and leaf/inner node to optimize for performance
   *
   * NOTE: We do not count node type as node metadata
   */
  class NodeMetaData {
   public:
    KeyType lbound;
    KeyType ubound;
    
    NodeID next_node_id;
    
    int depth;
    
    /*
     * Constructor
     *
     * NOTE: We save key objects rather than key pointers in this function
     * in order to prevent hard-to-debug pointer problems
     *
     * This constructor is called by InnerNode and LeafNode, so the depth
     * is by default set to 0
     */
    NodeMetaData(const KeyType &p_lbound,
                 const KeyType &p_ubound,
                 NodeID p_next_node_id) :
      lbound{p_lbound},
      ubound{p_ubound},
      next_node_id{p_next_node_id},
      depth{0}
    {}
    
    /*
     * Copy Constructor
     *
     * This could be used to copy a metadata from other node. 
     */
    NodeMetaData(const NodeMetaData &nmd) :
      lbound{nmd.lbound},
      ubound{nmd.ubound},
      next_node_id{nmd.next_node_id},
      depth{nmd.depth}
    {}
    
    /*
     * Default Constructor - This is used to initialize a base node since
     *                       BaseNode needs to initialize a blank NodeMetaData
     *                       structure first
     */
    NodeMetaData() {}
  };

  /*
   * class BaseNode - Generic node class; inherited by leaf, inner
   *                  and delta node
   */
  class BaseNode {
   public:
    const NodeType type;
    
    // This holds low key, high key, and next node ID
    NodeMetaData metadata;

    /*
     * Constructor - Initialize type and metadata
     */
    BaseNode(NodeType p_type) :
      type{p_type}
    {}

    /*
     * Destructor - This must be virtual in order to properly destroy
     * the object only given a base type key
     */
    ~BaseNode() {}

    /*
     * GetType() - Return the type of node
     *
     * This method does not allow overridding
     */
    inline NodeType GetType() const {
      return type;
    }
    
    /*
     * GetNodeMetaData() - Returns a const reference to node metadata
     *
     * Please do not override this method
     */
    inline const NodeMetaData &GetNodeMetaData() const {
      return metadata;
    }
    
    /*
     * SetNodeMetaData() - This is a wrapper to the expanded version below
     *
     * Argument node_p must be the child node of current node being initialized
     *
     * The other node being copied from should be the child node of the
     * current node, since we assume the depth of current metadata object
     * always equals the from node's depth + 1
     */
    inline void SetNodeMetaData(const BaseNode *node_p) {
      SetNodeMetaData(node_p->metadata.lbound,
                      node_p->metadata.ubound,
                      node_p->metadata.next_node_id,
                      node_p->metadata.depth + 1);

      return;
    }
    
    /*
     * SetNodeMetaData() - Set node metadata using given values
     *
     * Note: Node depth is by default set to 0, since this function
     * is called for InnerNode and LeafNode, the depth of which is always 0
     */
    inline void SetNodeMetaData(const KeyType &p_lbound,
                                const KeyType &p_ubound,
                                NodeID p_next_node_id) {
      metadata.lbound = p_lbound;
      metadata.ubound = p_ubound;
      metadata.next_node_id = p_next_node_id;
      metadata.depth = 0;
      
      return;
    }

    /*
     * IsDeltaNode() - Return whether a node is delta node
     *
     * All nodes that are neither inner nor leaf type are of
     * delta node type
     */
    inline bool IsDeltaNode() const {
      if(type == NodeType::InnerType || \
         type == NodeType::LeafType) {
        return false;
      } else {
        return true;
      }
    }
    
    /*
     * IsInnerNode() - Returns true if the node is an inner node
     *
     * This is useful if we want to collect all seps on an inner node
     * If the top of delta chain is an inner node then just do not collect
     * and use the node directly
     */
    inline bool IsInnerNode() const {
      return type == NodeType::InnerType;
    }
    
    /*
     * IsRemoveNode() - Returns true if the node is of inner/leaf remove type
     *
     * This is used in JumpToLeftSibling() as an assertion
     */
    inline bool IsRemoveNode() const {
      return (type == NodeType::InnerRemoveNode) || \
             (type == NodeType::LeafRemoveNode);
    }

    /*
     * IsOnLeafDeltaChain() - Return whether the node is part of
     *                        leaf delta chain
     *
     * This is true even for NodeType::LeafType
     *
     * NOTE: WHEN ADDING NEW NODE TYPES PLEASE UPDATE THIS LIST
     *
     * Note 2: Avoid calling this in multiple places. Currently we only
     * call this in TakeNodeSnapshot() or in the debugger
     */
    inline bool IsOnLeafDeltaChain() const {
      return (type == NodeType::LeafType || \
              type == NodeType::LeafInsertType || \
              type == NodeType::LeafDeleteType || \
              type == NodeType::LeafSplitType || \
              type == NodeType::LeafMergeType || \
              type == NodeType::LeafRemoveType || \
              type == NodeType::LeafUpdateType || \
              // This is necessary, since we need to first load
              // node ID and then judge its type
              type == NodeType::LeafAbortType);
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
    const BaseNode *child_node_p;

    /*
     * Constructor
     */
    DeltaNode(NodeType p_type,
              int p_depth,
              const BaseNode *p_child_node_p) :
      BaseNode{p_type},
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
    // We always hold data within a vector of KeyValuePair
    std::vector<KeyValuePair> data_list;
    
    // This stores accumulated number of items for each key for fast access
    std::vector<int> item_prefix_sum;

    /*
     * Constructor - Initialize bounds and next node ID
     */
    LeafNode(const KeyType &p_lbound,
             const KeyType &p_ubound,
             NodeID p_next_node_id) :
      BaseNode{NodeType::LeafType} {
      // This needs to be done inside BaseNode
      // so we call BaseNode::SetNodeMetaData() to initialize
      // metadata
      this->SetNodeMetaData(p_lbound, p_ubound, p_next_node_id);
      
      return;
    }

    /*
     * GetSplitSibling() - Split the node into two halves
     *
     * Although key-values are stored as independent pairs, we always split
     * on the point such that no keys are separated on two leaf nodes
     * This decision is made to make searching easier since now we could
     * just do a binary search on the base page to decide whether a
     * given key-value pair exists or not
     *
     * This function splits a leaf node into halves based on key rather
     * than items. This implies the number of keys would be even, but no
     * guarantee is made about the number of key-value items, which might
     * be very unbalanced, cauing node size varing much.
     *
     * NOTE: This function allocates memory, and if it is not used
     * e.g. by a CAS failure, then caller needs to delete it
     *
     * NOTE 2: Split key is stored in the low key of the new leaf node
     *
     * NOTE 3: This function assumes no out-of-bound key, i.e. all keys
     * stored in the leaf node are < high key. This is valid since we
     * already filtered out those key >= high key in consolidation.
     */
    LeafNode *GetSplitSibling() const {
      int key_num = static_cast<int>(item_prefix_sum.size());
      assert(key_num >= 2);
      
      // This is the index of the key in prefix sum array
      int split_key_index = key_num / 2;
      
      // This is the index of the actual key-value pair in data_list
      // We need to substract this value from the prefix sum in the new
      // inner node
      int split_item_index = item_prefix_sum[split_key_index];
      
      // This points to the prefix sum array and we use this to copy
      // the prefix sum array
      auto prefix_sum_start_it = item_prefix_sum.begin();
      std::advance(prefix_sum_start_it, split_key_index);
      
      auto prefix_sum_end_it = item_prefix_sum.end();

      // This is an iterator pointing to the split point in the vector
      // note that std::advance() operates efficiently on std::vector's
      // RandomAccessIterator
      auto copy_start_it = data_list.begin();
      std::advance(copy_start_it, split_item_index);
      
      // This is the end point for later copy of data
      auto copy_end_it = data_list.end();

      // This is the key part of the key-value pair, also the low key
      // of the new node and new high key of the current node (will be
      // reflected in split delta later in its caller)
      const KeyType &split_key = copy_start_it->first;

      // This will call SetMetaData inside its constructor
      LeafNode *leaf_node_p = new LeafNode{split_key,
                                           this->metadata.ubound,
                                           this->metadata.next_node_id};
      
      // Copy data item into the new node using batch assign()
      leaf_node_p->data_list.assign(copy_start_it, copy_end_it);
      
      // Copy prefix sum into the new node and later we will modify it
      leaf_node_p->item_prefix_sum.assign(prefix_sum_start_it,
                                          prefix_sum_end_it);
                                          
      // Adjust prefix sum in the new leaf node by subtracting them
      // from the index of item on the split point
      for(auto &prefix_sum : leaf_node_p->item_prefix_sum) {
        prefix_sum -= split_item_index;
      }

      return leaf_node_p;
    }
  };

  /*
   * class LeafInsertNode - Insert record into a leaf node
   */
  class LeafInsertNode : public DeltaNode {
   public:
    // Use an item to store key-value internally
    // to make leaf consolidation faster
    KeyValuePair inserted_item;

    /*
     * Constructor
     */
    LeafInsertNode(const KeyType &p_insert_key,
                   const ValueType &p_value,
                   const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::LeafInsertType, p_child_node_p},
      inserted_item{p_insert_key, p_value} {
      // We set node metadata using parent node (direct copy)
      this->SetNodeMetaData(p_child_node_p);
      
      return;
    }
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
    // Use an deleted item to store deleted key and value
    // to make leaf consolidation faster
    KeyValuePair deleted_item;

    /*
     * Constructor
     */
    LeafDeleteNode(const KeyType &p_delete_key,
                   const ValueType &p_value,
                   const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::LeafDeleteType, p_child_node_p},
      deleted_item{p_delete_key, p_value} {
      // Set node metadata using child node
      this->SetNodeMetaData(p_child_node_p);
      
      return;
    }
  };

  /*
   * class LeafUpdateNode - Update a key atomically
   *
   * Without using this node there is no guarantee that an insert
   * after delete is atomic
   */
  class LeafUpdateNode : public DeltaNode {
   public:
    KeyType update_key;
    ValueType old_value;
    ValueType new_value;

    /*
     * Constructor
     */
    LeafUpdateNode(const KeyType &p_update_key,
                   const ValueType &p_old_value,
                   const ValueType &p_new_value,
                   const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::LeafUpdateType, p_child_node_p},
      update_key{p_update_key},
      old_value{p_old_value},
      new_value{p_new_value} {
      // Set node metadata using child node
      this->SetNodeMetaData(p_child_node_p);
      
      return;
    }
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
                  const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::LeafSplitType, p_child_node_p},
      split_key{p_split_key},
      split_sibling{p_split_sibling} {
      // For a split node, the metadata should be set using split key
      // Low key does not change
      // high key is the split key
      // next node ID should be the new node's ID
      this->SetNodeMetaData(p_child_node_p->metadata.lbound,
                            p_split_key,
                            p_split_sibling);

      return;
    }
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
    LeafRemoveNode(const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::LeafRemoveType, p_child_node_p} {
      // Set node metadata using child node
      this->SetNodeMetaData(p_child_node_p);
      
      return;
    }
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
    
    NodeID deleted_node_id;

    /*
     * Constructor
     */
    LeafMergeNode(const KeyType &p_merge_key,
                  const BaseNode *p_right_merge_p,
                  NodeID p_deleted_node_id,
                  const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::LeafMergeType, p_child_node_p},
      merge_key{p_merge_key},
      right_merge_p{p_right_merge_p},
      deleted_node_id{p_deleted_node_id} {
      // For merge node, node metadata should be set according to
      // two merge branches
      // Low key is the low key of the left branch
      // High key is the high key of the right branch
      // Next node ID is the next node ID of the right branch
      this->SetNodeMetaData(p_child_node_p->metadata.lbound,
                            p_right_merge_p->metadata.ubound,
                            p_right_merge_p->metadata.next_node_id);
                      
      return;
    }
  };

  /*
   * class InnerNode - Inner node that holds separators
   */
  class InnerNode : public BaseNode {
   public:
    std::vector<KeyNodeIDPair> sep_list;

    /*
     * Constructor
     */
    InnerNode(const KeyType &p_lbound,
              const KeyType &p_ubound,
              NodeID p_next_node_id) :
      BaseNode{NodeType::InnerType} {
      this->SetNodeMetaData(p_lbound, p_ubound, p_next_node_id);
      
      return;
    }

    /*
     * GetSplitSibling() - Split the node into two halves
     *
     * This functions is similar to that in LeafNode
     */
    InnerNode *GetSplitSibling() const {
      int key_num = static_cast<int>(sep_list.size());
      
      // Inner node size must be > 2 to avoid empty split node
      assert(key_num >= 2);
      
      int split_item_index = key_num / 2;

      // This is the split point of the inner node
      auto copy_start_it = sep_list.begin();
      std::advance(copy_start_it, split_item_index);
      
      // We copy key-NodeID pairs till the end of the inner node
      auto copy_end_it = sep_list.end();

      const KeyType &split_key = copy_start_it->first;

      // This sets metddata inside BaseNode by calling SetMetaData()
      // inside inner node constructor
      InnerNode *inner_node_p = new InnerNode{split_key,
                                              this->metadata.ubound,
                                              this->metadata.next_node_id};

      // Batch copy from the current node to the new node
      inner_node_p->sep_list.assign(copt_start_it, copy_end_it);

      return inner_node_p;
    }
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
                    const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::InnerInsertType, p_child_node_p},
      insert_key{p_insert_key},
      next_key{p_next_key},
      new_node_id{p_new_node_id} {
      // Set node metadata using child node
      this->SetNodeMetaData(p_child_node_p);
      
      return;
    }
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
                    const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::InnerDeleteType, p_child_node_p},
      delete_key{p_delete_key},
      next_key{p_next_key},
      prev_key{p_prev_key},
      prev_node_id{p_prev_node_id} {
      // Set node metadata using child node
      this->SetNodeMetaData(p_child_node_p);
      
      return;
    }
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
                   const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::InnerSplitType, p_child_node_p},
      split_key{p_split_key},
      split_sibling{p_split_sibling} {
      // For split node metddata is set in the following way:
      // Low key is the low key of child node
      // High key is the split key
      // Next node ID is the split sibling's node ID
      this->SetNodeMetaData(p_child_node_p->metadata.ubound,
                            p_split_key,
                            p_split_sibling);
      
      return;
    }
  };

  /*
   * class InnerRemoveNode
   */
  class InnerRemoveNode : public DeltaNode {
   public:

    /*
     * Constructor
     */
    InnerRemoveNode(const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::InnerRemoveType, p_child_node_p} {
      // Set node metadata using child node
      this->SetNodeMetaData(p_child_node_p);
      
      return;
    }
  };

  /*
   * class InnerMergeNode - Merge delta for inner nodes
   */
  class InnerMergeNode : public DeltaNode {
   public:
    KeyType merge_key;

    const BaseNode *right_merge_p;
    
    // This is beneficial for us to cross validate IndexTermDelete
    // and also to recycle NodeID and remove delta
    NodeID deleted_node_id;

    /*
     * Constructor
     */
    InnerMergeNode(const KeyType &p_merge_key,
                   const BaseNode *p_right_merge_p,
                   NodeID p_deleted_node_id,
                   const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::InnerMergeType, p_child_node_p},
      merge_key{p_merge_key},
      right_merge_p{p_right_merge_p},
      deleted_node_id{p_deleted_node_id} {
      // Set node metadata using two branches
      // Low key is the low key of the left branch
      // High key is the high key of the right branch
      // Next node ID is the next node ID of the right branch
      this->SetNodeMetaData(p_child_node_p->metadata.lbound,
                            p_right_merge_p->metadata.ubound,
                            p_right_merge_p->metadata.next_node_id);

      return;
    }
  };

  /*
   * class InnerAbortNode - Same as LeafAbortNode
   */
  class InnerAbortNode : public DeltaNode {
   public:

    /*
     * Constructor
     */
    InnerAbortNode(const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::InnerAbortType, p_child_node_p} {
      // Set node metadata using child node
      this->SetNodeMetaData(p_child_node_p);
      
      return;
    }
  };

  /*
   * struct NodeSnapshot - Describes the states in a tree when we see them
   *
   * node_id and node_p are pairs that represents the state when we traverse
   * the node and use GetNode() to resolve the node ID.
   *
   * Also we need to distinguish between leaf snapshot and inner node snapshots
   * which is achieved by a flag is_leaf
   */
  class NodeSnapshot {
   public:
    NodeID node_id;
    const BaseNode *node_p;

    // Whether logical node pointer points to a logical leaf node
    // or logical inner node
    // NOTE: This will not change once it is fixed
    const bool is_leaf;

    // Set to true only if the snapshot is created for a root node
    // such that we know there is no more parent node.
    // A node with this flag set to true must be the first node
    // in the stack
    bool is_root;

    /*
     * Constructor - Initialize every member to invalid state
     *
     * Identity of leaf or inner needs to be provided as constructor
     * argument.
     *
     * NOTE: We do not allocate any logical node structure here
     */
    NodeSnapshot(bool p_is_leaf,
                 BwTree *tree_p) :
      node_id{INVALID_NODE_ID},
      node_p{nullptr},
      is_leaf{p_is_leaf},
      is_root{false}
    {}

    /*
     * GetHighKey() - Return the high key of a snapshot
     *
     * Since we store node metadata inside BaseNode it could
     * quickly find the high key without worrying about type casting
     */
    inline const KeyType *GetHighKey() {
      return &node_p->metadata.ubound;
    }

    /*
     * GetLowKey() - Return the low key of a snapshot
     *
     * This is useful sometimes for assertion conditions
     */
    inline const KeyType *GetLowKey() {
      return &node_p->metadata.lbound;
    }
  };

  ////////////////////////////////////////////////////////////////////
  // Interface Method Implementation
  ////////////////////////////////////////////////////////////////////

 public:
  /*
   * Constructor - Set up initial environment for BwTree
   *
   * Any tree instance must start with an intermediate node as root, together
   * with an empty leaf node as child
   */
  BwTree(KeyComparator p_key_cmp_obj = KeyComparator{},
         KeyEqualityChecker p_key_eq_obj = KeyEqualityChecker{},
         KeyHashFunc p_key_hash_obj = KeyHashFunc{},
         ValueComparator p_value_cmp_obj = ValueComparator{},
         ValueEqualityChecker p_value_eq_obj = ValueEqualityChecker{},
         ValueHashFunc p_value_hash_obj = ValueHashFunc{}) :
      // Key Comparison object: raw key, wrapped key and wrapped raw key
      key_cmp_obj{p_key_cmp_obj},
      wrapped_key_cmp_obj{&key_cmp_obj},
      wrapped_raw_key_cmp_obj{&key_cmp_obj},

      // Key equality checker object: raw key, wrapped key and wrapped raw key
      key_eq_obj{p_key_eq_obj},
      wrapped_key_eq_obj{&key_eq_obj},
      wrapped_raw_key_eq_obj{&key_eq_obj},

      // Key hasher object: raw key, wrapped key and wrapped raw key
      key_hash_obj{p_key_hash_obj},
      wrapped_key_hash_obj{&key_hash_obj},
      wrapped_raw_key_hash_obj{&key_hash_obj},

      // Value comparator, equality checker and hasher
      value_cmp_obj{p_value_cmp_obj},
      value_eq_obj{p_value_eq_obj},
      value_hash_obj{p_value_hash_obj},

      // key-node ID pair comparator, equality checker and hasher
      key_node_id_pair_cmp_obj{this},
      key_node_id_pair_eq_obj{this},
      key_node_id_pair_hash_obj{this},
      
      // key-value pair comparator, equality checker and hasher
      Key_value_pair_cmp_obj{this},
      key_value_pair_eq_obj{this},
      key_value_pair_hash_obj{this},
      
      // Statistical information
      next_unused_node_id{0},
      insert_op_count{0},
      insert_abort_count{0},
      delete_op_count{0},
      delete_abort_count{0},
      update_op_count{0},
      update_abort_count{0},
      
      // Interactive debugger
      idb{this},
      
      // Epoch Manager that does garbage collection
      epoch_manager{} {
    bwt_printf("Bw-Tree Constructor called. "
               "Setting up execution environment...\n");

    InitMappingTable();
    InitNodeLayout();

    bwt_printf("Starting epoch manager thread...\n");
    epoch_manager.StartThread();
    
    dummy("Call it here to avoid compiler warning\n");

    return;
  }

  /*
   * Destructor - Destroy BwTree instance
   *
   * NOTE: object member's destructor is called first prior to
   * the destructor of the container. So epoch manager's destructor
   * has been called before we free the whole tree
   */
  ~BwTree() {
    bwt_printf("Next node ID at exit: %lu\n", next_unused_node_id.load());
    bwt_printf("Destructor: Free tree nodes\n");

    // Free all nodes recursively
    //FreeAllNodes(GetNode(root_id.load()));

    return;
  }

  /*
   * FreeAllNodes() - Free all nodes currently in the tree
   *
   * For normal destruction, we do not accept InnerAbortNode,
   * InnerRemoveNode, and LeafRemoveNode, since these three types
   * only appear as temporary states, and must be completed before
   * a thread could finish its job and exit (after posting remove delta
   * the thread always aborts and restarts traversal from the root)
   *
   * NOTE: Do not confuse this function with FreeEpochDeltaChain
   * in EpochManager. These two functions differs in details (see above)
   * and could not be used interchangeably.
   *
   * NOTE 2: This function should only be called in single threaded
   * environment since it assumes sole ownership of the entire tree
   * This is trivial during normal destruction, but care must be taken
   * when this is not true.
   *
   * This node calls destructor according to the type of the node, considering
   * that there is not virtual destructor defined for sake of running speed.
   */
  void FreeAllNodes(const BaseNode *node_p) {
    const BaseNode *next_node_p = node_p;
    int freed_count = 0;

    // These two tells whether we have seen InnerSplitNode
    // NOTE: ubound does not need to be passed between function calls
    // since ubound is only used to prevent InnerNode containing
    // SepItem that has been invalidated by a split
    bool has_ubound = false;
    KeyType ubound{RawKeyType{}};

    while(1) {
      node_p = next_node_p;
      assert(node_p != nullptr);

      NodeType type = node_p->GetType();
      bwt_printf("type = %d\n", (int)type);

      switch(type) {
        case NodeType::LeafInsertType:
          next_node_p = ((LeafInsertNode *)node_p)->child_node_p;

          delete (LeafInsertNode *)node_p;
          freed_count++;

          break;
        case NodeType::LeafDeleteType:
          next_node_p = ((LeafDeleteNode *)node_p)->child_node_p;

          delete (LeafDeleteNode *)node_p;

          break;
        case NodeType::LeafSplitType:
          next_node_p = ((LeafSplitNode *)node_p)->child_node_p;

          delete (LeafSplitNode *)node_p;
          freed_count++;

          break;
        case NodeType::LeafMergeType:
          FreeAllNodes(((LeafMergeNode *)node_p)->child_node_p);
          FreeAllNodes(((LeafMergeNode *)node_p)->right_merge_p);

          delete (LeafMergeNode *)node_p;
          freed_count++;

          // Leaf merge node is an ending node
          return;
        case NodeType::LeafType:
          delete (LeafNode *)node_p;
          freed_count++;

          // We have reached the end of delta chain
          return;
        case NodeType::InnerInsertType:
          next_node_p = ((InnerInsertNode *)node_p)->child_node_p;

          delete (InnerInsertNode *)node_p;
          freed_count++;

          break;
        case NodeType::InnerDeleteType:
          next_node_p = ((InnerDeleteNode *)node_p)->child_node_p;

          delete (InnerDeleteNode *)node_p;
          freed_count++;

          break;
        case NodeType::InnerSplitType: {
          const InnerSplitNode *split_node_p = \
            static_cast<const InnerSplitNode *>(node_p);

          next_node_p = split_node_p->child_node_p;

          // We only save ubound for the first split delta from top
          // to bottom
          // Since the first is always the most up-to-date one
          if(has_ubound == false) {
            // Save the upper bound of the node such that we do not
            // free child nodes that no longer belong to this inner
            // node when we see the InnerNode
            // NOTE: Cannot just save pointer since the pointer
            // will be invalidated after deleting node
            ubound = split_node_p->split_key;
            has_ubound = true;
          }

          delete (InnerSplitNode *)node_p;
          freed_count++;

          break;
        }
        case NodeType::InnerMergeType:
          FreeAllNodes(((InnerMergeNode *)node_p)->child_node_p);
          FreeAllNodes(((InnerMergeNode *)node_p)->right_merge_p);

          delete (InnerMergeNode *)node_p;
          freed_count++;

          // Merge node is also an ending node
          return;
        case NodeType::InnerType: { // Need {} since we initialized a variable
          const InnerNode *inner_node_p = \
            static_cast<const InnerNode *>(node_p);

          // Grammar issue: Since the inner node is const, all its members
          // are const, and so we could only declare const iterator on
          // the vector member, and therefore ":" only returns const reference
          for(const SepItem &sep_item : inner_node_p->sep_list) {
            NodeID node_id = sep_item.node;

            // Load the node pointer using child node ID of the inner node
            const BaseNode *child_node_p = GetNode(node_id);

            // If there is a split node and the item key >= split key
            // then we know the item has been invalidated by the split
            if((has_ubound == true) && \
               (KeyCmpGreaterEqual(sep_item.key, ubound))) {
              break;
            }

            // Then free all nodes in the child node (which is
            // recursively defined as a tree)
            FreeAllNodes(child_node_p);
          }

          // Access its content first and then delete the node itself
          delete inner_node_p;
          freed_count++;

          // Since we free nodes recursively, after processing an inner
          // node and recursively with its child nodes we could return here
          return;
        }
        default:
          // This does not include INNER ABORT node
          bwt_printf("Unknown node type: %d\n", (int)type);

          assert(false);
          return;
      } // switch

      bwt_printf("Freed node of type %d\n", (int)type);
    } // while 1

    bwt_printf("Freed %d nodes during destruction\n", freed_count);

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

    InnerNode *root_node_p = \
      new InnerNode{GetNegInfKey(), GetPosInfKey(), INVALID_NODE_ID};

    root_node_p->sep_list.push_back(neg_si);

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
   *
   * NOTE: As an optimization we do not set the mapping table to zero
   * since installing new node could be done as directly writing into
   * the mapping table rather than CAS with nullptr
   */
  void InitMappingTable() {
    bwt_printf("Initializing mapping table.... size = %lu\n",
               MAPPING_TABLE_SIZE);
    bwt_printf("Fast initialization: Do not set to zero\n");

    return;
  }

  /*
   * GetWrappedKey() - Return an internally wrapped key type used
   *                   to traverse the index
   */
  inline KeyType GetWrappedKey(RawKeyType key) {
    return KeyType{key};
  }

  /*
   * GetPosInfKey() - Get a positive infinite key
   *
   * Assumes there is a trivial constructor for RawKeyType
   */
  inline KeyType GetPosInfKey() {
    return KeyType{ExtendedKeyValue::PosInf};
  }

  /*
   * GetNegInfKey() - Get a negative infinite key
   *
   * Assumes there is a trivial constructor for RawKeyType
   */
  inline KeyType GetNegInfKey() {
    return KeyType{ExtendedKeyValue::NegInf};
  }

  /*
   * GetNextNodeID() - Thread-safe lock free method to get next node ID
   *
   * This function basically compiles to LOCK XADD instruction on x86
   * which is guaranteed to execute atomically
   */
  inline NodeID GetNextNodeID() {
    // fetch_add() returns the old value and increase the atomic
    // automatically
    return next_unused_node_id.fetch_add(1);
  }

  /*
   * InstallNodeToReplace() - Install a node to replace a previous one
   *
   * If installation fails because CAS returned false, then return false
   * This function does not retry
   */
  inline bool InstallNodeToReplace(NodeID node_id,
                                   const BaseNode *node_p,
                                   const BaseNode *prev_p) {
    // Make sure node id is valid and does not exceed maximum
    assert(node_id != INVALID_NODE_ID);
    assert(node_id < MAPPING_TABLE_SIZE);

    // If idb is activated, then all operation will be blocked before
    // they could call CAS and change the key
    #ifdef INTERACTIVE_DEBUG
    debug_stop_mutex.lock();
    debug_stop_mutex.unlock();
    #endif

    return mapping_table[node_id].compare_exchange_strong(prev_p, node_p);
  }

  /*
   * InstallRootNode() - Replace the old root with a new one
   *
   * There is change that this function would fail. In that case it returns
   * false, which implies there are some other threads changing the root ID
   */
  inline bool InstallRootNode(NodeID old_root_node_id,
                              NodeID new_root_node_id) {
    return root_id.compare_exchange_strong(old_root_node_id,
                                           new_root_node_id);
  }

  /*
   * InstallNewNode() - Install a new node into the mapping table
   *
   * This function does not return any value since we assume new node
   * installation would always succeed
   */
  inline void InstallNewNode(NodeID node_id,
                             const BaseNode *node_p) {
    mapping_table[node_id] = node_p;

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
  inline const BaseNode *GetNode(const NodeID node_id) {
    assert(node_id != INVALID_NODE_ID);
    assert(node_id < MAPPING_TABLE_SIZE);

    return mapping_table[node_id].load();
  }

  /*
   * Traverse() - Traverse down the tree structure, handles abort
   *
   * This function is implemented as a state machine that allows a thread
   * to jump back to Init state when necessary (probably a CAS failed and
   * it does not want to resolve it from bottom up)
   *
   * It stops at page level, and then the top on the context stack is a
   * leaf level snapshot, with all SMOs, consolidation, and possibly
   * split/remove finished
   *
   * NOTE: We could choose whether to let this function collect value given
   * the key, or not collect value. Because sometimes we just want to locate
   * the correct page that contains a certain key, so the option is useful
   */
  void Traverse(Context *context_p,
                bool collect_value) {
    // We must start from a clean state
    assert(context_p->path_list.size() == 0);

    // This will use lock
    #ifdef INTERACTIVE_DEBUG
    idb.AddKey(context_p->search_key);
    #endif

    while(1) {
      // NOTE: break only breaks out this switch
      switch(context_p->current_state) {
        case OpState::Init: {
          assert(context_p->path_list.size() == 0);
          assert(context_p->abort_flag == false);
          assert(context_p->current_level == 0);

          // This is the serialization point for reading/writing root node
          NodeID start_node_id = root_id.load();

          // We need to call this even for root node since there could
          // be split delta posted on to root node
          LoadNodeID(start_node_id,
                     context_p);

          // There could be an abort here, and we could not directly jump
          // to Init state since we would like to do some clean up or
          // statistics after aborting
          if(context_p->abort_flag == true) {
            context_p->current_state = OpState::Abort;

            // This goes to the beginning of loop
            break;
          }

          bwt_printf("Successfully loading root node ID\n");

          // root node must be an inner node
          // NOTE: We do not traverse down in this state, just hand it
          // to Inner state and let it do all generic job
          context_p->current_state = OpState::Inner;
          context_p->current_level++;

          break;
        } // case Init
        case OpState::Inner: {
          NodeID child_node_id = NavigateInnerNode(context_p);

          // Navigate could abort since it might go to another NodeID
          // if there is a split delta and the key is >= split key
          if(context_p->abort_flag == true) {
            bwt_printf("Navigate Inner Node abort. ABORT\n");

            // If NavigateInnerNode() aborts then it retrns INVALID_NODE_ID
            // as a double check
            // This is the only situation that this function returns
            // INVALID_NODE_ID
            assert(child_node_id == INVALID_NODE_ID);

            context_p->current_state = OpState::Abort;

            break;
          }

          // This might load a leaf child
          LoadNodeID(child_node_id,
                     context_p);

          if(context_p->abort_flag == true) {
            bwt_printf("LoadNodeID aborted. ABORT\n");

            context_p->current_state = OpState::Abort;

            break;
          }

          // This is the node we have just loaded
          NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);

          if(snapshot_p->is_leaf == true) {
            bwt_printf("The next node is a leaf\n");

            // If there is an abort later on then we just go to
            // abort state
            context_p->current_state = OpState::Leaf;
          }

          // Take care: Do not use conflict names in the
          // outer scope
          // NOTE: We directly use the metadata stored inside node
          // here to avoid copying them around
          const KeyType *snapshot_lbound_p = \
            &snapshot_p->node_p->metadata.lbound;

          // This must be true since the NodeID to low key mapping
          // is constant (if the child node is merged into its
          // left sibling, then we just see a remove node and go left
          // in LoadNodeID() called above)
          assert(KeyCmpGreaterEqual(context_p->search_key,
                                    *snapshot_lbound_p) == true);

          // go to next level
          context_p->current_level++;

          break;
        } // case Inner
        case OpState::Leaf: {
          // We do not collect value for this call
          // since we only want to go to the right sibling
          // if there is one
          NavigateLeafNode(context_p,
                           collect_value); // This is argument

          if(context_p->abort_flag == true) {
            bwt_printf("NavigateLeafNode aborts. ABORT\n");

            context_p->current_state = OpState::Abort;

            break;
          }

          bwt_printf("Found leaf node. Abort count = %lu, level = %d\n",
                     context_p->abort_counter,
                     context_p->current_level);

          // If there is no abort then we could safely return
          return;

          break;
        }
        case OpState::Abort: {
          std::vector<NodeSnapshot> *path_list_p = \
            &context_p->path_list;

          assert(path_list_p->size() > 0);

          // We roll back for at least one level
          // and stop at the first
          NodeSnapshot *snapshot_p = nullptr;
          do {
            path_list_p->pop_back();
            context_p->current_level--;

            if(path_list_p->size() == 0) {
              context_p->current_state = OpState::Init;
              context_p->current_level = 0;

              break;
            }

            snapshot_p = &path_list_p->back();

            // Even if we break on leaf level we are now
            // on inner level
            context_p->current_state = OpState::Inner;

            // If we see a match after popping the first one
            // then quit aborting
            if(snapshot_p->node_p == GetNode(snapshot_p->node_id)) {
              break;
            }
          }while(1);
          // This calls destructor for all NodeSnapshot object, and
          // will release memory for logical node object
          //path_list_p->clear();

          context_p->abort_counter++;
          //context_p->current_level = 0;
          //context_p->current_state = OpState::Init;
          context_p->abort_flag = false;

          break;
        } // case Abort
        default: {
          bwt_printf("ERROR: Unknown State: %d\n",
                     static_cast<int>(context_p->current_state));
          assert(false);
          break;
        }
      } // switch current_state
    } // while(1)

    return;
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
  inline NodeID LocateSeparatorByKey(const KeyType &search_key,
                                     const InnerNode *inner_node_p,
                                     const KeyType *ubound_p) {
    const std::vector<KeyNodeIDPair> *sep_list_p = &inner_node_p->sep_list;
    
    // Inner node could not be empty
    assert(sep_list_p->size() != 0UL);
    
    // If search key >= upper bound (natural or artificial) then
    // we have hit the wrong inner node
    assert(KeyCmpLess(search_key, *ubound_p));

    // Search key must be greater than or equal to the lower bound
    // which is assumed to be a constant associated with a NodeID
    assert(KeyCmpGreaterEqual(search_key, inner_node_p->metadata.lbound));

    // Hopefully std::upper_bound would use binary search here
    auto it = std::upper_bound(sep_list_p->begin(),
                               sep_list_p->end(),
                               std::make_pair{search_key, INVALID_NODE_ID},
                               [this](const KeyNodeIDPair &knp1,
                                      const KeyNodeIDPair &knp2) {
                                 return this->key_node_id_pair_cmp_obj(knp1,
                                                                       knp2);
                               });
                               
    // This is impossible since if the first element greater than key
    // then key < low key which is a violation of the invariant that search key
    // must >= low key and < high key
    assert(it != sep_list_p->begin());
    
    // Since upper_bound returns the first element > given key
    // so we need to decrease it to find the last element <= given key
    // which is out separator key
    it--;

    // This assertion failure could only happen if we
    // hit +Inf as separator
    assert(it->node != INVALID_NODE_ID);

    return it->node;
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
   * After returning of this function please remember to check the flag
   * and update path history. (Such jump may happen multiple times, so
   * do not make any assumption about how jump is performed)
   *
   * NOTE 2: This function is different from Navigate leaf version because it
   * takes an extra argument for remembering the separator key associated
   * with the NodeID.
   */
  inline NodeID NavigateInnerNode(Context *context_p) {
    // First get the snapshot from context
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);
    
    // Save some keystrokes
    const BaseNode *node_p = snapshot_p->node_p;

    // This search key will not be changed during navigation
    const KeyType &search_key = context_p->search_key;

    // Make sure the structure is valid
    assert(snapshot_p->is_leaf == false);
    assert(snapshot_p->node_p != nullptr);
    assert(snapshot_p->node_id != INVALID_NODE_ID);

    while(1) {
      NodeType type = node_p->GetType();

      switch(type) {
        case NodeType::InnerType: {
          // If we reach here, then either there is no split node
          // in which case the high key does not change compares with the top
          // node's high key
          // or there is a split node, but the split key > search key
          // so we know search key definitely < current top node high key
          if(KeyGreaterEqual(context_p->search_key,
                             node_p->metadata.ubound) == true) {
            // The node has splited but we did not see split node
            // it must have been consolidated after partial split delta being
            // finished. Abort here and go back one level to get the latest
            // Key-NodeID pair
            context_p->abort_flag = true;
            
            return;
          }
          
          const InnerNode *inner_node_p = \
            static_cast<const InnerNode *>(node_p);

          // We always use the ubound recorded inside the top of the
          // delta chain
          NodeID target_id = LocateSeparatorByKey(search_key,
                                                  inner_node_p,
                                                  &node_p->metadata.ubound);

          bwt_printf("Found child in inner node; child ID = %lu\n",
                     target_id);

          return target_id;
        } // case InnerType
        case NodeType::InnerRemoveType: {
          bwt_printf("ERROR: InnerRemoveNode not allowed\n");

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

            // Try to jump to the right branch
            // If jump fails just abort
            JumpToNodeID(branch_id,
                         context_p); 

            if(context_p->abort_flag == true) {
              bwt_printf("JumpToNodeID aborts. ABORT\n");

              return INVALID_NODE_ID;
            }

            snapshot_p = GetLatestNodeSnapshot(context_p);
            node_p = snapshot_p->node_p;

            // Continue in the while loop to avoid setting first_time to false
            continue;
          } else {
            node_p = split_node_p->child_node_p;
          }

          break;
        } // case InnerSplitType
        case NodeType::InnerMergeType: {
          const InnerMergeNode *merge_node_p = \
            static_cast<const InnerMergeNode *>(node_p);

          const KeyType &merge_key = merge_node_p->merge_key;

          // Here since we will only take one branch, so
          // high key does not need to be updated
          // Since we still could not know the high key
          if(KeyCmpGreaterEqual(search_key, merge_key)) {
            node_p = merge_node_p->right_merge_p;
          } else {
            node_p = merge_node_p->child_node_p;
          }

          break;
        } // InnerMergeType
        default: {
          bwt_printf("ERROR: Unknown node type = %d",
                     static_cast<int>(type));

          assert(false);
        }
      } // switch type
    } // while 1

    // Should not reach here
    assert(false);
    return INVALID_NODE_ID;
  }

  /*
   * CollectAllSepsOnInner() - Collect all separators given a snapshot
   *
   * This function consolidates a given delta chain given the node snapshot.
   * Consolidation takes place by using a deleted set and present set to record
   * values already deleted and still exist in the current node, to achieve
   * the same effect as a log replay.
   *
   * This function returns an inner node with all Key-NodeID pairs
   * sorted by the key order
   */
  inline InnerNode *CollectAllSepsOnInner(NodeSnapshot *snapshot_p) {

    // TODO: May need to find a better bucket size to initialize the hash
    // set in order to decrease rehash
    
    KeyNodeIDPairSet present_set{INNER_NODE_SIZE_UPPER_THRESHOLD,
                                 key_node_id_pair_hash_obj,
                                 key_node_id_pair_eq_obj};
                                 
    KeyNodeIDPairSet deleted_set{INNER_NODE_SIZE_UPPER_THRESHOLD,
                                 key_node_id_pair_hash_obj,
                                 key_node_id_pair_eq_obj};

    // Note that in the recursive call node_p might change
    // but we should not change the metadata
    const BaseNode *node_p = snapshot_p->node_p;
    
    // This will fill in two sets with values present in the inner node
    // and values deleted
    CollectAllSepsOnInnerRecursive(node_p,
                                   node_p->metadata,
                                   present_set,
                                   deleted_set);

    // The effect of this function is a comsolidation into inner node
    InnerNode *inner_node_p = new InnerNode{node_p->metadata.lbound,
                                            node_p->metadata.ubound,
                                            node_p->metadata.next_node_id};

    // The size of the inner node is exactly the size of the present set
    size_t key_num = present_set.size();
    
    // Save some typing
    auto sep_list_p = &inner_node_p->sep_list;
    
    // So we reserve that much space inside sep list to avoid allocation
    // overhead
    sep_list_p->reserve(key_num);
    
    // Copy the set into the vector using bulk load
    sep_list_p->assign(present_set.begin(), present_set.end());

    // Sort the key-NodeId array using std::sort and the comparison object
    // we defined
    std::sort(sep_list_p->begin(),
              sep_list_p->end(),
              key_node_id_pair_cmp_obj);

    return inner_node_p;
  }

  /*
   * CollectAllSepsOnInnerRecursive() - This is the counterpart on inner node
   *
   * Please refer to the function on leaf node for details. These two have
   * almost the same logical flow
   */
  void
  CollectAllSepsOnInnerRecursive(const BaseNode *node_p,
                                 const NodeMetadata &metadata,
                                 KeyNodeIDPairSet &present_set,
                                 KeyNodeIDPairSet &deleted_set) const {

    while(1) {
      NodeType type = node_p->GetType();

      switch(type) {
        case NodeType::InnerType: {
          const InnerNode *inner_node_p = \
            static_cast<const InnerNode *>(node_p);

          // This search for the first key >= high key of the current node
          // being consolidated
          // This is exactly where we should stop copying
          // The return value might be end() iterator, but it is also
          // consistent
          auto copy_end_it = std::lower_bound(inner_node_p->sep_list.begin(),
                                              inner_node_p->sep_list.end(),
                                              metadata.ubound,
                                              key_node_id_pair_cmp_obj);
          // This could not be true since if this happens then all elements
          // in the sep list of inner node >= high key
          // which also implies low key >= high key
          assert(copy_end_it != inner_node_p->sep_list.begin());

          for(auto it = inner_node_p->sep_list.begin();
              it != copy_end_it;
              it++) {
            if(deleted_set.find(*it) == deleted_set.end()) {
              // 1. If present_set does not contain the key, then it is inserted
              // 2. If present_set already contains the key, then
              //    it is not inserted by definition. This is desired behavior
              //    since it means a more up-to-date insertion happened
              present_set.insert(*it);
            }
          }

          return;
        } // case InnerType
        case NodeType::InnerRemoveType: {
          bwt_printf("ERROR: InnerRemoveNode not allowed\n");

          assert(first_time == true);
          assert(false);
          return;
        } // case InnerRemoveType
        case NodeType::InnerInsertType: {
          const InnerInsertNode *insert_node_p = \
            static_cast<const InnerInsertNode *>(node_p);

          const KeyType &insert_key = insert_node_p->insert_key;
          assert(insert_node_p->new_node_id != INVALID_NODE_ID);

          // Only consider insertion if it is inside the range of
          // current node
          if(KeyCmpLess(insert_key, metadata.ubound) == true) {
            if(deleted_set.find(*it) == deleted_set.end()) {
              present_set.insert(*it);
            }
          }

          // Go to next node
          node_p = insert_node_p->child_node_p;

          break;
        } // case InnerInsertType
        case NodeType::InnerDeleteType: {
          const InnerDeleteNode *delete_node_p = \
            static_cast<const InnerDeleteNode *>(node_p);

          const KeyType &delete_key = delete_node_p->delete_key;

          if(KeyCmpLess(delete_key, metadata.ubound) == true) {
            if(present_set.find(*it) == present_set.end()) {
              deleted_set.insert(*it);
            }
          }

          node_p = delete_node_p->child_node_p;

          break;
        } // case InnerDeleteType
        case NodeType::InnerSplitType: {
          node_p = split_node_p->child_node_p;

          break;
        } // case InnerSplitType
        case NodeType::InnerMergeType: {
          const InnerMergeNode *merge_node_p = \
            static_cast<const InnerMergeNode *>(node_p);

          // NOTE: We alaways use the same metadata object which is the
          // one passed by the wrapper. Though node_p changes for each
          // recursive call, metadata should not change and should remain
          // constant
          
          CollectAllSepsOnInnerRecursive(merge_node_p->child_node_p,
                                         metadata,
                                         present_set,
                                         deleted_set);

          CollectAllSepsOnInnerRecursive(merge_node_p->right_merge_p,
                                         metadata,
                                         present_set,
                                         deleted_set);

          // There is no unvisited node
          return;
        } // case InnerMergeType
        default: {
          bwt_printf("ERROR: Unknown inner node type = %d\n",
                 static_cast<int>(type));

          assert(false);
          return;
        }
      } // switch type
    } // while(1)

    // Should not get to here
    assert(false);
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
   * This function takes an extra argument to decide whether it collects any
   * value. If collect_value is set to false, then this function only traverses
   * the leaf node without collecting any actual value, while still being
   * able to traverse to its left sibling and potentially abort
   *
   * NOTE: If there is prior data in its logical node when calling this function
   * then we simply turn off collect_value flag in order to make use of
   * these values and avoid map insert conflict.
   *
   * NOTE 2: After jumping to a new NodeID in this function there will be similar
   * problems since now the snapshot has been changed, and we need to check
   * whether there is data in the map again to make sure there is no
   * map insert conflict
   */
  void NavigateLeafNode(Context *context_p,
                        std::vector<ValueType> value_list) {
    // This contains information for current node
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);
    const BaseNode *node_p = snapshot_p->node_p;
    
    // This is used to control which values will be collected
    const NodeMetaData &metadata = node_p->metadata;
    
    // We only collect values for this key
    const KeyType &search_key = context_p->search_key;

    assert(snapshot_p->is_leaf == true);
    
    // This two are used to replay the log
    // As an optimization we just use value set
    ValueSet present_set{LEAF_NODE_SIZE_UPPER_THRESHOLD,
                         value_hash_obj,
                         value_eq_obj};
                         
    ValueSet deleted_set{LEAF_NODE_SIZE_UPPER_THRESHOLD,
                         value_hash_obj,
                         value_eq_obj};

    while(1) {
      NodeType type = node_p->GetType();
      
      switch(type) {
        case NodeType::LeafType: {
          // If the node has been splited but we did not see
          // a split delta such that the search path was not directed
          // to the new path
          if(KeyCmpGreaterEqual(search_key, metadata.ubound) == true) {
            context_p->abort_flag = true;
            
            return;
          }
          
          const LeafNode *leaf_node_p = \
            static_cast<const LeafNode *>(node_p);

          // Here we know the search key < high key of current node
          // NOTE: We only compare keys here, so it will get to the first
          // element >= search key
          auto copy_start_it = \
            std::lower_bound(leaf_node_p->data_list.begin(),
                             leaf_node_p->data_list.end(),
                             std::make_pair<search_key, ValueType{}>,
                             key_value_pair_cmp_obj);

          // If there is something to copy
          while((copy_start_it != leaf_node_p->data_list.end()) && \
                (KeyCmpEqual(copy_start_it->first, search_key))) {
            // If the value has not been deleted then just insert
            if(deleted_set.find(copy_start_it->first) == deleted_set.end()) {
              present_set.insert(copy_start_it->second);
            }
            
            copy_start_it++;
          }
          
          // Reserve that much space to hold all values.
          // We use reserve to avoid reallocation
          value_list.reserve(present_set.size());
          
          // Copy all elements in present_set to the vector
          value_list.assign(present_set.begin(), present_set.end());

          return;
        }
        case NodeType::LeafInsertType: {
          const LeafInsertNode *insert_node_p = \
            static_cast<const LeafInsertNode *>(node_p);

          if(KeyCmpEqual(search_key, insert_node_p->inserted_item.first)) {
            if(deleted_set.find(insert_node_p->inserted_item.second) == \
               deleted_set.end()) {
              present_set.insert(insert_node_p->inserted_item.second);
            }
          }

          node_p = insert_node_p->child_node_p;

          break;
        } // case LeafInsertType
        case NodeType::LeafDeleteType: {
          const LeafDeleteNode *delete_node_p = \
            static_cast<const LeafDeleteNode *>(node_p);

          if(KeyCmpEqual(search_key, delete_node_p->deleted_item.first)) {
            if(present_set.find(delete_node_p->deleted_item.srcond) == \
               present_set.end()) {
              deleted_set.insert(delete_node_p->deleted_item.second);
            }
          }

          node_p = delete_node_p->child_node_p;

          break;
        } // case LeafDeleteType
        case NodeType::LeafUpdateType: {
          const LeafUpdateNode *update_node_p = \
            static_cast<const LeafUpdateNode *>(node_p);

          // Internally we treat update node as two operations
          // but they must be packed together to make an atomic step
          if(KeyCmpEqual(search_key, update_node_p->update_key)) {
            if(deleted_set.find(update_node_p->new_value) == deleted_set.end()) {
              present_set.insert(update_node_p->new_value);
            }
            
            if(present_set.find(update_node_p->old_value) == present_set.end()) {
              deleted_set.insert(update_node_p->old_value);
            }
          }

          node_p = update_node_p->child_node_p;

          break;
        } // case LeafUpdateType
        case NodeType::LeafRemoveType: {
          bwt_printf("ERROR: Observed LeafRemoveNode in delta chain\n");

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

          const KeyType &split_key = split_node_p->split_key;

          if(KeyCmpGreaterEqual(search_key, split_key)) {
            bwt_printf("Take leaf split right (NodeID branch)\n");

            // Since we are on the branch side of a split node
            // there should not be any record with search key in
            // the chain from where we come since otherwise these
            // records are misplaced
            assert(present_set.size() == 0UL);
            assert(deleted_set.size() == 0UL);

            NodeID split_sibling_id = split_node_p->split_sibling;

            // Jump to right sibling, with possibility that it aborts
            JumpToNodeID(split_sibling_id,
                         context_p);

            if(context_p->abort_flag == true) {
              bwt_printf("JumpToNodeID aborts. ABORT\n");

              return;
            }

            // These three needs to be refreshed after switching node
            snapshot_p = GetLatestNodeSnapshot(context_p);
            node_p = snapshot_p->node_p;
            
            // Must update metadata here since we have changed to a different
            // logical node
            metadata = node_p->metadata;
          } else {
            node_p = split_node_p->child_node_p;
          }

          break;
        } // case LeafSplitType
        default: {
          bwt_printf("ERROR: Unknown leaf delta node type: %d\n",
                     static_cast<int>(node_p->GetType()));

          assert(false);
        } // default
      } // switch
    } // while

    // We cannot reach here
    assert(false);
    return;
  }
  
  /*
   * NavigateLeafNode() - Check existence for a certain value
   *
   * Please note that this function is overloaded. This version is used for
   * insert/delete/update that only checks existence of values, rather than
   * collecting all values for a single key.
   *
   * This function works by traversing down the delta chain and compare
   * values with those in delta nodes and in the base node. No special data
   * structure is required
   *
   * This function calls JumoToNodeID() to switch to a split sibling node
   * There are possibility that the switch aborts, and in this case this
   * function returns with value false.
   */
  bool void NavigateLeafNode(Context *context_p,
                             ValueType &search_value) {
    assert(snapshot_p->is_leaf == true);
    
    // Snapshot pointer, node pointer, and metadata reference all need
    // updating once LoadNodeID() returns with success
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);
    const BaseNode *node_p = snapshot_p->node_p;

    // This is used to decide whether we have reached the incorrect
    // leaf base node, by checking the high key against search key
    // This could happen if there had been a split delta and was
    // consolidated such that the current thread does not have a change
    // to even jump to the left sibling
    const NodeMetaData &metadata = node_p->metadata;

    // Save some typing
    const KeyType &search_key = context_p->search_key;

    while(1) {
      NodeType type = node_p->GetType();

      switch(type) {
        case NodeType::LeafType: {
          // If the node has been splited but we did not see
          // a split delta such that the search path was not directed
          // to the new path
          if(KeyCmpGreaterEqual(search_key, metadata.ubound) == true) {
            context_p->abort_flag = true;

            return;
          }

          const LeafNode *leaf_node_p = \
            static_cast<const LeafNode *>(node_p);

          // Here we know the search key < high key of current node
          // NOTE: We only compare keys here, so it will get to the first
          // element >= search key
          auto scan_start_it = \
            std::lower_bound(leaf_node_p->data_list.begin(),
                             leaf_node_p->data_list.end(),
                             std::make_pair<search_key, ValueType{}>,
                             key_value_pair_cmp_obj);

          // Search all values with the search key
          while((scan_start_it != leaf_node_p->data_list.end()) && \
                (KeyCmpEqual(scan_start_it->first, search_key))) {
            // If there is a value matching the search value then return true
            // We do not need to check any delete set here, since if the
            // value has been deleted earlier then this function would
            // already have returned
            if(ValueCmpEqual(scan_start_it->second, search_value) == true) {
              return true;
            }

            copy_start_it++;
          }

          return false;
        }
        case NodeType::LeafInsertType: {
          const LeafInsertNode *insert_node_p = \
            static_cast<const LeafInsertNode *>(node_p);

          if(KeyCmpEqual(search_key, insert_node_p->inserted_item.first)) {
            if(ValueCmpEqual(insert_node_p->inserted_item.second,
                             search_value) == true) {
              return true;
            }
          }

          node_p = insert_node_p->child_node_p;

          break;
        } // case LeafInsertType
        case NodeType::LeafDeleteType: {
          const LeafDeleteNode *delete_node_p = \
            static_cast<const LeafDeleteNode *>(node_p);

          // If the value was deleted then return false
          if(KeyCmpEqual(search_key, delete_node_p->delete_key)) {
            if(ValueCmpEqual(delete_node_p->deleted_item.second,
                             search_value) == true) {
              return false;
            }
          }

          node_p = delete_node_p->child_node_p;

          break;
        } // case LeafDeleteType
        case NodeType::LeafUpdateType: {
          const LeafUpdateNode *update_node_p = \
            static_cast<const LeafUpdateNode *>(node_p);

          // Internally we treat update node as two operations
          // but they must be packed together to make an atomic step
          if(KeyCmpEqual(search_key, update_node_p->update_key)) {
            if(ValueCmpEqual(update_node_p->new_value, search_value) == true) {
              return true;
            }

            if(ValueCmpEqual(update_node_p->old_value, search_value) == true) {
              return false;
            }
          }

          node_p = update_node_p->child_node_p;

          break;
        } // case LeafUpdateType
        case NodeType::LeafRemoveType: {
          bwt_printf("ERROR: Observed LeafRemoveNode in delta chain\n");

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

          const KeyType &split_key = split_node_p->split_key;

          if(KeyCmpGreaterEqual(search_key, split_key)) {
            bwt_printf("Take leaf split right (NodeID branch)\n");

            NodeID split_sibling_id = split_node_p->split_sibling;

            // Jump to right sibling, with possibility that it aborts
            JumpToNodeID(split_sibling_id,
                         context_p);

            if(context_p->abort_flag == true) {
              bwt_printf("JumpToNodeID aborts. ABORT\n");

              return false;
            }

            // These three needs to be refreshed after switching node
            snapshot_p = GetLatestNodeSnapshot(context_p);
            node_p = snapshot_p->node_p;

            // Must update metadata here since we have changed to a different
            // logical node
            metadata = node_p->metadata;
          } else {
            node_p = split_node_p->child_node_p;
          }

          break;
        } // case LeafSplitType
        default: {
          bwt_printf("ERROR: Unknown leaf delta node type: %d\n",
                     static_cast<int>(node_p->GetType()));

          assert(false);
        } // default
      } // switch
    } // while

    // We cannot reach here
    assert(false);
    return false;
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
   */
  void
  CollectAllValuesOnLeafRecursive(const BaseNode *node_p,
                                  const NodeMetadata &metadata,
                                  KeyValuePairSet &present_set,
                                  KeyValuePairSet &deleted_set) const {
    while(1) {
      NodeType type = node_p->GetType();

      switch(type) {
        // When we see a leaf node, just copy all keys together with
        // all values into the value set
        case NodeType::LeafType: {
          const LeafNode *leaf_node_p = \
            static_cast<const LeafNode *>(node_p);

          // This points copy_end_it to the first element >= current high key
          // If no such element exists then copy_end_it is end() iterator
          // which is also consistent behavior
          // However, copy_end_it could not be begin() iterator since it
          // implies all low key (which equals the first element) >= high key
          auto copy_end_it = std::lower_bound(leaf_node_p->data_list.begin(),
                                              leaf_node_p->data_list.end(),
                                              metadata.ubound,
                                              key_value_pair_cmp_obj);
                                              
          assert(copy_end_it != leaf_node_p->data_list.begin());

          for(auto it = leaf_node_p->data_list.begin();
              it != copy_end_it;
              it++) {
            if(deleted_set.find(*it) == deleted_set.end()) {
              // 1. If present_set does not contain the key, then it is inserted
              // 2. If present_set already contains the key, then
              //    it is not inserted by definition. This is desired behavior
              //    since it means a more up-to-date insertion happened
              present_set.insert(*it);
            }
          }

          return;
        } // case LeafType
        case NodeType::LeafInsertType: {
          const LeafInsertNode *insert_node_p = \
            static_cast<const LeafInsertNode *>(node_p);

          if(deleted_set.find(insert_node_p->inserted_item) == \
             deleted_set.end()) {
            present_set.insert(insert_node_p->inserted_item);
          }

          node_p = insert_node_p->child_node_p;

          break;
        } // case LeafInsertType
        case NodeType::LeafDeleteType: {
          const LeafDeleteNode *delete_node_p = \
            static_cast<const LeafDeleteNode *>(node_p);

          if(present_set.find(delete_node_p->deleted_item) == \
             present_set.end()) {
            deletd_set.insert(delete_node_p->deleted_item);
          }

          node_p = delete_node_p->child_node_p;

          break;
        } // case LeafDeleteType
        case NodeType::LeafUpdateType: {
          const LeafUpdateNode *update_node_p = \
            static_cast<const LeafUpdateNode *>(node_p);

          KeyValuePair old_item = std::make_pair(update_node_p->update_key,
                                                 update_node_p->old_value);
          KeyValuePair new_item = std::make_pair(update_node_p->update_key,
                                                 update_node_p->new_value);
                                                 
          if(deleted_set.find(new_item) == deleted_set.end()) {
            present_set.insert(new_item);
          }
          
          if(present_set.find(old_item) == present_set.end()) {
            deletd_set.insert(old_item);
          }

          node_p = update_node_p->child_node_p;

          break;
        } // case LeafUpdateType
        case NodeType::LeafRemoveType: {
          bwt_printf("ERROR: LeafRemoveNode not allowed\n");

          assert(false);
        } // case LeafRemoveType
        case NodeType::LeafSplitType: {
          const LeafSplitNode *split_node_p = \
            static_cast<const LeafSplitNode *>(node_p);
            
          node_p = split_node_p->child_node_p;

          break;
        } // case LeafSplitType
        case NodeType::LeafMergeType: {
          const LeafMergeNode *merge_node_p = \
            static_cast<const LeafMergeNode *>(node_p);

          /**** RECURSIVE CALL ON LEFT AND RIGHT SUB-TREE ****/
          CollectAllValuesOnLeafRecursive(merge_node_p->child_node_p,
                                          metadata,
                                          present_set,
                                          deleted_set);

          CollectAllValuesOnLeafRecursive(merge_node_p->right_merge_p,
                                          metadata,
                                          present_set,
                                          deleted_set);

          return;
        } // case LeafMergeType
        default: {
          bwt_printf("ERROR: Unknown node type: %d\n",
                     static_cast<int>(type));

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
  inline LeafNode *CollectAllValuesOnLeaf(NodeSnapshot *snapshot_p) {
    assert(snapshot_p->is_leaf == true);
    
    // These two are used to replay the log
    // NOTE: We use the threshold for splitting leaf node
    // as the number of bucket
    KeyValuePairSet present_set{LEAF_NODE_SIZE_UPPER_THRESHOLD,
                                key_value_pair_hash_obj,
                                key_value_pair_eq_obj};
    KeyValuePairSet deleted_set{LEAF_NODE_SIZE_UPPER_THRESHOLD,
                                key_value_pair_hash_obj,
                                key_value_pair_eq_obj};

    const BaseNode *node_p = snashot_p->node_p;
    
    // We collect all valid values in present_set
    // and deleted_set is just for bookkeeping
    CollectAllValuesOnLeafRecursive(node_p,
                                    node_p->metadata,
                                    present_set,
                                    deleted_set);

    LeafNode *leaf_node_p = new LeafNode{node_p->metadata.lbound,
                                         node_p->metadata.ubound,
                                         node_p->metadata.next_node_id};

    std::vector<KeyValuePair> *data_list_p = &leaf_node_p->data_list;
    
    // Reserve that much space for items to avoid allocation in the future
    size_t item_num = present_set.size();
    data_list_p->reserve(item_num);
    
    // Copy the entire set into the vector
    data_list_p->assign(present_set.begin(), present_set.end());
    
    // Sort using only key value
    std::sort(data_list_p->begin(), data_list_p->end(), key_value_pair_cmp_obj);

    // We reserve that many space for storing the prefix sum
    // Note that if the node is going to be consolidated then this will
    // definitely cause reallocation
    leaf_node_p->item_prefix_sum.reserve(LEAF_NODE_SIZE_UPPER_THRESHOLD);

    auto range_begin_it = data_list_p->begin();
    auto end_it = data_list_p->end();

    // This is used to compute prefix sum of distinct elements
    int prefix_sum = 0;
    
    while(range_begin_it != end_it) {
      // Search for the first item whose key > current key
      // and their difference is the number of elements
      auto range_end_it = std::upper_bound(range_begin_it,
                                           end_it,
                                           key_value_pair_cmp_obj);
      
      // The first element is always 0 since the index starts with 0
      data_list_p->item_prefix_sum.push_back(prefix_sum);
      
      // Then increase prefix sum with the length of the range
      // which is also the distance between the two variables
      prefix_sum += (std::distance(range_begin_it, range_end_it));
      
      // Start from the end of current range which is the next key
      // If there is no more elements then std::upper_bound() returns
      // end() iterator, which would fail while loop testing
      range_begin_it = range_end_it;
    }

    return leaf_node_p;
  }

  /*
   * GetLatestNodeSnapshot() - Return a pointer to the most recent snapshot
   *
   * This is wrapper that includes size checking
   *
   * NOTE: The snapshot object is not valid after it is popped out of
   * the vector, since at that time the snapshot object will be destroyed
   * which also freed up the logical node object
   */
  inline NodeSnapshot *GetLatestNodeSnapshot(Context *context_p) const {
    std::vector<NodeSnapshot> *path_list_p = \
      &context_p->path_list;

    assert(path_list_p->size() > 0);

    return &(path_list_p->back());
  }

  /*
   * GetLatestParentNodeSnapshot() - Return the pointer to the parent node of
   *                                 the current node
   *
   * This function assumes the current node is always the one on the top of
   * the stack.
   *
   * NOTE: The same as GetLatestNodeSnapshot(), be careful when popping
   * snapshots out of the stack
   */
  inline NodeSnapshot *GetLatestParentNodeSnapshot(Context *context_p) {
    std::vector<NodeSnapshot> *path_list_p = \
      &context_p->path_list;

    // Make sure the current node has a parent
    size_t path_list_size = path_list_p->size();
    assert(path_list_size >= 2);

    // This is the address of the parent node
    return &((*path_list_p)[path_list_size - 2]);
  }

  /*
   * JumpToLeftSibling() - Jump to the left sibling given a node
   *
   * When this function is called, we assume path_list_p includes the
   * NodeSnapshot object for the current node being inspected, since
   * we want to pass the information of whether the current node is
   * the left most child.
   *
   * This function uses the fact that the mapping relationship between NodeID
   * and low key does not change during the lifetime of the NodeID. This way,
   * after we have fixed a snapshot of the current node, even though the node
   * could keep changing, using the information given in the snapshot
   * we could know at least a starting point for us to traverse right until
   * we see a node whose high key equals the low key we are searching for,
   * or whose range covers the low key (in the latter case, we know the
   * merge delta has already been posted)
   *
   * NOTE: This function might abort. Please check context and make proper
   * decisions
   */
  void JumpToLeftSibling(Context *context_p) {
    bwt_printf("Jumping to the left sibling\n");

    assert(context_p->HasParentNode());

    // Get last record which is the current node's context
    // and we must make sure the current node is not left mode node
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);

    // Check currently we are on a remove node
    assert(snapshot_p->node_p->IsRemoveType());

    // This is the low key of current removed node. Also
    // the low key of the separator-ID pair
    const KeyType *removed_lbound_p = &snapshot_p->node_p->metadata.lbound;
    
    // We use this to test whether we have found the real
    // left sibling whose next node id equals this one
    const NodeID removed_node_id = snapshot_p->node_id;
    
    // After this point snapshot_p could be overwritten

    // Get its parent node
    NodeSnapshot *parent_snapshot_p = GetLatestParentNodeSnapshot(context_p);
    assert(parent_snapshot_p->is_leaf == false);

    // We either consolidate the parent node to get an inner node
    // or directly use the parent node_p if it is already inner node
    const InnerNode *inner_node_p = \
      static_cast<const InnerNode *>(parent_snapshot_p->node_p);
    
    // If the parent node is not inner node (i.e. has delta chain)
    // then consolidate it to get an inner node
    if(parent_snapshot_p->node_p->IsInnerNode() == false) {
      inner_node_p = CollectAllSepsOnInner(parent_snapshot_p);
      
      // As an optimization, we CAS the consolidated version of InnerNode
      // here. If CAS fails, we SHOULD NOT delete the inner node immediately
      // since its value is still being used by this function
      // So we delay allocation by putting it into the garbage chain
      bool ret = InstallNodeToReplace(parent_snapshot_p.node_id,
                                      inner_node_p,
                                      parent_snapshot_p->node_p);

      // If CAS succeeds we update the node_p in parent snapshot
      // If CAS fails we delete inner node allocated in CollectAllSeps..()
      if(ret == true) {
        parent_snapshot_p->node_p = inner_node_p;
      } else {
        epoch_manager.AddGarbageNode(inner_node_p);
      }
    }
    
    // This returns the first iterator >= current low key
    // And we decrease
    auto it = std::lower_bound(inner_node_p->sep_list.begin(),
                               inner_node_p->sep_list.end(),
                               // We use INVALID_NODE_ID since the comp obj does
                               // not compare node id field
                               std::make_pair(*removed_lbound_p,
                                              INVALID_NODE_ID),
                               key_node_id_pair_cmp_obj);
    
    if(it == inner_node_p->sep_list.begin()) {
      bwt_printf("Current parent snapshot indicates we"
                 " are on leftmost child\n");
      bwt_printf("    But actually seen RemoveDelta."
                 " Parent must have merged. ABORT\n");
      
      context_p->abort_flag = true;
      
      return;
    }
    
    it--;
    
    // Note that after this point the inner node is still being used since
    // we have iterator referring to the internal structure of the inner node
    // So we could not try CAS here

    // This is our starting point to traverse right
    NodeID left_sibling_id = it->second;

    while(1) {
      // It is impossible that the current node is merged
      // and its next node ID is INVALID_NODE_ID
      // In this case we would catch that by the range check
      if(left_sibling_id == INVALID_NODE_ID) {
        bwt_printf("Have reached the end of current level. "
                   "But it should be caught by range check\n");

        assert(false);
      }

      // This might incur recursive update
      // We need to pass in the low key of left sibling node
      JumpToNodeID(left_sibling_id, context_p);

      if(context_p->abort_flag == true) {
        bwt_printf("JumpToLeftSibling()'s call to JumpToNodeID() ABORT\n")
        
        return;
      }

      // Read the potentially redirected snapshot
      // (but do not pop it - so we directly return)
      snapshot_p = GetLatestNodeSnapshot(context_p);

      // Get high key and left sibling NodeID
      const KeyType *left_ubound_p = &snapshot_p->node_p->metadata.ubound;

      // This will be used in next iteration
      // NOTE: The variable name is a little bit confusing, because
      // this variable was created outside of the loop
      left_sibling_id = snapshot_p->node_p->metadata.next_node_id;

      // If the high key of the left sibling equals the low key
      // then we know it is the real left sibling
      // Or the node has already been consolidated, then the range
      // of the node would cover current low key
      if(KeyCmpEqual(*left_ubound_p, *removed_lbound_p)) {
        bwt_printf("Find a exact match of low/high key\n");

        // We need to take care that high key is not sufficient for identifying
        // the correct left sibling
        if(left_sibling_id == removed_node_id) {
          bwt_printf("Find real left sibling, next id == removed id\n");

          // Quit the loop. Do not return
          break;
        } else {
          bwt_printf("key match but next node ID does not match. ABORT\n");
          bwt_printf("    (Maybe it has been merged and then splited?)\n");

          // If key match but next node ID does not match then just abort
          // since it is the case that left sibling has already been merged
          // with the removed node, and then it splits
          // We could not handle such case, so just abort
          context_p->abort_flag = true;

          // Return and propagate abort information
          return;
        }
      } else if(KeyCmpGreater(*left_ubound_p, *removed_lbound_p)) {
        bwt_printf("The range of left sibling covers current node\n");
        bwt_printf("    Don't know for sure what happened\n");

        // We also do not know what happened since it could be possible
        // that the removed node is merged, consolidated, and then splited
        // on a split key greater than the previous merge key
        context_p->abort_flag = true;

        return;
      } else {
        // We know it will never be invalid node id since ubound_p < lbound_p
        // which implies the high key is not +Inf, so there are other nodes to
        // its right side
        assert(left_sibling_id != INVALID_NODE_ID);
      }
    } // while(1)

    return;
  }

  /*
   * TakeNodeSnapshot() - Take the snapshot of a node by pushing node information
   *
   * This function simply copies NodeID, and physical pointer into
   * the snapshot object. Node that the snapshot object itself is directly
   * constructed on the path list which is a vector. This avoids copying the
   * NodeSnapshot object from the stack to vector
   */
  void TakeNodeSnapshot(NodeID node_id,
                        Context *context_p) {
    const BaseNode *node_p = GetNode(node_id);
    std::vector<NodeSnapshot> *path_list_p = &context_p->path_list;
    
    // As an optimization we construct NodeSnapshot inside the vector
    // instead of copying it
    path_list_p->emplace_back(node_p->IsOnLeafDeltaChain(), this);
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);

    snapshot_p->node_id = node_id;
    
    // If current state is Init state then we know we are now on the root
    snapshot_p->is_root = (context_p->current_state == OpState::Init);

    return;
  }

  /*
   * UpdateNodeSnapshot() - Update an existing node snapshot
   *
   * This function does not create and push new NodeSnapshot object into the
   * stack; instead it modifies existing NodeSnapshot object on top of the
   * path stack, using the given NodeID and low key
   *
   * NOTE: The left most child flag will be ignored because in current
   * design when this is called we always go within the same parent node
   *
   * NOTE 2: If right traversal cross parent node boundary then NOTE 1
   * is no longer true
   *
   * NOTE 3: Since we always traverse on the same level, the leaf/inner identity
   * does not change
   *
   * NOTE 4: We need to clear all cached data and metadata inside the logical
   * node since it is not changed to another NodeID
   *
   * NOTE 5: IF NODE ID DOES NOT CHANGE, DO NOT CALL THIS FUNCTION
   * SINCE THIS FUNCTION RESETS ROOT IDENTITY
   * Call SwitchPhysicalPointer() instead
   *
   * TODO: In the future add an option to make it turn off self-checking
   * (i.e. checking whether switching to itself) whenever requested
   */
  void UpdateNodeSnapshot(NodeID node_id,
                          Context *context_p) {
    const BaseNode *node_p = GetNode(node_id);

    // We operate on the latest snapshot instead of creating a new one
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);

    // Assume we always use this function to traverse on the same
    // level
    assert(node_p->IsOnLeafDeltaChain() == snapshot_p->is_leaf);

    // Make sure we are not switching to itself
    assert(snapshot_p->node_id != node_id);
    snapshot_p->node_id = node_id;

    // We only call UpdateNodeSnapshot() when we switch to split
    // sibling in Navigate function and jump to left sibling
    // When updating the physical pointer of a root node after posting
    // a delta, please call SwitchPhysicalPointer() instead
    snapshot_p->is_root = false;

    return;
  }

  /*
   * LoadNodeID() - Push a new snapshot for the node pointed to by node_id
   *
   * If we just want to modify existing snapshot object in the stack, we should
   * call JumpToNodeID() instead
   *
   * NOTE: If this function is called when we are still in Init mode, then
   * root flag is set, since we know now we are definitely
   * loading the ID for root node
   */
  inline void LoadNodeID(NodeID node_id,
                         Context *context_p) {
    bwt_printf("Loading NodeID = %lu\n", node_id);

    // This pushes a new snapshot into stack
    TakeNodeSnapshot(node_id, context_p);
    
    bool recommend_consolidation = FinishPartialSMO(context_p);

    if(context_p->abort_flag == true) {
      return;
    }

    ConsolidateNode(context_p, recommend_consolidation);

    if(context_p->abort_flag == true) {
      return;
    }
    
    AdjustNodeSize(context_p);

    if(context_p->abort_flag == true) {
      return;
    }

    return;
  }

  /*
   * JumpToNodeID() - Given a NodeID, change the top of the path list
   *                  by loading the delta chain of the node ID
   *
   * NOTE: This function could also be called to traverse right, in which case
   * we need to check whether the target node is the left most child
   */
  void JumpToNodeID(NodeID node_id,
                    Context *context_p) {
    bwt_printf("Jumping to node ID = %lu\n", node_id);

    // This updates the current snapshot in the stack
    UpdateNodeSnapshot(node_id, context_p);
    
    bool recommend_consolidation = FinishPartialSMO(context_p);

    if(context_p->abort_flag == true) {
      return;
    }

    ConsolidateNode(context_p, recommend_consolidation);

    if(context_p->abort_flag == true) {
      return;
    }

    AdjustNodeSize(context_p);

    if(context_p->abort_flag == true) {
      return;
    }

    return;
  }

  /*
   * FinishPartialSMO() - Finish partial completed SMO if there is one
   *
   * This function defines the help-along protocol, i.e. if we find
   * a SMO on top of the delta chain, we should help-along that SMO before
   * doing our own job. This might incur a recursive call of this function.
   *
   * If this function returns true, then a node consolidation on the current
   * top snapshot is recommended, because we have seen and processed (or
   * confirmed that it has been processed) a merge/split delta on current
   * top snapshot. To prevent other threads from seeing them and taking
   * a full snapshot of the parent snapshot everytime they are seen, we
   * return true to inform caller to do a consolidation
   *
   * If we see a remove node, then the actual NodeID pushed into the path
   * list stack may not equal the NodeID passed in this function. So when
   * we need to read NodeID, always use the one stored in the NodeSnapshot
   * vector instead of using a previously passed one
   */
  bool FinishPartialSMO(Context *context_p) {
    // Note: If the top of the path list changes then this pointer
    // must also be updated
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);
    
before_switch:
    switch(snapshot_p->node_p->GetType()) {
      case NodeType::InnerAbortType: {
        bwt_printf("Observed Inner Abort Node; ABORT\n");
        
        // This is an optimization - when seeing an ABORT
        // node, we continue but set the physical pointer to be ABORT's
        // child, to make CAS always fail on this node to avoid
        // posting on ABORT, especially posting split node
        snapshot_p->node_p = \
          (static_cast<const DeltaNode *>(snapshot_p->node_p))->child_node_p;

        goto before_switch;
      }
      case NodeType::LeafRemoveType:
      case NodeType::InnerRemoveType: {
        bwt_printf("Helping along remove node...\n");

        // The right branch for merging is the child node under remove node
        const BaseNode *merge_right_branch = \
          (static_cast<const DeltaNode *>(snapshot_p->node_p))->child_node_p;
          
        // This serves as the merge key
        const KeyType *merge_key_p = &snapshot_p->node_p->metadata.lbound;
        
        // This will also be recorded in merge delta such that when
        // we finish merge delta we could recycle the node id as well
        // as the RemoveNode
        NodeID deleted_node_id = snapshot_p->node_id;

        JumpToLeftSibling(context_p);

        // If this aborts then we propagate this to the state machine driver
        if(context_p->abort_flag == true) {
          bwt_printf("Jump to left sibling in Remove help along ABORT\n");

          return false;
        }

        // That is the left sibling's snapshot
        NodeSnapshot *left_snapshot_p = GetLatestNodeSnapshot(context_p);

        // This is the left sibling of the merge delta node
        const BaseNode *left_sibling_p = left_snapshot_p->node_p;
        
        // This holds the merge node if installation is successful
        // Not changed if CAS fails
        const BaseNode *merge_node_p = nullptr;

        // Update snapshot pointer if we fall through to posting
        // index term delete delta for merge node
        snapshot_p = left_snapshot_p;

        bool ret = false;

        // If we are currently on leaf, just post leaf merge delta
        if(left_snapshot_p->is_leaf == true) {
          ret = \
            PostMergeNode<LeafMergeNode>(left_snapshot_p,
                                         merge_key_p,
                                         merge_right_branch,
                                         deleted_node_id,
                                         &merge_node_p);
        } else {
          ret = \
            PostMergeNode<InnerMergeNode>(left_snapshot_p,
                                          merge_key_p,
                                          merge_right_branch,
                                          deleted_node_id,
                                          &merge_node_p);
        }

        // If CAS fails just abort and return
        if(ret == true) {
          bwt_printf("Merge delta CAS succeeds. ABORT\n");

          // TODO: Do not abort here and directly proceed to processing
          // merge delta
          context_p->abort_flag = true;

          return false;
        } else {
          bwt_printf("Merge delta CAS fails. ABORT\n");

          assert(merge_node_p == nullptr);
          context_p->abort_flag = true;

          return false;
        } // if ret == true

        assert(false);
      } // case Inner/LeafRemoveType
      case NodeType::InnerMergeType:
      case NodeType::LeafMergeType: {
        bwt_printf("Helping along merge delta\n");

        // First consolidate parent node and find the left/right
        // sep pair plus left node ID
        NodeSnapshot *parent_snapshot_p = \
          GetLatestParentNodeSnapshot(context_p);

        // This is stored inside merge delta node. We fill in the value
        // below
        NodeID deleted_node_id = INVALID_NODE_ID;

        // This is the key being deleted in parent node, if exists
        const KeyType *merge_key_p = nullptr;

        // These three needs to be found in the parent node
        // Since we do not alter parent node's snapshot since last time we
        // saw it, we could always find the sep and its prev/next/node ID
        const KeyType *prev_key_p = nullptr;
        const KeyType *next_key_p = nullptr;
        NodeID prev_node_id = INVALID_NODE_ID;

        // Type of the merge delta
        NodeType type = snapshot_p->node_p->GetType();
        if(type == NodeType::InnerMergeType) {
          const InnerMergeNode *merge_node_p = \
            static_cast<const InnerMergeNode *>(node_p);

          // Extract merge key and deleted node ID from the merge delta
          merge_key_p = &merge_node_p->merge_key;
          deleted_node_id = merge_node_p->deleted_node_id;
        } else if(type == NodeType::LeafMergeType) {
          const LeafMergeNode *merge_node_p = \
            static_cast<const LeafMergeNode *>(node_p);

          merge_key_p = &merge_node_p->merge_key;
          deleted_node_id = merge_node_p->deleted_node_id;
        } else {
          bwt_printf("ERROR: Illegal node type: %d\n",
                     static_cast<int>(type));

          assert(false);
        } // If on type of merge node

        // if this is false then we have already deleted the index term
        bool merge_key_found = \
          FindMergePrevNextKey(parent_snapshot_p,
                               merge_key_p,
                               &prev_key_p,
                               &next_key_p,
                               &prev_node_id,
                               deleted_node_id);

        // If merge key is not found then we know we have already deleted the
        // index term
        if(merge_key_found == false) {
          bwt_printf("Index term is absent; No need to remove\n");

          // If we have seen a merge delta but did not find
          // corresponding sep in parent then it has already been removed
          // so we propose a consolidation on current node to
          // get rid of the merge delta
          return true;
        }

        const InnerDeleteNode *delete_node_p = \
          new InnerDeleteNode{*merge_key_p,
                              *next_key_p,
                              *prev_key_p,
                              prev_node_id,
                              parent_snapshot_p->node_p};

        // Assume parent has not changed, and CAS the index term delete delta
        // If CAS failed then parent has changed, and we have no idea how it
        // could be modified. The safest way is just abort
        bool ret = InstallNodeToReplace(parent_snapshot_p->node_id,
                                        delete_node_p,
                                        parent_snapshot_p->node_p);

        // If we installed the IndexTermDeleteDelta successfully the next
        // step is to put the remove node into garbage chain
        // and also recycle the deleted NodeID since now no new thread
        // could access that NodeID until it is reused
        if(ret == true) {
          bwt_printf("Index term delete delta installed, ID = %lu; ABORT\n",
                     parent_snapshot_p->node_id);

          // The deleted node ID must resolve to a RemoveNode of either
          // Inner or Leaf category
          const BaseNode *gargabe_node_p = GetNode(deleted_node_id);
          assert(garbage_node_p->IsRemoveNode());
          
          // Put the remove node into garbage chain
          // This will not remove the child node of the remove node, which
          // should be removed together with the merge node above it
          epoch_manager.AddGarbageNode(gargabe_node_p);
          
          ///////////////////////////////////////////
          // TODO: Also recycle NodeID here
          ///////////////////////////////////////////
          
          context_p->abort_flag = true;

          return false;
        } else {
          bwt_printf("Index term delete delta install failed. ABORT\n");

          // DO NOT FORGET TO DELETE THIS
          delete delete_node_p;
          context_p->abort_flag = true;

          return false;
        }

        break;
      } // case Inner/LeafMergeNode
      case NodeType::InnerSplitType:
      case NodeType::LeafSplitType: {
        bwt_printf("Helping along split node\n");

        // We need to read these three from split delta node
        const KeyType *split_key_p = nullptr;
        const KeyType *next_key_p = nullptr;
        NodeID split_node_id = INVALID_NODE_ID;
        
        NodeType type = snapshot_p->node_p->GetType();

        // NOTE: depth should not be read here, since we
        // need to know the depth on its parent node
        if(type == NodeType::InnerSplitType) {
          const InnerSplitNode *split_node_p = \
            static_cast<const InnerSplitNode *>(node_p);

          split_key_p = &split_node_p->split_key;
          split_node_id = split_node_p->split_sibling;
        } else {
          const LeafSplitNode *split_node_p = \
            static_cast<const LeafSplitNode *>(node_p);

          split_key_p = &split_node_p->split_key;
          split_node_id = split_node_p->split_sibling;
        }

        assert(context_p->path_list.size() > 0);

        if(context_p->path_list.size() == 1) {
          /***********************************************************
           * Root splits (don't have to consolidate parent node)
           ***********************************************************/
          bwt_printf("Root splits!\n");

          // Allocate a new node ID for the newly created node
          // If CAS fails we need to free the root ID
          NodeID new_root_id = GetNextNodeID();

          // [-Inf, +Inf] -> INVALID_NODE_ID, for root node
          // NOTE: DO NOT MAKE IT CONSTANT since we will push separator into it
          InnerNode *inner_node_p = \
            new InnerNode{GetNegInfKey(), GetPosInfKey(), INVALID_NODE_ID};

          inner_node_p->sep_list.push_back(std::make_pair(GetNegInfKey(),
                                                          node_id));
          inner_node_p->sep_list.push_back(std::make_pair(*split_key_p,
                                                          split_node_id));

          // First we need to install the new node with NodeID
          // This makes it visible
          InstallNewNode(new_root_id, inner_node_p);
          bool ret = InstallRootNode(snapshot_p->node_id,
                                     new_root_id);

          if(ret == true) {
            bwt_printf("Install root CAS succeeds\n");
            
            //context_p->abort_flag = true;

            return false;
          } else {
            bwt_printf("Install root CAS failed. ABORT\n");

            delete inner_node_p;
            // TODO: REMOVE THE NEWLY ALLOCATED ID
            context_p->abort_flag = true;

            return false;
          } // if CAS succeeds/fails
        } else {
          /***********************************************************
           * Index term insert for non-root nodes
           ***********************************************************/

          // First consolidate parent node and find the right sep
          NodeSnapshot *parent_snapshot_p = \
            GetLatestParentNodeSnapshot(context_p);

          // If this is false then we know the index term has already
          // been inserted
          bool split_key_absent = \
            FindSplitNextKey(parent_snapshot_p,
                             split_key_p,
                             &next_key_p,
                             split_node_id);

          if(split_key_absent == false) {
            bwt_printf("Index term is present. No need to insert\n");

            // We have seen a split, but the sep in parent node is already
            // installed. In this case we propose a consolidation on current
            // node to prevent further encountering the "false" split delta
            return true;
          }

          const InnerInsertNode *insert_node_p = \
            new InnerInsertNode{*split_key_p,
                                *next_key_p,
                                split_node_id,
                                parent_snapshot_p->node_p};

          // CAS
          bool ret = InstallNodeToReplace(parent_snapshot_p->node_id,
                                          insert_node_p,
                                          parent_snapshot_p->node_p);
          if(ret == true) {
            bwt_printf("Index term insert (from %lu to %lu) delta CAS succeeds\n",
                       node_id,
                       split_node_id);

            context_p->abort_flag = true;

            return false;
          } else {
            bwt_printf("Index term insert (from %lu to %lu) delta CAS failed. ABORT\n",
                       node_id,
                       split_node_id);

            // Set abort, and remove the newly created node
            context_p->abort_flag = true;
            delete insert_node_p;

            return false;
          } // if CAS succeeds/fails
        } // if split root / else not split root

        break;
      } // case split node
      default: {
        return false;
        // By default we do not do anything special
        break;
      }
    } // switch

    assert(false);
    return false;
  }

  /*
   * ConsolidateNode() - Consolidate current node if its length exceeds the
   *                     threshold value
   *
   * If the length of delta chain does not exceed the threshold then this
   * function does nothing
   *
   * If function returns true then we have successfully consolidated a node
   * Otherwise no consolidation happens. The return value might be used
   * as a hint for deciding whether to adjust node size or not
   *
   * NOTE: If consolidation fails then this function does not do anything
   * and will just continue with its own job. There is no chance of abort
   *
   * TODO: If strict mode is on, then whenever consolidation fails we should
   * always abort and start from the beginning, to keep delta chain length
   * upper bound intact
   */
  bool ConsolidateNode(Context *context_p,
                       bool recommend_consolidation) {
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);

    // Do not overwrite this pointer since we will use this
    // to locate garbage delta chain
    const BaseNode *node_p = snapshot_p->node_p;
    NodeID node_id = snapshot_p->node_id;

    // We could only perform consolidation on delta node
    // because we want to see depth field
    if(node_p->IsDeltaNode() == false) {
      assert(recommend_consolidation == false);
      
      return false;
    }

    // If depth does not exceeds threshold then we check recommendation flag
    int depth = node_p->metadata.depth;
    if(snapshot_p->is_leaf == true) {
      // Adjust the length a little bit using this variable
      // NOTE: The length of the delta chain on leaf coule be a
      // little bit longer than on inner
      // so this is usually a negative value
      // This improves performance
      depth += DELTA_CHAIN_LENGTH_THRESHOLD_LEAF_DIFF;
    }
    
    if(depth < DELTA_CHAIN_LENGTH_THRESHOLD) {
      // If there is not recommended consolidation just return
      if(recommend_consolidation == false) {
        return false;
      } else {
        bwt_printf("Delta chian length < threshold, "
                   "but consolidation is recommended\n");
      }
    }

    // After this pointer we decide to consolidate node

    // This is for debugging
    if(snapshot_p->is_root == true) {
      bwt_printf("Consolidate root node\n");
    }

    if(snapshot_p->is_leaf) {
      // This function returns a leaf node object
      const LeafNode *leaf_node_p = CollectAllValuesOnLeaf(snapshot_p);

      // CAS leaf node
      bool ret = InstallNodeToReplace(node_id, leaf_node_p, node_p);
      if(ret == true) {
        bwt_printf("Leaf node consolidation (ID %lu) CAS succeeds\n",
                   node_id);

        // Update current snapshot using our best knowledge
        snapshot_p->node_p = leaf_node_p;

        // Add the old delta chain to garbage list and its
        // deallocation is delayed
        epoch_manager.AddGarbageNode(node_p);
      } else {
        bwt_printf("Leaf node consolidation CAS failed. NO ABORT\n");

        // TODO: If we want to keep delta chain length constant then it
        // should abort here

        delete leaf_node_p;
        
        // Return false here since we did not consolidate
        return false;
      } // if CAS succeeds / fails
    } else {
      const InnerNode *inner_node_p = CollectAllSepsOnInner(snapshot_p);

      bool ret = InstallNodeToReplace(node_id, inner_node_p, node_p);
      if(ret == true) {
        bwt_printf("Inner node consolidation (ID %lu) CAS succeeds\n",
                   node_id);

        snapshot_p->node_p = inner_node_p;

        // Add the old delta into garbage list
        epoch_manager.AddGarbageNode(node_p);
      } else {
        bwt_printf("Inner node consolidation CAS failed. NO ABORT\n");

        context_p->abort_flag = true;

        delete inner_node_p;

        return false;
      } // if CAS succeeds / fails
    } // if it is leaf / is inner

    return true;
  }

  /*
   * AdjustNodeSize() - Post split or merge delta if a node becomes overflow
   *                    or underflow
   *
   * For leftmost children nodes and for root node (which has is_leftmost
   * flag set) we never post remove delta, the merge of which would change
   * its parent node's low key
   *
   * NOTE: This function will abort after installing a node remove delta,
   * in order not to have to call LoadNodeID recursively
   *
   * TODO: In the future we might want to change this
   */
  void AdjustNodeSize(Context *context_p) {
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);
    const BaseNode *node_p = snapshot_p->node_p;

    // We do not adjust size for delta nodes
    if(node_p->IsDeltaNode() == true) {
      // TODO: If we want to strictly enforce the size of any node
      // then we should aggressively consolidate here and always
      // Though it brings huge overhead because now we are consolidating
      // every node on the path

      return;
    }
    
    NodeID node_id = snapshot_p->node_id;

    if(snapshot_p->is_leaf == true) {
      const LeafNode *leaf_node_p = \
        static_cast<const LeafNode *>(node_p);

      // NOTE: We use key number as the size of the node
      // because using item count might result in a very unstable
      // split, in a sense that we could not always split the node
      // evenly, and in the worst case if there is one key in the
      // node the node could not be splited while having a large
      // item count
      size_t node_size = leaf_node_p->item_prefix_sum.size();

      // Perform corresponding action based on node size
      if(node_size >= LEAF_NODE_SIZE_UPPER_THRESHOLD) {
        bwt_printf("Node size >= leaf upper threshold. Split\n");

        const LeafNode *new_leaf_node_p = leaf_node_p->GetSplitSibling();
        const KeyType *split_key_p = &new_leaf_node_p->metadata.lbound;

        NodeID new_node_id = GetNextNodeID();

        // TODO: If we add support to split base node with a delta chain
        // then this is the length of the delta chain
        int depth = 1;

        const LeafSplitNode *split_node_p = \
          new LeafSplitNode{*split_key_p, new_node_id, depth, node_p};

        //  First install the NodeID -> split sibling mapping
        // If CAS fails we also need to recycle the node ID allocated here
        InstallNewNode(new_node_id, new_leaf_node_p);
        
        // Then CAS split delta into current node's NodeID
        bool ret = InstallNodeToReplace(node_id, split_node_p, node_p);

        if(ret == true) {
          bwt_printf("Leaf split delta (from %lu to %lu) CAS succeeds. ABORT\n",
                     node_id,
                     new_node_id);

          // TODO: WE ABORT HERE TO AVOID THIS THREAD POSTING ANYTHING
          // ON TOP OF IT WITHOUT HELPING ALONG AND ALSO BLOCKING OTHER
          // THREAD TO HELP ALONG
          context_p->abort_flag = true;

          return;
        } else {
          bwt_printf("Leaf split delta CAS fails\n");
          
          // TODO: Recycle node ID here

          // We have two nodes to delete here
          delete split_node_p;
          delete new_leaf_node_p;

          return;
        }

      } else if(node_size <= LEAF_NODE_SIZE_LOWER_THRESHOLD) {
        // Leaf node could not be root
        assert(snapshot_p->is_root == false);
        
        NodeSnapshot *parent_snapshot_p = \
          GetLatestParentNodeSnapshot(context_p);

        bool is_leftmost_child = \
          KeyCmpEqual(snapshot_p->node_p->metadata.lbound,
                      parent_snapshot_p->node_p->metadata.lbound);

        // We could not remove leftmost node
        if(is_leftmost_child == true) {
          bwt_printf("Left most leaf node cannot be removed\n");

          return;
        }

        // After this point we decide to remove leaf node
        
        bwt_printf("Node size <= leaf lower threshold. Remove\n");

        // Install an abort node on parent
        const BaseNode *abort_node_p = nullptr;
        const BaseNode *abort_child_node_p = nullptr;
        NodeID parent_node_id = INVALID_NODE_ID;

        bool abort_node_ret = \
          PostAbortOnParent(context_p,
                            &parent_node_id,
                            &abort_node_p,
                            &abort_child_node_p);

        // If we could not block the parent then the parent has changed
        // (splitted, etc.)
        if(abort_node_ret == true) {
          bwt_printf("Blocked parent node (current node is leaf)\n");
        } else {
          bwt_printf("Unable to block parent node "
                     "(current node is leaf). ABORT\n");

          // ABORT and return
          context_p->abort_flag = true;

          return;
        }

        const LeafRemoveNode *remove_node_p = new LeafRemoveNode{node_p};

        bool ret = InstallNodeToReplace(node_id, remove_node_p, node_p);
        if(ret == true) {
          bwt_printf("LeafRemoveNode CAS succeeds. ABORT.\n");

          context_p->abort_flag = true;

          RemoveAbortOnParent(parent_node_id,
                              abort_node_p,
                              abort_child_node_p);

          return;
        } else {
          bwt_printf("LeafRemoveNode CAS failed\n");

          delete remove_node_p;

          context_p->abort_flag = true;

          RemoveAbortOnParent(parent_node_id,
                              abort_node_p,
                              abort_child_node_p);

          return;
        }
      }
    } else {   // If this is an inner node
      const InnerNode *inner_node_p = static_cast<const InnerNode *>(node_p);

      size_t node_size = inner_node_p->item_prefix_sum.size();

      if(node_size >= INNER_NODE_SIZE_UPPER_THRESHOLD) {
        bwt_printf("Node size >= inner upper threshold. Split\n");

        // This flag is only set when
        if(snapshot_p->is_root) {
          bwt_printf("Posting split delta on root node\n");
        }

        const InnerNode *new_inner_node_p = inner_node_p->GetSplitSibling();
        const KeyType *split_key_p = &new_inner_node_p->metadata.lbound;

        // New node has at least one item (this is the basic requirement)
        assert(new_inner_node_p->sep_list.size() > 0);

        const SepItem &first_item = new_inner_node_p->sep_list[0];
        // This points to the left most node on the right split sibling
        // If this node is being removed then we abort
        NodeID split_key_child_node_id = first_item.node;

        // This must be the split key
        assert(KeyCmpEqual(first_item.key, *split_key_p));

        // NOTE: WE FETCH THE POINTER WITHOUT HELP ALONG SINCE WE ARE
        // NOW ON ITS PARENT
        const BaseNode *split_key_child_node_p = \
          GetNode(split_key_child_node_id);

        // If the type is remove then we just continue without abort
        // If we abort then it might introduce deadlock
        if(split_key_child_node_p->IsRemoveType()) {
          bwt_printf("Found a removed node (type %d) on split key child "
                     "CONTINUE \n",
                     static_cast<int>(split_key_child_node_type));

          //context_p->abort_flag = true;

          return;
        }

        NodeID new_node_id = GetNextNodeID();

        const InnerSplitNode *split_node_p = \
          new InnerSplitNode{*split_key_p, new_node_id, node_p};

        //  First install the NodeID -> split sibling mapping
        InstallNewNode(new_node_id, new_inner_node_p);
        
        // Then CAS split delta into current node's NodeID
        bool ret = InstallNodeToReplace(node_id, split_node_p, node_p);

        if(ret == true) {
          bwt_printf("Inner split delta (from %lu to %lu) CAS succeeds. ABORT\n",
                     node_id,
                     new_node_id);

          // Same reason as in leaf node
          context_p->abort_flag = true;

          return;
        } else {
          bwt_printf("Inner split delta CAS fails\n");

          // We have two nodes to delete here
          delete split_node_p;
          delete new_inner_node_p;
          
          // TODO: Also need to remove the allocated NodeID

          return;
        } // if CAS fails
      } else if(node_size <= INNER_NODE_SIZE_LOWER_THRESHOLD) {
        if(snapshot_p->is_root == true) {
          bwt_printf("Root underflow - let it be\n");
          
          return;
        }
        
        // After this point we know there is at least a parent
        NodeSnapshot *parent_snapshot_p = \
          GetLatestParentNodeSnapshot(context_p);
        
        // Check whether current inner node is left most child by comparing
        // the low key of its parent node when we traverse down and the current
        // node's low key
        // NOTE: If the parent node has changed (e.g. split on the current
        // node's low key) we will not be able to catch it. But we will
        // find it later by posting an InnerAbortNode on parent which would
        // result in CAS failing and aborting
        bool is_leftmost_child = \
          KeyCmpEqual(snapshot_p->node_p->metadata.lbound,
                      parent_snapshot_p->node_p->metadata.lbound);
        
        // We could not remove leftmost node
        if(is_leftmost_child == true) {
          bwt_printf("Left most inner node cannot be removed\n");

          return;
        }
        
        // After this point we decide to remove

        bwt_printf("Node size <= inner lower threshold. Remove\n");

        // Then we abort its parent node
        // These two are used to hold abort node and its previous child
        const BaseNode *abort_node_p = nullptr;
        const BaseNode *abort_child_node_p = nullptr;
        NodeID parent_node_id = INVALID_NODE_ID;

        bool abort_node_ret = \
          PostAbortOnParent(context_p,
                            &parent_node_id,
                            &abort_node_p,
                            &abort_child_node_p);

        // If we could not block the parent then the parent has changed
        // (splitted, etc.)
        if(abort_node_ret == true) {
          bwt_printf("Blocked parent node (current node is inner)\n");
        } else {
          bwt_printf("Unable to block parent node "
                     "(current node is inner). ABORT\n");

          // ABORT and return
          context_p->abort_flag = true;

          return;
        }

        const InnerRemoveNode *remove_node_p = new InnerRemoveNode{node_p};

        bool ret = InstallNodeToReplace(node_id, remove_node_p, node_p);
        if(ret == true) {
          bwt_printf("LeafRemoveNode CAS succeeds. ABORT\n");

          // We abort after installing a node remove delta
          context_p->abort_flag = true;

          // Even if we success we need to remove the abort
          // on the parent, and let parent split thread to detect the remove
          // delta on child
          RemoveAbortOnParent(parent_node_id,
                              abort_node_p,
                              abort_child_node_p);

          return;
        } else {
          bwt_printf("LeafRemoveNode CAS failed\n");

          delete remove_node_p;

          // We must abort here since otherwise it might cause
          // merge nodes to underflow
          context_p->abort_flag = true;

          // Same as above
          RemoveAbortOnParent(parent_node_id,
                              abort_node_p,
                              abort_child_node_p);

          return;
        }
      } // if split/remove
    }

    return;
  }

  /*
   * RemoveAbortOnParent() - Removes the abort node on the parent
   *
   * This operation must succeeds since only the thread that installed
   * the abort node could remove it
   */
  void RemoveAbortOnParent(NodeID parent_node_id,
                           const BaseNode *abort_node_p,
                           const BaseNode *abort_child_node_p) {
    bwt_printf("Remove abort on parent node\n");

    // We switch back to the child node (so it is the target)
    bool ret = \
      InstallNodeToReplace(parent_node_id, abort_child_node_p, abort_node_p);

    // This CAS must succeed since nobody except this thread could remove
    // the ABORT delta on parent node
    assert(ret == true);
    (void)ret;

    // NOTE: DO NOT FORGET TO REMOVE THE ABORT AFTER
    // UNINSTALLING IT FROM THE PARENT NODE
    // NOTE 2: WE COULD NOT DIRECTLY DELETE THIS NODE
    // SINCE SOME OTHER NODES MIGHT HAVE TAKEN A SNAPSHOT
    // AND IF ABORT NODE WAS REMOVED, THE TYPE INFORMATION
    // CANNOT BE PRESERVED AND IT DOES NOT ABORT; INSTEAD
    // IT WILL TRY TO CALL CONSOLIDATE ON ABORT
    // NODE, CAUSING TYPE ERROR
    //delete (InnerAbortNode *)abort_node_p;

    // This delays the deletion of abort node until all threads has exited
    // so that existing pointers to ABORT node remain valid
    epoch_manager.AddGarbageNode(abort_node_p);

    return;
  }

  /*
   * PostAbortOnParent() - Posts an inner abort node on the parent
   *
   * This function blocks all accesses to the parent node, and blocks
   * all CAS efforts for threads that took snapshots before the CAS
   * in this function.
   *
   * Return false if CAS failed. In that case the memory is freed
   * by this function
   *
   * This function DOES NOT ABORT. Do not have to check for abort flag. But
   * if CAS fails then returns false, and caller needs to abort after
   * checking the return value.
   */
  bool PostAbortOnParent(Context *context_p,
                         NodeID *parent_node_id_p,
                         const BaseNode **abort_node_p_p,
                         const BaseNode **abort_child_node_p_p) {
    // This will make sure the path list has length >= 2
    NodeSnapshot *parent_snapshot_p = \
      GetLatestParentNodeSnapshot(context_p);

    const BaseNode *parent_node_p = parent_snapshot_p->node_p;
    NodeID parent_node_id = parent_snapshot_p->node_id;

    // Save original node pointer
    *abort_child_node_p_p = parent_node_p;
    *parent_node_id_p = parent_node_id;

    InnerAbortNode *abort_node_p = new InnerAbortNode{parent_node_p};

    bool ret = InstallNodeToReplace(parent_node_id,
                                    abort_node_p,
                                    parent_node_p);

    if(ret == true) {
      bwt_printf("Inner Abort node CAS succeeds\n");

      // Copy the new node to caller since after posting remove delta we will
      // remove this abort node to enable accessing again
      *abort_node_p_p = abort_node_p;
    } else {
      bwt_printf("Inner Abort node CAS failed\n");

      delete abort_node_p;
    }

    return ret;
  }

  /*
   * FindSplitNextKey() - Given a parent snapshot, find the next key of
   *                      the current split key
   *
   * This function will search for the next key of the current split key
   * If the split key is found, it just return false. In that case
   * the key has already been inserted, and we should not post duplicate
   * record
   *
   * Returns true if split key is not found (i.e. we could insert index term)
   *
   * NOTE 2: This function checks whether the PID has already been inserted
   * using both the sep key and NodeID. These two must match, otherwise we
   * observed an inconsistent state
   */
  bool FindSplitNextKey(NodeSnapshot *snapshot_p,
                        const KeyType *split_key_p,
                        const KeyType **next_key_p_p,
                        const NodeID insert_pid) {
    assert(snapshot_p->is_leaf == false);

    const InnerNode *inner_node_p = \
      static_cast<const InnerNode *>(parent_snapshot_p->node_p);

    if(parent_snapshot_p->node_p->IsInnerNode() == false) {
      inner_node_p = CollectAllSepsOnInner(parent_snapshot_p);

      bool ret = InstallNodeToReplace(parent_snapshot_p.node_id,
                                      inner_node_p,
                                      parent_snapshot_p->node_p);

      if(ret == true) {
        parent_snapshot_p->node_p = inner_node_p;
      } else {
        // Must delay deallocation since we return pointers pointing
        // into this node's data
        epoch_manager.AddGarbageNode(inner_node_p);
      }
    }
    
    // This returns an it pointing to the pair whose key >= split key
    // If it is not split key then the iterator exactly points
    // to the key we are looking for
    // If it is split key then we do not need to insert and return false
    auto split_key_it = std::lower_bound(inner_node_p->sep_list.begin(),
                                         inner_node_p->sep_list.end(),
                                         std::make_pair(*split_key_p,
                                                        INVALID_NODE_ID),
                                         key_node_id_pair_cmp_obj);

    // If the split key already exists then just return false
    if(KeyCmpEqual(split_key_it->first, *split_key_p) == true) {
      assert(split_key_it->second == insert_pid);
      
      return false;
    }

    // If lower_bound() returns end() then the split key will be the largest
    // in the inner node. So just use the high key of the inner node as next
    // key
    if(split_key_it == inner_node_p->sep_list.end()) {
      *next_key_p_p = &inner_node_p->metadata.ubound;
    } else {
      *next_p_p = &split_key_it->first;
    }

    return true;
  }

  /*
   * FindMergePrevNextKey() - Find merge next and prev key and node ID
   *
   * This function loops through keys in the snapshot, and if there is a
   * match of the merge key, its previous key and next key are collected.
   * Also the node ID of its previous key is collected
   *
   * NOTE: There is possibility that the index term has already been
   * deleted in the parent node, in which case this function does not
   * fill in the given pointer but returns false instead.
   *
   * NOTE 2: This function assumes the snapshot already has data and metadata
   *
   * Return true if the merge key is found. false if merge key not found
   */
  inline bool FindMergePrevNextKey(NodeSnapshot *snapshot_p,
                                   const KeyType *merge_key_p,
                                   const KeyType **prev_key_p_p,
                                   const KeyType **next_key_p_p,
                                   NodeID *prev_node_id_p,
                                   NodeID deleted_node_id) {
    // We could only post merge key on merge node
    assert(snapshot_p->is_leaf == false);

    const InnerNode *inner_node_p = \
      static_cast<const InnerNode *>(parent_snapshot_p->node_p);

    if(parent_snapshot_p->node_p->IsInnerNode() == false) {
      inner_node_p = CollectAllSepsOnInner(parent_snapshot_p);

      bool ret = InstallNodeToReplace(parent_snapshot_p.node_id,
                                      inner_node_p,
                                      parent_snapshot_p->node_p);

      if(ret == true) {
        parent_snapshot_p->node_p = inner_node_p;
      } else {
        // Must delay deallocation since we return pointers pointing
        // into this node's data
        epoch_manager.AddGarbageNode(inner_node_p);
      }
    }
    
    // Find the merge key
    auto merge_key_it = std::lower_bound(inner_node_p->sep_list.begin(),
                                         inner_node_p->sep_list.end(),
                                         std::make_pair(*merge_key_p,
                                                        INVALID_NODE_ID),
                                         key_node_id_pair_cmp_obj);
                                         
    // If the lower bound of merge_key_p is not itself, then we
    // declare such key does not exist
    if(KeyCmpEqual(merge_key_it->first, *merge_key_p) == false) {
      return false;
    }
    
    // If we have found the deleted entry then it must be associated
    // with the node id that has been deleted
    assert(merge_key_it->second == deleted_node_id);
    
    // In the parent node merge key COULD NOT be the left most key
    // since the merge node itself has a low key, which < merge key
    // and the low key >= parent node low key
    assert(merge_key_it != inner_node_p->sep_list.begin());
    
    auto merge_key_prev_it = std::advance(merge_key_it, -1);
    auto merge_key_next_it = std::advance(merge_key_it, 1);
    
    *prev_key_p_p = &merge_key_prev_it->first;
    *prev_node_id_p = merge_key_prev_it->second;
    
    // If the merge key is the last key in the inner node
    // then the next key is the high key of the inner node
    if(merge_key_next_it == inner_node_p->sep_list.end()) {
      *next_key_p_p = &inner_node_p->metadata.ubound;
    } else {
      *next_key_p_p = &merge_key_next_it->first;
    }
    
    return true;
  }

  /*
   * PostMergeNode() - Post a leaf merge node on top of a delta chain
   *
   * This function is made to be a template since all logic except the
   * node type is same.
   *
   * NOTE: This function takes an argument, such that if CAS succeeds it
   * sets that pointer's value to be the new node we just created
   * If CAS fails we do not touch that pointer
   *
   * NOTE: This function deletes memory for the caller, so caller do
   * not have to free memory when this function returns false
   */
  template <typename MergeNodeType>
  bool PostMergeNode(const NodeSnapshot *snapshot_p,
                     const KeyType *merge_key_p,
                     const BaseNode *merge_branch_p,
                     NodeID deleted_node_id,
                     const BaseNode **node_p_p) {
    // This is the child node of merge delta
    const BaseNode *node_p = snapshot_p->node_p;
    NodeID node_id = snapshot_p->node_id;

    // NOTE: DO NOT FORGET TO DELETE THIS IF CAS FAILS
    const MergeNodeType *merge_node_p = \
      new MergeNodeType{*merge_key_p,
                        merge_branch_p,
                        deleted_node_id,
                        node_p};

    // Compare and Swap!
    bool ret = InstallNodeToReplace(node_id, merge_node_p, node_p);

    // If CAS fails we delete the node and return false
    if(ret == false) {
      delete merge_node_p;
    } else {
      *node_p_p = merge_node_p;
    }

    return ret;
  }

  /*
   * IsKeyPresent() - Check whether the key is present in the tree
   *
   * This function calls Traverse with value option in order to
   * collect all values associated with the search key. If there is
   * no such key in the resulting key value map, then return false
   *
   * TODO: This function is not the optimal one since there is overhead
   * for copying the values which is discarded. In the future we may want
   * to revise Traverse to make it able to detect for key existence request
   */
  bool IsKeyPresent(const KeyType &search_key) {
    Context context{&search_key};

    Traverse(&context, true);

    NodeSnapshot *snapshot = GetLatestNodeSnapshot(&context);
    LogicalLeafNode *logical_node_p = snapshot->GetLogicalLeafNode();
    KeyValueSet &container = logical_node_p->GetContainer();

    if(container.find(search_key) == container.end()) {
      return false;
    } else {
      return true;
    }

    assert(false);
    return false;
  }

  /*
   * Insert() - Insert a key-value pair
   *
   * This function returns false if value already exists
   * If CAS fails this function retries until it succeeds
   */
  bool Insert(const KeyType &key, const ValueType &value) {
    bwt_printf("Insert called\n");

    insert_op_count.fetch_add(1);

    EpochNode *epoch_node_p = epoch_manager.JoinEpoch();

    while(1) {
      Context context{key};

      // Collect values with node navigation
      Traverse(&context, true);

      NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(&context);
      LogicalLeafNode *logical_node_p = snapshot_p->GetLogicalLeafNode();
      KeyValueSet &container = logical_node_p->GetContainer();

      // For insertion for should iterate through existing values first
      // to make sure the key-value pair does not exist
      typename KeyValueSet::iterator it = container.find(key);
      if(it != container.end()) {
        // Look for value in consolidated leaf with the given key
        auto it2 = it->second.find(value);

        if(it2 != it->second.end()) {
          epoch_manager.LeaveEpoch(epoch_node_p);

          return false;
        }
      }

      // We will CAS on top of this
      const BaseNode *node_p = snapshot_p->node_p;
      NodeID node_id = snapshot_p->node_id;

      // If node_p is a delta node then we have to use its
      // delta value
      int depth = 1;

      if(node_p->IsDeltaNode() == true) {
        const DeltaNode *delta_node_p = \
          static_cast<const DeltaNode *>(node_p);

        depth = delta_node_p->depth + 1;
      }

      const LeafInsertNode *insert_node_p = \
        new LeafInsertNode{key, value, depth, node_p};

      bool ret = InstallNodeToReplace(node_id,
                                      insert_node_p,
                                      node_p);
      if(ret == true) {
        bwt_printf("Leaf Insert delta CAS succeed\n");

        // This will actually not be used anymore, so maybe
        // could save this assignment
        snapshot_p->SwitchPhysicalPointer(insert_node_p);

        // If install is a success then just break from the loop
        // and return
        break;
      } else {
        bwt_printf("Leaf insert delta CAS failed\n");

        context.abort_counter++;

        delete insert_node_p;
      }

      // Update abort counter
      // NOTW 1: We could not do this before return since the context
      // object is cleared at the end of loop
      // NOTE 2: Since Traverse() might abort due to other CAS failures
      // context.abort_counter might be larger than 1 when
      // LeafInsertNode installation fails
      insert_abort_count.fetch_add(context.abort_counter);

      // We reach here only because CAS failed
      bwt_printf("Retry installing leaf insert delta from the root\n");
    }

    epoch_manager.LeaveEpoch(epoch_node_p);

    return true;
  }
  
#ifdef BWTREE_PELOTON

  /*
   * ConditionalInsert() - Insert a key-value pair only if a given
   *                       predicate fails for all values with a key
   *
   * If return true then the value has been inserted
   * If return false then the value is not inserted. The reason could be
   * predicates returning true for one of the values of a given key
   * or because the value is already in the index
   *
   * Argument value_p is set to nullptr if all predicate tests returned false
   *
   * NOTE: We first test the predicate, and then test for duplicated values
   * so predicate test result is always available
   */
  bool ConditionalInsert(const KeyType &key,
                         const ValueType &value,
                         std::function<bool(const ItemPointer &)> predicate,
                         bool *predicate_satisfied) {
    bwt_printf("Consitional Insert called\n");

    insert_op_count.fetch_add(1);

    EpochNode *epoch_node_p = epoch_manager.JoinEpoch();

    while(1) {
      Context context{key};

      // Collect values with node navigation
      Traverse(&context, true);

      NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(&context);
      LogicalLeafNode *logical_node_p = snapshot_p->GetLogicalLeafNode();
      KeyValueSet &container = logical_node_p->GetContainer();
      
      // At the beginning of each iteration we just set the value pointer
      // to be empty
      // Note that in Peloton we always store value as ItemPointer * so
      // in order for simplicity we just assume the value itself
      // is a pointer type
      *predicate_satisfied = false;

      // For insertion for should iterate through existing values first
      // to make sure the key-value pair does not exist
      typename KeyValueSet::iterator it = container.find(key);
      if(it != container.end()) {
        // v is a reference to ValueType
        for(auto &v : it->second) {
          if(predicate(*v) == true) {
            // To notify the caller that predicate
            // has been satisfied and we cannot insert
            *predicate_satisfied = true;
            
            // Do not forget this!
            epoch_manager.LeaveEpoch(epoch_node_p);
            
            return false;
          }
        }
        
        // After evaluating predicate on all values we continue to find
        // whether there is duplication for the value
        auto it2 = it->second.find(value);

        if(it2 != it->second.end()) {
          epoch_manager.LeaveEpoch(epoch_node_p);

          // In this case, value_p is set to nullptr
          // and return value is false
          return false;
        }
      }

      // We will CAS on top of this
      const BaseNode *node_p = snapshot_p->node_p;
      NodeID node_id = snapshot_p->node_id;

      // If node_p is a delta node then we have to use its
      // delta value
      int depth = 1;

      if(node_p->IsDeltaNode() == true) {
        const DeltaNode *delta_node_p = \
          static_cast<const DeltaNode *>(node_p);

        depth = delta_node_p->depth + 1;
      }

      const LeafInsertNode *insert_node_p = \
        new LeafInsertNode{key, value, depth, node_p};

      bool ret = InstallNodeToReplace(node_id,
                                      insert_node_p,
                                      node_p);
      if(ret == true) {
        bwt_printf("Leaf Insert delta (cond) CAS succeed\n");

        // This will actually not be used anymore, so maybe
        // could save this assignment
        snapshot_p->SwitchPhysicalPointer(insert_node_p);

        // If install is a success then just break from the loop
        // and return
        break;
      } else {
        bwt_printf("Leaf Insert delta (cond) CAS failed\n");

        context.abort_counter++;

        delete insert_node_p;
      }

      // Update abort counter
      // NOTE 1: We could not do this before return since the context
      // object is cleared at the end of loop
      // NOTE 2: Since Traverse() might abort due to other CAS failures
      // context.abort_counter might be larger than 1 when
      // LeafInsertNode installation fails
      insert_abort_count.fetch_add(context.abort_counter);

      // We reach here only because CAS failed
      bwt_printf("Retry installing leaf insert delta from the root\n");
    }

    epoch_manager.LeaveEpoch(epoch_node_p);

    return true;
  }
  
#endif

  /*
   * Update() - Update an existing value for a key to a new value
   *
   * This function returns false if the key does not exist or
   * value does not exist with the key
   *
   * This function also returns false if the new value already exists
   *
   * TODO: Use a flag set to represent all these error conditions
   * instead of only one boolean flag
   */
  bool Update(const KeyType &key,
              const ValueType &old_value,
              const ValueType &new_value) {
    bwt_printf("Update called\n");

    update_op_count.fetch_add(1);

    EpochNode *epoch_node_p = epoch_manager.JoinEpoch();

    while(1) {
      Context context{key};

      Traverse(&context, true);

      NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(&context);
      LogicalLeafNode *logical_node_p = snapshot_p->GetLogicalLeafNode();
      KeyValueSet &container = logical_node_p->GetContainer();

      // first check whether the key exists or not
      // if key does not exist return false
      typename KeyValueSet::iterator it = container.find(key);
      if(it == container.end()) {
        epoch_manager.LeaveEpoch(epoch_node_p);

        return false;
      }

      // Then check whether the value exists for this key
      // given the value set for the key
      // and if value does not exist return false
      auto it2 = it->second.find(old_value);
      if(it2 == it->second.end()) {
        epoch_manager.LeaveEpoch(epoch_node_p);

        return false;
      }

      // If the new value already exists in the leaf
      // we also will not do update and simply return false
      auto it3 = it->second.find(new_value);
      if(it3 != it->second.end()) {
        epoch_manager.LeaveEpoch(epoch_node_p);

        return false;
      }

      // We will CAS on top of this
      const BaseNode *node_p = snapshot_p->node_p;
      NodeID node_id = snapshot_p->node_id;

      // If node_p is a delta node then we have to use its
      // delta value
      int depth = 1;

      if(node_p->IsDeltaNode() == true) {
        const DeltaNode *delta_node_p = \
          static_cast<const DeltaNode *>(node_p);

        depth = delta_node_p->depth + 1;
      }

      const LeafUpdateNode *update_node_p = \
        new LeafUpdateNode{key, old_value, new_value, depth, node_p};

      bool ret = InstallNodeToReplace(node_id,
                                      update_node_p,
                                      node_p);
      if(ret == true) {
        bwt_printf("Leaf Update delta CAS succeed\n");

        // This will actually not be used anymore, so maybe
        // could save this assignment
        snapshot_p->SwitchPhysicalPointer(update_node_p);

        // If install is a success then just break from the loop
        // and return
        break;
      } else {
        bwt_printf("Leaf update delta CAS failed\n");

        delete update_node_p;

        context.abort_counter++;
      }

      update_abort_count.fetch_add(context.abort_counter);

      // We reach here only because CAS failed
      bwt_printf("Retry installing leaf update delta from the root\n");
    }

    epoch_manager.LeaveEpoch(epoch_node_p);

    return true;
  }

  /*
   * Delete() - Remove a key-value pair from the tree
   *
   * This function returns false if the key and value pair does not
   * exist. Return true if delete succeeds
   *
   * This functions shares a same structure with the Insert() one
   */
  bool Delete(const KeyType &key,
              const ValueType &value) {
    bwt_printf("Delete called\n");

    delete_op_count.fetch_add(1);

    EpochNode *epoch_node_p = epoch_manager.JoinEpoch();

    while(1) {
      Context context{key};

      // Collect values with node navigation
      Traverse(&context, true);

      NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(&context);
      LogicalLeafNode *logical_node_p = snapshot_p->GetLogicalLeafNode();
      KeyValueSet &container = logical_node_p->GetContainer();

      // If the key or key-value pair does not exist then we just
      // return false
      typename KeyValueSet::iterator it = container.find(key);
      if(it == container.end()) {
        epoch_manager.LeaveEpoch(epoch_node_p);

        return false;
      }

      // Look for value in consolidated leaf with the given key
      auto it2 = it->second.find(value);
      if(it2 == it->second.end()) {
        epoch_manager.LeaveEpoch(epoch_node_p);

        return false;
      }

      // We will CAS on top of this
      const BaseNode *node_p = snapshot_p->node_p;
      NodeID node_id = snapshot_p->node_id;

      // If node_p is a delta node then we have to use its
      // delta value
      int depth = 1;

      if(node_p->IsDeltaNode() == true) {
        const DeltaNode *delta_node_p = \
          static_cast<const DeltaNode *>(node_p);

        depth = delta_node_p->depth + 1;
      }

      const LeafDeleteNode *delete_node_p = \
        new LeafDeleteNode{key, value, depth, node_p};

      bool ret = InstallNodeToReplace(node_id,
                                      delete_node_p,
                                      node_p);
      if(ret == true) {
        bwt_printf("Leaf Delete delta CAS succeed\n");

        // This will actually not be used anymore, so maybe
        // could save this assignment
        snapshot_p->SwitchPhysicalPointer(delete_node_p);

        // If install is a success then just break from the loop
        // and return
        break;
      } else {
        bwt_printf("Leaf Delete delta CAS failed\n");

        delete delete_node_p;

        context.abort_counter++;
      }

      delete_abort_count.fetch_add(context.abort_counter);

      // We reach here only because CAS failed
      bwt_printf("Retry installing leaf delete delta from the root\n");
    }

    epoch_manager.LeaveEpoch(epoch_node_p);

    return true;
  }
  
  #ifdef BWTREE_PELOTON
  
  /*
   * DeleteItemPointer() - Deletes an item pointer from the index by comparing
   *                       the target of the pointer, rather than pointer itself
   *
   * Note that this function assumes the value always being ItemPointer *
   * and in this function we compare item pointer's target rather than
   * the value of pointers themselves. Also when a value is deleted, we
   * free the memory, which is allocated when the value is inserted
   */
  bool DeleteItemPointer(const KeyType &key,
                         const ItemPointer &value) {
    bwt_printf("Delete Item Pointer called\n");

    delete_op_count.fetch_add(1);

    EpochNode *epoch_node_p = epoch_manager.JoinEpoch();

    while(1) {
      Context context{key};

      // Collect values with node navigation
      Traverse(&context, true);

      NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(&context);
      LogicalLeafNode *logical_node_p = snapshot_p->GetLogicalLeafNode();
      KeyValueSet &container = logical_node_p->GetContainer();

      // If the key or key-value pair does not exist then we just
      // return false
      typename KeyValueSet::iterator it = container.find(key);
      if(it == container.end()) {
        epoch_manager.LeaveEpoch(epoch_node_p);

        return false;
      }

      bool found_flag = false;
      ItemPointer *found_value = nullptr;
      // We iterator over values stored with the given key
      // v should be ItemPointer *
      for(ItemPointer *v : it->second) {
        if((v->block == value.block) &&
           (v->offset == value.offset)) {
          found_flag = true;
          found_value = v;
        }
      }
      
      // If the value was not found, then just leave the epoch
      // and return false to notify the caller
      if(found_flag == false) {
        assert(found_value == nullptr);
        
        epoch_manager.LeaveEpoch(epoch_node_p);

        return false;
      }

      // We will CAS on top of this
      const BaseNode *node_p = snapshot_p->node_p;
      NodeID node_id = snapshot_p->node_id;

      // If node_p is a delta node then we have to use its
      // delta value
      int depth = 1;

      if(node_p->IsDeltaNode() == true) {
        const DeltaNode *delta_node_p = \
          static_cast<const DeltaNode *>(node_p);

        depth = delta_node_p->depth + 1;
      }

      const LeafDeleteNode *delete_node_p = \
        new LeafDeleteNode{key, found_value, depth, node_p};

      bool ret = InstallNodeToReplace(node_id,
                                      delete_node_p,
                                      node_p);
      if(ret == true) {
        bwt_printf("Leaf Delete delta CAS succeed\n");

        // This will actually not be used anymore, so maybe
        // could save this assignment
        snapshot_p->SwitchPhysicalPointer(delete_node_p);
        
        // This piece of memory holds ItemPointer, and is allocated by
        // InsertEntry() in its wrapper class. We need to free the memory
        // when the index is deleted
        delete found_value;

        // If install is a success then just break from the loop
        // and return
        break;
      } else {
        bwt_printf("Leaf Delete delta CAS failed\n");

        delete delete_node_p;

        context.abort_counter++;
      }

      delete_abort_count.fetch_add(context.abort_counter);

      // We reach here only because CAS failed
      bwt_printf("Retry installing leaf delete delta from the root\n");
    }

    epoch_manager.LeaveEpoch(epoch_node_p);

    return true;
  }
  
  #endif

  /*
   * GetValue() - Fill a value list with values stored
   *
   * This function accepts a value list as argument,
   * and will copy all values into the list
   *
   * The return value is used to indicate whether the value set
   * is empty or not
   */
  bool GetValue(const KeyType &search_key,
                std::vector<ValueType> &value_list) {
    EpochNode *epoch_node_p = epoch_manager.JoinEpoch();

    Context context{search_key};

    Traverse(&context, true);

    NodeSnapshot *snapshot = GetLatestNodeSnapshot(&context);
    LogicalLeafNode *logical_node_p = snapshot->GetLogicalLeafNode();
    KeyValueSet &container = logical_node_p->GetContainer();

    typename KeyValueSet::iterator it = container.find(search_key);
    if(it != container.end()) {
      value_list = std::move(std::vector<ValueType>{it->second.begin(),
                                                    it->second.end()});

      epoch_manager.LeaveEpoch(epoch_node_p);

      return true;
    } else {
      epoch_manager.LeaveEpoch(epoch_node_p);

      return false;
    }

    assert(false);
    return false;
  }

  /*
   * GetValue() - Return an unordered set of values given the key
   *
   * This is an overridden version of the first GetValue(). The
   * overhead is slightly less than the previous one since it does
   * not construct a vector
   */
  ValueSet GetValue(const KeyType &search_key) {
    EpochNode *epoch_node_p = epoch_manager.JoinEpoch();

    Context context{search_key};

    Traverse(&context, true);

    NodeSnapshot *snapshot = GetLatestNodeSnapshot(&context);
    LogicalLeafNode *logical_node_p = snapshot->GetLogicalLeafNode();
    KeyValueSet &container = logical_node_p->GetContainer();

    typename KeyValueSet::iterator it = container.find(search_key);
    if(it != container.end()) {
      epoch_manager.LeaveEpoch(epoch_node_p);

      return ValueSet{it->second};
    } else {
      epoch_manager.LeaveEpoch(epoch_node_p);

      return ValueSet{};
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
  // Key comparator
  const KeyComparator key_cmp_obj;

  // Wrapped key comparator
  const WrappedKeyComparator wrapped_key_cmp_obj;
  
  // Wrapped raw key comparator
  const WrappedRawKeyComparator wrapped_raw_key_cmp_obj;

  // Raw key eq checker
  const KeyEqualityChecker key_eq_obj;
  
  // Wrapped key eq checker
  const WrappedKeyEqualityChecker wrapped_key_eq_obj;
  
  // Wrapped raw key eq checker
  const WrappedRawKeyEqualityChecker wrapped_raw_key_eq_obj;

  // Raw key hasher
  const KeyHashFunc key_hash_obj;
  
  // Wrapped key hasher
  const WrappedKeyHashHashFunc wrapped_key_hash_obj;

  // Wrapped raw key hasher
  const WrappedRawKeyHashFunc wrapped_raw_key_hash_obj;

  // Compares values for a < relation
  const ValueComparator value_cmp_obj;

  // Check whether values are equivalent
  const ValueEqualityChecker value_eq_obj;
  
  // Hash ValueType into a size_t
  const ValueHashFunc value_hash_obj;
  
  // The following three are used for std::pair<KeyType, NodeID>
  const KeyNodeIDPairComparator key_node_id_pair_cmp_obj;
  const KeyNodeIDPairEqualityChecker key_node_id_pair_eq_obj;
  const KeyNodeIDPairHashFunc key_node_id_pair_hash_obj;
  
  // The following three are used for
  // std::unordered_set<std::pair<KeyTYpe, ValueType>>
  // and for searching
  const KeyValuePairComparator key_value_pair_cmp_obj;
  const KeyValuePairEqualityChecker key_value_pair_eq_obj;
  const KeyValuePairHashFunc key_value_pair_hash_obj;

  std::atomic<NodeID> root_id;
  NodeID first_node_id;
  std::atomic<NodeID> next_unused_node_id;
  std::array<std::atomic<const BaseNode *>, MAPPING_TABLE_SIZE> mapping_table;

  std::atomic<uint64_t> insert_op_count;
  std::atomic<uint64_t> insert_abort_count;

  std::atomic<uint64_t> delete_op_count;
  std::atomic<uint64_t> delete_abort_count;

  std::atomic<uint64_t> update_op_count;
  std::atomic<uint64_t> update_abort_count;

  InteractiveDebugger idb;

  EpochManager epoch_manager;

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
    for(int i = 0;i < (int)raw_key_list.size();i++) {
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

    for(int i = 0;i < (int)raw_key_list.size();i++) {
      // Construct the data item using a key and a list of values
      DataItem item{KeyType{raw_key_list[i]},
                    value_list_list[i]};

      temp_node_p->data_list.push_back(item);
    }

    return temp_node_p;
  }

  /*
   * DebugUninstallNode() - Uninstall a node forcibally
   *
   * This could not be used inside normal operation since this operation
   * is not thread safe (not using CAS)
   */
  void DebugUninstallNode(NodeID node_id) {
    mapping_table[node_id] = 0LU;
  }

  /*
   * DebugGetContext() - Get a context object pointer for debugging
   *
   * It assumes non-leftmost, root, no lbound_p context
   * If you need more than that just change the member of Context
   */
  Context *DebugGetContext(const KeyType &search_key,
                           NodeSnapshot *snapshot_p) {
    Context *context_p = new Context{search_key};

    context_p->path_list.push_back(std::move(snapshot_p));

    return context_p;
  }
  
  /*
   * Iterator Interface
   */
  class ForwardIterator;
  
  /*
   * Begin() - Return an iterator pointing the first element in the tree
   *
   * If the tree is currently empty, then the iterator is both a begin
   * iterator and an end iterator (i.e. both flags are set to true). This
   * is a valid state.
   */
  ForwardIterator Begin() {
    return ForwardIterator{this};
  }
  
  /*
   * Begin() - Return an iterator using a given key
   *
   * The iterator returned will points to a data item whose key is greater than
   * or equal to the given start key. If such key does not exist then it will
   * be the smallest key that is greater than start_key
   */
  ForwardIterator Begin(const KeyType &start_key) {
    return ForwardIterator{this, start_key};
  }

  /*
   * Iterators
   */

  /*
   * class ForwardIterator - Iterator that supports forward iteration of
   *                         tree elements
   *
   * NOTE: An iterator could be begin() and end() iterator at the same time,
   * as long as the container is empty. To correctly identify this case, we
   * need to try loading the first page with key = -Inf to test whether
   * there is any element stored in the tree. And if not then the iterator
   * is both begin() and end() iterator at the same time
   */
  class ForwardIterator {
   public:
    /*
     * Default Constructor
     *
     * NOTE: We try to load the first page using -Inf as the next key
     * during construction in order to correctly identify the case where
     * the tree is empty, and such that begin() iterator equals end() iterator
     */
    ForwardIterator(BwTree *p_tree_p) :
      tree_p{p_tree_p},
      logical_node_p{nullptr},
      raw_key_p{nullptr},
      value_set_p{nullptr},
      key_it{},
      value_it{},
      // On initialization, next key is set to -Inf
      // to indicate it is begin() vector
      next_key{tree_p->GetNegInfKey()},
      // These two will be set in LoadNextKey()
      is_begin{false},
      is_end{false},
      key_distance{0},
      value_distance{0} {
      // 1. If the tree is not empty then this prepares the first page
      // 2. If LoadNextKey() sees +Inf on the page, then it set is_end flag
      // 3. If LoadNextKey() sees next_key being -Inf then it set is_begin flag
      // and now the iterator is a begin() and end() iterator at the same time
      LoadNextKey();

      return;
    }
    
    /*
     * Constructor - Construct an iterator given a key
     *
     * The given key would locate the iterator on an data item whose
     * key is >= the given key. This is useful in range query if
     * a starting key could be derived according to conditions
     */
    ForwardIterator(BwTree *p_tree_p,
                    const KeyType start_key) :
      tree_p{p_tree_p},
      logical_node_p{nullptr},
      raw_key_p{nullptr},
      value_set_p{nullptr},
      key_it{},
      value_it{},
      // We set next_key here, and it will be loaded by LoadNextKey()
      next_key{start_key},
      // These two will be set in LoadNextKey()
      is_begin{false},
      is_end{false},
      key_distance{0},
      value_distance{0} {
      // For a given key rather than -Inf, noe we have an iterator
      // whose first element is on a key >= given start key
      LoadNextKey();

      return;
    }
    
    /*
     * Copy Constructor - Constructs a new iterator instance from existing one
     *
     * In ForwardIterator we always maintain the property that logical leaf node
     * is not shared between iterators. This implies we need to also copy the
     * logical leaf node during copy construction.
     */
    ForwardIterator(const ForwardIterator &it) :
      tree_p{it.tree_p},
      // We allocate memory here and call copy constructor
      logical_node_p{new LogicalLeafNode{*it.logical_node_p}},
      next_key{it.next_key},
      // These two also need to be copied from the source
      is_begin{it.is_begin},
      is_end{it.is_end},
      key_distance{it.key_distance},
      value_distance{it.value_distance} {
      // First locate key using key distance
      key_it = logical_node_p->key_value_set.begin();
      std::advance(key_it, key_distance);
      
      // Next locate value using value distance
      value_it = key_it->second.begin();
      std::advance(value_it, value_distance);
      
      // This refers to the raw key pointer in the new object
      raw_key_p = &key_it->first.key;
      value_set_p = &key_it->second;
      
      return;
    }
    
    /*
     * operator= - Assigns one object to another
     *
     * DONE: As an optimization we could define an assignment operation
     * for logical leaf node, and direct assign the logical leaf node from
     * the source object to the current object
     */
    ForwardIterator &operator=(const ForwardIterator &it) {
      // It is crucial to prevent self assignment since we do pointer
      // operation here
      if(this == &it) {
        return *this;
      }
      
      // First copy the logical node into current instance
      // DO NOT NEED delete; JUST DO A VALUE COPY
      assert(logical_node_p != nullptr);
      *logical_node_p = *it.logical_node_p;
      
      // Copy everything that could be copied
      tree_p = it.tree_p;
      next_key = it.next_key;
      
      is_begin = it.is_begin;
      is_end = it.is_end;
      
      key_distance = it.key_distance;
      value_distance = it.value_distance;
      
      // The following is copied from the copy constructor
      
      // NOTE: It is possible that for an empty container
      // the key value set is empty. In that case key_distance
      // and value_distance must be 0
      key_it = logical_node_p->key_value_set.begin();
      std::advance(key_it, key_distance);

      value_it = key_it->second.begin();
      std::advance(value_it, value_distance);

      raw_key_p = &key_it->first.key;
      value_set_p = &key_it->second;
      
      // The above is copied from the copy constructor
      
      return *this;
    }
    
    /*
     * operator*() - Return the value reference currently pointed to by this
     *               iterator
     *
     * NOTE: We need to return a constant reference to both save a value copy
     * and also to prevent caller modifying value using the reference
     */
    inline const ValueType &operator*() {
      // This itself is a ValueType reference
      return (*value_it);
    }
    
    /*
     * operator->() - Returns the value pointer pointed to by this iterator
     *
     * Note that this function returns a contsnat pointer which can be used
     * to access members of the value, but cannot modify
     */
    inline const ValueType *operator->() {
      return &(*value_it);
    }
    
    /*
     * operator< - Compares two iterators by comparing their current key
     *
     * Since the structure of the tree keeps changing, there is not a
     * universal coordinate system that allows us to compare absolute
     * positions of two iterators, all comparisons are done using the current
     * key associated with values.
     *
     * NOTE: It is possible that for an iterator, no raw keys are stored. This
     * happens when the tree is empty, or the requested key is larger than
     * all existing keys in the tree. In that case, end flag is set, so
     * in this function we check end flag first
     */
    inline bool operator<(const ForwardIterator &it) const {
      if(it.is_end == true) {
        if(is_end == true) {
          // If both are end iterator then no one is less than another
          return false;
        } else {
          // Otherwise, the left one is smaller as long as the
          // RHS is an end iterator
          return true;
        }
      }
      
      // If none of them is end iterator, we simply do a key comparison
      return tree_p->RawKeyCmpLess(*raw_key_p, *it.raw_key_p);
    }
    
    /*
     * operator==() - Compares whether two iterators refer to the same key
     *
     * If both iterators are end iterator then we return true
     * If one of them is not then the result is false
     * Otherwise the result is the comparison result of current key
     */
    inline bool operator==(const ForwardIterator &it) const {
      if(it.is_end == true) {
        if(is_end == true) {
          // Two end iterators are equal to each other
          return true;
        } else {
          // Otherwise they are not equal
          return false;
        }
      }
      
      return tree_p->RawKeyCmpEqual(*raw_key_p, *it.raw_key_p);
    }

    /*
     * Destructor
     *
     * NOTE: Since we always make copies of logical node object when copy
     * constructing the iterator, we could always safely delete the logical
     * node, because its memory is not shared between iterators
     */
    ~ForwardIterator() {
      // This holds even if the tree is empty
      assert(logical_node_p != nullptr);

      delete logical_node_p;

      return;
    }
    
    /*
     * Prefix operator++ - Move the iterator ahead and return the new iterator
     *
     * This operator moves iterator first, and then return the operator itself
     * as value of expression
     *
     * If the iterator is end() iterator then we do nothing
     */
    inline ForwardIterator &operator++() {
      if(is_end == true) {
        return *this;
      }
      
      is_begin = false;
      MoveAheadByOne();
      
      return *this;
    }
    
    /*
     * Postfix operator++ - Move the iterator ahead, and return the old one
     *
     * For end() iterator we do not do anything but return the same iterator
     */
    inline ForwardIterator operator++(int) {
      if(is_end == true) {
        return *this;
      }
      
      // Make a copy of the current one before advancing
      ForwardIterator temp = *this;
      
      // Since it is forward iterator, it is sufficient
      // to set begin flag to false everytime the iterator is advanced
      // But this has to be done after saving temp
      is_begin = false;
      
      MoveAheadByOne();
      
      return temp;
    }
    
    /*
     * IsEnd() - Returns true if we have reached the end of iteration
     *
     * This is just a wrapper of the private member is_end
     */
    inline bool IsEnd() const {
      return is_end;
    }
    
    /*
     * IsBegin() - Returns true if this iterator points to the first element
     *             in the tree
     *
     * NOTE: Since the tree keeps changing potentially, this method only
     * guarantees "first element" at some time in the past, which may not
     * reflect the current state of the container
     */
    inline bool IsBegin() const {
      return is_begin;
    }
    
    /*
     * GetCurrentKey() - Returns a const pointer to current key asociated with
     *                   the value
     *
     * We need to return constant pointer to avoid caller modifying the key
     * returned by this method.
     */
    inline const RawKeyType *GetCurrentKey() {
      // OK This is not elegant
      // but the wrapper insists that
      return raw_key_p;
    }

   private:
    // We need access to the tree in order to traverse down using
    // a low key to leaf node level
    BwTree *tree_p;

    // The consolidated version of current leaf node
    // the following four members all refer to objects inside this
    // logical node
    // NOTE: If iterator is copy constructed, then we need to allocate a new
    // logical node and copy-construct the new logical leaf node using this one
    // since iterator is responsible for deallocating memory of the logical
    // leaf node, we need to prevent sharing of logical node pointers.
    // NOTE 2: However, as an optimization we move the logical leaf node
    // out of NodeSnapshot to save unnecessary memory (de)allocation
    LogicalLeafNode *logical_node_p;

    // Pointers tracking current key and value set
    const RawKeyType *raw_key_p;
    const ValueSet *value_set_p;

    // Iterators tracking current position inside the bwtree leaf node
    typename KeyValueSet::iterator key_it;
    typename ValueSet::iterator value_it;

    // The upper bound of current logical leaf node. Used to access the next
    // position (i.e. leaf node) inside bwtree
    // NOTE: We cannot use next_node_id to access next node since
    // it might become invalid since the logical node was created. The only
    // pivotal point we could rely on is the high key, which indicates a
    // lowerbound of keys we have not seen
    // NOTE 2: This has to be an object rather than pointer. The reason is that
    // after this iterator is returned to the user, the actual bwtree node
    // might be recycled by the epoch manager since thread has exited current
    // epoch. Therefore we need to copy the wrapped key from bwtree physical
    // node into the iterator
    KeyType next_key;

    // The flag indicating whether the iterator is the begin() iterator
    // This flag is set by LoadNextKey() when the function sees next_key being
    // -Inf (which implies we are scanning pages from the left most one)
    // And on each ++ or ++(int) operation, this flag is set back to false
    // since this is a forward iterator, any operation would make it
    // unqualified for begin iterator
    bool is_begin;
    
    // We use this flag to indicate whether we have reached the end of
    // iteration.
    // NOTE: We could not directly check for next_key being +Inf, since
    // there might still be keys not scanned yet even if next_key is +Inf
    // LoadNextKey() checks whether the current page has no key >= next_key
    // and the next key is +Inf for current page. If these two conditions hold
    // then we know we have reached the last key of the last page
    bool is_end;
    
    // The following two variables are used in copy construction
    // since copy construction would invaidate the key and value
    // iterator in the object being copied from for the new object,
    // we need numerical values to record current position
    
    // How many ++ operations have been performed on key-value set map
    int key_distance;
    // How many ++ operations have been performed on value set
    int value_distance;

    /*
     * LoadNextKey() - Load logical leaf page whose low key <= next_key
     *
     * NOTE: Consider the case where there are two nodes [1, 2, 3] [4, 5, 6]
     * after we have scanned [1, 2, 3] (i.e. got its logical leaf node object)
     * the node [4, 5, 6] is merged into [1, 2, 3], forming [1, 2, 3, 4, 5, 6]
     * then when we query again using the saved high key = 4, we would still
     * be reading [1, 2, 3] first, which is not consistent (duplicated values)
     *
     * To address this problem, after loading a logical node, we need to advance
     * the key iterator to locate the first key that >= next_key
     */
    void LoadNextKey() {
      // Caller needs to guarantee this function not being called if
      // we have already reached the end
      assert(is_end == false);

      // Special case: If the next key is NegInf
      // then we know this is begin() iterator
      // and the other members are either nullptr or empty
      if(next_key.IsNegInf() == true) {
        assert(logical_node_p == nullptr);
        assert(raw_key_p == nullptr);
        assert(value_set_p == nullptr);
        
        // When we load -Inf then we know it is the first time
        // we load a page, from the first page
        is_begin = true;
      } else {
        assert(is_begin == false);
      }

      // First join the epoch to prevent physical nodes being deallocated
      // too early
      EpochNode *epoch_node_p = tree_p->epoch_manager.JoinEpoch();

    load_next_key:
      Context context{next_key};

      // Traverse to the leaf page whose range includes the key
      // NOTE: We do not collect values associated with the key
      // by setting the second argument to false
      // Since we are actually just using the low key to find page
      // rather than trying to collect its values
      tree_p->Traverse(&context, false);

      // Then collect all data in the logical leaf node
      NodeSnapshot *snapshot_p = tree_p->GetLatestNodeSnapshot(&context);
      if(snapshot_p->has_data == true) {
        assert(snapshot_p->has_metadata == true);
      } else {
        tree_p->CollectAllValuesOnLeaf(snapshot_p);

        assert(snapshot_p->has_data == true);
        assert(snapshot_p->has_metadata == true);
      }

      // First move out the logical leaf node from the last snapshot
      // in path histoty
      logical_node_p = snapshot_p->MoveLogicalLeafNode();

      // It is a map interator
      key_it = logical_node_p->key_value_set.begin();
      key_distance = 0;
      
      do {
        // This way we could hit the end of key value set
        // Consider the case [1, 2, 3] [4, 5, 6], after we
        // have scanned [1, 2, 3], all keys were deleted except 1
        // then we query the tree with next key = 4. In that case
        // we cannot find a key >= 4 in the page returned
        // In this case we should proceed to the next page using 
        // the high key of current page
        if(key_it == logical_node_p->key_value_set.end()) {
          if(logical_node_p->ubound_p->IsPosInf() == true) {
            // Set the flag to direcyly indicate caller that we
            // have reached the end of iteration
            is_end = true;

            // NOTE: At this point, raw_key_p is not set
            // so we cannot compare the iterator with other
            // iterators using keys
            // We need to inspect is_end first
            return;
          } else {
            // Need to set next key as the high key of current page, since
            // in this situation, current node does not contain any key
            // that >= the next key, so we go to the next page by using high
            // key as the search key
            next_key = *logical_node_p->ubound_p;

            goto load_next_key;
          }
        }
        
        if(tree_p->KeyCmpLess(key_it->first, next_key) == true) {
          // These two should always be increamented together
          key_it++;
          key_distance++;
        } else {
          break;
        }
      } while(1);

      // It is the first value associated with the key
      value_it = key_it->second.begin();
      value_distance = 0;

      // Then prepare for next key
      // NOTE: Must copy the key, since the key needs to be preserved
      // across many query sessions
      // NOTE 2: Once we have reached the last logical leaf page, next_key
      // would be +Inf. That is the reason we could not check for next_key
      // being +Inf to decide whether end has been reached
      next_key = *logical_node_p->ubound_p;

      // Set the two shortcut pointers (they are not really needed
      // but we keep them as a shortcut)
      raw_key_p = &key_it->first.key;
      value_set_p = &key_it->second;

      // Leave the epoch, since we have already had all information
      // NOTE: Before this point we need to collect all data inside
      // shared data structure since after this point they might be
      // deallocated by other threads
      tree_p->epoch_manager.LeaveEpoch(epoch_node_p);

      return;
    }
    
    /*
     * IsLastLeafPage() - Return true if the leaf page is the last one
     *
     * We check for last page by checking whether the next key is +Inf
     * If it is true then we know we are now on the last page, and if keys
     * run out then we further know we are at the end of iteration
     */
    bool IsLastLeafPage() {
      // If the next key is +Inf then we are on the last page
      return next_key.IsPosInf();
    }
    
    /*
     * MoveAheadByOne() - Move the iterator ahead by one
     *
     * The caller is responsible for checking whether the iterator has reached
     * its end. If iterator has reached end then assertion fails.
     */
    inline void MoveAheadByOne() {
      // This invalidates the iterator as the begin iterator
      is_begin = false;
      
      value_it++;
      value_distance++;
      
      // If we have reached the last element for a given key,
      // then change to the next key (in key order)
      // and readjust value iterator to the first element
      if(value_it == key_it->second.end()) {
        // Try to increament the key iterator first, and if we
        // run out of keys we know this is the end of page
        key_it++;
        key_distance++;
        
        if(key_it == logical_node_p->key_value_set.end()) {
          // If we ran out of keys, and we are on the last page
          if(IsLastLeafPage() == true) {
            is_end = true;
            
            return;
          } else {
            // This will set key_it to the first key >= next_key
            // but not sure about key distance
            // NOTE: This function could result in reaching the end iterator
            // in that case key_it is invalid
            LoadNextKey();
            
            // If current page is not last page, but load next key
            // sets is_end flag, then we know the page after this page
            // has been merged into current page, and all keys >= next_key
            // has been deleted
            // This is an extremely complicated case, and it worth that many
            // lines of comments to make it clear
            if(is_end == true) {
              return;
            }
          } // if is last leaf page == true
        } else {
          // NOTE: If we switch key and do not have to load a new page
          // then we need to update the raw key pointer to reflect
          // the key change
          // During iteration this key is very important
          raw_key_p = &key_it->first.key;
        } // if key_it == end()
        
        // If we switched to the next key in current page
        // or we have loaded a new page and readjusted the key to be
        // the first element in the page, we need also to readjust the
        // value iterator to the first element for the current key
        value_it = key_it->second.begin();
        value_distance = 0;
      } // if value_it == end()
      
      return;
    }
  };

  /*
   * Interactive Debugger
   */

  /*
   * class InteractiveDebugger - Allows us to explore the tree interactively
   */
  class InteractiveDebugger {
   public:
    BwTree *tree;

    NodeID current_node_id;

    // This always points to the current top node of
    // delta chain which is exactly the node pointer
    // for current_pid
    const BaseNode *top_node_p;

    const BaseNode *current_node_p;
    NodeType current_type;

    // We use unique, order-preserving int as identity for
    // opaque key and value types
    uint64_t next_key_id;
    uint64_t next_value_id;

    // This stores mapping from key to value
    // These values are irrelevant to actual values
    // except that they preserve order
    std::map<KeyType, uint64_t, WrappedKeyComparator> key_map;
    std::unordered_map<ValueType,
                       uint64_t,
                       ValueHashFunc,
                       ValueEqualityChecker> value_map;

    // Stacks to record history position and used for backtracking
    std::stack<const BaseNode *> node_p_stack;
    std::stack<const BaseNode *> top_node_p_stack;
    std::stack<NodeID> node_id_stack;
    std::stack<bool> need_switch_stack;

    // Used as a buffer to hold keys
    std::vector<KeyType> key_list;

    // Also used as a buffer to hold PID
    std::vector<NodeID> node_id_list;

    // We catch this for debugging from the place where we put it
    Context *context_p;

    std::mutex key_map_mutex;

    /*
     * Constructor - Takes BwTree object pointer as argument
     *
     * NOTE: key_map is an std::map object which uses WrappedKeyComparator
     * to compare keys. However, WrappedKeyComparator cannot be trivially
     * constructed. As a solution, we pass comparator object to std::map
     * as construction argument to force std::map to use our
     * given constructor instead of trying to construct one itself
     */
    InteractiveDebugger(BwTree *p_tree)
        : tree(p_tree),
          current_node_id(0),
          current_node_p(nullptr),
          next_key_id(0),
          next_value_id(0),
          // The map object could take a comparator as argument
          // and use the given comparator to compare keys
          key_map{p_tree->wrapped_key_cmp_obj} {
      InitKeyMap();

      return;
    }

    /*
     * Destructor
     */
    ~InteractiveDebugger() {}

    /*
     * AddKey() - Record the key in its map such that we could sort them
     *
     * This should be called in Traverse() since all function must
     * call Traverse()
     */
    void AddKey(const KeyType &key) {
      key_map_mutex.lock();

      key_map[key] = 0;

      key_map_mutex.unlock();

      return;
    }

    /*
     * GetKeyID() - Return a unique ID for each unique key
     *
     * The ID is just tentative, and they will be sorted once all keys
     * have been collected
     */
    std::string GetKeyID(const KeyType& key) {
      // We append this to every value
      std::string type_str = \
        "(type " + std::to_string(static_cast<int>(key.type)) + ")";

      auto it = key_map.find(key);
      if (it == key_map.end()) {
        uint64_t id = next_key_id++;
        key_map[key] = id;

        // This value is usually not used
        return std::string("key-") + std::to_string(id) + type_str;
      }

      // 0 is -inf, max is +inf
      if (key_map[key] == 0) {
        return "-Inf" + type_str;
      } else if (key_map[key] == key_map.size() - 1) {
        return "+Inf" + type_str;
      } else {
        return std::string("key-") + std::to_string(key_map[key]) + type_str;
      }

      assert(false);
      return "";
    }

    /*
     * GetValueID() - Get a unique ID for each unique value
     */
    std::string GetValueID(const ValueType& value) {
      // If the value has not yet been recorded, allocate
      // an ID for it, and return the new ID
      auto it = value_map.find(value);
      if (it == value_map.end()) {
        uint64_t id = next_value_id++;
        value_map[value] = id;

        return std::string("val-") + std::to_string(id);
      }

      return std::string("val-") + std::to_string(value_map[value]);
    }

    /*
     * PrintPrompt() - Print the debugger prompt
     */
    void PrintPrompt() {
      dummy_out << "[(" << NodeTypeToString(current_type)
                << ") NodeID=" << current_node_id << "]>> ";

      return;
    }

    /*
     * PrepareNode() - Save current information for switching to new node
     *
     * This function does not deal with any NodeID change
     */
    void PrepareNode(const BaseNode *node_p,
                     bool need_switch = false) {
      // Node pointer must be valid
      assert(node_p != nullptr);

      node_p_stack.push(current_node_p);

      current_node_p = node_p;
      current_type = node_p->GetType();

      need_switch_stack.push(need_switch);

      return;
    }

    /*
     * PrepareNodeByID() - Change NodeID to a new one, and switch node
     *
     * NOTE: If the node ID is invalid, this function will return false
     * This would happen if we are on the last node
     */
    bool PrepareNodeByID(NodeID node_id,
                         bool init_flag = false) {
      // If NodeId is invalid just return false
      if(node_id == INVALID_NODE_ID) {
        return false;
      }

      // If We are loading root node
      if(init_flag == true) {
        current_node_id = node_id;
        current_node_p = tree->GetNode(node_id);

        // The top pointer of a delta chain
        top_node_p = current_node_p;
        current_type = current_node_p->GetType();

        return true;
      } else {
        // Save the root of delta chain and the corresponding NodeID
        node_id_stack.push(current_node_id);
        top_node_p_stack.push(top_node_p);

        // Also need to check for each element's identity somewhere else
        assert(node_id_stack.size() == top_node_p_stack.size());

        current_node_id = node_id;

        // It calls push for node_stack and renew the type
        // We also need to switch in this case
        PrepareNode(tree->GetNode(node_id), true);

        // We only change this node when PID changes
        // NOTE: This must be called after PrepareNode() since
        // only after that current_node_p is updated
        top_node_p = current_node_p;

        return true;
      }

      assert(false);
      return false;
    }

    /*
     * NodeTypeToString() - Convert NodeType enum variable to a string
     */
    std::string NodeTypeToString(NodeType type) {
      switch(type) {
        case NodeType::LeafType:
          return "Leaf";
        case NodeType::InnerType:
          return "Inner";
        case NodeType::LeafInsertType:
          return "Leaf Insert";
        case NodeType::LeafDeleteType:
          return "Leaf Delete";
        case NodeType::LeafSplitType:
          return "Leaf Split";
        case NodeType::LeafRemoveType:
          return "Leaf Remove";
        case NodeType::LeafMergeType:
          return "LeafMerge";
        case NodeType::InnerInsertType:
          return "Inner Insert";
        case NodeType::InnerDeleteType:
          return "Inner Delete";
        case NodeType::InnerSplitType:
          return "Inner Split";
        case NodeType::InnerRemoveType:
          return "Inner Remove";
        case NodeType::InnerMergeType:
          return "InnerMerge";
        case NodeType::LeafUpdateType:
          return "LeafUpdate";
        default:
          return "Unknown Type (Error!)";
      }

      assert(false);
      return "";
    }

    /*
     * ProcessPrint() - Process print command
     *
     * print node-pointer - Prints current pointer address
     * print type - Prints current type
     */
    void ProcessPrint() {
      std::string s;
      std::cin >> s;

      if (s == "") {
        dummy_out << "Nothing to print!" << std::endl;
        return;
      } else if (s == "node-pointer") {
        dummy_out << current_node_p << std::endl;
      } else if (s == "type") {
        dummy_out << static_cast<int>(current_type)
                  << " ("
                  << NodeTypeToString(current_type)
                  << ")"
                  << std::endl;
      } else {
        dummy_out << "Unknown print argument: " << s << std::endl;
      }

      return;
    }

    /*
     * ProcessGotoChild() - Go to the child node of a delta node
     *
     * This does not work for inner and leaf nodes
     */
    void ProcessGotoChild() {
      switch (current_node_p->type) {
        case NodeType::LeafInsertType:
        case NodeType::LeafDeleteType:
        case NodeType::LeafSplitType:
        case NodeType::LeafRemoveType:
        case NodeType::LeafMergeType:
        case NodeType::LeafAbortType:
        case NodeType::InnerInsertType:
        case NodeType::InnerDeleteType:
        case NodeType::InnerSplitType:
        case NodeType::InnerRemoveType:
        case NodeType::InnerMergeType:
        case NodeType::InnerAbortType:
        case NodeType::LeafUpdateType: {
          const DeltaNode *delta_node_p = \
            static_cast<const DeltaNode *>(current_node_p);

          // Make this current node, do not refresh PID
          PrepareNode(delta_node_p->child_node_p);
          break;
        }
        case NodeType::LeafType:
        case NodeType::InnerType:
          dummy_out << "Type (" << NodeTypeToString(current_type)
                    << ") does not have child node" << std::endl;
          break;
      }

      return;
    }

    /*
     * ProcessGotoSplitSibling() - Go to split sibling of a split delta node
     */
    void ProcessGotoSplitSibling() {
      NodeID sibling_node_id = INVALID_NODE_ID;

      if(current_type == NodeType::InnerSplitType) {
        const InnerSplitNode *split_node_p = \
          static_cast<const InnerSplitNode *>(current_node_p);

        sibling_node_id = split_node_p->split_sibling;
      } else if(current_type == NodeType::LeafSplitType) {
        const LeafSplitNode *split_node_p = \
          static_cast<const LeafSplitNode *>(current_node_p);

        sibling_node_id = split_node_p->split_sibling;
      } else {
        dummy_out << "Type (" << NodeTypeToString(current_type)
                  << ") does not have split sibling" << std::endl;
        return;
      }

      // This should not happen, but just in case
      if(sibling_node_id == INVALID_NODE_ID) {
        dummy_out << "NodeID is INVALID_NODE_ID";

        return;
      }

      PrepareNodeByID(sibling_node_id);

      return;
    }

    /*
     * ProcessGotoSibling() - Goto sibling node (only for leaf nodes)
     */
    void ProcessGotoSibling() {
      NodeID next_node_id = current_node_p->metadata.next_node_id;

      // This may happen for the last node in the chain
      if(next_node_id == INVALID_NODE_ID) {
        dummy_out << "NodeID is INVALID_NODE_ID";

        return;
      }

      PrepareNodeByID(next_node_id);

      return;
    }

    /*
     * ProcessGotoMergeSibling() - Go to the right branch of a merge node
     */
    void ProcessGotoMergeSibling() {
      const BaseNode *node_p = nullptr;

      if(current_type == NodeType::InnerMergeType) {
        const InnerMergeNode *merge_node_p = \
          static_cast<const InnerMergeNode *>(current_node_p);

        node_p = merge_node_p->right_merge_p;
      } else if(current_type == NodeType::LeafMergeType) {
        const LeafMergeNode *merge_node_p = \
          static_cast<const LeafMergeNode *>(current_node_p);

        node_p = merge_node_p->right_merge_p;
      } else {
        dummy_out << "Type (" << NodeTypeToString(current_type)
                  << ") does not have merge sibling" << std::endl;
        return;
      }

      // Physical pointer in merge delta (PID in split delta)
      PrepareNode(node_p);

      return;
    }

    /*
     * ProcessPrintSep() - Print separator for inner node
     */
    void ProcessPrintSep() {
      if (current_type != NodeType::InnerType) {
        dummy_out << "Type (" << NodeTypeToString(current_type)
                  << ") does not have separator array" << std::endl;
        return;
      }

      const InnerNode *inner_node_p = \
        static_cast<const InnerNode *>(current_node_p);

      dummy_out << "Number of separators: " << inner_node_p->sep_list.size()
                << std::endl;

      for (SepItem it : inner_node_p->sep_list) {
        dummy_out << "[" << GetKeyID(it.key) << ", " << it.node << "]"
                  << ", ";
      }

      dummy_out << std::endl;

      return;
    }

    /*
     * ProcessPrintBound() - Process high key and low key printing
     */
    void ProcessPrintBound() {
      dummy_out << "Lower, Upper: "
                << GetKeyID(current_node_p->metadata.lbound)
                << ", "
                << GetKeyID(current_node_p->metadata.ubound)
                << std::endl;

      return;
    }

    /*
     * ProcessPrintLeaf() - Print content of leaf page
     */
    void ProcessPrintLeaf() {
      if (current_type != NodeType::LeafType) {
        dummy_out << "Type ("
                  << NodeTypeToString(current_type)
                  << ") does not have leaf array"
                  << std::endl;

        return;
      }

      const LeafNode *leaf_node_p = \
        static_cast<const LeafNode *>(current_node_p);

      dummy_out << "Node size: "
                << leaf_node_p->data_list.size()
                << std::endl;

      // Iterate through keys
      for (const DataItem &it : leaf_node_p->data_list) {
        dummy_out << GetKeyID(it.key)
                  << ": [";

        // Print all values in one line, separated by comma
        for (auto &it2 : it.value_list) {
          dummy_out << GetValueID(it2) << ", ";
        }

        dummy_out << "], " << std::endl;
      }

      return;
    }

    /*
     * ProcessPrintDelta() - Print delta node contents
     */
    void ProcessPrintDelta() {
      dummy_out << "Node type: "
                << static_cast<int>(current_type)
                << " ("
                << NodeTypeToString(current_type) << ")"
                << std::endl;

      switch (current_type) {
        case NodeType::LeafType:
        case NodeType::InnerType:
        case NodeType::LeafRemoveType:
        case NodeType::InnerRemoveType: {
          dummy_out << "Type ("
                    << NodeTypeToString(current_type)
                    << ") does not have record"
                    << std::endl;

          return;
        }
        case NodeType::LeafSplitType: {
          const LeafSplitNode *split_node_p = \
            static_cast<const LeafSplitNode *>(current_node_p);

          dummy_out << "Separator key: "
                    << GetKeyID(split_node_p->split_key)
                    << std::endl;

          dummy_out << "Sibling NodeID: "
                    << split_node_p->split_sibling
                    << std::endl;
          break;
        }
        case NodeType::InnerSplitType: {
          const InnerSplitNode *split_node_p = \
            static_cast<const InnerSplitNode *>(current_node_p);

          dummy_out << "Separator key: "
                    << GetKeyID(split_node_p->split_key)
                    << std::endl;

          dummy_out << "Sibling NodeID: "
                    << split_node_p->split_sibling
                    << std::endl;
          break;
        }
        case NodeType::LeafMergeType: {
          const LeafMergeNode *merge_node_p = \
            static_cast<const LeafMergeNode *>(current_node_p);

          dummy_out << "Separator key: "
                    << GetKeyID(merge_node_p->merge_key)
                    << std::endl;

          break;
        }
        case NodeType::InnerMergeType: {
          const InnerMergeNode *merge_node_p = \
            static_cast<const InnerMergeNode *>(current_node_p);

          dummy_out << "Separator key: "
                    << GetKeyID(merge_node_p->merge_key)
                    << std::endl;

          break;
        }
        case NodeType::LeafInsertType: {
          const LeafInsertNode *insert_node_p = \
            static_cast<const LeafInsertNode *>(current_node_p);

          dummy_out << "key, value = ["
                    << GetKeyID(insert_node_p->insert_key)
                    << ", "
                    << GetValueID(insert_node_p->value)
                    << "]"
                    << std::endl;

          break;
        }
        case NodeType::LeafDeleteType: {
          const LeafDeleteNode *delete_node_p = \
            static_cast<const LeafDeleteNode*>(current_node_p);

          dummy_out << "key, value = ["
                    << GetKeyID(delete_node_p->delete_key)
                    << ", "
                    << GetValueID(delete_node_p->value)
                    << "]"
                    << std::endl;

          break;
        }
        case NodeType::LeafUpdateType: {
          const LeafUpdateNode *update_node_p = \
            static_cast<const LeafUpdateNode *>(current_node_p);

          dummy_out << "key, old value, new value = ["
                    << GetKeyID(update_node_p->update_key)
                    << ", "
                    << GetValueID(update_node_p->old_value)
                    << ", "
                    << GetValueID(update_node_p->new_value)
                    << "]"
                    << std::endl;

          break;
        }
        case NodeType::InnerInsertType: {
          const InnerInsertNode *insert_node_p = \
              static_cast<const InnerInsertNode *>(current_node_p);

          dummy_out << "New split sep: "
                    << GetKeyID(insert_node_p->insert_key)
                    << std::endl;

          dummy_out << "Next split sep: "
                    << GetKeyID(insert_node_p->next_key)
                    << std::endl;

          dummy_out << "New child PID: "
                    << insert_node_p->new_node_id
                    << std::endl;

          break;
        }
        case NodeType::InnerDeleteType: {
          const InnerDeleteNode *delete_node_p =
              static_cast<const InnerDeleteNode *>(current_node_p);

          dummy_out << "Deleted key: "
                    << GetKeyID(delete_node_p->delete_key)
                    << std::endl;

          dummy_out << "Low key: "
                    << GetKeyID(delete_node_p->prev_key)
                    << std::endl;

          dummy_out << "High key: "
                    << GetKeyID(delete_node_p->next_key)
                    << std::endl;

          break;
        }
        default:
          dummy_out << "Node a delta node type: "
                    << NodeTypeToString(current_type)
                    << std::endl;

          assert(false);
          return;
      }

      // We need to print out depth for debugging
      dummy_out << "Delta depth: "
                << static_cast<const DeltaNode *>(current_node_p)->depth
                << std::endl;

      return;
    }

    /*
     * ProcessBack() - Process go back command
     */
    void ProcessBack() {
      assert(node_id_stack.size() == top_node_p_stack.size());

      if (node_p_stack.size() == 0) {
        dummy_out << "Already at root. Cannot go back" << std::endl;
        return;
      }

      // We know we are on top of a PID delta chain
      if (need_switch_stack.top() == true) {
        dummy_out << "Return to previous PID: "
                  << node_id_stack.top()
                  << std::endl;

        top_node_p = top_node_p_stack.top();
        top_node_p_stack.pop();

        current_node_id = node_id_stack.top();
        node_id_stack.pop();
      }

      need_switch_stack.pop();

      current_node_p = node_p_stack.top();
      node_p_stack.pop();

      current_type = current_node_p->type;

      return;
    }

    /*
     * ProcessPrintPath() - Print path in context_p object
     */
    void ProcessPrintPath() {
      if(context_p == nullptr) {
        dummy_out << "Context object does not exist" << std::endl;

        return;
      } else {
        dummy_out << "Path list length: "
                  << context_p->path_list.size()
                  << std::endl;
        dummy_out << "Root NodeID: "
                  << tree->root_id.load() << std::endl;
      }

      for(auto &snapshot : context_p->path_list) {
        dummy_out << snapshot.node_id;
        dummy_out << "; is_leaf = "
                  << snapshot.is_leaf;
        dummy_out << "; low key (from metadata): "
                  << GetKeyID(snapshot.node_p->metadata.lbound);

        dummy_out << std::endl;
      }

      return;
    }

    /*
     * ProcessConsolidate() - Consolidate current node and print
     */
    void ProcessConsolidate() {
      bool is_leaf = current_node_p->IsOnLeafDeltaChain();

      NodeSnapshot snapshot{is_leaf, tree};
      snapshot.node_p = current_node_p;

      if(is_leaf == true) {
        tree->CollectAllValuesOnLeaf(&snapshot);

        dummy_out << "Value list = [";
        for(auto &item : snapshot.GetLogicalLeafNode()->GetContainer()) {
          dummy_out << GetKeyID(item.first)
                    << ", ";
        }
      } else {
        tree->CollectAllSepsOnInner(&snapshot);

        dummy_out << "Sep list = [";
        for(auto &item : snapshot.GetLogicalInnerNode()->GetContainer()) {
          dummy_out << "("
                    << GetKeyID(item.first)
                    << ", "
                    << item.second
                    << "), ";
        }
      }

      NodeID id = snapshot.GetNextNodeID();
      dummy_out << "]" << std::endl;
      dummy_out << "Low key = "
                << GetKeyID(*snapshot.GetLowKey())
                << std::endl;
      dummy_out << "High key = "
                << GetKeyID(*snapshot.GetHighKey())
                << std::endl;
      dummy_out << "Next ID = "
                << ((id == INVALID_NODE_ID) ? "INVALID_ID" : std::to_string(id))
                << std::endl;

      return;
    }

    /*
     * InitKeyMap() - Initialize representation for +/-Inf key
     *
     * This function only makes sure that +/-Inf are in the map
     */
    void InitKeyMap() {
      GetKeyID(tree->GetNegInfKey());
      GetKeyID(tree->GetPosInfKey());

      return;
    }

    /*
     * SortKeyMap() - Sort all keys so that key ID reflects key order
     *
     * Assign order-preserve identifier to keys
     */
    void SortKeyMap() {
      uint64_t counter = 0;

      key_map_mutex.lock();

      for (auto it = key_map.begin(); it != key_map.end(); it++) {
        // -inf = 0; + inf = key_map.size() - 1
        it->second = counter++;
      }

      key_map_mutex.unlock();

      return;
    }

    void Start() {
      // We could not start with empty root node
      assert(PrepareNodeByID(tree->root_id.load(), true) == true);
      SortKeyMap();

      dummy_out << "********* Interactive Debugger *********\n";

      while (1) {
        PrintPrompt();

        std::string opcode;
        std::cin >> opcode;

        // If read EOF then resume BWTree execution
        if (!std::cin) {
          return;
        }

        // Ctrl-D (EOF)  will resume execution
        if (opcode == "exit") {
          exit(0);
        } else if (opcode == "continue") {
          break;
        } else if (opcode == "print") {
          ProcessPrint();
        } else if (opcode == "print-sep") {
          // print sep-PID pairs for inner node
          ProcessPrintSep();
        } else if (opcode == "print-leaf") {
          // print key-value_list pairs for leaf node
          ProcessPrintLeaf();
        } else if (opcode == "print-bound") {
          // print lower and upper bound for leaf and inenr node
          ProcessPrintBound();
        } else if (opcode == "type") {
          dummy_out << static_cast<int>(current_type)
                    << " ("
                    << NodeTypeToString(current_type)
                    << ")"
                    << std::endl;
        } else if (opcode == "goto-child") {
          // Goto child node pointed to by a physical pointer
          // in delta nodes
          ProcessGotoChild();
        } else if (opcode == "goto-split-sibling") {
          ProcessGotoSplitSibling();
        } else if (opcode == "goto-sibling") {
          ProcessGotoSibling();
        } else if (opcode == "goto-merge-sibling") {
          ProcessGotoMergeSibling();
        } else if (opcode == "print-delta") {
          // For delta nodes, print the content
          ProcessPrintDelta();
        } else if (opcode == "back") {
          // Go back to the previous position
          // (switch between PID and PID root and current node
          // is done using stacks)
          ProcessBack();
        } else if (opcode == "goto-id") {
          // Goto the first page pointed to by a PID
          NodeID target_node_id;

          std::cin >> target_node_id;
          PrepareNodeByID(target_node_id);
        } else if (opcode == "get-key-id") {
          // We push the target key into key_list
          // when we hit the assertion, and then invoke the debugger
          // then we could use its index to see the relative position of the key
          size_t key_index;
          std::cin >> key_index;

          if (key_index < 0 || key_index >= key_list.size()) {
            dummy_out << "Key index " << key_index << " invalid!" << std::endl;
          } else {
            dummy_out << GetKeyID(key_list[key_index]) << std::endl;
          }
        } else if (opcode == "get-id") {
          size_t node_id_index;
          std::cin >> node_id_index;

          if (node_id_index < 0 || node_id_index >= node_id_list.size()) {
            dummy_out << "PID index " << node_id_index << " invalid!" << std::endl;
          } else {
            dummy_out << "pid_list[" << node_id_index
                      << "] = " << node_id_list[node_id_index] << std::endl;
          }
        } else if(opcode == "get-thread-id") {
          dummy_out << std::hash<std::thread::id>()(std::this_thread::get_id()) << std::endl;
        } else if(opcode == "print-path") {
          ProcessPrintPath();
        } else if(opcode == "consolidate") {
          ProcessConsolidate();
        } else {
          dummy_out << "Unknown command: " << opcode << std::endl;
        }
      }

      return;
    }
  };  // class Interactive Debugger



  /*
   * class EpochManager - Maintains a linked list of deleted nodes
   *                      for threads to access until all threads
   *                      entering epochs before the deletion of
   *                      nodes have exited
   */
  class EpochManager {
   public:
    // Garbage collection interval (milliseconds)
    constexpr static int GC_INTERVAL = 50;

    /*
     * struct GarbageNode - A linked list of garbages
     */
    struct GarbageNode {
      const BaseNode *node_p;

      // This does not have to be atomic, since we only
      // insert at the head of garbage list
      GarbageNode *next_p;
    };

    /*
     * struct EpochNode - A linked list of epoch node that records thread count
     *
     * This struct is also the head of garbage node linked list, which must
     * be made atomic since different worker threads will contend to insert
     * garbage into the head of the list
     */
    struct EpochNode {
      // We need this to be atomic in order to accurately
      // count the number of threads
      std::atomic<int64_t> active_thread_count;
      
      // We need this to be atomic to be able to
      // add garbage nodes without any race condition
      // i.e. GC nodes are CASed onto this pointer
      std::atomic<GarbageNode *> garbage_list_p;

      // This does not need to be atomic since it is
      // only maintained by the epoch thread
      EpochNode *next_p;
    };

    // The head pointer does not need to be atomic
    // since it is only accessed by epoch manager
    EpochNode *head_epoch_p;

    // This does not need to be atomic because it is only written
    // by the epoch manager and read by worker threads. But it is
    // acceptable that allocations are delayed to the next epoch
    EpochNode *current_epoch_p;

    // This flag will be set to true sometime after the bwtree destructor is
    // called - no synchronization is guaranteed, but it is acceptable
    // since this flag is checked by the epoch thread periodically
    // so even if it missed one, it will not miss the next
    bool exited_flag;

    std::thread *thread_p;

    // The counter that counts how many free is called
    // inside the epoch manager
    // NOTE: We cannot precisely count the size of memory freed
    // since sizeof(Node) does not reflect the true size, since
    // some nodes are embedded with complicated data structure that
    // maintains its own memory
    #ifdef BWTREE_DEBUG
    std::atomic<size_t> freed_count;
    #endif

    /*
     * Constructor - Initialize the epoch list to be a single node
     *
     * NOTE: We do not start thread here since the init of bw-tree itself
     * might take a long time
     */
    EpochManager() {
      current_epoch_p = new EpochNode{};
      // These two are atomic variables but we could
      // simply assign to them
      current_epoch_p->active_thread_count = 0;
      current_epoch_p->garbage_list_p = nullptr;

      current_epoch_p->next_p = nullptr;

      head_epoch_p = current_epoch_p;

      // We allocate and run this later
      thread_p = nullptr;

      // This is used to notify the cleaner thread that it has ended
      exited_flag = false;

      // Initialize atomic counter to record how many
      // freed has been called inside epoch manager
      #ifdef BWTREE_DEBUG
      freed_count = 0UL;
      #endif

      return;
    }

    /*
     * Destructor - Stop the worker thread and cleanup resources not freed
     *
     * This function waits for the worker thread using join() method. After the
     * worker thread has exited, it synchronously clears all epochs that have
     * not been recycled by calling ClearEpoch()
     */
    ~EpochManager() {
      // Set stop flag and let thread terminate
      exited_flag = true;

      bwt_printf("Waiting for thread\n");
      thread_p->join();

      // Free memory
      delete thread_p;

      // So that in the following function the comparison
      // would always fail, until we have cleaned all epoch nodes
      current_epoch_p = nullptr;

      // If all threads has exited then all thread counts are
      // 0, and therefore this should proceed way to the end
      ClearEpoch();

      assert(head_epoch_p == nullptr);
      bwt_printf("Clean up for garbage collector\n");

      #ifdef BWTREE_DEBUG
      bwt_printf("Stat: Freed %lu nodes by epoch manager\n",
                 freed_count.load());
      #endif

      return;
    }

    /*
     * CreateNewEpoch() - Create a new epoch node
     *
     * This functions does not have to consider race conditions
     */
    void CreateNewEpoch() {
      bwt_printf("Creating new epoch...\n");

      EpochNode *epoch_node_p = new EpochNode{};

      epoch_node_p->active_thread_count = 0;
      epoch_node_p->garbage_list_p = nullptr;

      // We always append to the tail of the linked list
      // so this field for new node is always nullptr
      epoch_node_p->next_p = nullptr;

      // Update its previous node (current tail)
      current_epoch_p->next_p = epoch_node_p;

      // And then switch current epoch pointer
      current_epoch_p = epoch_node_p;

      return;
    }

    /*
     * AddGarbageNode() - Add garbage node into the current epoch
     *
     * NOTE: This function is called by worker threads so it has
     * to consider race conditions
     */
    void AddGarbageNode(const BaseNode *node_p) {
      // We need to keep a copy of current epoch node
      // in case that this pointer is increased during
      // the execution of this function
      //
      // NOTE: Current epoch must not be recycled, since
      // the current thread calling this function must
      // come from an epoch <= current epoch
      // in which case all epochs before that one should
      // remain valid
      EpochNode *epoch_p = current_epoch_p;

      // These two could be predetermined
      GarbageNode *garbage_node_p = new GarbageNode;
      garbage_node_p->node_p = node_p;

      while(1) {
        garbage_node_p->next_p = epoch_p->garbage_list_p.load();

        // Then CAS previous node with new garbage node
        bool ret = \
          epoch_p->garbage_list_p.compare_exchange_strong(garbage_node_p->next_p,
                                                          garbage_node_p);

        // If CAS succeeds then just return
        if(ret == true) {
          break;
        } else {
          bwt_printf("Add garbage node CAS failed. Retry\n");
        }
      } // while 1

      return;
    }

    /*
     * JoinEpoch() - Let current thread join this epoch
     *
     * The effect is that all memory deallocated on and after
     * current epoch will not be freed before current thread leaves
     *
     * NOTE: It is possible that prev_count < 0, because in ClearEpoch()
     * the cleaner thread will decrease the epoch counter by a large amount
     * to prevent this function using an epoch currently being recycled
     */
    EpochNode *JoinEpoch() {
try_join_again:
      // We must make sure the epoch we join and the epoch we
      // return are the same one because the current point
      // could change in the middle of this function
      EpochNode *epoch_p = current_epoch_p;

      int64_t prev_count = epoch_p->active_thread_count.fetch_add(1);
      
      // We know epoch_p is now being cleaned, so need to read the
      // current epoch again because it must have been moved
      if(prev_count < 0) {
        goto try_join_again;
      }

      return epoch_p;
    }

    /*
     * LeaveEpoch() - Leave epoch a thread has once joined
     *
     * After an epoch has been cleared all memories allocated on
     * and before that epoch could safely be deallocated
     */
    void LeaveEpoch(EpochNode *epoch_p) {
      epoch_p->active_thread_count.fetch_sub(1);

      return;
    }

    /*
     * FreeEpochDeltaChain() - Free a delta chain (used by EpochManager)
     *
     * This function differs from the one of the same name in BwTree definition
     * in the sense that for tree destruction there are certain node
     * types not being accepted. But in EpochManager we must support a wider
     * range of node types.
     *
     * For more details please refer to FreeDeltaChain in BwTree class definition
     */
    void FreeEpochDeltaChain(const BaseNode *node_p) {
      const BaseNode *next_node_p = node_p;

      while(1) {
        node_p = next_node_p;
        assert(node_p != nullptr);

        NodeType type = node_p->GetType();

        switch(type) {
          case NodeType::LeafInsertType:
            next_node_p = ((LeafInsertNode *)node_p)->child_node_p;

            delete (LeafInsertNode *)node_p;
            #ifdef BWTREE_DEBUG
            freed_count.fetch_add(1);
            #endif
            break;
          case NodeType::LeafDeleteType:
            next_node_p = ((LeafDeleteNode *)node_p)->child_node_p;

            delete (LeafDeleteNode *)node_p;
            #ifdef BWTREE_DEBUG
            freed_count.fetch_add(1);
            #endif
            break;
          case NodeType::LeafSplitType:
            next_node_p = ((LeafSplitNode *)node_p)->child_node_p;

            delete (LeafSplitNode *)node_p;
            #ifdef BWTREE_DEBUG
            freed_count.fetch_add(1);
            #endif
            break;
          case NodeType::LeafMergeType:
            FreeEpochDeltaChain(((LeafMergeNode *)node_p)->child_node_p);
            FreeEpochDeltaChain(((LeafMergeNode *)node_p)->right_merge_p);

            delete (LeafMergeNode *)node_p;
            #ifdef BWTREE_DEBUG
            freed_count.fetch_add(1);
            #endif

            // Leaf merge node is an ending node
            return;
          case NodeType::LeafRemoveType:
            delete (LeafRemoveNode *)node_p;
            #ifdef BWTREE_DEBUG
            freed_count.fetch_add(1);
            #endif

            // We never try to free those under remove node
            // since they will be freed by recursive call from
            // merge node
            //
            // TODO: Put remove node into garbage list after
            // IndexTermDeleteDelta was posted (this could only be done
            // by one thread that succeeds CAS)
            return;
          case NodeType::LeafType:
            delete (LeafNode *)node_p;
            #ifdef BWTREE_DEBUG
            freed_count.fetch_add(1);
            #endif

            // We have reached the end of delta chain
            return;
          case NodeType::InnerInsertType:
            next_node_p = ((InnerInsertNode *)node_p)->child_node_p;

            delete (InnerInsertNode *)node_p;
            #ifdef BWTREE_DEBUG
            freed_count.fetch_add(1);
            #endif
            break;
          case NodeType::InnerDeleteType:
            next_node_p = ((InnerDeleteNode *)node_p)->child_node_p;

            delete (InnerDeleteNode *)node_p;
            #ifdef BWTREE_DEBUG
            freed_count.fetch_add(1);
            #endif
            break;
          case NodeType::InnerSplitType:
            next_node_p = ((InnerSplitNode *)node_p)->child_node_p;

            delete (InnerSplitNode *)node_p;
            #ifdef BWTREE_DEBUG
            freed_count.fetch_add(1);
            #endif
            break;
          case NodeType::InnerMergeType:
            FreeEpochDeltaChain(((InnerMergeNode *)node_p)->child_node_p);
            FreeEpochDeltaChain(((InnerMergeNode *)node_p)->right_merge_p);

            delete (InnerMergeNode *)node_p;
            #ifdef BWTREE_DEBUG
            freed_count.fetch_add(1);
            #endif

            // Merge node is also an ending node
            return;
          case NodeType::InnerRemoveType:
            delete (InnerRemoveNode *)node_p;
            #ifdef BWTREE_DEBUG
            freed_count.fetch_add(1);
            #endif

            // We never free nodes under remove node
            return;
          case NodeType::InnerType:
            delete (InnerNode *)node_p;
            #ifdef BWTREE_DEBUG
            freed_count.fetch_add(1);
            #endif

            return;
          case NodeType::InnerAbortType:
            // NOTE: Deleted abort node is also appended to the
            // garbage list, to prevent other threads reading the
            // wrong type after the node has been put into the
            // list (if we delete it directly then this will be
            // a problem)
            delete (InnerAbortNode *)node_p;

            #ifdef BWTREE_DEBUG
            freed_count.fetch_add(1);
            #endif

            // Inner abort node is also a terminating node
            // so we do not delete the beneath nodes, but just return
            return;
          default:
            // This does not include INNER ABORT node
            bwt_printf("Unknown node type: %d\n", (int)type);

            assert(false);
            return;
        } // switch
      } // while 1

      return;
    }

    /*
     * ClearEpoch() - Sweep the chain of epoch and free memory
     *
     * The minimum number of epoch we must maintain is 1 which means
     * when current epoch is the head epoch we should stop scanning
     *
     * NOTE: There is no race condition in this function since it is
     * only called by the cleaner thread
     */
    void ClearEpoch() {
      bwt_printf("Start to clear epoch\n");

      while(1) {
        // Even if current_epoch_p is nullptr, this should work
        if(head_epoch_p == current_epoch_p) {
          bwt_printf("Current epoch is head epoch. Do not clean\n");

          break;
        }

        // If we have seen an epoch whose count is not zero then all
        // epochs after that are protected and we stop
        if(head_epoch_p->active_thread_count.load() != 0) {
          bwt_printf("Head epoch is not empty. Return\n");

          break;
        }
        
        // If some thread joins the epoch between the previous branch
        // and the following fetch_sub(), then fetch_sub() returns a positive
        // number, which is the number of threads that have joined the epoch
        // since last epoch counter testing.
        
        if(head_epoch_p->active_thread_count.fetch_sub(max_thread_count) > 0) {
          bwt_printf("Some thread sneaks in after we have decided"
                     " to clean. Return\n");

          // Must add it back to let the next round of cleaning correctly
          // identify empty epoch
          head_epoch_p->active_thread_count.fetch_add(max_thread_count);
                     
          break;
        }
        
        // After this point all fetch_add() on the epoch counter would return
        // a negative value which will cause re-read of current_epoch_p
        // to prevent joining an epoch that is being deleted

        // If the epoch has cleared we just loop through its garbage chain
        // and then free each delta chain

        const GarbageNode *next_garbage_node_p = nullptr;

        // Walk through its garbage chain
        for(const GarbageNode *garbage_node_p = head_epoch_p->garbage_list_p.load();
            garbage_node_p != nullptr;
            garbage_node_p = next_garbage_node_p) {
          FreeEpochDeltaChain(garbage_node_p->node_p);

          // Save the next pointer so that we could
          // delete current node directly
          next_garbage_node_p = garbage_node_p->next_p;

          // This invalidates any further reference to its
          // members (so we saved next pointer above)
          delete garbage_node_p;
        } // for

        // First need to save this in order to delete current node
        // safely
        EpochNode *next_epoch_node_p = head_epoch_p->next_p;
        
        delete head_epoch_p;
        //*(reinterpret_cast<unsigned char *>(head_epoch_p) - 1) = 0x66;
        //printf("delete head_epoch_p = %p\n", head_epoch_p);

        // Then advance to the next epoch
        // It is possible that head_epoch_p becomes nullptr
        // this happens during destruction, and should not
        // cause any problem since that case we also set current epoch
        // pointer to nullptr
        head_epoch_p = next_epoch_node_p;
      } // while(1) through epoch nodes

      return;
    }

    /*
     * ThreadFunc() - The cleaner thread executes this every GC_INTERVAL ms
     *
     * This function exits when exit flag is set to true
     */
    void ThreadFunc() {
      // While the parent is still running
      // We do not worry about race condition here
      // since even if we missed one we could always
      // hit the correct value on next try
      while(exited_flag == false) {
        //printf("Start new epoch cycle\n");
        CreateNewEpoch();
        ClearEpoch();

        // Sleep for 50 ms
        std::chrono::milliseconds duration(GC_INTERVAL);
        std::this_thread::sleep_for(duration);
      }

      bwt_printf("exit flag is true; thread return\n");

      return;
    }

    /*
     * StartThread() - Start cleaner thread for garbage collection
     *
     * NOTE: This is not called in the constructor, and needs to be
     * called manually
     */
    void StartThread() {
      thread_p = new std::thread{[this](){this->ThreadFunc();}};

      return;
    }

  }; // Epoch manager

}; // class BwTree

#ifdef BWTREE_PELOTON
}  // End index namespace
}  // End peloton namespace
#endif

