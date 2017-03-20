//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// skiplist.h
//
// Identification: src/include/index/skiplist.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * This is a skiplist with multi-key support, with an epoch-based garbage
 * collector.
 *
 *           Frontier  Tower         Tower              Tower
 *          +---------------+  +---------------+  +---------------+
 *          | +-----------+ |  |               |  |               |
 * level n: | | TowerNode |--------------------------------------------> ...
 *          | +-----------+ |  |               |  |               |
 *          |       |       |  |               |  |               |
 *          |       v       |  |               |  |               |
 *          |      ...      |  |               |  |               |
 *          |       |       |  |               |  |               |
 *          |       v       |  |               |  |               |
 *          | +-----------+ |  |               |  | +-----------+ |
 * level 1: | | TowerNode |------------------------>| TowerNode |------> ...
 *          | +-----------+ |  |               |  | +-----------+ |
 *          |       |       |  |               |  |       |       |
 *          |       v       |  |               |  |       v       |
 *          | +-----------+ |  | +-----------+ |  | +-----------+ |
 * level 0: | | TowerNode |----->| TowerNode |----->| TowerNode |------> ...
 *          | +-----------+ |  | +-----------+ |  | +-----------+ |
 *          |               |  |               |  |               |
 *          |               |  |   ValueList   |  |   ValueList   |
 *          +---------------+  +---------------+  +---------------+
 *
 * Each key is represented by a tower, storing all nodes in the skiplist, as
 * well as a value list.
 */

#ifndef _SKIPLIST_H
#define _SKIPLIST_H
#define SKIPLIST_PELOTON

#pragma once

#include <atomic>
#include <cassert>
#include <cstdlib>
#include <deque>
#include <iostream>
#include <map>
#include <random>
#include <sstream>
#include <thread>
#include <vector>

#endif

static std::atomic<ssize_t> size{0};

/*
 * SKIPLIST_TEMPLATE_ARGUMENTS - Save some key strokes
 */
#define SKIPLIST_TEMPLATE_ARGUMENTS                                       \
  template <typename KeyType, typename ValueType, typename KeyComparator, \
            typename KeyEqualityChecker, typename ValueEqualityChecker>

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker, typename ValueEqualityChecker>
class SkipList {
 public:
  // Here are some return states for various SkipList manipulation functions.
  // Consult their corresponding functions.

  enum class SetNextReturn {
    SUCCESS,
    DEL_SET,
    PTR_MISMATCH,
  };

  enum class DeleteNodeReturn {
    SUCCESS,
    DEL_SET,
    PTR_MISMATCH,
    INCORRECT,
  };

  enum class InsertValReturn {
    SUCCESS,
    LIST_DEL,
    DUP_VAL,
  };

  enum class InsertBetweenReturn {
    SUCCESS,
    KEY_EXISTS,
    DEL_SET,
    PTR_MISMATCH,
    INCORRECT
  };

  /**
   * @brief  NextPtrDelMark - A helper class solving the "lost update" problem.
   *
   * Consider deleting a node @c node from a linked list:
   *
   * @code
   * Before:
   * +-----+        +------+        +-----+
   * | lhs | -----> | node | -----> | rhs |
   * +-----+        +------+        +-----+
   *
   * After:
   * +-----+        +------+        +-----+
   * | lhs | --+    | node | --+--> | rhs |
   * +-----+   |    +------+   |    +-----+
   *           +---------------+
   * @endcode
   *
   * Our CAS operation can make sure that we atomically change @c lhs.next from
   * @c node to @c rhs, so that no other thread can come in and insert a new
   * node between @c lhs and @c node (because that would require modifying
   * @c lhs.next).
   *
   * However, our CAS operation cannot prevent another thread from inserting
   * a node between @c node and @c rhs:
   *
   * @code
   * Before:
   * +-----+        +------+                       +-----+
   * | lhs | -----> | node | --------------------> | rhs |
   * +-----+        +------+                       +-----+
   *
   * After:
   * +-----+        +------+        +-----+        +-----+
   * | lhs | --+    | node | -----> | bad | --+--> | rhs |
   * +-----+   |    +------+        +-----+   |    +-----+
   *           +------------------------------+
   * @endcode
   *
   * Now the newly inserted node @c bad is lost.
   *
   * Why does this happen? Well, first, why doesn't the problem happen between
   * @c lhs and @c node? The reason is that the CAS operation can make sure that
   * **one value can atomically change from one state to another state**. Our
   * CAS is like first taking a latch on @c lhs.next and then modify it.
   *
   * However, when deleting a node, we would want to lock both @c lhs.next and
   * @c node.next, since they together form a consistent state.
   *
   * Therefore, we must find a way to lock @c node.next, and disallow any change
   * by another thread.
   *
   * The solution is to embed one bit in @c node.next to mark whether this node
   * @c node is being deleted. If the mark is set, then @c node.next cannot be
   * modified.
   */
  template <typename NodeType>
  class NextPtrDelMark final {
   private:
    std::atomic<uintptr_t> data;
    static constexpr size_t delete_mark_bit = 0;
    static constexpr uintptr_t delete_mark_mask = (1 << delete_mark_bit);

   public:
    NextPtrDelMark(NodeType *next) : data{reinterpret_cast<uintptr_t>(next)} {}

    /**
     * @brief Atomically set the @c next pointer of this node.
     *
     * This function sets the @c next pointer atomically if:
     *  * The current @c next matches the expected value
     *  * The delete mark is not currently set.
     *
     * This function doesn't set the @c next pointer if the above conditions are
     * not met. When this happens, the current @c next pointer will be written
     * to @c expected_curr_next, and an error code will be returned.
     *
     * @param expected_curr_next A pointer to the expected value of the current
     * @c next pointer. If this function fails to set the @c next pointer, the
     * current value of the @c next pointer will be written out.
     *
     * @param next The value to be written to the @c next pointer.
     *
     * @code
     * SetNext(ref expected_curr_next, next) {
     *   synchronized(this) {
     *     if this.delete {
     *       expected_curr_next = this.next
     *       return DEL_SET
     *     }
     *
     *     if this.next != expected_curr_next {
     *       expected_curr_next = this.next
     *       return PTR_MISMATCH
     *     }
     *
     *     this.next = next
     *     return SUCCESS
     *   }
     * }
     * @endcode
     *
     * @return An error code.
     * If this function successfully sets the @c next pointer, return
     * @c SUCCESS.
     * Otherwise, if this function sees that the delete mark is set, return
     * @c DEL_SET.
     * Otherwise, if this function sees that the current @c next pointer doesn't
     * match the expection, return @c PTR_MISMATCH.
     */
    SetNextReturn SetNext(NodeType *expected_curr_next, NodeType *next) {
      // Prepare for our CAS operation.
      uintptr_t expected = reinterpret_cast<uintptr_t>(expected_curr_next);
      uintptr_t desired = reinterpret_cast<uintptr_t>(next);

      if (this->data.compare_exchange_strong(expected, desired)) {
        // Great! Compare-and-Swap succeeded!
        return SetNextReturn::SUCCESS;

      } else {
        // Ahh! Compare-and-Seap failed!
        // Let's figure out what caused the failure.
        SetNextReturn ret;
        if (expected & delete_mark_mask) {
          ret = SetNextReturn::DEL_SET;
        } else {
          ret = SetNextReturn::PTR_MISMATCH;
        }

        // Now we've check the delete mark. Let's put the current next pointer
        // back to *expected_curr_next, so that the user might examine it.

        // Clear the delete mark.
        expected &= ~delete_mark_mask;

        // Write out the pointer.
        expected_curr_next = reinterpret_cast<NodeType *>(expected);

        return ret;
      }
    }

    /**
     * @brief Get the value of the @c next pointer of this node.
     *
     * @warning Since our skiplist is latch-free, by the time this function
     * returns, the @c next pointer might have been changed.
     *
     * However, please note
     *  * We have enforced that once the @c delete mark is set, the @c next
     *    pointer will no longer change.
     *  * Thanks to our epoch-based garbage collection, even if the @c next
     *    pointer changes (so that the return value of this function is no
     *    longer valid), the return value still points to a node that hasn't
     *    been reclaimed memory.
     *
     * @return The value of the @c next pointer.
     */
    NodeType *GetNext() const {
      uintptr_t next = data.load();
      next &= ~delete_mark_mask;
      return reinterpret_cast<NodeType *>(next);
    }

    /**
     * @brief Atomically set the delete mark to true.
     *
     * This function sets the delete mark atomically, if
     *  * The current delete mark is not already set.
     *
     * @code
     * SetDeleteMark() {
     *   synchronized(this) {
     *     if this.delete {
     *       return false
     *     }
     *
     *     this.delete = true
     *     return true
     *   }
     * }
     * @endcode
     *
     * @return @c true if successfully set the delete mark. @c false if the
     * delete mark is already set.
     */
    bool SetDeleteMark() {
      // Atomically bitwise-or the delete_mark_mask.
      uintptr_t old_data = data.fetch_or(delete_mark_mask);

      // If delete mark already set by someone else, return false!
      return !(old_data & delete_mark_mask);
    }

    /**
     * @brief Check whether the delete mark is set.
     */
    bool DeleteMarkSet() const { return (data.load() & delete_mark_mask); }

  };  // class NextPtrDelMark<NodeType>

  class DynamicType;
  class EpochManager;
  class ValueList;
  class Tower;
  class Path;
  class Boundary;
  const static ssize_t tower_levels = 10;

  using TowerNode = typename Tower::TowerNode;
  using ValueNode = typename ValueList::ValueNode;
  using EpochNode = typename EpochManager::EpochNode;

  /**
   * @brief Any object that goes to the garbage collector should be derived
   * from this class.
   */
  class DynamicType {
   public:
    virtual ~DynamicType() {}

    /** @brief The size of the (real dynamic type of the) object. */
    virtual ssize_t GetSize() const = 0;
  };

  /**
   * @brief A tower stores all data for a specific key.
   *
   * It contains
   * - The key;
   * - A list of values;
   * - An array of nodes for that key in the skiplist.
   */
  class Tower final : public DynamicType {
   public:
    /**
     * @brief A TowerNode is a node in the skiplist, for searching.
     */
    class TowerNode final {
     private:
      /**
       * @brief The level of this node in the skiplist, 0 for a leaf node.
       */
      size_t level;

      /**
       * @brief The next pointer and the delete mark.
       */
      NextPtrDelMark<TowerNode> next_del;

     public:
      friend class Tower;

      TowerNode() : next_del{nullptr} {}

      /**
       * @brief Get the node below the current node.
       * If the current node is a leaf node, return @c nullptr.
       */
      TowerNode *GetDown() {
        if (level == 0) {
          return nullptr;
        }
        return GetTower()->GetNodeByLevel(static_cast<ssize_t>(level) - 1);
      }

      /**
       * @brief Get the tower that stores this node.
       *
       * This function relies on the fact that TowerNode's are stored inside
       * arrays in corresponding Tower's.
       */
      Tower *GetTower() const {
        const void *ptr = reinterpret_cast<const void *>(this);
        return reinterpret_cast<Tower *>((size_t)ptr -
                                         offsetof(Tower, tower_nodes[level]));
      }

      /**
       * @brief Get the key of this node.
       */
      KeyType GetKey() const { return GetTower()->GetKey(); }

      /**
       * @brief Get the value list of this key.
       */
      ValueList *GetValList() { return GetTower()->GetValList(); }

      /**
       * @brief Get the node to the right of this node.
       */
      TowerNode *GetNext() { return next_del.GetNext(); }

      /**
       * @brief Atomically set the next pointer of this node. This could fail.
       *
       * @return SetNextReturn. Could beSUCCESS, DEL_SET, or PTR_MISMATCH.
       */
      SetNextReturn SetNext(TowerNode *expected_curr_next, TowerNode *next) {
        return next_del.SetNext(expected_curr_next, next);
      }

      /**
       * @brief Atomically set the delete mark of this node. This could fail.
       *
       * @return True if delete mark wasn't set, and has been set by us.
       * False if the delete mark was already set.
       */
      bool SetDeleteMark() { return next_del.SetDeleteMark(); }

      /**
       * @brief Get the level this node is at.
       */
      ssize_t GetTowerNodeLevel() { return static_cast<ssize_t>(level); }

      /**
       * @brief Check whether the delete mark is set.
       */
      bool DeleteMarkSet() { return next_del.DeleteMarkSet(); }
    };

    /**
     * @brief Get the size of a tower. For garbage collection.
     */
    virtual ssize_t GetSize() const override final { return sizeof(Tower); }

    /**
     * @brief Get the node in this tower at a specific level.
     */
    TowerNode *GetNodeByLevel(ssize_t level) {
      assert(level < tower_levels);

      if (level < 0) {
        return nullptr;
      }

      return &tower_nodes[level];
    }

    static Tower *InlineAllocateTower(const KeyType &key_,
                                      const ValueType &val_,
                                      const size_t &true_level_) {
      Tower *tower = new Tower;
      tower->SetLevel();
      tower->key = key_;
      tower->val_list = new ValueList(val_);
      tower->true_level = true_level_;
      return tower;
    }

    /**
     * @brief Get the key of this tower.
     */
    KeyType GetKey() const { return key; }

    /**
     * @brief Get the value list of this tower.
     */
    ValueList *GetValList() { return val_list; }

    /**
     * @brief Get the number of levels in this tower.
     */
    ssize_t GetNumLevel() { return static_cast<ssize_t>(true_level); }

    ~Tower() { delete val_list; }

   private:
    TowerNode tower_nodes[tower_levels];
    KeyType key;
    ValueList *val_list;
    size_t true_level;

    void SetLevel() {
      for (size_t i = 0; i < tower_levels; ++i) {
        tower_nodes[i].level = i;
      }
    }

  };  // class Tower

  /**
   * @brief A boundary is like an iterator in a linked-list, except that it
   * stores pointers to two consecutive nodes.
   * This allows us to insert a node in between these two nodes.
   */
  class Boundary {
   public:
    Boundary(KeyEqualityChecker key_equal = KeyEqualityChecker{},
             KeyComparator key_cmp = KeyComparator{})
        : lhs_{nullptr},
          rhs_{nullptr},
          rhs_equal_{false},
          key_less_{key_cmp},
          key_equal_{key_equal} {}

    bool rhs_equal() const { return rhs_equal_; }

    TowerNode *rhs() const { return rhs_; }
    TowerNode *lhs() const { return lhs_; }

    /**
     * @brief Start a linear scan from a specific node in a linked list.
     *
     * Search for a boundary (lhs, rhs) such that
     * - lhs.key < key
     * - rhs.key >= key or rhs == null
     *
     * If we get rhs.key == key, we set rhs_equal_ = true.
     * If we get rhs.key > key or rhs == null, we set rhs_equal_ = false.
     */
    void SetBoundary(TowerNode *lhs, const KeyType &key) {
      lhs_ = lhs;

      while ((rhs_ = lhs_->GetNext()) != nullptr) {
        if (!key_less_(rhs_->GetKey(), key)) {
          break;
        } else {
          lhs_ = rhs_;
        }
      }

      rhs_equal_ = (rhs_ != nullptr) && key_equal_(rhs_->GetKey(), key);
    }

   private:
    TowerNode *lhs_;
    TowerNode *rhs_;
    bool rhs_equal_;
    KeyComparator key_less_;
    KeyEqualityChecker key_equal_;
  };  // class Boundary

  /**
   * @brief A path is just an array of boundaries from the top level to the
   * bottom level.
   *
   * We use this class as the context for a search.
   */
  class Path {
   private:
    Boundary boundaries[tower_levels];
    const KeyType key_;
    ssize_t height;
    TowerNode *from;

   public:
    Path(const KeyType &key, TowerNode *from) : key_(key), from{from} {}

    /**
     * @brief Start a search for a key from the top level until a target level.
     *
     * Try to find a boundary (lhs, rhs) such that
     * - lhs.key > key
     * - rhs.key == key
     *
     * If we found such a boundary, record the level at which we found it, and
     * return the corresponding ValueList.
     *
     * If we couldn't find such a boundary, return nullptr.
     */
    ValueList *FindPath(ssize_t target_level) {
      TowerNode *lhs = from;

      for (ssize_t i = tower_levels - 1; i >= target_level; --i) {
        // Search level i from lhs.
        boundaries[i].SetBoundary(lhs, key_);

        // Found the boundary at this level.
        if (boundaries[i].rhs_equal()) {
          height = i;
          return boundaries[i].rhs()->GetValList();
        }

        lhs = boundaries[i].lhs()->GetDown();
      }

      height = 0;
      return nullptr;
    }

    /**
     * @brief Search down to the bottom level and record the first level we
     * find a node of the given key.
     */
    bool FindDeletePath() {
      TowerNode *lhs = from;
      height = tower_levels;

      for (ssize_t i = tower_levels - 1; i >= 0; --i) {
        // Search level i from lhs.
        boundaries[i].SetBoundary(lhs, key_);

        // This is the first time we find a node of the key.
        if (boundaries[i].rhs_equal() && (height == tower_levels)) {
          height = i;
          if (height + 1 < boundaries[i].rhs()->GetTower()->GetNumLevel()) {
            return false;
          }
        }

        lhs = boundaries[i].lhs()->GetDown();
      }

      return true;
    }

    ssize_t GetHeight() { return height; }

    Boundary &GetBoundary(ssize_t level) {
      assert(level >= 0);
      assert(level < tower_levels);
      return boundaries[level];
    }

    void AdvanceBoundary(ssize_t level) {
      assert(level >= 0);
      assert(level < tower_levels);
      TowerNode *prev = boundaries[level].lhs();
      boundaries[level].SetBoundary(prev, key_);
    }
  };  // class Path

  // Continuing class SkipList...

  ValueList *SearchValueList(Path &path) { return path.FindPath(0); }

  /**
   * @brief Try once to insert a TowerNode between a boundary (lhs, rhs).
   *
   * Clearly, after a successful insertion, the boundary becomes invalid.
   *
   * @return InsertBetweenReturn, which could be SUCCESS, DEL_SET, or
   * PTR_MISMATCH.
   */
  InsertBetweenReturn InsertBetween(TowerNode *node, const Boundary &boundary) {
    if (boundary.rhs_equal()) {
      return InsertBetweenReturn::KEY_EXISTS;
    }

    TowerNode *lhs = boundary.lhs();
    TowerNode *rhs = boundary.rhs();

    TowerNode *prev = node->GetNext();
    if (node->SetNext(prev, rhs) != SetNextReturn::SUCCESS) {
      assert(0 && "set next pointer of invisible node shouldn't fail");
    }

    switch (lhs->SetNext(rhs, node)) {
      case SetNextReturn::SUCCESS:
        return InsertBetweenReturn::SUCCESS;

      case SetNextReturn::DEL_SET:
        return InsertBetweenReturn::DEL_SET;

      case SetNextReturn::PTR_MISMATCH:
        return InsertBetweenReturn::PTR_MISMATCH;

      default:
        assert(0 && "invalid return state");
        return InsertBetweenReturn::INCORRECT;
    }
  }

  /**
   * @brief MAIN API - Insert a key-value pair.
   *
   * This simply calls CondInsert with a dummy predicate. This also gives us
   * more confidence that our CondInsert is correct.
   *
   * The compiler should be smart enough to avoid huge overhead.
   */
  bool Insert(const KeyType &key, const ValueType &val) {
    static const std::function<bool(const void *)> predicate =
        [](const void *) { return false; };

    bool predicate_satisfied;
    return CondInsert(key, val, predicate, &predicate_satisfied);
  }

  /**
   * @brief Try once to delete rhs of a boundary (lhs, rhs).
   *
   * @return DeleteNodeReturn, which could be SUCCESS, DEL_SET, or PTR_MISMATCH.
   */
  DeleteNodeReturn DeleteNode(Boundary &boundary) {
    TowerNode *lhs = boundary.lhs();
    TowerNode *rhs = boundary.rhs();
    assert(boundary.rhs_equal());

    rhs->SetDeleteMark();

    TowerNode *next = rhs->GetNext();
    switch (lhs->SetNext(rhs, next)) {
      case SetNextReturn::SUCCESS:
        return DeleteNodeReturn::SUCCESS;

      case SetNextReturn::DEL_SET:
        return DeleteNodeReturn::DEL_SET;

      case SetNextReturn::PTR_MISMATCH:
        return DeleteNodeReturn::PTR_MISMATCH;

      default:
        assert(0 && "invalid return state");
        return DeleteNodeReturn::INCORRECT;
    }
  }

  /**
   * @brief MAIN API - Delete a key-value pair from the SkipList.
   *
   * @return True if we found the kv and deleted it. False if we didn't.
   */
  bool Delete(const KeyType &key, const ValueType &value) {
    auto epoch = epoch_manager->JoinEpoch();

    // 1. Search value list by key.

    // Initialize our search context.
    Path path(key, Frontier->GetNodeByLevel(tower_levels - 1));

    // Try to find the key in our skip list.
    ValueList *val_list = SearchValueList(path);

    if (!val_list) {
      // key not found.
      return false;
    }

    // 2. Delete value node from value list.

    // If we successfully deleted the value, we need to GC the value node in
    // the value list.
    ValueNode *value_node;

    // If when deleting the value, we found that the value list becomes empty,
    // we will try to mark the whole value list as deleted.
    bool value_list_deleted;

    std::tie(value_node, value_list_deleted) = val_list->DeleteVal(value);

    if (value_node == nullptr) {
      // Didn't delete a value node.
      return false;
    } else {
      epoch_manager->AddGarbageNode(value_node);
    }

    if (!value_list_deleted) {
      // Didn't delete the whole value list.
      return true;
    }

    // 3. Delete tower.

    while (!path.FindDeletePath())
      ;

    Tower *tower = path.GetBoundary(0).rhs()->GetTower();

    for (ssize_t level = path.GetHeight(); level >= 0; --level) {
      bool retry = true;

      while (retry) {
        switch (DeleteNode(path.GetBoundary(level))) {
          case DeleteNodeReturn::SUCCESS:
            // Brilliant! We have successfully deleted one node on one level.
            retry = false;
            break;

          case DeleteNodeReturn::DEL_SET:
            // Oops, the lhs node has been deleted
            path.FindDeletePath();
            break;

          case DeleteNodeReturn::PTR_MISMATCH:
            // Oops, someone has inserted a node between lhs and our node.
            // We need to rerun the search.
            path.AdvanceBoundary(level);
            break;

          default:
            assert(0 && "invalid return state");
            break;
        }
      }
    }

    epoch_manager->AddGarbageNode(tower);
    return true;
  }

  /**
   * @brief MAIN API - Get all values in the skip list from an offset, with a
   * count limit.
   */
  void GetAllValue(std::vector<ValueType> &value_list, uint64_t limit = 0,
                   uint64_t offset = 0) {
    auto epoch = epoch_manager->JoinEpoch();

    TowerNode *node = Frontier->GetNodeByLevel(0)->GetNext();

    while (node) {
      node->GetTower()->GetValList()->ScanVal(value_list, limit, offset);
      node = node->GetNext();
    }
  }

  /**
   * @brief MAIN API - Get all values of a key, from an offset, with a count
   * limit.
   */
  void GetValue(const KeyType &key, std::vector<ValueType> &value_list,
                uint64_t limit = 0, uint64_t offset = 0) {
    auto epoch = epoch_manager->JoinEpoch();
    Path path(key, Frontier->GetNodeByLevel(tower_levels - 1));
    ValueList *val_list = SearchValueList(path);

    if (val_list) {
      val_list->ScanVal(value_list, limit, offset);
    }
  }

  /**
   * @brief MAIN API - Check whether garbage collection is needed.
   */
  bool NeedGC() { return epoch_manager->NeedGC(); }

  /**
   * @brief MAIN API - Perform garbage collection.
   */
  void PerformGC() { epoch_manager->PerformGC(); }

  /**
   * @brief Retrieve all values within key range [@c low_key, @c high_key].
   *
   * @param result The vector to write all found values to.
   * @param low_key The lower bound of the key range.
   * @param high_key The higher bound of the key range.
   *
   * @return @c void.
   */
  void GetRangeValue(std::vector<ValueType> &result, const KeyType &low_key,
                     const KeyType &high_key, uint64_t limit = 0,
                     uint64_t offset = 0) {
    auto epoch = epoch_manager->JoinEpoch();

    Path path(low_key, Frontier->GetNodeByLevel(tower_levels - 1));
    SearchValueList(path);
    TowerNode *node = path.GetBoundary(path.GetHeight()).rhs();

    assert(limit >= 0);
    assert(offset >= 0);

    while (node != nullptr && !key_less(high_key, node->GetTower()->GetKey())) {
      if (node->GetTower()->GetValList()->ScanVal(result, limit, offset)) {
        break;
      }
      node = node->GetNext();
    }
  }

  /**
   * @brief MAIN API - Insert a key-value pair based on a predicate.
   */
  bool CondInsert(const KeyType &key, const ValueType &value,
                  std::function<bool(const void *)> predicate,
                  bool *predicate_satisfied) {
    auto epoch = epoch_manager->JoinEpoch();

    // Initialize the search context.
    Path path(key, Frontier->GetNodeByLevel(tower_levels - 1));

    // Try to find the key.
    ValueList *val_list = SearchValueList(path);

  cond_insert_val:
    while (val_list != nullptr) {
      // If we are in unique key mode, we can never insert a different value
      // once the key is already present.
      if (unique_key) {
        *predicate_satisfied = false;
        return false;
      }

      // Let's try to insert the value in the value list.
      switch (val_list->CondInsertVal(value, predicate, predicate_satisfied)) {
        case InsertValReturn::SUCCESS:
          size.fetch_add(sizeof(ValueNode));
          return true;

        case InsertValReturn::DUP_VAL:
          return false;

        case InsertValReturn::LIST_DEL:
          val_list = SearchValueList(path);
          break;

        default:
          assert(0);
      }
    }

    // The key isn't present in the skiplist. Let's insert a new tower.
    Tower *tower;
    ssize_t level = GetRandomLevel();
    tower = Tower::InlineAllocateTower(key, value, level);

    for (ssize_t i = 0; i < level; ++i) {
      bool retry = true;
      while (retry) {
        switch (InsertBetween(tower->GetNodeByLevel(i), path.GetBoundary(i))) {
          case InsertBetweenReturn::SUCCESS:
            // Great! We have successfully inserted the node at this level.
            // Let's goto the next level.
            retry = false;
            break;

          case InsertBetweenReturn::KEY_EXISTS:
            // Someone beats us inserting a node of the same key.
            // Our tower is useless now.
            delete tower;

            // If we are in non-unique key mode, just return that tower.
            if (!unique_key) {
              val_list = path.GetBoundary(i).rhs()->GetTower()->GetValList();
              goto cond_insert_val;
            }

            // If we are in unique key mode, we have failed.
            *predicate_satisfied = false;
            return false;

          case InsertBetweenReturn::DEL_SET:
            // lhs is already deleted, we have to restart our search to get
            // another lhs.
            path.FindPath(i);
            break;

          case InsertBetweenReturn::PTR_MISMATCH:
            // Someone has already inserted a node between (lhs, rhs).
            // Let's narrow down our boundary.
            // Note that someone could have inserted a node with our key.
            path.AdvanceBoundary(i);
            break;

          default:
            assert(0 && "invalid return state");
        }
      }
    }

    size.fetch_add(sizeof(Tower) + sizeof(ValueNode));
    *predicate_satisfied = true;
    return true;
  }

  /**
   *  Essentially a lock free linked list.
   *  The structure is used to store multiple value for a particular key.
   **/
  class ValueList final {
   public:
    /**
     * @brief The node class that makes up the linked list.
     *
     * Basically a wrapper round base node and NextPtrDelMark.
     */
    class ValueNode final : public DynamicType {
     private:
      NextPtrDelMark<ValueNode> next_del;

     public:
      ValueNode(const ValueType &val_, ValueNode *nxt_)
          : next_del{nxt_}, val{val_} {}

      ValueType val;

      virtual ssize_t GetSize() const override { return sizeof(ValueNode); }

      ValueNode *GetNext() { return next_del.GetNext(); }

      SetNextReturn SetNext(ValueNode *expected_curr_next, ValueNode *next) {
        return next_del.SetNext(expected_curr_next, next);
      }

      bool SetDeleteMark() { return next_del.SetDeleteMark(); }

      bool DeleteMarkSet() { return next_del.DeleteMarkSet(); }

    };  // class ValueNode

   private:
    /**
     * @brief The head of the linked list.
     * This could be
     *  - A valid pointer: this list is non-empty.
     *  - nullptr: this list is empty - the corresponding tower will be deleted.
     */
    std::atomic<ValueNode *> head;

    /** @brief Value comparator. */
    ValueEqualityChecker val_equal;

   public:
    ValueList(const ValueType &val,
              ValueEqualityChecker value_eq_obj = ValueEqualityChecker{})
        : head{new ValueNode(val, nullptr)}, val_equal{value_eq_obj} {}

    // A function to destroy all data structure in the valuelist
    // Supposed to be called only during ~Skiplist
    ~ValueList() {
      ValueNode *node = nullptr;

      while (1) {
        node = head.load();
        ValueNode *null_node = nullptr;
        if (head.compare_exchange_strong(node, null_node)) {
          break;
        }
      }

      while (node != nullptr) {
        ValueNode *next = node->GetNext();
        if (node->SetDeleteMark()) {
          delete node;
        }
        node = next;
      }
    }

    /**
     * @brief Insert a value into the value list.
     *
     * @return InsertValReturn, which could be SUCCESS, LIST_DEL, or DUP_VAL.
     */
    InsertValReturn InsertVal(const ValueType &val) {
      bool predicate_satisfied;
      static const std::function<bool(const void *)> predicate =
          [](const void *) { return false; };
      return CondInsertVal(val, predicate, &predicate_satisfied);
    }

    /**
     * @brief Insert a value into the value list based on a predicate.
     *
     * @return InsertValReturn, which could be SUCCESS, LIST_DEL, or DUP_VAL.
     */
    InsertValReturn CondInsertVal(const ValueType &val,
                                  std::function<bool(const void *)> predicate,
                                  bool *predicate_satisfied) {
      ValueNode *node = new ValueNode{val, nullptr};
      *predicate_satisfied = true;

      while (1) {
        // Find the value invalidating the predicate.
        ValueNode *tail = head.load();
        ValueNode *lhs = nullptr;

        if (tail == nullptr) {
          delete node;
          return InsertValReturn::LIST_DEL;
        }

      cond_find_tail:
        while (tail != nullptr) {
          if (predicate(tail->val) || val_equal(tail->val, val)) {
            // abandon insert
            *predicate_satisfied = false;
            delete node;
            return InsertValReturn::DUP_VAL;
          }
          lhs = tail;
          tail = tail->GetNext();
        }

        assert(lhs != nullptr);

        ValueNode *null_node = nullptr;
        switch (lhs->SetNext(null_node, node)) {
          case SetNextReturn::SUCCESS:
            return InsertValReturn::SUCCESS;
          case SetNextReturn::PTR_MISMATCH:
            tail = lhs;
            goto cond_find_tail;
            break;
          default:
            break;
        }
      }

      assert(0);
      return InsertValReturn::SUCCESS;
    }

    /**
     * @brief Delete a value from the ValueList.
     *
     * @return (The deleted node, ValueList deleted).
     */
    std::pair<ValueNode *, bool> DeleteVal(const ValueType &val) {
      // find the value from the begining
      ValueNode *node = head.load();
      bool flag = false;

      while (node != nullptr) {
        if (val_equal(node->val, val) && node->SetDeleteMark()) {
          // found the node
          flag = true;
          break;
        }
        node = node->GetNext();
      }

      /*
       * If the target value is found, that means:
       *  - The ValueList is not empty.
       *  - We have already set the deleted mark, so that no one can touch the
       *    node.
       */
      while (flag) {
        ValueNode *lhs = head.load();
        bool ret = false;

        // If the node happens to be the head.
        if (lhs == node) {
          // Delete the node.
          head.compare_exchange_strong(node, node->GetNext());

          // Try delete the whole ValueList.
          ret = node->GetNext() == nullptr;

          return std::make_pair(node, ret);
        }

      // The node is not the head.
      // Since we have set the delete mark, there has to be a lhs.
      find_lhs:
        while (lhs->GetNext() != node) {
          lhs = lhs->GetNext();
        }

        switch (lhs->SetNext(node, node->GetNext())) {
          case SetNextReturn::SUCCESS:
            ret = node->GetNext() == nullptr;
            return std::make_pair(node, ret);

          case SetNextReturn::PTR_MISMATCH:
            // Someone inserted between `lhs` and `node`.
            // We should extend our search.
            goto find_lhs;
            break;

          case SetNextReturn::DEL_SET:
            // `lhs` is deleted! Search for `lhs` again!
            break;

          default:
            assert(0 && "Invalid return status!");
        }
      }  // while (flag)

      return std::make_pair(nullptr, false);
    }

    bool ScanVal(std::vector<ValueType> &value_list, uint64_t limit,
                 uint64_t &offset) {
      ValueNode *node = head.load();
      while (node != nullptr) {
        if (!node->DeleteMarkSet()) {
          if (offset > 0) {
            offset--;
          } else {
            value_list.emplace_back(node->val);
            if (value_list.size() == limit) {
              return true;
            }
          }
        }
        node = node->GetNext();
      }
      return false;
    }

  };  // class ValueList

  /**
   * @brief Get a random number of levels.
   */
  ssize_t GetRandomLevel() {
    static thread_local std::mt19937 generator;

    // Constructing a distribution is cheap.
    ssize_t max = (1 << (tower_levels - 1)) - 1;
    std::uniform_int_distribution<ssize_t> distribution(0, max);

    ssize_t bits = distribution(generator);
    ssize_t level = 0;
    while ((bits & 1) && level < (tower_levels - 1)) {
      level++;
      bits >>= 1;
    }

    return level + 1;
  }

  SkipList(bool unique_key_, KeyComparator p_key_cmp_obj = KeyComparator{},
           KeyEqualityChecker p_key_eq_obj = KeyEqualityChecker{},
           ValueEqualityChecker p_value_eq_obj = ValueEqualityChecker{})
      :  // Key comparator, equality checker and hasher
        key_less{p_key_cmp_obj},
        key_equal{p_key_eq_obj},

        // Value equality checker and hasher
        val_equal{p_value_eq_obj},

        unique_key{unique_key_} {
    epoch_manager = new EpochManager();
    epoch_manager->StartEpochManager();

    KeyType *key = reinterpret_cast<KeyType *>(
        reinterpret_cast<void *>(new char[sizeof(KeyType)]));
    ValueType *val = reinterpret_cast<ValueType *>(
        reinterpret_cast<void *>(new char[sizeof(ValueType)]));
    const KeyType key_ = *key;
    const ValueType val_ = *val;
    delete key;
    delete val;
    Frontier = Tower::InlineAllocateTower(key_, val_, tower_levels);
    size.store(0);
  }

  ~SkipList() {
    for (auto node = Frontier->GetNodeByLevel(0); node != nullptr;) {
      Tower *tower = node->GetTower();

      node = node->GetNext();

      delete tower;
    }
    delete epoch_manager;
  }

  Tower *Frontier;

  KeyComparator key_less;
  KeyEqualityChecker key_equal;
  ValueEqualityChecker val_equal;
  bool unique_key;

  EpochManager *epoch_manager;

  /**
   * @brief EpochManager periodically creates epoch node.
   *
   * Every operation entering the index should register itself with EpochManager
   * using JoinEpoch() method.
   *
   * JoinEpoch() returns a reference to the newest EpochNode.
   * When the reference is deleted, a call to LeaveEpoch is automatically
   * called, so that EpochManager knows how many threads joined at that epoch
   * are still active.
   *
   * When the number of active thread drops to 0, EpochManager knows that it's
   * safe to reclaim all nodes marked deleted at that epoch.
   *
   * EpochManager always puts a new node at the end of the EpochNode chain.
   * During PerformGC, it scans from the begining of the chain, and if some
   * EpochNode in the middle still has active threads, then it's not safe to
   * reclaim nodes in the epoch as well as the epochs after this one.
   *
   *  EpochManager:
   *  +-----------------+
   *  |   EpochManager  |
   *  |                 |
   *  | +-------------+ |
   *  | | Epoch Chain | |
   *  | +-------------+ |
   *  +-----------------+
   *
   *  Epoch Chain:
   *  Oldest (epoch_head) -> ... -> Newest (curr_epoch)
   *  +---------------------+         +---------------------+
   *  |      Epoch Node     |    +----+>     Epoch Node     |
   *  |                     |    |    |                     |
   *  |+-------------------+|    |    |+-------------------+|
   *  || active_thread_num ||    |    || active_thread_num ||
   *  |+-------------------+|    |    |+-------------------+|
   *  |   +--------------+  |    |    |   +--------------+  |
   *  |   | GarbageChain |  |    |    |   | GarbageChain |  |
   *  |   +------------- +  |    |    |   +------------- +  |
   *  |   +--------------+  |    |    |   +--------------+  |
   *  |   |     Next     +--+----+    |   |     Next     |  |
   *  |   +--------------+  |         |   +--------------+  |
   *  +---------------------+         +---------------------+
   *
   *  Garbage Chain:
   *  +---------------+         +---------------+
   *  |  GarbageNode  |    +----+> GarbageNode  |
   *  | +-----------+ |    |    | +-----------+ |
   *  | |DeletedNode| |    |    | |DeletedNode| |
   *  | +-----------+ |    |    | +-----------+ |
   *  | +-----------+ |    |    | +-----------+ |
   *  | |    Next   +-+----+    | |    Next   | |
   *  | +-----------+ |         | +-----------+ |
   *  +---------------+         +---------------+
   *
   */
  class EpochManager final {
   public:
    /** @brief The maximum value that active_thread_num can possibly reach. */
    static constexpr int MAX = 0x7FFFFFFF;

    /**
     * @Brief The number of milliseconds between epochs.
     *
     * This number comes from testing on autolab.
     * We set it to 50 as in bwtree but failed memtest several times.
     * Setting it to 8 make it safe enough to pass test.
     */
    static constexpr int EPOCH_INTERVAL = 8;

    /**
     *  A structure chaining all nodes reclaimed at a certain epoch.
     **/
    struct GarbageNode {
      GarbageNode(DynamicType *node_, GarbageNode *next_)
          : node{node_}, next{next_} {}
      DynamicType *node;
      GarbageNode *next;
    };

    /**
     *  A structure chaining all epochs from oldest to newest,
     *  where each node maintains the number of threads active in that epoch,
     *  and all nodes reclaimed at that epoch.
     **/
    struct EpochNode {
      EpochNode() : next{nullptr} {
        head.store(nullptr);
        active_thread_num.store(0);
      }
      std::atomic<GarbageNode *> head;
      std::atomic<int> active_thread_num;
      EpochNode *next;
    };

    EpochNode *epoch_head, *curr_epoch;
    std::atomic<bool> exit_flag;
    std::thread *thread_ptr;

#ifdef SKIPLIST_DEBUG
    // nodes logically deleted
    std::atomic<size_t> nodes_deleted;
    // nodes freed physically
    std::atomic<size_t> nodes_freed;
    // threads joined in total
    std::atomic<size_t> threads_joined;
    // threads left in total
    std::atomic<size_t> threads_left;
    // epoch nodes created in total
    std::atomic<size_t> epochs_created;
    // epoch nodes freed in total
    std::atomic<size_t> epochs_freed;
#endif

    EpochManager() : thread_ptr{nullptr} {
      epoch_head = new EpochNode();
      curr_epoch = epoch_head;
      exit_flag.store(false);

#ifdef SKIPLIST_DEBUG
      nodes_deleted.store(0);
      nodes_freed.store(0);
      threads_joined.store(0);
      threads_left.store(0);
      epochs_created.store(0);
      epochs_freed.store(0);
#endif
    }

    ~EpochManager() {
      exit_flag.store(true);
      if (thread_ptr != nullptr) {
        thread_ptr->join();
        delete thread_ptr;
      }
      ClearEpochs();
      if (epoch_head != nullptr) {
        for (curr_epoch = epoch_head; curr_epoch != nullptr;
             curr_epoch = curr_epoch->next) {
#ifdef SKIPLIST_DEBUG
          LOG_DEBUG("Left epoch's Active thread number: %d",
                    curr_epoch->active_thread_num.load());
#endif

          curr_epoch->active_thread_num.store(0);
        }
        ClearEpochs();
      }

#ifdef SKIPLIST_DEBUG
      LOG_DEBUG("Epochs created: %d", epochs_created.load());
      LOG_DEBUG("Epochs freed: %d", epochs_freed.load());
      LOG_DEBUG("Nodes deleted: %d", nodes_deleted.load());
      LOG_DEBUG("Nodes freed: %d", nodes_freed.load());
      LOG_DEBUG("Threads joined: %d", threads_joined.load());
      LOG_DEBUG("Threads left: %d", threads_left.load());
#endif
    }

    EpochManager(const EpochManager &) = delete;
    void operator=(const EpochManager &) = delete;

    /**
     * @brief Periodically create a new epoch.
     *
     * A special thread is created for this function.
     */
    void PeriodicFunc() {
      while (exit_flag.load() == false) {
        CreateNewEpoch();
        std::chrono::milliseconds interval(EPOCH_INTERVAL);
        std::this_thread::sleep_for(interval);
      }
    }

    /** @brief Start the periodic function. */
    void StartEpochManager() {
      thread_ptr = new std::thread{[this]() { this->PeriodicFunc(); }};
    }

    /**
     * @brief A reference to the epoch that a thread joined.
     *
     * When this object gets destructed, it calls LeaveEpoch to de-register.
     */
    class EpochRef {
     public:
      EpochRef(EpochManager *epoch_manager, EpochNode *epoch_node)
          : epoch_manager_{epoch_manager}, epoch_node_{epoch_node} {}

      ~EpochRef() { epoch_manager_->LeaveEpoch(epoch_node_); }

     private:
      EpochManager *epoch_manager_;
      EpochNode *epoch_node_;
    };

    /**
     * @brief Join the newest epoch, returning a reference to that epoch.
     *
     * When the reference is deleted, LeaveEpoch will be atomically called.
     */
    EpochRef JoinEpoch() {
      while (1) {
        EpochNode *epoch = curr_epoch;
        if (epoch->active_thread_num.fetch_add(1) < 0) {
          // the epochnode has already been reclaimed
          // @todo I don't think that would happen.
          epoch->active_thread_num.fetch_sub(1);
          continue;
        } else {
#ifdef SKIPLIST_DEBUG
          threads_joined.fetch_add(1);
#endif
          return EpochRef(this, epoch);
        }
      }
      assert(0 && "shouldn't reach here");
    }

    /**
     * @brief Input should be the epoch get from invoking JoinEpoch.
     * Leave the same epoch joined, update active_thread_num in that epoch.
     */
    void LeaveEpoch(EpochNode *epoch) {
      epoch->active_thread_num.fetch_sub(1);

#ifdef SKIPLIST_DEBUG
      threads_left.fetch_add(1);
#endif
    }

    /**
     * @brief Add the node into epoch garbage chain.
     *
     * Do not need to be precise about when it was deleted,
     * because the function performing deletion is not leaving yet,
     * and must be in the very epoch or a prior epoch.
     **/
    void AddGarbageNode(DynamicType *node) {
      EpochNode *epoch = curr_epoch;
      GarbageNode *garbage = new GarbageNode(node, nullptr);
      while (1) {
        GarbageNode *head = epoch->head.load();
        garbage->next = head;
        if ((epoch->head).compare_exchange_strong(head, garbage)) {
          break;
        }
      }

#ifdef SKIPLIST_DEBUG
      nodes_deleted.fetch_add(1);
#endif
    }

    /**
     * @brief Create a newer epoch and attach it to the epoch chain.
     *
     * This function is only called by the periodic function, so we don't have
     * to worry about synchronization issues.
     *
     * @return @c void.
     */
    void CreateNewEpoch() {
      curr_epoch->next = new EpochNode;
      curr_epoch = curr_epoch->next;

#ifdef SKIPLIST_DEBUG
      epochs_created.fetch_add(1);
#endif
    }

    /**
     * @brief Check whether garbage collection is needed.
     *
     * @warning This function is only called by a special GC thread.
     *
     * In particular, check whether:
     * - There are at least 2 epochs (if there is only 1 epoch, then even if
     *   that epoch is empty, it might just be temporary);
     * - The oldest epoch has no threads in it.
     *
     * @return @c true if garbage collection is needed, @c false if not.
     */
    bool NeedGC() {
      if (curr_epoch == epoch_head) {
        // work well even when they are both nullptr
        return false;
      }
      if (epoch_head->active_thread_num.load() != 0) {
        return false;
      }
      // case when garbage chain of the oldest epochnode is empty
      if (epoch_head->head.load() == nullptr) {
        auto head = epoch_head;
        epoch_head = epoch_head->next;
        delete head;
        return NeedGC();
      }
      return true;
    }

    /**
     * @brief Perform garbage collection.
     *
     * @warning This function is only called by a special GC thread.
     *
     * In particular, iteratively:
     * - If the oldest epoch has no threads in it, free all garbage attached
     *   to that epoch, and shrink the chain.
     *
     * @return @c void.
     */
    void ClearEpochs() {
      while (NeedGC()) {
        if (epoch_head->active_thread_num.fetch_sub(MAX) != 0) {
          // active_thread_num had been modified before sub
          epoch_head->active_thread_num.fetch_add(MAX);
          return;
        }
        EpochNode *prev_head = epoch_head;
        epoch_head = epoch_head->next;
        GarbageNode *garbage = prev_head->head.load();
        delete prev_head;
        while (garbage != nullptr) {
          size.fetch_sub(garbage->node->GetSize());
          delete garbage->node;
          GarbageNode *next = garbage->next;
          delete garbage;
          garbage = next;

#ifdef SKIPLIST_DEBUG
          nodes_freed.fetch_add(1);
#endif
        }

#ifdef SKIPLIST_DEBUG
        epochs_freed.fetch_add(1);
#endif
      }
    }

    /**
     * @brief Perform garbage collection.
     *
     * @warning This function is only called by a special GC thread.
     *
     * @return @c void.
     */
    void PerformGC() { ClearEpochs(); }

  };  // class EpochManager

  /**
   * @brief Validates:
   * - For every key, there could only be one tower.
   * - A node should be at the level it claims to be.
   * - A tower should have the number of nodes it claims to have.
   * - A node retrieved from GetDown() does appear in the lower level.
   */
  void ValidateStructure() {
    std::map<KeyType, Tower *> key2tower;
    std::map<KeyType, ssize_t> key_cnt;
    std::vector<std::vector<TowerNode *>> nodes;

    for (ssize_t level = 0; level < tower_levels; ++level) {
      nodes.push_back(std::vector<TowerNode *>());

      auto node = Frontier->GetNodeByLevel(level)->GetNext();
      for (; node != nullptr; node = node->GetNext()) {
        // A node should be at the level it claims to be.
        assert(node->GetTowerNodeLevel() == level);

        auto key = node->GetKey();

        nodes.back().push_back(node);

        if (key2tower.find(key) != key2tower.end()) {
          // There could only be one tower for this key.
          assert(key2tower[key] == node->GetTower());
        } else {
          key2tower.insert(std::make_pair(key, node->GetTower()));
        }

        key_cnt[key]++;
      }
    }

    for (auto pair : key2tower) {
      auto key = pair.first;
      auto tower = pair.second;
      // A tower should have the number of nodes it claims to have.
      assert(tower->GetNumLevel() == key_cnt[key]);
    }

    for (ssize_t level = 1; level < tower_levels; ++level) {
      for (auto node : nodes[level]) {
        auto down = node->GetDown();

        // A node retrieved from GetDown() does appear in the lower level.
        assert(std::find(nodes[level - 1].begin(), nodes[level - 1].end(),
                         down) != nodes[level - 1].end());
      }
    }
  }

};  // End class Skiplist

