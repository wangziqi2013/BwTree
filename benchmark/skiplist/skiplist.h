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

#pragma once
#include <stdint.h>
#include <algorithm>
#include <atomic>
#include <cassert>
#include <vector>
#include <functional>

#define MAX_HEIGHT 32
#define MAX_THREAD_COUNT ((int)0x7FFFFFFF)
#define PROBABILITY 0.5
#define GC_SIZE_THRESHOLD 2048

/*
 * SKIPLIST_TEMPLATE_ARGUMENTS - Save some key strokes
 */
#define SKIPLIST_TEMPLATE_ARGUMENTS                                       \
  template <typename KeyType, typename ValueType, typename KeyComparator, \
            typename KeyEqualityChecker, typename ValueEqualityChecker>

#define SKIPLIST_TYPE                                             \
  SkipList<KeyType, ValueType, KeyComparator, KeyEqualityChecker, \
           ValueEqualityChecker>

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker, typename ValueEqualityChecker>
class SkipList {
 private:
  // An wrapper class around pointer. The lowest 1 bit is used to mark deletion
  template <typename T>
  class TaggedPtr {
   public:
    static const uintptr_t tag_mask = 0x1;
    static const uintptr_t pointer_mask = ~tag_mask;

    TaggedPtr() = default;
    TaggedPtr(T *pointer, bool tag) { Set(pointer, tag); }

    inline T *GetPointer() {
      return reinterpret_cast<T *>(tagged_ptr & pointer_mask);
    }

    inline bool GetTag() { return tagged_ptr & tag_mask; }

    inline void Set(T *pointer, bool tag) {
      // Make sure the raw pointer is properly aligned
      assert(((uintptr_t)pointer & tag_mask) == 0);
      tagged_ptr = (uintptr_t)pointer | tag;
    }

   private:
    uintptr_t tagged_ptr;
  };

  template <typename T>
  class AtomicTaggedPointer {
   private:
    std::atomic<TaggedPtr<T>> atomic_ptr;

   public:
    AtomicTaggedPointer(T *pointer = nullptr, bool tag = false) {
      atomic_ptr.store(TaggedPtr<T>(pointer, tag));
    }

    // Only used for initalization
    void Set(T *pointer, bool tag) {
      atomic_ptr.store(TaggedPtr<T>(pointer, tag));
    }

    // Do compare and swap, return success ;
    bool CompareAndSet(T *cmp_pointer, bool cmp_tag, T *swp_pointer,
                       bool swp_tag) {
      TaggedPtr<T> cmp_tagged_ptr(cmp_pointer, cmp_tag);
      TaggedPtr<T> swp_tagged_ptr(swp_pointer, swp_tag);
      return atomic_ptr.compare_exchange_strong(cmp_tagged_ptr, swp_tagged_ptr);
    }

    inline T *GetPointer() { return atomic_ptr.load().GetPointer(); }

    inline TaggedPtr<T> GetTaggPtr() { return atomic_ptr.load(); }

    inline bool GetTag() { return atomic_ptr.load().GetTag(); }

    inline void Get(T *&pointer, bool &tag) {
      auto ret_ptr = atomic_ptr.load();
      pointer = ret_ptr.GetPointer();
      tag = ret_ptr.GetTag();
    }
  };

  class ValueComparator {
   public:
    // Return true if v1 < v2
    inline bool operator()(const ValueType &v1, const ValueType &v2) const {
      return (uintptr_t)v1 < (uintptr_t)v2;
    }
  };

  class Tower {
   public:
    AtomicTaggedPointer<Tower> *next;
    int height;
    KeyType key;
    ValueType value;

    Tower(int height = 1) : height(height) {
      assert(height >= 1);
      next = new AtomicTaggedPointer<Tower>[height];
    }

    Tower(KeyType k, ValueType v, int height = 1)
        : height(height), key(k), value(v) {
      assert(height >= 1);
      next = new AtomicTaggedPointer<Tower>[height];
    }

    size_t GetMemorySize() const {
      return sizeof(Tower) + height * sizeof(AtomicTaggedPointer<Tower>);
    }

    ~Tower() { delete[] next; }
  };

  class EpochManager {
   private:
    // a linked list of garbage node
    struct GarbageNode {
      const Tower *tower_p;
      GarbageNode *next_p;
    };

    // a linked list of epoch node
    struct EpochNode {
      // threads that is active in this epoch
      // if it reaches 0 then we will try to
      // garbage collect GarbageNodes in this epoch
      std::atomic<int> thread_count;

      std::atomic<size_t> memory_count;

      std::atomic<GarbageNode *> garbage_list_p;

      EpochNode *next_p;
    };

   public:
    EpochManager(std::atomic<size_t> *mem_footprint);

    // try to clear epoch and
    // release the memory allocated by EpochManager
    ~EpochManager();

    // append a node to the end of
    // current epoch list, update curr_epoch_p
    void CreateNewEpoch();

    // register a new garbage node to curr_epoch_p
    // then update memory count in curr_epoch_p
    void AddGarbageNode(const Tower *tower_p);

    // try to add 1 to curr_epoch_p->thread_count
    // return a EpochNode* which could be used by LeaveEpoch
    inline EpochNode *JoinEpoch();

    // atomic --eopch_p->thread_count
    inline void LeaveEpoch(EpochNode *epoch_p);

    // free tower by calling destructor
    // TODO update memory footprint
    void FreeTower(const Tower *tower_p);

    // try to reclaim epoch starting from reclaim_epoch_p
    // by setting(CAS) thread count to INT_MIN
    // aggregate total memory needed to be reclaimed
    void ReclaimEpoch();

    // garbage collect head_epoch_p
    // until head_epoch_p == reclaim_epoch_p
    // set reclaim size to 0
    void ClearEpoch();

    // When a seperate thread call PerformGarbageCollection()
    // we first clear epoch from head_epoch_p
    // then create a new epoch
    void PerformGarbageCollection();

    // return ReclaimEpoch()
    bool NeedGarbageCollection();

   private:
    // the head of EpochNode linked list
    EpochNode *head_epoch_p;

    // the first epoch we haven't reclaimed
    EpochNode *reclaim_epoch_p;

    // Used to identify current epoch
    std::atomic<EpochNode *> curr_epoch_p;

    size_t reclaim_size;

    std::atomic<size_t> *mem_footprint;
  };  // Epoch Manager

  bool Find(const KeyType &key, const ValueType &value,
            Tower *preds[MAX_HEIGHT], Tower *succs[MAX_HEIGHT], bool is_insert,
            int bottom_level = 0);

  // Given a key, return a tower whose value is less than key
  Tower *LookUp(const KeyType &key, Tower *curr);

  bool InsertHelper();

  // Keep tossing a coin to get a random height. Range from [1, MAX_HEIGHT]
  int GetRandomHeight();

  void CheckIntegrity();

 public:
  SkipList(bool p_is_key_unique = true,
           KeyComparator p_key_cmp_obj = KeyComparator{},
           KeyEqualityChecker p_key_eq_obj = KeyEqualityChecker{},
           ValueEqualityChecker p_value_eq_obj = ValueEqualityChecker{});

  ~SkipList();

  bool Delete(KeyType &key, ValueType &value);

  bool ConditionalInsert(const KeyType &key, const ValueType &value,
                         std::function<bool(const void *)> predicate,
                         bool *predicate_satisfied);

  bool Insert(const KeyType &key, const ValueType &value);

  void GetValue(const KeyType &search_key, std::vector<ValueType> &value_list);

  void GetAllValue(std::vector<ValueType> &value_list);

  void GetRange(KeyType &low_key, KeyType &high_key,
                std::vector<ValueType> &value_list);

  void GetRangeLimit(KeyType &low_key, KeyType &high_key,
                     std::vector<ValueType> &value_list, uint64_t offset = 0,
                     uint64_t limit = -1);

  inline bool NeedGarbageCollection() {
    return epoch_manager.NeedGarbageCollection();
  }

  inline void PerformGarbageCollection() {
    epoch_manager.PerformGarbageCollection();
  }

  inline size_t GetMemoryFootprint() { return mem_footprint.load(); }

 private:
  Tower head = Tower(MAX_HEIGHT);
  Tower tail = Tower(MAX_HEIGHT);

  std::atomic<size_t> mem_footprint;
  const bool is_key_unique;
  const KeyComparator key_cmp_obj;
  const KeyEqualityChecker key_eq_obj;
  const ValueEqualityChecker value_eq_obj;
  const ValueComparator value_cmp_obj;
  EpochManager epoch_manager;
};

