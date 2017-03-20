//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// skiplist.cpp
//
// Identification: src/index/skiplist.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "skiplist.h"

#define USE_GC

//===----------------------SkipList Member Functions ----------------------===//
//===========================================================================//
SKIPLIST_TEMPLATE_ARGUMENTS
SKIPLIST_TYPE::SkipList(bool p_is_key_unique, KeyComparator p_key_cmp_obj,
                        KeyEqualityChecker p_key_eq_obj,
                        ValueEqualityChecker p_value_eq_obj)
    : mem_footprint(0),
      is_key_unique{p_is_key_unique},
      key_cmp_obj{p_key_cmp_obj},
      key_eq_obj{p_key_eq_obj},
      value_eq_obj{p_value_eq_obj},
      value_cmp_obj(ValueComparator()),
      epoch_manager(&mem_footprint) {
  for (int i = 0; i < MAX_HEIGHT; ++i) head.next[i].Set(&tail, false);
}

SKIPLIST_TEMPLATE_ARGUMENTS
SKIPLIST_TYPE::~SkipList() {
  // Delete all remaining tower
  Tower *next_tower_ptr;
  for (Tower *curr = head.next[0].GetPointer(); curr != &tail;
       curr = next_tower_ptr) {
    next_tower_ptr = curr->next[0].GetPointer();
    delete curr;
  }
}

SKIPLIST_TEMPLATE_ARGUMENTS
int SKIPLIST_TYPE::GetRandomHeight() {
  int level = 1;
  while (((double)rand() / RAND_MAX < PROBABILITY) && (level < MAX_HEIGHT)) {
    level++;
  }
  return level;
}

SKIPLIST_TEMPLATE_ARGUMENTS
bool SKIPLIST_TYPE::Find(const KeyType &key, const ValueType &value,
                         Tower *preds[MAX_HEIGHT], Tower *succs[MAX_HEIGHT],
                         bool is_insert, int bottom_level) {
restart:
  Tower *pred = &head;
  Tower *curr = nullptr;
  for (int level = MAX_HEIGHT - 1; level >= bottom_level; --level) {
    curr = pred->next[level].GetPointer();

    while (true) {
      Tower *succ;
      bool deleted;
      curr->next[level].Get(succ, deleted);

      // Find a node that has not been marked
      while (deleted) {
        // CAS failed means pred has been changed. Restart the whole process
        if (!pred->next[level].CompareAndSet(curr, false, succ, false))
          goto restart;

        curr = pred->next[level].GetPointer();
        curr->next[level].Get(succ, deleted);
      }

      if (curr == &tail) {
        // Reach the end of this level
        break;
      } else if (key_cmp_obj(curr->key, key) ||
                 (key_eq_obj(curr->key, key) &&
                  value_cmp_obj(curr->value, value))) {
        pred = curr;
        curr = succ;
      } else {
        // curr has bigger key. Go down to next level
        break;
      }
    }

    preds[level] = pred;
    succs[level] = curr;
  }
  assert(pred != &tail);

  if (is_key_unique && is_insert) {
    // tower with same key could be pred or succ depends on value.
    return (preds[bottom_level] != &head &&
            key_eq_obj(preds[bottom_level]->key, key)) ||
           (succs[bottom_level] != &tail &&
            key_eq_obj(succs[bottom_level]->key, key));
  } else {
    // If duplicate value exist, it must be in succs.
    return succs[bottom_level] != &tail &&
           key_eq_obj(succs[bottom_level]->key, key) &&
           value_eq_obj(succs[bottom_level]->value, value);
  }
}

SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_TYPE::CheckIntegrity() {
  std::vector<ValueType> values;
#ifdef USE_GC
  auto epoch_p = epoch_manager.JoinEpoch();
#endif
  for (int level = MAX_HEIGHT - 1; level >= 0; level--) {
    Tower *prev = &head;
    Tower *curr = prev->next[level].GetPointer();
    while (curr != &tail) {
      // Check key order
      if (prev != &head && key_cmp_obj(curr->key, prev->key))
        assert(false && "Key order is incorrect");

      // Check Value Uniqueness
      if (level == 0) {
        for (auto &value : values) {
          if (value_eq_obj(value, curr->value))
            assert(false && "Item pointer is not unique");
        }
        values.push_back(curr->value);
      }
      prev = curr;
      curr = curr->next[level].GetPointer();
    }
  }
#ifdef USE_GC
  epoch_manager.LeaveEpoch(epoch_p);
#endif
}

SKIPLIST_TEMPLATE_ARGUMENTS
bool SKIPLIST_TYPE::Delete(KeyType &key, ValueType &value) {
  Tower *preds[MAX_HEIGHT];
  Tower *succs[MAX_HEIGHT];
#ifdef USE_GC
  auto epoch_p = epoch_manager.JoinEpoch();
#endif
  while (true) {
    bool found_key_value = Find(key, value, preds, succs, false);
    if (!found_key_value) {
#ifdef USE_GC
      epoch_manager.LeaveEpoch(epoch_p);
#endif
      return false;
    } else {
      // logical delete curr by marking the tags
      // in curr->next from top level to level1
      Tower *curr = succs[0];
      assert(curr != &tail);

      Tower *succ;
      bool deleted;

      for (int level = curr->height - 1; level >= 1; --level) {
        // Do CAS and check Mark Bit
        // TODO it would be better if we use fetch_or() here
        curr->next[level].Get(succ, deleted);
        while (!deleted) {
          curr->next[level].CompareAndSet(succ, false, succ, true);
          // We only care about whether the mark bit is set
          curr->next[level].Get(succ, deleted);
        }
      }

      // Try to CAS the bottom mark bit
      // if this CAS success, then we successfully remove this tower
      // if others marked it, we should retry if multiple key is supported
      curr->next[0].Get(succ, deleted);
      while (!deleted) {
        bool success_set = curr->next[0].CompareAndSet(succ, false, succ, true);
        if (success_set) {
          // physical delete
          Find(key, value, preds, succs, false);
#ifdef USE_GC
          // curr tower has been physical deleted. Add to Epoch manager
          epoch_manager.AddGarbageNode(curr);
          epoch_manager.LeaveEpoch(epoch_p);
#endif
          return true;
        }
        curr->next[0].Get(succ, deleted);
      }
#ifdef USE_GC
      // This key has been marked by other thread. Just return
      epoch_manager.LeaveEpoch(epoch_p);
#endif
      return false;
    }
  }
}

SKIPLIST_TEMPLATE_ARGUMENTS
bool SKIPLIST_TYPE::ConditionalInsert(
    const KeyType &key, const ValueType &value,
    std::function<bool(const void *)> predicate, bool *predicate_satisfied) {
  // Check predicates
  std::vector<ValueType> value_list;
  GetValue(key, value_list);
  for (auto &value : value_list) {
    if (predicate(value)) {
      *predicate_satisfied = true;
      return false;
    }
  }

  *predicate_satisfied = false;
  return Insert(key, value);
}

SKIPLIST_TEMPLATE_ARGUMENTS
bool SKIPLIST_TYPE::Insert(const KeyType &key, const ValueType &value) {
  int new_tower_height = GetRandomHeight();
  Tower *new_tower = new Tower(key, value, new_tower_height);
  Tower *preds[MAX_HEIGHT];
  Tower *succs[MAX_HEIGHT];
#ifdef USE_GC
  auto epoch_p = epoch_manager.JoinEpoch();
#endif
  while (true) {
    bool is_found = Find(key, value, preds, succs, true);
    if (is_found) {
#ifdef USE_GC
      epoch_manager.LeaveEpoch(epoch_p);
#endif
      delete new_tower;
      return false;
    }

    Tower *pred = preds[0];
    Tower *succ = succs[0];
    new_tower->next[0].Set(succs[0], false);
    bool succeed = pred->next[0].CompareAndSet(succ, false, new_tower, false);
    // Predecessor has been deleted or some new nodes has been insert. Retry
    if (!succeed) continue;
    mem_footprint.fetch_add(new_tower->GetMemorySize());

    // Link other levels
    for (int level = 1; level < new_tower_height; level++) {
      // Keep trying until that level is linked
      while (true) {
        pred = preds[level];
        succ = succs[level];
        if (!new_tower->next[level].CompareAndSet(nullptr, false, succ,
                                                  false)) {
#ifdef USE_GC
          epoch_manager.LeaveEpoch(epoch_p);
#endif
          // A concurrent delete has set this level as marked. Simply return
          return true;
        }
        if (pred->next[level].CompareAndSet(succ, false, new_tower, false))
          break;
        // Optimization: no need to go down to level 0
        Find(key, value, preds, succs, true, level);
      }
    }
#ifdef USE_GC
    epoch_manager.LeaveEpoch(epoch_p);
#endif
    return true;
  }
}

SKIPLIST_TEMPLATE_ARGUMENTS
typename SKIPLIST_TYPE::Tower *SKIPLIST_TYPE::LookUp(const KeyType &key,
                                                     Tower *start) {
  assert(start != nullptr);
  Tower *pred = start;
  Tower *curr = nullptr;

  for (int level = pred->height - 1; level >= 0; --level) {
    curr = pred->next[level].GetPointer();

    while (true) {
      Tower *succ;
      bool deleted;
      curr->next[level].Get(succ, deleted);

      // skip deleted tower
      while (deleted) {
        assert(curr != &tail);
        pred = curr;
        curr = succ;
        curr->next[level].Get(succ, deleted);
      }

      if (curr == &tail) {
        // curr is tail. Go down to next level
        break;
      } else if (key_cmp_obj(curr->key, key)) {
        // curr->key < key then continue to search,
        pred = curr;
        curr = succ;
      } else {
        // curr->key >= key. Go down to next level
        break;
      }
    }
  }

  assert(pred != &tail);

  // return pred
  return pred;
}

SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_TYPE::GetValue(const KeyType &key,
                             std::vector<ValueType> &value_list) {
// Find the first value within that key
#ifdef USE_GC
  auto epoch_p = epoch_manager.JoinEpoch();
#endif
  Tower *pre_tower = LookUp(key, &head);
  for (TaggedPtr<Tower> cur = pre_tower->next[0].GetTaggPtr();
       cur.GetPointer() != &tail && key_eq_obj(cur.GetPointer()->key, key);
       cur = cur.GetPointer()->next[0].GetTaggPtr()) {
    if (cur.GetTag()) continue;
    value_list.push_back(cur.GetPointer()->value);
  }
#ifdef USE_GC
  epoch_manager.LeaveEpoch(epoch_p);
#endif
  return;
}

SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_TYPE::GetAllValue(std::vector<ValueType> &value_list) {
// Traverses the bottom level of all the towers and adds value to the list.
#ifdef USE_GC
  auto epoch_p = epoch_manager.JoinEpoch();
#endif
  for (TaggedPtr<Tower> cur = head.next[0].GetTaggPtr();
       cur.GetPointer() != &tail;
       cur = cur.GetPointer()->next[0].GetTaggPtr()) {
    if (cur.GetTag()) continue;
    value_list.push_back(cur.GetPointer()->value);
  }
#ifdef USE_GC
  epoch_manager.LeaveEpoch(epoch_p);
#endif
  return;
}

SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_TYPE::GetRange(KeyType &low_key, KeyType &high_key,
                             std::vector<ValueType> &value_list) {
// Starts from the head tower and get value from the bottom level until
// reaching the the high_key or the tail.
#ifdef USE_GC
  auto epoch_p = epoch_manager.JoinEpoch();
#endif
  Tower *pre_tower = LookUp(low_key, &head);
  for (TaggedPtr<Tower> cur = pre_tower->next[0].GetTaggPtr();
       cur.GetPointer() != &tail &&
           !key_cmp_obj(high_key, cur.GetPointer()->key);
       cur = cur.GetPointer()->next[0].GetTaggPtr()) {
    if (cur.GetTag() || key_cmp_obj(cur.GetPointer()->key, low_key)) continue;

    value_list.push_back(cur.GetPointer()->value);
  }
#ifdef USE_GC
  epoch_manager.LeaveEpoch(epoch_p);
#endif
  return;
}

SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_TYPE::GetRangeLimit(KeyType &low_key, KeyType &high_key,
                                  std::vector<ValueType> &value_list,
                                  uint64_t offset, uint64_t limit) {
// Starts from the head tower and get value from the bottom level until
// reaching the the high_key or the tail.
#ifdef USE_GC
  auto epoch_p = epoch_manager.JoinEpoch();
#endif
  Tower *pre_tower = LookUp(low_key, &head);

  uint64_t cur_offset = 0;
  for (TaggedPtr<Tower> cur = pre_tower->next[0].GetTaggPtr();
       cur.GetPointer() != &tail &&
           !key_cmp_obj(high_key, cur.GetPointer()->key) && limit > 0;
       cur = cur.GetPointer()->next[0].GetTaggPtr()) {
    if (cur.GetTag() || key_cmp_obj(cur.GetPointer()->key, low_key)) continue;
    if (cur_offset < offset) {
      cur_offset++;
      continue;
    }
    value_list.push_back(cur.GetPointer()->value);
    limit--;
  }
#ifdef USE_GC
  epoch_manager.LeaveEpoch(epoch_p);
#endif
  return;
}

//===---------------------EpochManager Member Functions ------------------===//
//===========================================================================//

SKIPLIST_TEMPLATE_ARGUMENTS
SKIPLIST_TYPE::EpochManager::EpochManager(std::atomic<size_t> *mem_footprint) {
  // Make sure there's always a node
  // in the epoch list
  EpochNode *epoch_p = new EpochNode{};
  epoch_p->thread_count = 0;
  epoch_p->memory_count = 0;
  epoch_p->garbage_list_p = nullptr;
  epoch_p->next_p = nullptr;
  curr_epoch_p.store(epoch_p);
  reclaim_epoch_p = head_epoch_p = epoch_p;
  this->mem_footprint = mem_footprint;
  reclaim_size = 0;
};

SKIPLIST_TEMPLATE_ARGUMENTS
SKIPLIST_TYPE::EpochManager::~EpochManager() {
  // Clear all the EpochNodes up all the way to the end
  reclaim_epoch_p = nullptr;
  ClearEpoch();
};

SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_TYPE::EpochManager::CreateNewEpoch() {
  EpochNode *new_epoch_p = new EpochNode{};
  new_epoch_p->thread_count = 0;
  new_epoch_p->memory_count = 0;
  new_epoch_p->garbage_list_p = nullptr;
  new_epoch_p->next_p = nullptr;
  curr_epoch_p.load()->next_p = new_epoch_p;
  // Advance curr_epoch_p
  curr_epoch_p.store(new_epoch_p);
}

SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_TYPE::EpochManager::AddGarbageNode(const Tower *tower_p) {
  // safe to add garbagenode to current epoch
  // because after this epoch, no one can see
  // this tower
  EpochNode *epoch_p = curr_epoch_p.load();
  GarbageNode *garbage_node_p = new GarbageNode;
  size_t tower_size = tower_p->GetMemorySize();
  garbage_node_p->tower_p = tower_p;
  garbage_node_p->next_p = epoch_p->garbage_list_p.load();
  // CAS until success
  // if failed garbage_node_p->next_p will be overwritten to
  // epoch->garbage_list_p by compare_exchange_strong
  while (!epoch_p->garbage_list_p.compare_exchange_strong(
      garbage_node_p->next_p, garbage_node_p))
    ;
  // update total memory in the garbage node
  epoch_p->memory_count.fetch_add(tower_size);
}

SKIPLIST_TEMPLATE_ARGUMENTS
inline typename SKIPLIST_TYPE::EpochManager::EpochNode *
SKIPLIST_TYPE::EpochManager::JoinEpoch() {
retry_join:
  EpochNode *epoch_p = curr_epoch_p.load();
  int prev_count = epoch_p->thread_count.fetch_add(1);

  if (prev_count < 0) {
    // curr_epoch_p must have been updated
    // because someone is trying to reclaim
    // this epoch
    epoch_p->thread_count.fetch_sub(1);
    goto retry_join;
  }
  return epoch_p;
}

SKIPLIST_TEMPLATE_ARGUMENTS
inline void SKIPLIST_TYPE::EpochManager::LeaveEpoch(EpochNode *epoch_p) {
  epoch_p->thread_count.fetch_sub(1);
}

SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_TYPE::EpochManager::FreeTower(const Tower *tower_p) {
  mem_footprint->fetch_sub(tower_p->GetMemorySize());
  delete tower_p;
}

SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_TYPE::EpochManager::ReclaimEpoch() {
  // Current epoch will not change during this function.
  EpochNode *curr_epoch = curr_epoch_p.load();
  while (true) {
    // Make sure that the current epoch is not cleared
    if (reclaim_epoch_p == curr_epoch) break;

    int thread_cnt = reclaim_epoch_p->thread_count.load();
    assert(thread_cnt >= 0);
    // If the epoch still contains active threads, stop GC
    if (thread_cnt != 0) break;

    // If some threads enter the epoch after the previous step, restore the
    // thread_cnt
    if (reclaim_epoch_p->thread_count.fetch_sub(MAX_THREAD_COUNT) > 0) {
      reclaim_epoch_p->thread_count.fetch_add(MAX_THREAD_COUNT);
      break;
    }

    // Safe point to perform GC
    reclaim_size += reclaim_epoch_p->memory_count.load();

    // Advance reclaim epoch pointer to mark epochs that need to be reclaimed
    reclaim_epoch_p = reclaim_epoch_p->next_p;
  }
}

SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_TYPE::EpochManager::ClearEpoch() {
  while (head_epoch_p != reclaim_epoch_p) {
    assert(head_epoch_p->thread_count.load() <= 0);
    // Perform real GC
    const GarbageNode *next_garbage_node_p = nullptr;
    for (const GarbageNode *garbage_node_p =
             head_epoch_p->garbage_list_p.load();
         garbage_node_p != nullptr; garbage_node_p = next_garbage_node_p) {
      FreeTower(garbage_node_p->tower_p);
      next_garbage_node_p = garbage_node_p->next_p;
      delete garbage_node_p;
    }

    // Continue to clear the previous epoch nodes
    EpochNode *next_epoch_node_p = head_epoch_p->next_p;
    delete head_epoch_p;
    head_epoch_p = next_epoch_node_p;
  }
  reclaim_size = 0;
}

SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_TYPE::EpochManager::PerformGarbageCollection() {
  ClearEpoch();
  // CreateNewEpoch();
  return;
}

SKIPLIST_TEMPLATE_ARGUMENTS
bool SKIPLIST_TYPE::EpochManager::NeedGarbageCollection() {
  CreateNewEpoch();
  ReclaimEpoch();
  return reclaim_size >= GC_SIZE_THRESHOLD;
}

