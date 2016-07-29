//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bwtree_index.cpp
//
// Identification: src/backend/index/bwtree_index.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/logger.h"
#include "backend/index/bwtree_index.h"
#include "backend/index/index_key.h"
#include "backend/storage/tuple.h"

namespace peloton {
namespace index {

BWTREE_TEMPLATE_ARGUMENTS
BWTREE_INDEX_TYPE::BWTreeIndex(IndexMetadata *metadata) :
      // Base class
      Index{metadata},
      // Key "less than" relation comparator
      comparator{metadata},
      // Key equality checker
      equals{metadata},
      // Key hash function
      hash_func{metadata},
      // NOTE: These two arguments need to be constructed in advance
      // and do not have trivial constructor
      container{comparator,
                equals,
                hash_func} {
  return;
}

BWTREE_TEMPLATE_ARGUMENTS
BWTREE_INDEX_TYPE::~BWTreeIndex() {
  return;
}

/*
 * InsertEntry() - insert a key-value pair into the map
 *
 * If the key value pair already exists in the map, just return false
 */
BWTREE_TEMPLATE_ARGUMENTS
bool
BWTREE_INDEX_TYPE::InsertEntry(const storage::Tuple *key,
                               const ItemPointer &location,
                              ItemPointer **itempointer_ptr) {
  KeyType index_key;

  index_key.SetFromKey(key);
  ItemPointer *itempointer = new ItemPointer(location);
  std::pair<KeyType, ValueType> entry(index_key,
                                      itempointer);
  if (itempointer_ptr != nullptr) {
    *itempointer_ptr = itempointer;
  }
  
  bool ret = container.Insert(index_key, itempointer);
  // If insertion fails we just delete the new value and return false
  // to notify the caller
  if(ret == false) {
    delete itempointer;
  }

  return ret;
}

BWTREE_TEMPLATE_ARGUMENTS
bool
BWTREE_INDEX_TYPE::InsertEntryInTupleIndex(const storage::Tuple *key, ItemPointer *location) {
  KeyType index_key;
  index_key.SetFromKey(key);

  ItemPointer *item_p = location;

  bool ret = container.Insert(index_key, item_p);
  // If insertion fails we just delete the new value and return false
  // to notify the caller
  if(ret == false) {
    delete item_p;
  }

  return ret;
}

/*
 * DeleteEntry() - Removes a key-value pair
 *
 * If the key-value pair does not exists yet in the map return false
 */
BWTREE_TEMPLATE_ARGUMENTS
bool
BWTREE_INDEX_TYPE::DeleteEntry(const storage::Tuple *key,
                               const ItemPointer &location) {
  KeyType index_key;
  index_key.SetFromKey(key);
  
  // Must allocate new memory here
  ItemPointer *ip_p = new ItemPointer{location};
  
  // In Delete() since we just use the value for comparison (i.e. read-only)
  // it is unnecessary for us to allocate memory
  bool ret = container.DeleteExchange(index_key, &ip_p);
  
  // IF delete succeeds then DeleteExchange() will exchange the deleted
  // value into this variable
  if(ret == true) {
    //delete ip_p;
  } else {
    // This will delete the unused memory
    delete ip_p;
  }

  return ret;
}

BWTREE_TEMPLATE_ARGUMENTS
bool
BWTREE_INDEX_TYPE::DeleteEntryInTupleIndex(const storage::Tuple *key, ItemPointer *location) {
  KeyType index_key;
  index_key.SetFromKey(key);

  // Must allocate new memory here
  ItemPointer *ip_p = location;

  // In Delete() since we just use the value for comparison (i.e. read-only)
  // it is unnecessary for us to allocate memory
  bool ret = container.DeleteExchange(index_key, &ip_p);

  // IF delete succeeds then DeleteExchange() will exchange the deleted
  // value into this variable
//  if(ret == true) {
//    //delete ip_p;
//  } else {
//    // This will delete the unused memory
//    // delete ip_p;
//  }

  return ret;
}

BWTREE_TEMPLATE_ARGUMENTS
bool
BWTREE_INDEX_TYPE::CondInsertEntry(const storage::Tuple *key,
                                   const ItemPointer &location,
                                   std::function<bool(const void *)> predicate,
                                   ItemPointer **itemptr_ptr) {
  KeyType index_key;
  index_key.SetFromKey(key);
  
  ItemPointer *item_p = new ItemPointer{location};
  bool predicate_satisfied = false;

  // This function will complete them in one step
  // predicate will be set to nullptr if the predicate
  // returns true for some value
  bool ret = container.ConditionalInsert(index_key,
                                         item_p,
                                         predicate,
                                         &predicate_satisfied);

  // If predicate is not satisfied then we know insertion successes
  if(predicate_satisfied == false) {
    // So it should always succeed?
    assert(ret == true);
    
    *itemptr_ptr = item_p;
  } else {
    assert(ret == false);
    
    // Otherwise insertion fails. and we need to delete memory
    *itemptr_ptr = nullptr;
    
    delete item_p;
  }

  return ret;
}

BWTREE_TEMPLATE_ARGUMENTS
bool
BWTREE_INDEX_TYPE::CondInsertEntryInTupleIndex(const storage::Tuple *key,
                                               ItemPointer *location,
                                               std::function<bool(const void *)> predicate) {
  KeyType index_key;
  index_key.SetFromKey(key);

  ItemPointer *item_p = location;
  bool predicate_satisfied = false;

  // This function will complete them in one step
  // predicate will be set to nullptr if the predicate
  // returns true for some value
  bool ret = container.ConditionalInsert(index_key,
                                         item_p,
                                         predicate,
                                         &predicate_satisfied);
  return ret;
}

BWTREE_TEMPLATE_ARGUMENTS
void
BWTREE_INDEX_TYPE::Scan(const std::vector<Value> &values,
                        const std::vector<oid_t> &key_column_ids,
                        const std::vector<ExpressionType> &expr_types,
                        const ScanDirectionType &scan_direction,
                        std::vector<ItemPointer> &result) {
  KeyType index_key;
  
  // This key contains tuples that are actually indexed
  // we fill the key with the min value if the attr is not involved
  // into any equality relation; Otherwise we fill it with the
  // value of equality relation
  //
  // No matter which case applies the start key is always filled
  // with the lower bound of traversal into the index after call
  // to ConstructLowerBound()
  std::unique_ptr<storage::Tuple> start_key;

  // First check whether it is the special case that all
  // constraints are equality relation such that we could use
  // point query
  start_key.reset(new storage::Tuple(metadata->GetKeySchema(), true));

  // This provides extra benefit if it is a point query
  // since the index is highly optimized for point query
  bool all_constraints_are_equal = ConstructLowerBoundTuple(
          start_key.get(), values, key_column_ids, expr_types);

  // Optimize for point query
  if (all_constraints_are_equal == true) {
    index_key.SetFromKey(start_key.get());

    container.GetValueDereference(index_key, result);
    
    return;
  }

  // This returns an iterator pointing to index_key's values
  auto scan_begin_itr = container.Begin(index_key);

  switch (scan_direction) {
    case SCAN_DIRECTION_TYPE_FORWARD:
    case SCAN_DIRECTION_TYPE_BACKWARD: {
      // TODO: This requires further optimization to at least
      // find an upper bound for scanning (i.e. when the highest columns
      // has < or <= relation). Otherwise we will have to scan till the
      // end of the index which is toooooooooooooooooooooooooooooo slow
      for (auto scan_itr = scan_begin_itr;
           scan_itr.IsEnd() == false;
           scan_itr++) {
        KeyType &scan_current_key = const_cast<KeyType &>(scan_itr->first);
        
        auto tuple =
            scan_current_key.GetTupleForComparison(metadata->GetKeySchema());

        // Compare the current key in the scan with "values" based on
        // "expression types"
        // For instance, "5" EXPR_GREATER_THAN "2" is true
        if (Compare(tuple, key_column_ids, expr_types, values) == true) {
          result.push_back(*(scan_itr->second));
        }
      }
      
      break;
    }

    case SCAN_DIRECTION_TYPE_INVALID:
    default:
      throw Exception("Invalid scan direction \n");
      break;
  }

  return;
}

BWTREE_TEMPLATE_ARGUMENTS
void
BWTREE_INDEX_TYPE::ScanAllKeys(std::vector<ItemPointer> &result) {
  auto it = container.Begin();

  // scan all entries
  while (it.IsEnd() == false) {
    result.push_back(*(it->second));
    it++;
  }

  return;
}

BWTREE_TEMPLATE_ARGUMENTS
void
BWTREE_INDEX_TYPE::ScanKey(const storage::Tuple *key,
                           std::vector<ItemPointer> &result) {
  KeyType index_key;
  index_key.SetFromKey(key);
  
  std::vector<ItemPointer *> temp_list{};

  // This function in BwTree fills a given vector
  container.GetValueDereference(index_key, result);

  return;
}

BWTREE_TEMPLATE_ARGUMENTS
void
BWTREE_INDEX_TYPE::Scan(const std::vector<Value> &values,
                        const std::vector<oid_t> &key_column_ids,
                        const std::vector<ExpressionType> &expr_types,
                        const ScanDirectionType &scan_direction,
                        std::vector<ItemPointer *> &result) {
  KeyType index_key;
  std::unique_ptr<storage::Tuple> start_key;

  start_key.reset(new storage::Tuple(metadata->GetKeySchema(), true));

  bool all_constraints_are_equal = ConstructLowerBoundTuple(
          start_key.get(), values, key_column_ids, expr_types);

  if (all_constraints_are_equal == true) {
    index_key.SetFromKey(start_key.get());

    container.GetValue(index_key, result);
    
    return;
  }

  // This returns an iterator pointing to index_key's values
  auto scan_begin_itr = container.Begin(index_key);

  switch (scan_direction) {
    case SCAN_DIRECTION_TYPE_FORWARD:
    case SCAN_DIRECTION_TYPE_BACKWARD: {
      // TODO: This requires further optimization to at least
      // find an upper bound for scanning (i.e. when the highest columns
      // has < or <= relation). Otherwise we will have to scan till the
      // end of the index which is toooooooooooooooooooooooooooooo slow
      for (auto scan_itr = scan_begin_itr;
           scan_itr.IsEnd() == false;
           scan_itr++) {
        KeyType &scan_current_key = const_cast<KeyType &>(scan_itr->first);

        auto tuple =
            scan_current_key.GetTupleForComparison(metadata->GetKeySchema());

        // Compare the current key in the scan with "values" based on
        // "expression types"
        // For instance, "5" EXPR_GREATER_THAN "2" is true
        if (Compare(tuple, key_column_ids, expr_types, values) == true) {
          result.push_back(scan_itr->second);
        }
      }

      break;
    }

    case SCAN_DIRECTION_TYPE_INVALID:
    default:
      throw Exception("Invalid scan direction \n");
      break;
  }

  return;
}

BWTREE_TEMPLATE_ARGUMENTS
void
BWTREE_INDEX_TYPE::ScanAllKeys(std::vector<ItemPointer *> &result) {
  auto it = container.Begin();

  // scan all entries
  while (it.IsEnd() == false) {
    result.push_back(it->second);
    it++;
  }

  return;
}

BWTREE_TEMPLATE_ARGUMENTS
void
BWTREE_INDEX_TYPE::ScanKey(const storage::Tuple *key,
                           std::vector<ItemPointer *> &result) {
  KeyType index_key;
  index_key.SetFromKey(key);

  // This function in BwTree fills a given vector
  container.GetValue(index_key, result);

  return;
}

BWTREE_TEMPLATE_ARGUMENTS
std::string
BWTREE_INDEX_TYPE::GetTypeName() const {
  return "BWTree";
}

template class BWTreeIndex<IntsKey<1>,
                           ItemPointer *,
                           IntsComparator<1>,
                           IntsEqualityChecker<1>,
                           IntsHasher<1>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;
template class BWTreeIndex<IntsKey<2>,
                           ItemPointer *,
                           IntsComparator<2>,
                           IntsEqualityChecker<2>,
                           IntsHasher<2>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;
template class BWTreeIndex<IntsKey<3>,
                           ItemPointer *,
                           IntsComparator<3>,
                           IntsEqualityChecker<3>,
                           IntsHasher<3>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;
template class BWTreeIndex<IntsKey<4>,
                           ItemPointer *,
                           IntsComparator<4>,
                           IntsEqualityChecker<4>,
                           IntsHasher<4>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;

// Generic key
template class BWTreeIndex<GenericKey<4>,
                           ItemPointer *,
                           GenericComparator<4>,
                           GenericEqualityChecker<4>,
                           GenericHasher<4>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;
template class BWTreeIndex<GenericKey<8>,
                           ItemPointer *,
                           GenericComparator<8>,
                           GenericEqualityChecker<8>,
                           GenericHasher<8>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;
template class BWTreeIndex<GenericKey<12>,
                           ItemPointer *,
                           GenericComparator<12>,
                           GenericEqualityChecker<12>,
                           GenericHasher<12>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;
template class BWTreeIndex<GenericKey<16>,
                           ItemPointer *,
                           GenericComparator<16>,
                           GenericEqualityChecker<16>,
                           GenericHasher<16>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;
template class BWTreeIndex<GenericKey<24>,
                           ItemPointer *,
                           GenericComparator<24>,
                           GenericEqualityChecker<24>,
                           GenericHasher<24>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;
template class BWTreeIndex<GenericKey<32>,
                           ItemPointer *,
                           GenericComparator<32>,
                           GenericEqualityChecker<32>,
                           GenericHasher<32>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;
template class BWTreeIndex<GenericKey<48>,
                           ItemPointer *,
                           GenericComparator<48>,
                           GenericEqualityChecker<48>,
                           GenericHasher<48>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;
template class BWTreeIndex<GenericKey<64>,
                           ItemPointer *,
                           GenericComparator<64>,
                           GenericEqualityChecker<64>,
                           GenericHasher<64>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;
template class BWTreeIndex<GenericKey<96>,
                           ItemPointer *,
                           GenericComparator<96>,
                           GenericEqualityChecker<96>,
                           GenericHasher<96>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;
template class BWTreeIndex<GenericKey<128>,
                           ItemPointer *,
                           GenericComparator<128>,
                           GenericEqualityChecker<128>,
                           GenericHasher<128>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;
template class BWTreeIndex<GenericKey<256>,
                           ItemPointer *,
                           GenericComparator<256>,
                           GenericEqualityChecker<256>,
                           GenericHasher<256>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;
template class BWTreeIndex<GenericKey<512>,
                           ItemPointer *,
                           GenericComparator<512>,
                           GenericEqualityChecker<512>,
                           GenericHasher<512>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;

// Tuple key
template class BWTreeIndex<TupleKey,
                           ItemPointer *,
                           TupleKeyComparator,
                           TupleKeyEqualityChecker,
                           TupleKeyHasher,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;

}  // End index namespace
}  // End peloton namespace
