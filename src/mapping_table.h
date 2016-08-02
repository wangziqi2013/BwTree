//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// mapping_table.h
//
// Identification: src/index/mapping_table.h
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

#include <atomic>
#include <cassert>
#include <cstddef>

/*
 * class MappingTable - An extensible mapping table used by BwTree
 */
template<size_t stack_size, size_t array_size>
class MappingTable {
  // High bits are all 1's. This is the mask to extract stack index
  // from a NodeID
  constexpr static size_t stack_index_mask = ~((1 << array_size) - 1);
  constexpr static size_t array_index_mask = ((1 << array_size) - 1);
  
  // This is the next available node id for allocation
  // This variable never decreases. GC on NodeID must be done by
  // external functions
  std::atomic<NodeID> next_node_id;
  
  // This is the next available slot inside the stack of arrays
  std::atomic<NodeID> next_stack_index;
  
  // This is the current size of the mapping table. If next node ID
  // exceeds this value then we must extend the table
  std::atomic<NodeID> current_table_size;
  
  std::atomic<NodeID> *stack_p[stack_size];
};
