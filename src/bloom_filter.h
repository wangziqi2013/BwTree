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
#include <cstring>
#include <functional>

/*
 * class BloomFilter
 */
template <typename ValueType,
          typename ValueHashFunc = std::hash<ValueType>>
class BloomFilter {
  // The size of the bit field
  static constexpr int ARRAY_SIZE = 256;

  // The number of individual arrays inside the filter
  static constexpr int FILTER_NUM = 8;
  
  static constexpr int RIGHT_SHIFT_BIT = 8;
  
  // The size of a single array
  static constexpr int FILTER_SIZE = ARRAY_SIZE / FILTER_NUM;
  
  // 0000 0000 0000 0000 0000 0000 0000 0111
  // This is the mask for extracting offset inside a byte
  static constexpr size_t BIT_OFFSET_MASK = 0x0000000000000007;
  
  // 0000 0000 0000 0000 0000 0000 1111 1000
  // This is the masj for extracting byte offset inside the array
  static constexpr size_t BYTE_OFFSET_MASK = 0x00000000000000F8;
  
 private:
  unsigned char bit_array_0[FILTER_SIZE];
  unsigned char bit_array_1[FILTER_SIZE];
  unsigned char bit_array_2[FILTER_SIZE];
  unsigned char bit_array_3[FILTER_SIZE];
  unsigned char bit_array_4[FILTER_SIZE];
  unsigned char bit_array_5[FILTER_SIZE];
  unsigned char bit_array_6[FILTER_SIZE];
  unsigned char bit_array_7[FILTER_SIZE];
  
  // This is the object used to hash values into a
  // size_t
  ValueHashFunc value_hash_obj;
  
 public:
  /*
   * Default Constructor - deleted
   * Copy Constructor - deleted
   * Copy assignment - deleted
   */
  BloomFilter(const BloomFilter &) = delete;
  BloomFilter &operator=(const BloomFilter &) = delete;
  
  /*
   * Constructor - Initialize value hash function
   *
   * This function zeros out memory by calling memset(). Holefully the compiler
   * would understand the meaning of this and optimize them into one function
   * call
   */
  BloomFilter(const ValueHashFunc &p_value_hash_obj = ValueHashFunc{}) :
    value_hash_obj{p_value_hash_obj} {
    memset(bit_array_0, 0, sizeof(bit_array_0));
    memset(bit_array_1, 0, sizeof(bit_array_1));
    memset(bit_array_2, 0, sizeof(bit_array_2));
    memset(bit_array_3, 0, sizeof(bit_array_3));
    memset(bit_array_4, 0, sizeof(bit_array_4));
    memset(bit_array_5, 0, sizeof(bit_array_5));
    memset(bit_array_6, 0, sizeof(bit_array_6));
    memset(bit_array_7, 0, sizeof(bit_array_7));
    
    return;
  }
  
  inline void __InsertScalar(const ValueType &value) {
    register size_t hash_value = value_hash_obj(value);
    
    bit_array_0[(hash_value & BYTE_OFFSET_MASK) >> 3] |= \
      (0x1 << (hash_value & BIT_OFFSET_MASK));
    hash_value >>= RIGHT_SHIFT_BIT;
    
    bit_array_1[(hash_value & BYTE_OFFSET_MASK) >> 3] |= \
      (0x1 << (hash_value & BIT_OFFSET_MASK));
    hash_value >>= RIGHT_SHIFT_BIT;
    
    bit_array_2[(hash_value & BYTE_OFFSET_MASK) >> 3] |= \
      (0x1 << (hash_value & BIT_OFFSET_MASK));
    hash_value >>= RIGHT_SHIFT_BIT;
    
    bit_array_3[(hash_value & BYTE_OFFSET_MASK) >> 3] |= \
      (0x1 << (hash_value & BIT_OFFSET_MASK));
    hash_value >>= RIGHT_SHIFT_BIT;
    
    bit_array_4[(hash_value & BYTE_OFFSET_MASK) >> 3] |= \
      (0x1 << (hash_value & BIT_OFFSET_MASK));
    hash_value >>= RIGHT_SHIFT_BIT;
    
    bit_array_5[(hash_value & BYTE_OFFSET_MASK) >> 3] |= \
      (0x1 << (hash_value & BIT_OFFSET_MASK));
    hash_value >>= RIGHT_SHIFT_BIT;
    
    bit_array_6[(hash_value & BYTE_OFFSET_MASK) >> 3] |= \
      (0x1 << (hash_value & BIT_OFFSET_MASK));
    hash_value >>= RIGHT_SHIFT_BIT;
    
    bit_array_7[(hash_value & BYTE_OFFSET_MASK) >> 3] |= \
      (0x1 << (hash_value & BIT_OFFSET_MASK));
    
    return;
  }
  
  inline void Insert(const ValueType &value) {
    __InsertScalar(value);
    
    return;
  }
  
  inline bool __ExistsScalar(const ValueType &value) {
    register size_t hash_value = value_hash_obj(value);

    if((bit_array_0[(hash_value & BYTE_OFFSET_MASK) >> 3] & \
      (0x1 << (hash_value & BIT_OFFSET_MASK))) == 0x00) {
      return false;
    } else {
      hash_value >>= RIGHT_SHIFT_BIT;
    }
    
    if((bit_array_1[(hash_value & BYTE_OFFSET_MASK) >> 3] & \
      (0x1 << (hash_value & BIT_OFFSET_MASK))) == 0x00) {
      return false;
    } else {
      hash_value >>= RIGHT_SHIFT_BIT;
    }
    
    if((bit_array_2[(hash_value & BYTE_OFFSET_MASK) >> 3] & \
      (0x1 << (hash_value & BIT_OFFSET_MASK))) == 0x00) {
      return false;
    } else {
      hash_value >>= RIGHT_SHIFT_BIT;
    }
    
    if((bit_array_3[(hash_value & BYTE_OFFSET_MASK) >> 3] & \
      (0x1 << (hash_value & BIT_OFFSET_MASK))) == 0x00) {
      return false;
    } else {
      hash_value >>= RIGHT_SHIFT_BIT;
    }
    
    if((bit_array_4[(hash_value & BYTE_OFFSET_MASK) >> 3] & \
      (0x1 << (hash_value & BIT_OFFSET_MASK))) == 0x00) {
      return false;
    } else {
      hash_value >>= RIGHT_SHIFT_BIT;
    }
    
    if((bit_array_5[(hash_value & BYTE_OFFSET_MASK) >> 3] & \
      (0x1 << (hash_value & BIT_OFFSET_MASK))) == 0x00) {
      return false;
    } else {
      hash_value >>= RIGHT_SHIFT_BIT;
    }
    
    if((bit_array_6[(hash_value & BYTE_OFFSET_MASK) >> 3] & \
      (0x1 << (hash_value & BIT_OFFSET_MASK))) == 0x00) {
      return false;
    } else {
      hash_value >>= RIGHT_SHIFT_BIT;
    }
    
    if((bit_array_7[(hash_value & BYTE_OFFSET_MASK) >> 3] & \
      (0x1 << (hash_value & BIT_OFFSET_MASK))) == 0x00) {
      return false;
    } else {
      hash_value >>= RIGHT_SHIFT_BIT;
    }
    
    return true;
  }
  
  inline bool Exists(const ValueType &value) {
    return __ExistsScalar(value);
  }
};
