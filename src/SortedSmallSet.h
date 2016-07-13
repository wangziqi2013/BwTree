
#include <cassert>
#include <algorithm>

/*
 * class SortedSmallSet - An implementation of small sorted set with known
 *                        element count upper bound
 *
 * This functions uses insertion sort in anticipation that the number
 * of elements would be small. Under such condition, array based
 * implementation exploits locality and is therefore is cache friendly
 *
 * If it is not the case then we should use a more lever container
 *
 * NOTE: The data array must be large enough to hold the values. There
 * is no bounds checking and/or safeguard
 */
template<typename ValueType,
         typename ValueComparator = std::less<ValueType>,
         typename ValueEqualityChecker = std::equal_to<ValueType>>
class SortedSmallSet {
 private:
  ValueType *data;

  // These two are only valid if the item count is not 0
  ValueType *start_p;
  ValueType *end_p;

  ValueComparator value_cmp_obj;
  ValueEqualityChecker value_eq_obj;

 public:
  SortedSmallSet(ValueType *p_data,
                 const ValueComparator &p_value_cmp_obj = ValueComparator{},
                 const ValueEqualityChecker &p_value_eq_obj = ValueEqualityChecker{}) :
    data{p_data},
    start_p{p_data},
    end_p{p_data},
    value_cmp_obj{p_value_cmp_obj},
    value_eq_obj{p_value_eq_obj}
  {}

  /*
   * Insert() - Inserts a value into the set
   *
   * If the value already exists do nothing. Otherwise
   * it will be inserted before the first element that is larger
   */
  inline void Insert(ValueType &value) {
    auto it = std::lower_bound(start_p, end_p, value, value_cmp_obj);

    if(value_eq_obj(*it, value) == true) {
      return;
    }

    // It is like backward shift operation
    std::copy_backward(it, end_p, end_p + 1);
    *it = value;

    end_p++;

    return;
  }

  /*
   * GetBegin() - Returns the begin pointer to the current begin
   *
   * Note that the begin pointer could advance
   */
  inline ValueType *GetBegin() {
    return start_p;
  }

  inline ValueType *GetEnd() {
    return end_p;
  }

  /*
   * IsEmpty() - Returns true if the container is empty
   *
   * Empty is defined as start pointer being equal to the end pointer
   */
  inline bool IsEmpty() {
    return start_p == end_p;
  }

  /*
   * PopFront() - Removes a value from the head and shrinks
   *              the size of the container by 1
   */
  inline ValueType &PopFront() {
    assert(start_p < end_p);;

    return *(start_p++);
  }
};
