
/*
 * iterator_test.cpp
 *
 * Tests basic iterator operations
 *
 * by Ziqi Wang
 */

#include "test_suite.h"


/*
 * IteratorTest() - Tests iterator functionalities
 */
void IteratorTest(TreeType *t) {
  const long key_num = 1024 * 1024;

  // First insert from 0 to 1 million
  for(long int i = 0;i < key_num;i++) {
    t->Insert(i, i);
  }

  auto it = t->Begin();

  long i = 0;
  while(it.IsEnd() == false) {
    assert(it->first == it->second);
    assert(it->first == i);

    i++;
    it++;
  }

  assert(i == (key_num));

  auto it2 = t->Begin(key_num - 1);
  auto it3 = it2;

  it2++;
  assert(it2.IsEnd() == true);

  assert(it3->first == (key_num - 1));

  auto it4 = t->Begin(key_num + 1);
  assert(it4.IsEnd() == true);

  return;
}
