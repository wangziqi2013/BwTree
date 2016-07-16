
#include <iostream>
#include "../src/sorted_small_set.h"

using namespace std;

int main() {
  int a[100] = {};

  SortedSmallSet<int> sss{a};

  int b[] = {9, 8, 7, 6, 5, 4, 3, 2, 1};

  for(size_t i = 0;i < sizeof(b) / sizeof(int);i++) {
    sss.Insert(b[i]);

    int *begin_p = sss.GetBegin();
    while(begin_p != sss.GetEnd()) {
      cout << *begin_p << " ";

      begin_p++;
    }

    cout << endl;
  }

  cout << "Final result: " << endl;

  while(sss.IsEmpty() == false) {
    cout << sss.PopFront() << " ";
  }

  cout << endl;

  return 0;
}
