
#include <iostream>
#include "../src/SortedSmallSet.h"

using namespace std;

int main() {
  int a[100];

  SortedSmallSet<int> sss{a};

  int b[] = {3, 6, 0, 2, 0, 3, 1, 9, 9, 3, 0, 6, 0 ,1, 3, 5, 5, 3};

  for(size_t i = 0;i < sizeof(b) / sizeof(int);i++) {
    sss.Insert(b[i]);

    int *begin_p = sss.GetBegin();
    while(begin_p != sss.GetEnd()) {
      cout << *begin_p << " ";

      begin_p++;
    }

    cout << endl;
  }

  while(sss.IsEmpty() == false) {
    cout << sss.PopFront() << " ";
  }

  cout << endl;

  return 0;
}
