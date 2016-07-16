
#include <iostream>
#include <algorithm>

using namespace std;

int main() {
  int a[] = {3, 6, 0, 2, 0, 3, 1, 9, 9, 3, 0, 6, 0, 1, 3, 5, 5, 3};

  int sz = sizeof(a) / sizeof(int);
  std::sort(a, a + sz);

  for(int i = 0;i < sz;i++) {
    cout << a[i] << ' ';
  }

  cout << endl;

  return 0;
}
