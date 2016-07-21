
#include <iostream>
#include <algorithm>

int main() {
  int a[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 0, 0, 0, 0};
  int len = (int)sizeof(a) / (int)sizeof(int);
  
  std::copy_backward(a, a + 10, a + len);
  
  for(int i = 0;i < len;i++) {
    std::cout << a[i] << " ";
  }
  
  std::cout << std::endl;
  
  return 0;
}
