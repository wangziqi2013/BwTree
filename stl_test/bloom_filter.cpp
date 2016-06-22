
#include <cassert>
#include <cstdio>
#include "../src/bloom_filter.h"
#include <iostream>

using namespace std;

int main() {
  const int *buffer[256];
  
  BloomFilter<int> bf{buffer};
  
  for(int i = 0;i < 256;i++) {
    bf.Insert(i);
  }
  
  cout << "Exists() result: ";
  
  for(int i = 0;i < 256;i++) {
    cout << i << "(" << bf.Exists(i) << ", " << std::hash<int>()(i) << ") ";
  }
  
  cout << endl;
  
  return 0;
}
