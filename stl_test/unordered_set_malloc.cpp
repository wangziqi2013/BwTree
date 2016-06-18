
#include <iostream>
#include <unordered_set>

using namespace std;

int main() {
  unordered_set<int> s{};
  
  for(int i = 0;i < 100;i++) {
    s.insert(i);
  }
  
  return 0;
}
