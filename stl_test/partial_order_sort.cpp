
#include <iostream>
#include <algorithm>
#include <utility>

using namespace std;

int main() {
  pair<int, int> array[10] = {{2, 4}, {1, 3}, {1, 1}, {2, 0}, {1, 1}, {2, 8}, {2, 7}, {4, 0}, {2, 6}, {2, 0}};
  
  stable_sort(array,
              array + 10,
              [](const pair<int, int> &a, const pair<int, int> &b) {
                return (a.first < b.first) && (a.second != b.second);
              });
              
  for(int i = 0;i < 10;i++) {
    cout << array[i].first << " " << array[i].second << endl;
  }
  
  return 0;
}
