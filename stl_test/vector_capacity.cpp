
#include <iostream>
#include <vector>

using namespace std;

int main() {
  vector<int> v{};
  v.reserve(17);

  cout << "Initial Capacity: " << v.capacity() << endl;
  cout << "Pushing back....." << endl;
  cout << "Capacity: ";
  
  for(int i = 0;i < 100;i++) {
    v.push_back(i);
    cout << v.capacity() << "; ";
  }
  
  cout << endl;
  cout << "Poping back....." << endl;
  cout << "Capacity: ";
  
  for(int i = 0;i < 100;i++) {
    v.pop_back();
    cout << v.capacity() << "; ";
  }
  
  cout << endl;

  return 0;
}
