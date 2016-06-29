
#include <atomic>
#include <iostream>

using namespace std;

struct DoubleWord {
  uint64_t a;
  uint64_t b;
};

int main() {
  atomic<DoubleWord> dw1;

  DoubleWord dw2{3, 4};
  DoubleWord dw3{5, 6};
  
  dw1 = DoubleWord{7, 8};
  
  // This will compile using LOCK CMPXCHG8B
  bool ret = dw1.compare_exchange_strong(dw2, dw3);
  
  cout << "ret = " << ret << endl;
  cout << "dw2.a, dw2.b = " << dw2.a << ", " << dw2.b << endl;
  
  __asm ("nop; nop");
  DoubleWord dw4 = dw1.load();
  __asm ("nop; nop; nop");
  cout << "dw4.a, dw4.b = " << dw4.a << ", " << dw4.b << endl;
  
  return 0;
}
