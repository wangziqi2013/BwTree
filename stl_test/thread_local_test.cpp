
#include <cstdio>

thread_local int a = 0;
thread_local double b = 1.1;
thread_local long c = 2;

int main() {
  printf("Thread local a = %d; b = %f; c = %ld\n",
         a, 
         b, 
         c);
  printf("Address: %p %p %p", &a, &b, &c);
  
  return 0;
}
