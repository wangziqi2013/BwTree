
#include <cstdio>
#include <cstddef>

class A {
 public:
  int a;
  char b;
  int *c;
};

class B : public A {
 public:
  int d;
  int e;
  int f;
};

int main() {
  printf("offset of f = %lu; sizeof(A) = %lu; sizeof(B) = %lu\n",
         offsetof(B, f), sizeof(A), sizeof(B));

  return 0;
}
