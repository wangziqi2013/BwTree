
#include <cstdio>

int main() {
	int *p = nullptr;

	p = new int[0];
        printf("After new int[0] p = %p\n", p);
        // Valgrind could detect this
	//delete[] p;

	return 0;
}
