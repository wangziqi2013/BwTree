
#include <iostream>
#include <vector>

using namespace std;

int main() {
	vector<int> v{};
	v.reserve(17);

	cout << "Initial Capacity: " << v.capacity() << endl;
	for(int i = 0;i < 100;i++) {
		v.push_back(i);
		cout << "Capacity: " << v.capacity() << endl;
	}

	return 0;
}
