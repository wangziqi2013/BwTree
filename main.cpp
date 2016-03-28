

#include "bwtree.h"
#include <thread>

using namespace peloton::index;

//template class BwTree<int, double>;
//template class BwTree<long, double>;
//template class BwTree<std::string, double>;
//template class BwTree<std::string, std::string>;

template <typename Fn, typename... Args>
void LaunchParallelTestID(uint64_t num_threads, Fn&& fn, Args &&... args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(fn, thread_itr, args...));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

void GetNextNodeIDTestThread(uint64_t thread_id, BwTree<int, double> *tree_p) {
  bwt_printf("ID = %lu\n", tree_p->GetNextNodeID());
  return;
}

void GetNextNodeIDTest(BwTree<int, double> *tree_p) {
  LaunchParallelTestID(100, GetNextNodeIDTestThread, tree_p);
  return;
}


int main() {
  BwTree<int, double> *t1 = new BwTree<int, double>{};
  //BwTree<long, double> *t2 = new BwTree<long, double>{};

  BwTree<int, double>::KeyType k1 = t1->GetWrappedKey(3);
  BwTree<int, double>::KeyType k2 = t1->GetWrappedKey(2);
  BwTree<int, double>::KeyType k3 = t1->GetNegInfKey();

  bwt_printf("KeyComp: %d\n", t1->KeyCmpLess(k2, k3));
  bwt_printf("sizeof(class BwTree) = %lu\n", sizeof(BwTree<long double, long double>));

  GetNextNodeIDTest(t1);

  return 0;
}
