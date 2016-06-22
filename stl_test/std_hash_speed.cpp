
#include <functional>
#include <iostream>
#include <chrono>


int main() {
  std::chrono::time_point<std::chrono::system_clock> start, end;
  
  start = std::chrono::system_clock::now();

  for(long i = 0;i < 1000 * 1024 * 1024;i++) {
    size_t h = std::hash<double>()(i * 1.11);
    (void)h;
  }
  
  end = std::chrono::system_clock::now();
  
  std::chrono::duration<double> elapsed_seconds = end - start;

  std::cout << (1000) / elapsed_seconds.count()
            << " million hash()/sec" << "\n";
  
  return 0;
}
