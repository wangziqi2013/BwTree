
all: main

main: main.o
	g++ main.o -o main -pthread --std=c++11 -O3 -ljemalloc

main.o: main.cpp bwtree.h
	g++ main.cpp -c -pthread --std=c++11 -o main.o -O3 -ljemalloc

clean:
	rm -f *.o *.log main
	
