
all: main

main: main.o
	g++ main.o -o main -pthread --std=gnu++11 -O3

main.o: main.cpp bwtree.h
	g++ main.cpp -c -pthread --std=gnu++11 -o main.o -O3

clean:
	rm -f *.o *.log
