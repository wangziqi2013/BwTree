
all: main

main: main.o bwtree.o
	g++ main.o bwtree.o -o main -pthread --std=gnu++11 -g -Wall -O3

main.o: main.cpp bwtree.h
	g++ main.cpp -c -pthread --std=gnu++11 -o main.o -g -Wall -O3

bwtree.o: bwtree.h bwtree.cpp
	g++ bwtree.cpp -c --std=gnu++11 -o bwtree.o -g -Wall -O3

clean:
	rm -f *.o *.log
	
