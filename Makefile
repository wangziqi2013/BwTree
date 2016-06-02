
all: main

main: main.o bwtree.o
	g++ main.o bwtree.o -o main -pthread --std=gnu++11 -g -O3 -Wall

main.o: main.cpp bwtree.h
	g++ main.cpp -c -pthread --std=gnu++11 -o main.o -g -O3 -Wall

bwtree.o: bwtree.h bwtree.cpp
	g++ bwtree.cpp -c --std=gnu++11 -o bwtree.o -g -O3 -Wall

clean:
	rm -f *.o *.log
	
