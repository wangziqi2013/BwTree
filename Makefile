
all: main

main: main.o
	g++ main.o -o main -pthread --std=gnu++11 -g -O3 -Wall

main.o: main.cpp bwtree.h
	g++ main.cpp -c -pthread --std=gnu++11 -o main.o -g -O3 -Wall

clean:
	rm -f *.o *.log
	
