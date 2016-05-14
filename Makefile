
all: main

main: main.o
	g++ main.o -o main -pthread --std=gnu++11 -g

main.o: main.cpp bwtree.h
	g++ main.cpp -c -pthread --std=gnu++11 -o main.o -g

clean:
	rm -f *.o *.log
	
