
all: main

main: main.o bwtree.o
	g++ main.o bwtree.o -o main -pthread

main.o: main.cpp bwtree.h
	g++ main.cpp -c -pthread --std=c++11 -o main.o

bwtree.o: bwtree.cpp bwtree.h
	g++ bwtree.cpp -c -pthread --std=c++11 -o bwtree.o
