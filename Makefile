
all: main

main: ./build/main.o ./build/bwtree.o
	g++ ./build/main.o ./build/bwtree.o -o ./main -pthread --std=gnu++11 -g -Wall -O3

./build/main.o: ./src/main.cpp ./src/bwtree.h
	g++ ./src/main.cpp -c -pthread --std=gnu++11 -o ./build/main.o -g -Wall -O3

./build/bwtree.o: ./src/bwtree.h ./src/bwtree.cpp
	g++ ./src/bwtree.cpp -c --std=gnu++11 -o ./build/bwtree.o -g -Wall -O3

benchmark: main
	LD_PRELOAD=./lib/libjemalloc.so ./main -b

test: main
	LD_PRELOAD=./lib/libjemalloc.so ./main -t

clean:
	rm -f ./build/*
	rm -f *.log
	rm -f ./main
	
