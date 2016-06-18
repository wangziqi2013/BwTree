
GMON_FLAG = 
OPT_FLAG = -O3

all: main

main: ./build/main.o ./build/bwtree.o
	g++ ./build/main.o ./build/bwtree.o -o ./main -pthread --std=gnu++11 -g -Wall $(OPT_FLAG) $(GMON_FLAG)

./build/main.o: ./src/main.cpp ./src/bwtree.h
	g++ ./src/main.cpp -c -pthread --std=gnu++11 -o ./build/main.o -g -Wall $(OPT_FLAG) $(GMON_FLAG)

./build/bwtree.o: ./src/bwtree.h ./src/bwtree.cpp
	g++ ./src/bwtree.cpp -c --std=gnu++11 -o ./build/bwtree.o -g -Wall $(OPT_FLAG) $(GMON_FLAG)

gprof:
	make clean
	make all GMON_FLAG=-pg

benchmark-all: main
	LD_PRELOAD=./lib/libjemalloc.so ./main --benchmark-all

benchmark-bwtree: main
	LD_PRELOAD=./lib/libjemalloc.so ./main --benchmark-bwtree

test: main
	LD_PRELOAD=./lib/libjemalloc.so ./main --test

stress-test: main
	LD_PRELOAD=./lib/libjemalloc.so ./main --stress-test

prepare:
	mkdir -p build
	mkdir -p ./stl_test/bin

clean:
	rm -f ./build/*
	rm -f *.log
	rm -f ./main
	
