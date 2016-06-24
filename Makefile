
CXX = g++
GMON_FLAG = 
OPT_FLAG = -O3
PRELOAD_LIB = LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so

all: main

main: ./build/main.o ./build/bwtree.o
	$(CXX) ./build/main.o ./build/bwtree.o -o ./main -pthread -std=c++11 -g -Wall $(OPT_FLAG) $(GMON_FLAG)

./build/main.o: ./src/main.cpp ./src/bwtree.h ./src/bloom_filter.h
	$(CXX) ./src/main.cpp -c -pthread -std=c++11 -o ./build/main.o -g -Wall $(OPT_FLAG) $(GMON_FLAG)

./build/bwtree.o: ./src/bwtree.h ./src/bwtree.cpp ./src/bloom_filter.h
	$(CXX) ./src/bwtree.cpp -c -std=c++11 -o ./build/bwtree.o -g -Wall $(OPT_FLAG) $(GMON_FLAG)

gprof:
	make clean
	make all GMON_FLAG=-pg

full-speed:
	make clean
	make OPT_FLAG=" -O3 -DNDEBUG -DBWTREE_NODEBUG"

benchmark-all: main
	$(PRELOAD_LIB) ./main --benchmark-all

benchmark-bwtree: main
	$(PRELOAD_LIB) ./main --benchmark-bwtree

benchmark-bwtree-full: main
	$(PRELOAD_LIB) ./main --benchmark-bwtree-full

test: main
	$(PRELOAD_LIB) ./main --test

stress-test: main
	$(PRELOAD_LIB) ./main --stress-test

epoch-test: main
	$(PRELOAD_LIB) ./main --epoch-test

prepare:
	mkdir -p build
	mkdir -p ./stl_test/bin

clean:
	rm -f ./build/*
	rm -f *.log
	rm -f ./main
	
