
GMON_FLAG = 
OPT_FLAG = -O3
JEMALLOC_LIB = /usr/lib/x86_64-linux-gnu/libjemalloc.so

all: main

main: ./build/main.o ./build/bwtree.o
	g++-5 ./build/main.o ./build/bwtree.o -o ./main -pthread -std=c++11 -g -Wall $(OPT_FLAG) $(GMON_FLAG)

./build/main.o: ./src/main.cpp ./src/bwtree.h
	g++-5 ./src/main.cpp -c -pthread -std=c++11 -o ./build/main.o -g -Wall $(OPT_FLAG) $(GMON_FLAG)

./build/bwtree.o: ./src/bwtree.h ./src/bwtree.cpp
	g++-5 ./src/bwtree.cpp -c -std=c++11 -o ./build/bwtree.o -g -Wall $(OPT_FLAG) $(GMON_FLAG)

gprof:
	make clean
	make all GMON_FLAG=-pg

benchmark-all: main
	LD_PRELOAD=$(JEMALLOC_LIB) ./main --benchmark-all

benchmark-bwtree: main
	LD_PRELOAD=$(JEMALLOC_LIB) ./main --benchmark-bwtree

test: main
	LD_PRELOAD=$(JEMALLOC_LIB) ./main --test

stress-test: main
	LD_PRELOAD=$(JEMALLOC_LIB) ./main --stress-test

prepare:
	mkdir -p build
	mkdir -p ./stl_test/bin

clean:
	rm -f ./build/*
	rm -f *.log
	rm -f ./main
	
