
CXX = g++ 
CXX_FLAG = -pthread -std=c++11 -g -Wall -mcx16
GMON_FLAG = 
OPT_FLAG = -O2
PRELOAD_LIB = LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so
SRC = ./src/main.cpp ./src/bwtree.h ./src/bloom_filter.h ./src/atomic_stack.h ./src/sorted_small_set.h ./src/test_suite.h ./src/test_suite.cpp ./src/random_pattern_test.cpp ./src/basic_test.cpp ./src/mixed_test.cpp ./src/performance_test.cpp ./src/stress_test.cpp ./src/iterator_test.cpp ./src/misc_test.cpp
OBJ = ./build/main.o ./build/bwtree.o ./build/test_suite.o ./build/random_pattern_test.o ./build/basic_test.o ./build/mixed_test.o ./build/performance_test.o ./build/stress_test.o ./build/iterator_test.o ./build/misc_test.o

all: main

main: $(OBJ)
	$(CXX) $(OBJ) -o ./main $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)

./build/main.o: $(SRC) ./src/bwtree.h
	$(CXX) ./src/main.cpp -c -o ./build/main.o $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)

./build/bwtree.o: ./src/bwtree.cpp ./src/bwtree.h
	$(CXX) ./src/bwtree.cpp -c -o ./build/bwtree.o $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)

./build/test_suite.o: ./src/test_suite.cpp ./src/bwtree.h
	$(CXX) ./src/test_suite.cpp -c -o ./build/test_suite.o $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)

./build/random_pattern_test.o: ./src/random_pattern_test.cpp ./src/bwtree.h
	$(CXX) ./src/random_pattern_test.cpp -c -o ./build/random_pattern_test.o $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)
	
./build/basic_test.o: ./src/basic_test.cpp ./src/bwtree.h
	$(CXX) ./src/basic_test.cpp -c -o ./build/basic_test.o $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)
	
./build/mixed_test.o: ./src/mixed_test.cpp ./src/bwtree.h
	$(CXX) ./src/mixed_test.cpp -c -o ./build/mixed_test.o $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)

./build/performance_test.o: ./src/performance_test.cpp ./src/bwtree.h
	$(CXX) ./src/performance_test.cpp -c -o ./build/performance_test.o $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)
	
./build/stress_test.o: ./src/stress_test.cpp ./src/bwtree.h
	$(CXX) ./src/stress_test.cpp -c -o ./build/stress_test.o $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)
	
./build/iterator_test.o: ./src/iterator_test.cpp ./src/bwtree.h
	$(CXX) ./src/iterator_test.cpp -c -o ./build/iterator_test.o $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)

./build/misc_test.o: ./src/misc_test.cpp ./src/bwtree.h
	$(CXX) ./src/misc_test.cpp -c -o ./build/misc_test.o $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)


gprof:
	make clean
	make all GMON_FLAG=-pg

full-speed:
	make clean
	make OPT_FLAG=" -Ofast -frename-registers -funroll-loops -flto -march=native -DNDEBUG -DBWTREE_NODEBUG"

small-size:
	make clean
	make OPT_FLAG=" -Os -DNDEBUG -DBWTREE_NODEBUG"

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
	
infinite-insert-test: main
	$(PRELOAD_LIB) ./main --infinite-insert-test

email-test: main
	$(PRELOAD_LIB) ./main --email-test

mixed-test: main
	$(PRELOAD_LIB) ./main --mixed-test

prepare:
	mkdir -p build
	mkdir -p ./stl_test/bin

clean:
	rm -f ./build/*
	rm -f *.log
	rm -f ./main
	
