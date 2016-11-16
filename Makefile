
CXX = g++-5
PAPI_FLAG = -lpapi
CXX_FLAG = -pthread -std=c++11 -g -Wall -mcx16 -Wno-invalid-offsetof $(PAPI_FLAG)
GMON_FLAG = 
OPT_FLAG = -O2
PRELOAD_LIB = LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so
SRC = ./test/main.cpp ./src/bwtree.h ./src/bloom_filter.h ./src/atomic_stack.h ./src/sorted_small_set.h ./test/test_suite.h ./test/test_suite.cpp ./test/random_pattern_test.cpp ./test/basic_test.cpp ./test/mixed_test.cpp ./test/performance_test.cpp ./test/stress_test.cpp ./test/iterator_test.cpp ./test/misc_test.cpp ./test/benchmark_bwtree_full.cpp ./benchmark/spinlock/spinlock.cpp ./test/benchmark_btree_full.cpp ./test/benchmark_art_full.cpp
OBJ = ./build/main.o ./build/bwtree.o ./build/test_suite.o ./build/random_pattern_test.o ./build/basic_test.o ./build/mixed_test.o ./build/performance_test.o ./build/stress_test.o ./build/iterator_test.o ./build/misc_test.o ./build/benchmark_bwtree_full.o ./build/spinlock.o ./build/benchmark_btree_full.o ./build/benchmark_art_full.o ./build/art.o


all: main

main: $(OBJ)
	$(CXX) $(OBJ) -o ./main $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)

./build/main.o: $(SRC) ./src/bwtree.h
	$(CXX) ./test/main.cpp -c -o ./build/main.o $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)

./build/art.o:
	$(CXX) ./benchmark/art/art.c -c -o ./build/art.o $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)

./build/bwtree.o: ./src/bwtree.cpp ./src/bwtree.h
	$(CXX) ./src/bwtree.cpp -c -o ./build/bwtree.o $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)

./build/test_suite.o: ./test/test_suite.cpp ./src/bwtree.h
	$(CXX) ./test/test_suite.cpp -c -o ./build/test_suite.o $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)

./build/random_pattern_test.o: ./test/random_pattern_test.cpp ./src/bwtree.h
	$(CXX) ./test/random_pattern_test.cpp -c -o ./build/random_pattern_test.o $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)
	
./build/basic_test.o: ./test/basic_test.cpp ./src/bwtree.h
	$(CXX) ./test/basic_test.cpp -c -o ./build/basic_test.o $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)
	
./build/mixed_test.o: ./test/mixed_test.cpp ./src/bwtree.h
	$(CXX) ./test/mixed_test.cpp -c -o ./build/mixed_test.o $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)

./build/performance_test.o: ./test/performance_test.cpp ./src/bwtree.h
	$(CXX) ./test/performance_test.cpp -c -o ./build/performance_test.o $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)

./build/benchmark_bwtree_full.o: ./test/benchmark_bwtree_full.cpp ./src/bwtree.h
	$(CXX) ./test/benchmark_bwtree_full.cpp -c -o ./build/benchmark_bwtree_full.o $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)

./build/benchmark_btree_full.o: ./test/benchmark_btree_full.cpp ./src/bwtree.h
	$(CXX) ./test/benchmark_btree_full.cpp -c -o ./build/benchmark_btree_full.o $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)

./build/benchmark_art_full.o:
	$(CXX) ./test/benchmark_art_full.cpp -c -o ./build/benchmark_art_full.o $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)
	
./build/stress_test.o: ./test/stress_test.cpp ./src/bwtree.h
	$(CXX) ./test/stress_test.cpp -c -o ./build/stress_test.o $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)
	
./build/iterator_test.o: ./test/iterator_test.cpp ./src/bwtree.h
	$(CXX) ./test/iterator_test.cpp -c -o ./build/iterator_test.o $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)

./build/misc_test.o: ./test/misc_test.cpp ./src/bwtree.h
	$(CXX) ./test/misc_test.cpp -c -o ./build/misc_test.o $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)


./build/spinlock.o:
	$(CXX) ./benchmark/spinlock/spinlock.cpp -c -o ./build/spinlock.o $(CXX_FLAG) $(OPT_FLAG) $(GMON_FLAG)

gprof:
	make clean
	make all GMON_FLAG=-pg

full-speed:
	make clean
	make OPT_FLAG=" -Ofast -frename-registers -funroll-loops -flto -march=native -DNDEBUG -DBWTREE_NODEBUG -lboost_system -lboost_thread"

small-size:
	make clean
	make OPT_FLAG=" -Os -DNDEBUG -DBWTREE_NODEBUG"

benchmark-all: main 
	$(PRELOAD_LIB) ./main --benchmark-all

benchmark-bwtree: main
	$(PRELOAD_LIB) ./main --benchmark-bwtree

benchmark-bwtree-full: main
	$(PRELOAD_LIB) ./main --benchmark-bwtree-full

benchmark-btree-full: main
	$(PRELOAD_LIB) ./main --benchmark-btree-full

benchmark-art-full: main
	$(PRELOAD_LIB) ./main --benchmark-art-full

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
	
