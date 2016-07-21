
CXX = g++
GMON_FLAG = 
OPT_FLAG = -O3
PRELOAD_LIB = LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so
SRC = ./src/main.cpp ./src/bwtree.h ./src/bloom_filter.h ./src/atomic_stack.h ./src/sorted_small_set.h ./src/test_suite.h ./src/test_suite.cpp ./src/random_pattern_test.cpp ./src/basic_test.cpp ./src/mixed_test.cpp ./src/performance_test.cpp ./src/stress_test.cpp ./src/iterator_test.cpp ./src/misc_test.cpp
OBJ = ./build/main.o ./build/bwtree.o ./build/test_suite.o ./build/random_pattern_test.o ./build/basic_test.o ./build/mixed_test.o ./build/performance_test.o ./build/stress_test.o ./build/iterator_test.o ./build/misc_test.o

all: main

main: $(OBJ)
	$(CXX) $(OBJ) -o ./main -pthread -std=c++11 -g -Wall -Winline -mcx16 $(OPT_FLAG) $(GMON_FLAG)

./build/main.o: $(SRC) ./src/bwtree.h
	$(CXX) ./src/main.cpp -c -pthread -std=c++11 -o ./build/main.o -g -Wall -Winline -mcx16 $(OPT_FLAG) $(GMON_FLAG)

./build/bwtree.o: ./src/bwtree.cpp ./src/bwtree.h
	$(CXX) ./src/bwtree.cpp -c -std=c++11 -o ./build/bwtree.o -g -Wall -Winline -mcx16 $(OPT_FLAG) $(GMON_FLAG)

./build/test_suite.o: ./src/test_suite.cpp ./src/bwtree.h
	$(CXX) ./src/test_suite.cpp -c -std=c++11 -o ./build/test_suite.o -g -Wall -Winline -mcx16 $(OPT_FLAG) $(GMON_FLAG)

./build/random_pattern_test.o: ./src/random_pattern_test.cpp ./src/bwtree.h
	$(CXX) ./src/random_pattern_test.cpp -c -std=c++11 -o ./build/random_pattern_test.o -g -Wall -Winline -mcx16 $(OPT_FLAG) $(GMON_FLAG)
	
./build/basic_test.o: ./src/basic_test.cpp ./src/bwtree.h
	$(CXX) ./src/basic_test.cpp -c -std=c++11 -o ./build/basic_test.o -g -Wall -Winline -mcx16 $(OPT_FLAG) $(GMON_FLAG)
	
./build/mixed_test.o: ./src/mixed_test.cpp ./src/bwtree.h
	$(CXX) ./src/mixed_test.cpp -c -std=c++11 -o ./build/mixed_test.o -g -Wall -Winline -mcx16 $(OPT_FLAG) $(GMON_FLAG)

./build/performance_test.o: ./src/performance_test.cpp ./src/bwtree.h
	$(CXX) ./src/performance_test.cpp -c -std=c++11 -o ./build/performance_test.o -g -Wall -Winline -mcx16 $(OPT_FLAG) $(GMON_FLAG)
	
./build/stress_test.o: ./src/stress_test.cpp ./src/bwtree.h
	$(CXX) ./src/stress_test.cpp -c -std=c++11 -o ./build/stress_test.o -g -Wall -Winline -mcx16 $(OPT_FLAG) $(GMON_FLAG)
	
./build/iterator_test.o: ./src/iterator_test.cpp ./src/bwtree.h
	$(CXX) ./src/iterator_test.cpp -c -std=c++11 -o ./build/iterator_test.o -g -Wall -Winline -mcx16 $(OPT_FLAG) $(GMON_FLAG)

./build/misc_test.o: ./src/misc_test.cpp ./src/bwtree.h
	$(CXX) ./src/misc_test.cpp -c -std=c++11 -o ./build/misc_test.o -g -Wall -Winline -mcx16 $(OPT_FLAG) $(GMON_FLAG)


gprof:
	make clean
	make all GMON_FLAG=-pg

full-speed:
	make clean
	make OPT_FLAG=" -O3 -DNDEBUG -DBWTREE_NODEBUG"

benchmark-all: main ./benchmark/btree.h ./benchmark/btree_multimap.h
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

email-test:
	$(PRELOAD_LIB) ./main --email-test

mixed-test:
	$(PRELOAD_LIB) ./main --mixed-test

prepare:
	mkdir -p build
	mkdir -p ./stl_test/bin

clean:
	rm -f ./build/*
	rm -f *.log
	rm -f ./main
	
