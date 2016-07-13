
SOURCE = unordered_set_malloc.cpp vector_capacity.cpp bloom_filter.cpp std_hash_speed.cpp dcas_test.cpp
CXXFLAG = -O3 --std=gnu++11 -g -Wall

all: $(SOURCE)
	g++ vector_capacity.cpp -o ./bin/vector_capacity $(CXXFLAG)
	g++ unordered_set_malloc.cpp -o ./bin/unordered_set_malloc $(CXXFLAG)
	g++ bloom_filter.cpp -o ./bin/bloom_filter $(CXXFLAG)
	g++ std_hash_speed.cpp -o ./bin/std_hash_speed $(CXXFLAG)
	g++ dcas_test.cpp -o ./bin/dcas_test $(CXXFLAG) -mcx16
	g++ std_sort_malloc.cpp -o ./bin/std_sort_malloc $(CXXFLAG)
	g++ sorted_small_set_test.cpp -o ./bin/sorted_small_set_test $(CXXFLAG)


clean:
	rm -f ./bin/*
