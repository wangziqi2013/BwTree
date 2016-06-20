
SOURCE = unordered_set_malloc.cpp vector_capacity.cpp bloom_filter.cpp
CXXFLAG = -O3 --std=gnu++11 -g -Wall

all: $(SOURCE)
	g++ vector_capacity.cpp -o ./bin/vector_capacity $(CXXFLAG)
	g++ unordered_set_malloc.cpp -o ./bin/unordered_set_malloc $(CXXFLAG)
	g++ bloom_filter.cpp -o ./bin/bloom_filter $(CXXFLAG)

clean:
	rm -f ./bin/*
