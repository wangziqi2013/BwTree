
SOURCE = unordered_set_malloc.cpp vector_capacity.cpp
CXXFLAG = -O3 --std=gnu++11 -g -Wall

all: $(SOURCE)
	g++ vector_capacity.cpp -o ./bin/vector_capacity $(CXXFLAG)
	g++ unordered_set_malloc.cpp -o ./bin/unordered_set_malloc $(CXXFLAG)

clean:
	rm -f ./bin/*
