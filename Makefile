
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall -O2 -fcommon

all: correctness persistence
 
correctness: kvstore.o correctness.o skipList.o splayArray.o Cache.o BloomFilter.o

persistence: kvstore.o persistence.o skipList.o splayArray.o Cache.o BloomFilter.o

skipListTest: kvstore.o skipList.o skipListTest.o tools.o splayArray.o Cache.o BloomFilter.o
 
clean:
	-rm -f correctness persistence *.o
