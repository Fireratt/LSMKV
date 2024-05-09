
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall -O2 -fcommon -g

all: correctness persistence

test: testRead skipListTest

correctness: kvstore.o correctness.o skipList.o splayArray.o DiskTableManager.o Cache.o BloomFilter.o

persistence: kvstore.o persistence.o skipList.o splayArray.o DiskTableManager.o Cache.o BloomFilter.o

skipListTest: kvstore.o skipList.o skipListTest.o tools.o splayArray.o DiskTableManager.o Cache.o BloomFilter.o

testRead: kvstore.o skipList.o testRead.o tools.o splayArray.o DiskTableManager.o Cache.o BloomFilter.o
clean:
	-rm -f correctness persistence *.o
