#pragma once
#include <stdlib.h>
#include <stdio.h>
#include <cassert>
#include <vector>
#include <stdint.h>
#include <algorithm>
#include "Constants.h"
#include "tools.h"
#include "BloomFilter.h"
// #define DEBUG
class Cache
{
    private :   // the cache nolonger have the size limit
        
        std::vector<char *> memory ;                        // The save in the Cache; Ending signal is \0
        int unitSize ;                          // each memory location's size . unit:byte
        int loaded ;                            // current loaded Number ; 
        BloomFilter * bf ;                      // the bloomFilter used to check for the key ; it will use a static method 
        uint64_t getMin(int index) const;            // get the minimun key of a sstable
        uint64_t getMax(int index) const;            // get the maximum key of a sstable
        // get the index according to the key ; if cant find it in the Cache , return 0. if may in it return the list in the variable and return 1.
        // The end tag of indexList is -1
        int getIndex(uint64_t key , int * indexList);
        // memory[index] dont have the key => return -1 . else return the offset of the key 
        int searchIndex(uint64_t key , int index) ; 
    public :
        Cache(int unitSize, BloomFilter * bf) ;           // Construct Cache according to n(maxNum) and unitSize
        void loadCache(FILE* toLoad) ;          // Load a File in the Cache

        // get the true cache index ; if it cant find , return -1  ; it will return the offset of the key's exact location in the para
        int access(uint64_t key,const BloomFilter *judger , int & offset) ; 
        // get the memory in the index 
        char * access(int index) ; 
        // get the bottom(newest) cache line 
        char * bottom() const ; 
        // get the loaded size of the cache
        int size() const  ; 
        // get the timeStamp of a index
        uint64_t getTimeStamp(int index) const ;
        // compare the timestamp to sort 
        bool sort_timeStamp(int index1 , int index2); 
        void shellSort(int arr[], int n) ; 
        // delete all the memory 
        void reset() ; 
        // scan the cache , return all the overlap cacheline and return them in lineList and order it 
        void scanPossibleLine(uint64_t key1 , uint64_t key2 , int * lineList) ;
        // for a cacheline , arrive at the index's location . if cant find the index , it should return the previous offset of it 
        int arrive(int index , uint64_t key) ; 
};

