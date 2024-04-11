#pragma once
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include "Constants.h"
#include "BloomFilter.h"
#define DEBUG
class Cache
{
    private : 
        int n ;                                 // Max Number in the Cache
        char ** memory ;                        // The save in the Cache; Ending signal is \0
        int * LRU_TIME ;                        // Save the time since last access
        int unitSize ;                          // each memory location's size . 
        int loaded ;                            // current loaded Number ; 
        // void replaceCache(FILE * toLoad) ;      // evict the cache and read a new table ; the evict method is LRU
        int getReplaceIndex();                  // get the next place to insert cache.
        uint64_t getMin(int index) ;            // get the minimun key of a sstable
        uint64_t getMax(int index) ;            // get the maximum key of a sstable
        void increaseTime() ;                   // increase LRU_TIME 
        // get the index according to the key ; if cant find it in the Cache , return 0. if may in it return the list in the variable and return 1.
        // The end tag of indexList is -1
        int getIndex(uint64_t key , int * indexList);
        // memory[index] dont have the key => return -1 . else return the offset of the key 
        int searchIndex(uint64_t key , int index) ; 
    public :
        Cache(int n , int unitSize) ;           // Construct Cache according to n(maxNum) and unitSize
        void loadCache(FILE* toLoad) ;          // Load a File in the Cache
        bool isfull() ;                         // Judge if a Cache is full 

        // get the true cache index ; if it cant find , return -1  ; it will return the offset of the key's exact location in the para
        int access(uint64_t key,const BloomFilter *judger , int & offset) ; 
        // get the memory in the index 
        char * access(int index) ; 
};