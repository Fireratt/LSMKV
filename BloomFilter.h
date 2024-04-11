#ifndef Bloom_Filter
#define Bloom_Filter
// #define DEBUG
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include "MurmurHash3.h"
// only use these 2 macros on the uint64_t ; 
#define HIGH(x) (x>>32 & 0xffffffff)   
#define LOW(x) (x & 0xffffffff) 
class BloomFilter
{
    private :
        char * hashArray ;              // the array that save the set ; 
        uint64_t arraySize ;                 // the array above's maximum size ; 
        int n ;                         // the number of unit in the set ; 
        int hashNumber ;                // numbers of used HashFunctions ; 
        uint64_t hash1(uint64_t * hashResult) const;       // hash Functions , maximum is 5
        uint64_t hash2(uint64_t * hashResult) const; 
        uint64_t hash3(uint64_t * hashResult) const; 
        uint64_t hash4(uint64_t * hashResult) const; 
        uint64_t hash5(uint64_t * hashResult) const;
    public:
        BloomFilter(uint64_t iArraySize , int hashSize) ;                // Constructor , input the arraySize and hashSize ; 
        int insert(uint64_t key) ;                                  // Insert a new key into the set . it will return -1 if error
        int isInclude(uint64_t key) ;                               // check if the key is in the set . it may be not correct someTimes.
        // Use extern array . and judge if a key is in the filter according to the array
        bool isInclude(char * array , uint64_t key) const ; 
        // reset the BF to the start status
        void reset () ; 
        // get the array's pointer
        char* access() ; 
} ; 

#endif