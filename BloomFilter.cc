#include "BloomFilter.h"
BloomFilter::BloomFilter(uint64_t iArraySize , int hashSize)
:arraySize(iArraySize) , 
hashNumber(hashSize)
{
    hashArray = (char *)malloc(arraySize) ; 
    // set the array's default value to 0 
    for(uint64_t i = 0 ; i < arraySize ; i++)
    {
        hashArray[i] = 0 ; 
    }
}
void BloomFilter::reset()
{
    // set value to zero
    for(uint64_t i = 0 ; i < arraySize ; i++)
    {
        hashArray[i] = 0 ; 
    }
}
int BloomFilter::insert(uint64_t key)
{
    uint64_t hashResult[2] = {0} ;                  // to call the MurmurHash3
    MurmurHash3_x64_128(&key, sizeof(key) , 1 , hashResult) ; 
    #ifdef DEBUG
        printf("\nDEBUG:: HashResults: \n") ; 
        printf("HashResults[0]: %llu  HIGH:%u LOW:%u\n" , hashResult[0] , HIGH(hashResult[0]),LOW(hashResult[0])) ; 
        printf("HashResults[1]: %llu  HIGH:%u LOW:%u\n" , hashResult[1] , HIGH(hashResult[1]),LOW(hashResult[1])) ; 
    #endif
    hashArray[hash1(hashResult)] = 1 ; 
    if(hashNumber > 1)
        hashArray[hash2(hashResult)] = 1 ; 
    if(hashNumber > 2)
        hashArray[hash3(hashResult)] = 1 ; 
    if(hashNumber > 3)
        hashArray[hash4(hashResult)] = 1 ; 
    if(hashNumber > 4)
        hashArray[hash5(hashResult)] = 1 ; 

    return 1 ; 
}

int BloomFilter::isInclude(uint64_t key)
{
    uint64_t hashResult[2] = {0} ;                  // to call the MurmurHash3
    MurmurHash3_x64_128(&key, sizeof(key) , 1 , hashResult) ; 
    int ret = 0 ; 
    if(!hashArray[hash1(hashResult)]) 
    {
        return 0 ; 
    }
    ret++ ; 
    if(hashNumber > 1)
        if(!hashArray[hash2(hashResult)])
        {
            return 0 ; 
        }
    ret++ ; 
    if(hashNumber > 2)
        if(!hashArray[hash3(hashResult)])
        {
            return 0 ; 
        }
    ret++ ; 
    if(hashNumber > 3)
        if(!hashArray[hash4(hashResult)])
        {
            return 0 ; 
        }
    ret++ ; 
    if(hashNumber > 4)
        if(!hashArray[hash5(hashResult)])
        {
            return 0 ; 
        }
    ret++ ; 
    return 1 ; 

}

uint64_t BloomFilter::hash1(uint64_t * hashResult) const
{
    // only use the whole hashResult[0]
    return hashResult[0] % arraySize ; 
}

uint64_t BloomFilter::hash5(uint64_t * hashResult) const
{
    // only use the whole hashResult[1]
    return HIGH(hashResult[1]) % arraySize ; 
}

uint64_t BloomFilter::hash3(uint64_t * hashResult) const
{
    // only use the hashResult[0]'s high 32 bit 
    return HIGH(hashResult[0]) % arraySize ; 
}

uint64_t BloomFilter::hash2(uint64_t * hashResult) const
{
    // only use the hashResult[0]'s low 32 bit 
    return LOW(hashResult[0]) % arraySize ; 
}

uint64_t BloomFilter::hash4(uint64_t * hashResult) const
{
    // only use the hashResult[1]'s low 32 bit 
    return LOW(hashResult[1]) % arraySize ; 
}

bool BloomFilter::isInclude(char * array , uint64_t key) const
{
    uint64_t hashResult[2] = {0} ;                  // to call the MurmurHash3
    MurmurHash3_x64_128(&key, sizeof(key) , 1 , hashResult) ; 
    int ret = 0 ; 
    if(!array[hash1(hashResult)]) 
    {
        return 0 ; 
    }
    ret++ ; 
    if(hashNumber > 1)
        if(!array[hash2(hashResult)])
        {
            return 0 ; 
        }
    ret++ ; 
    if(hashNumber > 2)
        if(!array[hash3(hashResult)])
        {
            return 0 ; 
        }
    ret++ ; 
    if(hashNumber > 3)
        if(!array[hash4(hashResult)])
        {
            return 0 ; 
        }
    ret++ ; 
    if(hashNumber > 4)
        if(!array[hash5(hashResult)])
        {
            return 0 ; 
        }
    ret++ ; 
    return 1 ; 

}

char * BloomFilter::access()
{
    return hashArray ; 
}