#include "Cache.h"
Cache::Cache(int n , int unitSize):n(n) , unitSize(unitSize)
{
    memory = (char **)malloc(sizeof(char**)*n) ; 
    LRU_TIME = (int*)malloc(sizeof(int)*n) ; 
    for(int i = 0 ; i < n ; i++)
    {
        memory[i] = 0 ; 
        // Not initialize ; time marked to be max 
        LRU_TIME[i] = INT32_MAX  ; 
    }
}

void Cache::loadCache(FILE* toLoad)
{
    if(!toLoad)
    {
        printf("ERROR:: Cant Read the FILE !\n") ; 
        return ; 
    }
    fseek(toLoad,0,SEEK_END) ; 
    int len = ftell(toLoad) ; 
    if(len > unitSize)                              // should not occur when the sstable's writing is normally
    {
        throw "DETECT SSTABLE SIZE OVERFLOW!\n Loading Cache END" ; 
    }

    rewind(toLoad) ; 
    int loc = getReplaceIndex() ; 
        // #ifdef DEBUG
		// 	printf("DEBUG:: Cache index:%d\n",loc) ; 
		// #endif
    if(!memory[loc])
    {
        memory[loc] = (char *)malloc(MAX_SIZE) ; 
    }
    fread(memory[loc],len,1,toLoad) ; 

    // reset the access times
    LRU_TIME[loc] = 0 ; 
    // update loaded
    if(loaded < n)
        loaded ++ ; 
    increaseTime() ; 
    return ; 
}

void Cache::increaseTime()
{
    for(int i = 0 ; i < loaded ; i++)
    {
        if(LRU_TIME[i] != INT32_MAX)
            LRU_TIME[i]++ ; 
    }
}
int Cache::getReplaceIndex()
{
    int max = -1 ; 
    int index = 0 ; 
    for(int i = 0 ; i < n ; i++)
    {
        int tem = LRU_TIME[i] ; 
        if(tem>max)
        {
            max = tem ; 
            index = i ; 
        }
    }
    return index ; 
}

uint64_t Cache::getMin(int index)
{
    return ((uint64_t*)(memory[index]))[2] ; 
}

uint64_t Cache::getMax(int index)
{
    return ((uint64_t*)(memory[index]))[3] ; 
}

int Cache::getIndex(uint64_t key , int * indexList)
{

    // use binary search , regard the Cache as full 

    int size = 0 ;              // the list size

    for(int mid = 0 ; mid < loaded ; mid++)
    {
        if(getMax(mid)>= key && getMin(mid) <= key )
        {
            indexList[size] = mid ; 
            size ++ ;
        }
    }
    if(size < n)
    {
        indexList[size] = -1 ; 
    }
    return size > 0 ;  
}

bool Cache::isfull()
{
    return n == loaded ; 
}

int Cache::access(uint64_t key , const BloomFilter *judger, int & offset)
{
    // record the posible index
    int * posList = (int *)malloc(sizeof(int)*n) ; 

    if(!getIndex(key,posList))
    {
        #ifdef DEBUG
            printf("Cant find the key when access!\n")  ; 
        #endif 
        return -1 ; 
    }
    // update times

    for(int i = 0 ; i < n ; i++)
    {
        // read the end tag
        if(posList[i]==-1)
        {
            break ;
        }
        int pos = posList[i] ; 
        char * cash = memory[pos] ; 
        // may in it 
        if(judger->isInclude(cash+headSize,key))
        {
            
            offset = searchIndex(key,pos) ; 

            if(offset == -1)
            {
                continue ; 
            }
            else
            {
                LRU_TIME[pos] = 0 ; 
                increaseTime() ; 
                #ifdef DEBUG
                    printf("DEBUG:: Cache::Access:pos= %d offset = %d \n" , pos , offset) ; 
                #endif
                return pos ; 
            }
        }
    }  
    return -1 ;
}

int Cache::searchIndex(uint64_t key , int index)
{
    char * cash = memory[index] ; 
    int offset = headSize + bloomSize ; 
    // load the Size from the header
    uint64_t cacheSize = ((int64_t *)cash)[1] ; 
    uint64_t min = ((int64_t *)cash)[2] ; 
    uint64_t max = ((int64_t *)cash)[3] ; 

    int head = 0 ; 
    int tail = cacheSize-1 ;                // last key
    int mid ;
    while(head < tail)
    {
        mid = (head + tail )/2 ; 
        uint64_t midKey = ((uint64_t*)(cash+KEY_INDEX(mid,offset)))[0] ; 
                #ifdef DEBUG
                printf("DEBUG:: Cache::SearchIndex:midKey = %lu mid = %d retVal = %d\n"
                ,midKey , mid , KEY_INDEX(mid,offset)) ; 
                            #endif

        if(midKey == key)
        {

            return KEY_INDEX(mid,offset) ; 

        }
        if (midKey < key)
        {
            head = mid+1 ; 
        }
        else 
        {
            tail = mid ; 
        }
    }
    return -1 ; 
}

char * Cache::access(int index)
{
    return memory[index] ; 
}