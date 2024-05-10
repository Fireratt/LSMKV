#include "Cache.h"
Cache::Cache(int unitSize , BloomFilter * bf):unitSize(unitSize) , bf(bf)
{
    memory.reserve(100) ; 
    loaded = 0 ;
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
        printf("filelength:%d\n",len) ; 
        throw "DETECT SSTABLE SIZE OVERFLOW!\n Loading Cache END" ; 
    }

    rewind(toLoad) ; 
    char * newCache = (char *)malloc(MAX_SIZE) ; 
    fread(newCache,len,1,toLoad) ; 
    memory.push_back(newCache) ; 
    loaded ++ ; 
    return ; 
}

uint64_t Cache::getMin(int index) const 
{
    return ((uint64_t*)(memory[index]))[2] ; 
}

uint64_t Cache::getMax(int index) const 
{
    return ((uint64_t*)(memory[index]))[3] ; 
}

int Cache::getIndex(uint64_t key , int * indexList)
{

    // use binary search , regard the Cache as full 

    int size = 0 ;              // the list size

    for(int mid = 0 ; mid < loaded ; mid++)
    {
        #ifdef DEBUG
            // printf("Get Memory Index : key :%lu , max :%lu , min:%lu , current:%d\n" ,
            //  key , getMax(mid) ,getMin(mid) , mid ) ; 
        #endif
        if(getMax(mid)>= key && getMin(mid) <= key )
        // && bf->isInclude(memory[mid] + headSize , key))
        {
            indexList[size] = mid ; 
            size ++ ;
        }
    }
    // sort the indexList , make it ordered by the timestamp , bigger timestamp should at front 
    shellSort(indexList,size) ; 
    #ifdef DEBUG
        printf("Cache : GetIndex : Start Print indexList Result:") ; 
        for(int i = 0 ; i < size ; i++)
        {
            printf("timestamp:%lu , index :%d" , getTimeStamp(indexList[i]) , indexList[i]); 
        }
        printf("\n") ; 
    #endif
    indexList[size] = -1 ;              // maximum size is loaded ; so need to initialzie indexList to loaded + 1
    return size > 0 ;  
}
int Cache::access(uint64_t key , const BloomFilter *judger, int & offset)
{
    // record the posible index
    int * posList = (int *)malloc(sizeof(int)*(loaded+1)) ; 

    if(!getIndex(key,posList))
    {
        #ifdef DEBUG
            printf("Cant find the key when access!\n")  ; 
        #endif 
        return -1 ; 
    }
    // update times

    for(int i = 0 ; i < loaded ; i++)
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
            offset = searchIndex(key,pos) ;             // find the location for the key 
            if(offset == -1)
            {
                continue ; 
            }
            int vlen = GET_VLEN( cash + offset) ;       // read the location's vlen ; if it is a 0 ; means deleted
            if(vlen == 0)
            {
                return -1 ; 
            }                                           
            else
            {
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
        if(((uint64_t*)(cash+KEY_INDEX(tail,offset)))[0] == key)// our head is mid + 1 and round below ; it will make the tail cant find 
        {
            int index = KEY_INDEX(tail,offset) ; 
            return index ; 
        }
        if(midKey == key)
        {
            int index = KEY_INDEX(mid,offset) ; 

            return index ; 

        }
        if (midKey < key)
        {
            head = mid+1 ; 
        }
        else 
        {
            tail = mid; 
        }
    }
    return -1 ; 
}

char * Cache::access(int index)
{
    return memory[index] ; 
}

char * Cache::bottom() const 
{
    return memory[loaded-1] ; 
}

int Cache::size() const 
{
    return loaded ; 
}

uint64_t Cache::getTimeStamp(int index) const
{
    return *(uint64_t *)memory[index] ; 
}

bool Cache::sort_timeStamp(int index1 , int index2)
{
    return getTimeStamp(index1) > getTimeStamp(index2) ; 
}

void Cache::shellSort(int arr[], int n) {
    int i, j, temp, gap;
 
    // 初始间隔设定为数组长度的一半
    for (gap = n / 2; gap > 0; gap /= 2) {
        // 对每个间隔进行插入排序
        for (i = gap; i < n; i++) {
            temp = arr[i];
            // 对当前间隔内的元素进行插入排序
            for (j = i; j >= gap && sort_timeStamp( temp , arr[j - gap] ); j -= gap) {
                arr[j] = arr[j - gap];
            }
            arr[j] = temp;
        }
    }
}

void Cache::reset() 
{
    loaded = 0 ; 
    memory.clear() ; 
}