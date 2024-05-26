#include "Cache.h"
Cache::Cache(int unitSize , BloomFilter * bf):unitSize(unitSize) , bf(bf)
{
    memory.reserve(100) ; 
    levelIndex.reserve(100) ; 
    loaded = 0 ;
}

void Cache::loadCache(FILE* toLoad , int level){ 
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
    levelIndex.push_back(level) ; 
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
        free(posList) ; 
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
                free(posList) ; 
                return -1 ; 
            }                                           
            else
            {
                free(posList) ; 
                return pos ; 
            }
        }
    }
    free(posList) ; 
    return -1 ;
}

int Cache::searchIndex(uint64_t key , int index)
{
    char * cash = memory[index] ; 
    int offset = headSize + bloomSize ; 
    // load the Size from the header
    uint64_t cacheSize = ((int64_t *)cash)[1] ; 
    uint64_t min = ((uint64_t *)cash)[2] ; 
    uint64_t max = ((uint64_t *)cash)[3] ; 

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
    // free the cacheLine in the list 
    auto begin = memory.begin() ;
    auto end = memory.end() ; 
    for(; begin != end ; begin++)
    {
        free(*begin) ; 
    }
    memory.clear() ; 
    levelIndex.clear() ; 

}

int Cache::arrive(int index , uint64_t key)
{
    char * cacheLine = memory[index] ; 
    int64_t keyNumber = GET_KEY_NUM(cacheLine) ; //Get the number of key in it
    int low = 0 ; 
    int mid = 0 ; 
    int high = keyNumber - 1 ; 
    int prevMid = 0;                   // record the previous Mid to find the key near the wanted key
    while(low <= high)
    {
        mid = (low + high)/ 2 ;
        uint64_t midKey =GET_KEY(cacheLine +  KEY_INDEX(mid , FRONT_OFFSET) ) ; 
        if(key == midKey)
        {
            return KEY_INDEX(mid , FRONT_OFFSET) ; 
        } 
        else if (key > midKey)
        {
            prevMid = mid ; 
            low = mid + 1 ; 
        }
        else
        {
            high = mid - 1 ; 
        }
    }
    // return the key near the key . 
    return KEY_INDEX(prevMid , FRONT_OFFSET);
}

void Cache::scanPossibleLine(uint64_t key1 , uint64_t key2 , int * lineList)
{
    int i = 0 ; 
    for(int j = 0 ; j < loaded ; j++)
    {
        int max = getMax(j) ; 
        int min = getMin(j) ; 
        if(isOverlap(min , max , key1 , key2))
        {
            lineList[i] = j ; 
            i++ ; 
        }
    }
    // sort the lineList due to the decrease order 
    shellSort(lineList , i) ; 
    lineList[i] = -1 ; 
}

void Cache::shellSort(std::vector<int> toSort) 
{
    int i, j, temp, gap;
    int n = toSort.size() ; 
    // 初始间隔设定为数组长度的一半
    for (gap =  n/ 2; gap > 0; gap /= 2) {
        // 对每个间隔进行插入排序
        for (i = gap; i < n; i++) {
            temp = toSort[i];
            // 对当前间隔内的元素进行插入排序
            for (j = i; j >= gap && sort_timeStamp( temp , toSort[j - gap] ); j -= gap) {
                toSort[j] = toSort[j - gap];
            }
            toSort[j] = temp;
        }
    }
}

void Cache::scanLevels(int level , std::vector<int>& sstables) const
{
    for(int i = 0 ; i < loaded ; i++)
    {
        if(levelIndex[i] == level)
        {
            sstables.push_back(i) ; 
        }
    }
}

void Cache::getRange(const std::vector<int>& sstables , uint64_t&  left , uint64_t&  right) const
{
    int length = sstables.size() ; 
    left = UINT64_MAX ; 
    right = 0 ; 
    for(int i = 0 ; i < length ; i++)
    {
        int index = sstables[i] ; 
        uint64_t max = getMax(index) ; 
        uint64_t min = getMin(index) ; 
        if(max > right)
        {
            right = max ; 
        }
        if(min < left)
        {
            left = min ; 
        }
    }
}

void Cache::getOverlap(uint64_t left , uint64_t right ,const std::vector<int>& sstables , std::vector<int>& overlap) const 
{
    int length = sstables.size() ;
    for(int i = 0 ; i < length ; i++)
    {
        int max = getMax(sstables[i]) ;
        int min = getMin(sstables[i]) ; 

        if(isOverlap(right , left , max , min))
        {
            overlap.push_back(sstables[i]) ; 
        }
    }
}

void Cache::evictTables(const std::vector<int>& sstables1 , const std::vector<int>& sstables2) 
{
    int length = sstables1.size() + sstables2.size() ;  // calculate the size that need to delete
    std::unordered_set<int> toDelete ;                  // record the index that need to be delete
    std::unordered_set<int> haveParsed ; 
    int length1 = sstables1.size() ; 
    int length2 = sstables2.size() ; 
    for(int i = 0 ; i < length1 ; i++)
    {
        toDelete.insert(sstables1[i]) ; 
    }

    for(int i = 0 ; i < length2 ; i++)
    {
        toDelete.insert(sstables2[i]) ; 
    }
    int size = memory.size() ;
    int hand = size - 1 ; 
    auto begin = toDelete.begin() ; 
    auto end = toDelete.end() ;
    while(hand >= size - length)
    {
        if(toDelete.find(hand) == end || haveParsed.find(hand) != haveParsed.end())          // dont have the hand in toDelete , can do the swap
        {
            char * tem = memory[hand] ; 
            int tem2 = levelIndex[hand] ; 
            memory[hand] = memory[*begin] ; 
            memory[*begin] = tem ; 
            levelIndex[hand] = levelIndex[*begin] ; 
            levelIndex[*begin] = tem2 ; 
            haveParsed.insert(*begin) ; 
            begin++ ;
        }
        else if(*begin == hand)
        {
            begin ++ ;
        }
        // have the hand , and not the current begin . make the *begin the same , move the hand to find the insert location ; 
        hand -- ;
        #ifdef GC_DEBUG
            if(sstables1[0] == 33)
            {
                PRINT_STATUS() ; 
            }
        #endif
    }
    // all the evict line at the end , pop them 
    memory.erase(memory.end() - length , memory.end()) ; 
    levelIndex.erase(levelIndex.end() - length , levelIndex.end()) ; 
    loaded -= length ; 
}   

void Cache::loadCache(char * toLoad , int level)
{
    int keyNum = GET_KEY_NUM(toLoad) ; 
    int totalLength = SIZE(keyNum) ; 

    char * newLine = (char *)malloc(totalLength) ; 
    memcpy(newLine , toLoad , totalLength) ; 
    memory.push_back(newLine) ;
    levelIndex.push_back(level) ; 
    #ifdef DEBUG
        printf("loadCache from line , level:%d\n", level) ; 
        PRINT_STATUS() ; 
    #endif
    loaded++ ; 
}

char * Cache::getKey(int keyIndex , int cacheLineIndex ) 
{
    char * cacheLine = access(cacheLineIndex) ;
    int keyNum = GET_KEY_NUM(cacheLine) ; 

    if(keyIndex < 0 || keyIndex >= keyNum)  // invalid index
    {
        return 0 ; 
    }

    return KEY_ADDR(keyIndex , cacheLine) ; 
}

void Cache::PRINT_STATUS()
{
    int length = memory.size() ; 
    printf("========== START PRINT CACHE STATE ==========\n") ; 
    printf("total Length:%d level Length:%d\n" , length , levelIndex.size()) ; 
    for(int i = 0 ; i < length ; i++)
    {
        printf("cacheLine%d : timeStamp = %lu , level = %d\n" , i , getTimeStamp(i) , levelIndex[i]) ;
    }
    printf("========== Cache State Print Finish ==========\n") ; 
}