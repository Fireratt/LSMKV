#include "splayArray.h"

splayArray::splayArray()
{
    data = (char*)malloc(DEFAULT_LENGTH) ; 
    maxSize = 20 ; 
    n = 0 ; 
}

void splayArray::splay()
{
    char * tem = (char *) malloc(2*maxSize) ; 
    memcpy(tem,data,maxSize) ; 

    free(data) ;
    data = tem ; 
    maxSize = 2 * maxSize ; 
}

char * splayArray::access()
{
    return data ; 
}

void splayArray::push(uint64_t num)
{
    int newSize = n+8 ; 
    if(newSize > maxSize)
        splay() ; 
    uint64_t* data_64 = (uint64_t*)(data+n) ; 
    data_64[0] = num ; 
    n = newSize ; 
}
void splayArray::push(uint32_t num)
{
    int newSize = n+4 ; 
    if(newSize > maxSize)
        splay() ; 
    uint32_t* data_32 = (uint32_t*)(data+n) ; 
    data_32[0] = num ; 
    n = newSize ; 
}
void splayArray::push(uint16_t num)
{
    int newSize = n+2 ; 
    if(newSize > maxSize)
        splay() ; 
    uint16_t* data_16 = (uint16_t*)(data+n) ; 
    data_16[0] = num ; 
    n = newSize ; 
}
void splayArray::push(char num)
{
    int newSize = n+1 ; 
    if(newSize > maxSize)
        splay() ; 
    data[n] = num ; 
    n = newSize ; 
}
char splayArray::access(int index)
{
    if(index < maxSize)
    {
        return data[index] ; 
    }
    return 0xff ; 
}

void splayArray::PRINT_STATUS()
{
    printf("##### Start Printing splayArray Status #####\n") ;  

    printf("n? : %d   ;   maxSize?: %d\n" ,n , maxSize) ; 

    printf("Array Content(16): \n") ; 

    for(int i = 0 ; i < n ; i++)
    {
        printf("%2x ", data[i]) ; 
    }

    printf("##### Print End #####") ; 
}
void splayArray::append( const std::string & str)
{
    int len = str.length() ; 

    int newSize = n+len ; 
    while(newSize > maxSize)
    {
        splay() ; 
    }
    strncpy(data+n , str.c_str() , len) ; 
    n = newSize ; 

}
splayArray::~splayArray()
{
    free(data) ; 
}

void splayArray::reset()
{
    free(data) ;
    data = (char*)malloc(DEFAULT_LENGTH) ; 
    maxSize = 20 ; 
    n = 0 ; 
}

uint64_t splayArray::size()
{
    return n ; 
}