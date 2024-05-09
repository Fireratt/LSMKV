#include "kvstore.h"
#include <string.h>
#include <iostream>
#include "tools.h"
#define empty(x) (strcmp(x,"")==0)
int main()
{
    KVStore testMemTable("./TEST1","./TEST1/TEST1_VLOG") ; 
    char * buf = (char *)malloc(BUFFER_SIZE) ; 
    // printf("GET 407?:%s",testMemTable.get(407).c_str()) ; 
    //     for(int i = 0 ; i < 512 ; i++)
    // {
    //     // printf("inserting::%d\n" , i) ; 
    //     // fflush(stdout)  ; 
    //     sprintf(buf,"%d",i) ; 
    //     testMemTable.put(i,std::string(buf)) ; 
    // }
    for(int i = 0 ; i < 512 ; i++)
    {
        // printf("inserting::%d\n" , i) ; 
        // fflush(stdout)  ; 
        printf("Main:Reading:%s\n" , testMemTable.get(i).c_str()) ; 
    }
    // testMemTable.reset() ; 
    return 0 ; 
}