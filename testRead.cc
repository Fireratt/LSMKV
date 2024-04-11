#include "kvstore.h"
#include <string.h>
#include <iostream>
#include "tools.h"
#define empty(x) (strcmp(x,"")==0)
int main()
{
    KVStore testMemTable("TESTREAD","TESTREAD/TESTREAD_VLOG") ; 
        for(int i = 0 ; i < 512 ; i++)
    {
        // printf("inserting::%d\n" , i) ; 
        // fflush(stdout)  ; 
        testMemTable.put(i,std::string(i + 1, 's')) ; 
    }
    for(int i = 0 ; i < 512 ; i++)
    {
        // printf("inserting::%d\n" , i) ; 
        // fflush(stdout)  ; 
        printf("Main:Reading:%s\n" , testMemTable.get(i).c_str()) ; 
    }
    testMemTable.reset() ; 
    return 0 ; 
}