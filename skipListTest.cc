#include "kvstore.h"
#include <string.h>
#include <iostream>
#include "tools.h"
#define empty(x) (strcmp(x,"")==0)
int main()
{
    KVStore testMemTable("TEST1","TEST1_VLOG") ; 
    for(int i = 0 ; i < 1024 ; i++)
    {
        // printf("inserting::%d\n" , i) ; 
        // fflush(stdout)  ; 
        testMemTable.put(i,"TESTSIGNAL") ; 
    }
    return 0 ; 
}