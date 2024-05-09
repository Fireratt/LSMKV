#include "kvstore.h"
#include <string.h>
#include <iostream>
#include "tools.h"
#define empty(x) (strcmp(x,"")==0)
int main()
{
    KVStore testMemTable("TEST1","TEST1_VLOG") ; 
    testMemTable.reset() ; 
    char * buf = (char *)malloc(BUFFER_SIZE) ; 
    int max = 1024 ; 
    for(int i = 0 ; i < max ; i++)
    {
        // printf("inserting::%d\n" , i) ; 
        // fflush(stdout)  ; 
        sprintf(buf,"SIGNAL%d",i) ; 
        testMemTable.put(i,buf) ; 
    }

    
		// Test deletions
		for (int i = 0; i < max; i += 2)
		{
			// EXPECT(true, store.del(i));
            printf("Delete even Number(all should true) : %d\n" , testMemTable.del(i)) ; 
		}

		for (int i = 0; i < max; ++i)
        {
            printf("test all Number(i=%d) : %s\n" ,i,testMemTable.get(i).c_str()) ; 
        }
			// EXPECT((i & 1) ? std::string(i + 1, 's') : not_found,
			// 	   store.get(i));

		for (int i = 0; i < max; ++i)
        {
            printf("test all Number(i=%d) : %d\n" ,i,testMemTable.del(i)) ; 
        }
			// EXPECT(i & 1, store.del(i));

    return 0 ; 
}