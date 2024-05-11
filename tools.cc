#include "tools.h"
void PRINT_LIST(const std::list<std::pair<uint64_t, std::string>>& toPrint)
{
    auto head = toPrint.begin () ; 
    auto tail = toPrint.end() ; 
    while(head!=tail)
    {
        std::cout << "#" ; 
        std::cout << head->first ; 
        std::cout << head->second ; 
        std::cout << "#" << "\n";
        head++ ; 
    }

}

bool isOverlap(int min1 , int max1 , int min2 , int max2)
{
    return !((min1 > max2)||(min2>max1)) ; 
}

void STR_DeepCopy(char * buffer , std::string & ret)
{
    std::string tem(buffer) ; 
    ret = tem ; 
    char placeHolder = ret[0] ; 
    ret[0] = ' ' ; 
    ret[0] = placeHolder ;
}