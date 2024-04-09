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