#pragma once
#include <cmath>
#include <list>
#include <vector>
#include <string>
#include <stdint.h>
#include <iostream>
void PRINT_LIST(const std::list<std::pair<uint64_t, std::string>>& toPrint) ; 
// deleted
// void GEN_CRC_VEC(std::vector<unsigned char> & crc , uint64_t key , uint32_t vlen , )
// judge if a range is overlap another range
bool isOverlap(int max1 , int min1 , int max2 , int min2) ; 
// utilize the write copy 
void STR_DeepCopy(char * buffer , std::string & ret) ; 