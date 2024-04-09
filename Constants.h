#pragma once
constexpr char * DELETE_SIGN = "~DELETED~" ; 
constexpr int keySize = 20; 					// unit : byte . It save per key 's size in the sstable
constexpr int bloomSize = 8192 ; 			// unit : byte . save the bloomFilter's size 
constexpr int headSize = 32 ; 
constexpr int MAX_SIZE = 8192*2 ;
constexpr char * LEVEL_NAME = "level-" ; 
constexpr char MAGIC = 0xff ; 
constexpr int BUFFER_SIZE = 512 ; 
constexpr int CACHE_SIZE = 20 ; 
#define SIZE(n) (keySize * (n) + bloomSize + headSize)
#define STR_EQUAL(x,y) (strcmp((x),(y))==0)
// input the index of keyi(start from 0) and the offset of the head; return the true INDEX in the array(from start point)
#define KEY_INDEX(n,offset) (n)*keySize + (offset)