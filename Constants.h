#pragma once
#include <stdint.h>
constexpr char * DELETE_SIGN = "~DELETED~" ; 
constexpr int keySize = 20; 					// unit : byte . It save per key 's size in the sstable
constexpr int bloomSize = 8192 ; 			// unit : byte . save the bloomFilter's size 
constexpr int headSize = 32 ; 
constexpr int FRONT_OFFSET = headSize + bloomSize ; // the sstable's front
constexpr int MAX_SIZE = 8192*2 ;
constexpr int KEY_MAXNUM = ((MAX_SIZE - FRONT_OFFSET) / keySize) ; 
constexpr char * LEVEL_NAME = "level-" ; 
constexpr char MAGIC = 0xff ; 
constexpr int BUFFER_SIZE = 1<<17 ; 
constexpr int CACHE_SIZE = 20 ; 
constexpr int VLOG_KEY_LOC = 3 ; 
constexpr int VLOG_VLEN_LOC = 11 ; 
constexpr int VLOG_HEAD = 15 ; 
constexpr int VLEN_LOC = 16 ; 
constexpr int OFFSET_LOC = 8 ; 
constexpr int KEYNUM_LOC = 8 ; 
#define SIZE(n) (keySize * (n) + bloomSize + headSize)
#define STR_EQUAL(x,y) (strcmp((x),(y))==0)
// input the index of keyi(start from 0) and the offset of the head; return the true INDEX in the array(from start point)
#define KEY_INDEX(n,offset) ((n)*keySize + (offset))
#define KEY_ADDR(index , cacheLine) ((char*)(cacheLine) + (index) * keySize + FRONT_OFFSET)
// diskTable will use
constexpr int MAX_LEVEL = 30 ; 
#define GETMIN(sstable) (((int64_t*)(sstable))[2])
#define GETMAX(sstable) (((int64_t*)(sstable))[3])
#define GETTIME(sstable) (((int64_t*)(sstable))[0])
#define GET_LEVEL_MAXNUM(level) (2<<level) 
#define SETTIME(sstable , timeStamp) (((int64_t*)(sstable))[0] = timeStamp) 
#define SETMIN(sstable , min) (((int64_t*)(sstable))[2] = min) 
#define SETMAX(sstable , max) (((int64_t*)(sstable))[3] = max) 
#define SETKEYNUM(sstable , num) (((int64_t*)(sstable))[1] = num) 
// the cache will use 
#define GET_VLEN(startKey) (*(int*)((char*)(startKey) + VLEN_LOC))
#define GET_OFFSET(startKey) (*(uint64_t*)((char*)(startKey) + OFFSET_LOC))
#define GET_KEY(startKey) (*(uint64_t*)((char*)(startKey) + 0))
#define GET_KEY_NUM(cacheLine) (*(uint64_t*)((char*)(cacheLine) + KEYNUM_LOC))
#define NEXT_KEY(keyOffset) ((keyOffset)+keySize)