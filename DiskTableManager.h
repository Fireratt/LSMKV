#pragma once 
#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <stdint.h>
#include <list>
#include <unordered_set>				// hashMap , used to save the scaned key (Find is the fatal factor)
#include "Constants.h"
#include "Cache.h"
#include "utils.h"
#include "BloomFilter.h"
#include "splayArray.h"
// #define DEBUG
// this class was used to manage the connection between disk and memory's jumptable
// it will be used as a proxy for memory to communicate with the cache as well 
class DiskTableManager
{
    private :
        

        // the sstable's cache
	    Cache * cache ; 
        std::string dir ;
	    const std::string vlog ; 
        // each save a set of file name
        std::vector<std::string> levels[MAX_LEVEL] ; 
        // put a bloomfilter pointer at the outside bf ; used to get .
        BloomFilter * bf ;

        // initialize the tail and head of the vlog
        void initVlog() ; 
        // The tail of Vlog ; unit : byte
        int tail ;
        // The head of vlog (the size of the file)
        int head ; 
        // The Number of Cached sstable
        int cached ;
        // the vlog's file descriptor
        FILE * vlogFile ;

        // the current , biggest TimeStamp ; 
        int timeStamp ; 
    public :
        DiskTableManager(const std::string &dir, const std::string &vlog , BloomFilter * bf) ; 
        ~DiskTableManager() ; 
        // output when memory full 
        void write(char * sstable , splayArray * vlog, int mmSize) ; 
        // scan all the sstable and return the list in the para
        void scan(uint64_t key1 , uint64_t key2 , std::list<std::pair<uint64_t, std::string>> &list
        , std::unordered_set<uint64_t>& scaned) ; 
        // get request from kvstore's memory section
        std::string get(uint64_t key) ; 
        //	initailize the cache until there is no sstable/cache full 
	    void initCache(const std::string & dir , const std::vector<std::string>& sstables) ;
        // delete the store
        void reset() ; 
        // check the directory , locate all the sstable in the disk , cache them in the cache if possible
        void scanDisk() ; 
        // insert new sstable name in the level vector
        void insertSS(int index , std::string& sstableName) ; 
        
        // get the tail and head for the vlog 
        int getVlogTail() ; 
        int getVlogHead() ; 
        // get the next timeStamp
        int getNextTimeStamp() ; 
        // get the val use the cacheIndex and offset in the cacheline 
        std::string getValByIndex(int cacheIndex , int offset) ; 
        // get the val use the cache line header 
        std::string getValByCacheLine(char * cacheLine, int offset) ; 
        // do the compaction for level i 
        void compaction(int level) ; 
        // destroy the merged sstable
        void destroySStable(int level , const std::vector<int>& first , const std::vector<int>& second) ; 
        // form the name from a sstable and a level
        void formName(const char * sstable , char * name) ; 
        // form the full address by the level and fileName 
        void formAddress(const char * fileName , int level , char * addr) ; 
        // merge 2 vector's cacheLine 
        void merge(int insertLevel , const std::vector<int>& first , const std::vector<int>& second) ; 
        // generate a singleline from the merge . Hands and results should be malloced previously
        int generateLine(const std::vector<int>& first , const std::vector<int>& second , int * hands , char * result) ;
        // write a cacheline to the specific level
        void writeLineToDisk(int level , char * singleLine) ; 
        // get the cache
        Cache*  getCache() ; 
};

