#pragma once 
#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <stdint.h>
#include <list>
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
    public :
        DiskTableManager(const std::string &dir, const std::string &vlog , BloomFilter * bf) ; 
        ~DiskTableManager() ; 
        // output when memory full 
        void write(char * sstable , splayArray * vlog, int mmSize) ; 
        // scan all the sstable and return the list in the para
        void scan(std::list<std::pair<uint64_t, std::string>> &list) ; 
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

};

