#pragma once
#include <string.h>
#include <bitset>

#include "kvstore_api.h"
#include "skipList.h"
#include "BloomFilter.h"
#include "utils.h"
#include "splayArray.h"
#include "DiskTableManager.h"
#include "Constants.h"
#include "Cache.h"
// get the memtable's current size ;
// #define DEBUG

class KVStore : public KVStoreAPI
{
	// You can add your implementation here
private:
	// The memTable
	skiplist::skiplist_type * memtable ; 						
	// the function for saveMem to write into sstable and vlog in memory 
	void writeTables(char * sstable , splayArray * vlog) const; 
		// the function for saveMem to write into the disk;
	void outputTables(char * sstable , splayArray * vlog , FILE * F_sstable , FILE * F_vlog) const ; 
	// the function to insert a key in the sstable 
	void writeSS(char * sstable, int index , uint64_t key ,  uint64_t biase , uint32_t len ) const ; 
	// The Number of Cached sstable
	int cached ;
	// initialize the tail and head of the vlog
	void initVlog() ; 
	// cache the sstable
	void loadSStable(const std::string & name) ; 
	// the bloom filter of the memtable ; it will be used by outside array to judge as well
	BloomFilter * bloomFilter ; 
	// the vlog's file descriptor
	FILE * vlogFile ;
	// the files that had been loaded in the cache 
	std::vector<std::string> cachedSS ; 
	// the disk managers 
	DiskTableManager * diskManager ; 
	
public:
	KVStore(const std::string &dir, const std::string &vlog);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;

	void gc(uint64_t chunk_size) override;
 // Push the MemTable into the ssTable and vlog ; the naming rules : use max and min key to name ; return the name of sstable
	void saveMem() const ;

//	initailize the cache until there is no sstable/cache full 
	void initCache(const std::string & dir , const std::vector<std::string>& sstables) ;

	// print the status of memTable
	void PRINT_STATUS() ; 
};
