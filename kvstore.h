#pragma once
#include <string.h>
#include <bitset>

#include "kvstore_api.h"
#include "skipList.h"
#include "BloomFilter.h"
#include "utils.h"
#include "splayArray.h"
#include "Constants.h"
#include "Cache.h"
// get the memtable's current size ;
#define DEBUG

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
	// The tail of Vlog ; unit : byte
	int tail ;
	// The head of vlog (the size of the file)
	int head ; 
	// The directory of sstable and the name of vlog
	const std::string sstableDir ;
	const std::string vlog ; 
	// The Number of Cached sstable
	int cached ;
	// initialize the tail and head of the vlog
	void initVlog() ; 
	// cache the sstable
	void loadSStable(const std::string & name) ; 
	// the sstable's cache
	Cache * cache ; 
	// the bloom filter of the memtable ; it will be used by outside array to judge as well
	BloomFilter * bloomFilter ; 
	// the vlog's file descriptor
	FILE * vlogFile ;

	
public:
	KVStore(const std::string &dir, const std::string &vlog);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;

	void gc(uint64_t chunk_size) override;
 // Push the MemTable into the ssTable and vlog ; the naming rules : use max and min key to name ; 
	void saveMem() const ;

//	initailize the cache until there is no sstable/cache full 
	void initCache(const std::string & dir , const std::vector<std::string>& sstables) ;
};
