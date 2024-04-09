#include "kvstore.h"
KVStore::KVStore(const std::string &dir, const std::string &vlog) : KVStoreAPI(dir, vlog),sstableDir(dir) , vlog(vlog),cached(0)
{
	// initialize memTable
	memtable = new skiplist::skiplist_type(0.5) ; 
	cache = new Cache(CACHE_SIZE,MAX_SIZE);
	bloomFilter = new BloomFilter(bloomSize,4);
	if(utils::dirExists(dir))
	{
		// read the directory and restart system
		tail = 0 ; 
		std::vector<std::string> ret ; 
		utils::scanDir(dir,ret) ; 
		// traverse the whole directory's name
		auto end = ret.end() ; 
		int vlogChecked = 0 ; 
		for(auto i = ret.begin() ; i != end ; i++)
		{
			if(*i!=vlog)
			{
				
				cached++ ; 
			}
		}
		initVlog() ; 
		initCache(ret) ;
		return ; 
	}
	utils::mkdir(dir) ;
}

KVStore::~KVStore()
{
	saveMem() ; 
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
	if(SIZE(memtable->size()+1)>MAX_SIZE)
	{
		saveMem() ; 
		#ifdef DEBUG
			// printf("LOG: CALL RESET\n") ; 
			// printf("LOG: Current SIZE?:%d n?:%d\n",SIZE(memtable->size()+1),memtable->size()) ; 
		#endif
		memtable->reset() ; 						// remove memtable 

	}
	memtable->put(key,s) ; 
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	const std::string& result = memtable->get(key) ; 
	if(result == "" || result == DELETE_SIGN)
		return "" ; 
	return result ; 
}	
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
	skiplist::skiplist_node * target = memtable->arrive(key) ; 
	if(target->key!=key || target->val == DELETE_SIGN)
	{
		return 0 ; 
	}
	target->updateValue(DELETE_SIGN) ; 
	return 1 ; 
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
	memtable->reset() ; 
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list)
{
	skiplist::skiplist_node * hand =  memtable->arrive(key1) ; 
	uint64_t key = hand->key ; 
	if(hand->key == key1)								// find key1 . 
	{
		list.push_back(std::pair<uint64_t, std::string>(key,hand->val)) ; 
	}
	hand = hand -> later ; 
	while(hand && hand->key<=key2)
	{
		list.push_back(std::pair<uint64_t, std::string>(hand->key,hand->val)) ; 
		hand = hand->later ; 
	}
}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size)
{
}

void KVStore::saveMem() const 
{
	std::string name = sstableDir ; 
	if(!utils::dirExists(name))
	{
		// dont exist so create
		utils::mkdir(name) ; 

	}
	char * sstable = (char*)(malloc(MAX_SIZE)) ; 	// it will be used as the int32* / int64* as well

	splayArray * vlog = new splayArray ; 
	char * buffer = (char *)malloc(BUFFER_SIZE) ; 
	sprintf(buffer,"%lu-%lu.sst",memtable->min()->key,memtable->max()->key) ; 
	FILE * F_sstable = fopen((name + "/" + buffer).c_str(),"w+") ; 
	FILE * F_vlog = fopen((this->vlog).c_str() , "wr+");  
	writeTables(sstable , vlog) ; 

	outputTables(sstable,vlog , F_sstable , F_vlog) ; 
// end

	free(sstable) ; 
	delete vlog ; 
	fclose(F_vlog) ; 
	fclose(F_sstable) ; 
}


void KVStore::writeTables(char * sstable , splayArray * vlog) const
{
	int n = memtable->size() ; 
	uint64_t * sstable_64 = (uint64_t *)sstable ; 
	uint32_t * sstable_32 =(uint32_t *) sstable ; 
	splayArray buffer ;

	skiplist::skiplist_node * firstEle = memtable->min() ; 	// use as a iterator
	skiplist::skiplist_node * tail = memtable->max()->later ; 	// use as a end signal
	// head 

	sstable_64[0] =  1 ; 
	sstable_64[1] = n ; 
	sstable_64[2] = firstEle->key ; 
	sstable_64[3] = tail->prev->key ; 
	int sstable_index = 0 ; 					// calculate the number of key . 
	while(firstEle!=tail)	
	{
					#ifdef DEBUG
		printf("DEBUG log :: inserting:%lu\n",firstEle->key) ; 
	#endif
		uint64_t key = firstEle->key ; 
		const std::string & val = firstEle->val ; 
		const uint32_t valLen = (uint32_t)(val.length()) ; 
		// calculate the biase in vlog
		uint64_t biase = vlog->size() ; 
		vlog->push(MAGIC) ; 
		// calculate CRC ; 
		buffer.push(key) ; 
		buffer.push(valLen) ; 
		buffer.append(val) ; 
		#ifdef DEBUG
	 		buffer.PRINT_STATUS() ; 
	 	#endif
		buffer.reset() ; 
		uint16_t crcCode = utils::crc16(buffer.access() , buffer.size()) ; 
		// insert the codes in vlog 
		vlog->push(crcCode) ; 
		vlog->push(key) ; 
		vlog->push(valLen) ; 
		vlog->append(val) ; 
		firstEle = firstEle->later ; 
		writeSS(sstable, sstable_index , key , biase , valLen) ; 
		sstable_index ++ ; 
	}

}

void KVStore::writeSS(char * sstable , int index , uint64_t key ,  uint64_t biase , uint32_t len) const
{

	int ssBiase = SIZE(index) ; 
	uint64_t * sstable_64 = (uint64_t*)(sstable + ssBiase) ; 
	uint32_t * sstable_32 = (uint32_t*)(sstable + ssBiase) ; 

	sstable_64[0] = key ; 
	sstable_64[1] = biase ; 
	sstable_32[4] = len ; 

}

void KVStore::outputTables(char * sstable , splayArray * vlog , FILE * F_sstable , FILE * F_vlog) const
{
	int sstableByte = SIZE(memtable->size()) ; 
	fwrite(sstable,sstableByte , 1 , F_sstable) ; 
	fwrite(vlog->access(),vlog->size(),1,F_vlog) ; 
}

void KVStore::initVlog()
{
	FILE * vlogFile = fopen(vlog.c_str(),"r") ; 
	fseek(vlogFile,0,SEEK_END) ; 
	head = ftell(vlogFile) ; 					// push the FILE to end and get the size 
	rewind(vlogFile) ; 
	
	char * buffer  = (char*)(malloc(BUFFER_SIZE)) ; 
	char magic = fgetc(vlogFile) ; 
	while(magic!=EOF)
	{
		if(magic==0xff)
		{
			tail = ftell(vlogFile) ; 
			break ; 
		}
		magic = fgetc(vlogFile) ; 
	}
	free(buffer) ; 
	return ; 
}

void KVStore::initCache(const std::vector<std::string>& sstables)
{
	auto i = sstables.begin() ; 
	auto last = sstables.end() ;
	for(; i != last ; i++)
	{
		if(cache->isfull())
			return ; 
		FILE * ss = fopen(i->c_str(),"r") ; 
		cache->loadCache(ss);
		fclose(ss) ; 
	}
}
