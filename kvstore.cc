#include "kvstore.h"
KVStore::KVStore(const std::string &dir, const std::string &vlog) : KVStoreAPI(dir, vlog) ,cached(0)
{
	// initialize memTable
	memtable = new skiplist::skiplist_type(0.5) ; 
	bloomFilter = new BloomFilter(bloomSize,4);
	diskManager = new DiskTableManager(	dir, vlog , bloomFilter) ; 
										#ifdef DEBUG
            printf("DEBUG::Cursor Moving!\n")  ; 
        #endif 
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
			printf("LOG: CALL RESET\n") ; 
			printf("LOG: Current SIZE?:%d n?:%d\n",SIZE(memtable->size()+1),memtable->size()) ; 
		#endif
		// if(ssFile)
		// {
		// 	cache->loadCache(ssFile) ; 
		// }
		memtable->reset() ; 						// remove memtable 
		bloomFilter->reset() ; 						// remove BF
		// cache the new sstable. 
		
	}

	memtable->put(key,s) ; 
	bloomFilter->insert(key) ; 

}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{			
	std::string result ; 
		#ifdef DEBUG
			if(key == 407)
                printf("407 in process!!, result?%s\n:",result.c_str()) ; 
        #endif
	if(bloomFilter->isInclude(key))
	{

		result = memtable->get(key) ; 

	}
	else
	{
		result = "" ; 
	}
	if(result == DELETE_SIGN)
		return "" ; 
	if(result == "")				// dont in the memory ; ask diskManager
	{
		result = diskManager->get(key) ;
	}
	// cant find in disk will return "" , else return the result 
	return result ; 
}	
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
	skiplist::skiplist_node * target = memtable->arrive(key) ; 
	if((target->key!=key || target->val == DELETE_SIGN)		// not in the memtable
	&& diskManager->get(key) == "")														// not in the storage
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
	diskManager->reset() ; 
	bloomFilter->reset() ; 
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
	if(memtable->isEmpty())
	{
		return ;
	}
	char * sstable = (char*)(malloc(MAX_SIZE)) ; 	// it will be used as the int32* / int64* as well

	splayArray * vlog = new splayArray ; 
	char * buffer = (char *)malloc(BUFFER_SIZE) ; 
	// sprintf(buffer,"%lu-%lu.sst",memtable->min()->key,memtable->max()->key) ; 
	// FILE * F_sstable = fopen((name + "/" + buffer).c_str(),"w+") ; 
	
	writeTables(sstable , vlog) ; 

	diskManager->write(sstable,vlog , memtable->size()) ; 

// end
	
	free(sstable) ; 
	free(buffer) ; 
	delete vlog ; 
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
	// set BF 
	memcpy(sstable + headSize , bloomFilter->access() , bloomSize) ;
	while(firstEle!=tail)	
	{
		uint64_t key = firstEle->key ; 
		const std::string & val = firstEle->val ; 
		uint32_t valLen = (uint32_t)(val.length()) ; 
		if(val == DELETE_SIGN)								// read the DELETESIGN , means the unit have been deleted , valLen should be 0 , Dont need to write vlog
		{
			valLen = 0 ; 
		}
		// calculate the biase in vlog
		uint64_t biase = vlog->size() + diskManager->getVlogHead(); 
		// calculate CRC ; 
		buffer.push(key) ; 
		buffer.push(valLen) ; 
		buffer.append(val) ; 
		buffer.reset() ; 
		uint16_t crcCode = utils::crc16(buffer.access() , buffer.size()) ; 
		// insert the codes in vlog if not null 
		if(valLen != 0)
		{
			vlog->push(MAGIC) ; 
			vlog->push(crcCode) ; 
			vlog->push(key) ; 
			vlog->push(valLen) ; 
		// #ifdef DEBUG
		// 	printf("PUSH END \n") ; 
		// 	printf("DEBUG::vlog::%s vlog.size:%d\n",vlog->access(),vlog->size()) ; 
		// #endif
			vlog->append(val) ; 
		}
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

// void KVStore::outputTables(char * sstable , splayArray * vlog , FILE * F_sstable , FILE * F_vlog) const
// {
// 	int sstableByte = SIZE(memtable->size()) ; 
// 	fwrite(sstable,sstableByte , 1 , F_sstable) ; 
// 	// #ifdef DEBUG
// 	// 	printf("DEBUG::vlog::%s vlog.size:%d\n",vlog->access(),vlog->size()) ; 
// 	// #endif
// 	fwrite(vlog->access(),vlog->size(),1,F_vlog) ; 
// }

// void KVStore::initVlog()
// {
// 	vlogFile = fopen(vlog.c_str(),"a+") ; 
// 	fseek(vlogFile,0,SEEK_END) ; 
// 	head = ftell(vlogFile) ; 					// push the FILE to end and get the size 
// 	rewind(vlogFile) ; 
	
// 	char * buffer  = (char*)(malloc(BUFFER_SIZE)) ; 
// 	char magic = fgetc(vlogFile) ; 
// 	while(magic!=EOF)
// 	{
// 		if(magic==0xff)
// 		{
// 			tail = ftell(vlogFile) ; 
// 			break ; 
// 		}
// 		magic = fgetc(vlogFile) ; 
// 	}
// 	free(buffer) ; 
// 	return ; 
// }

// void KVStore::initCache(const std::string & dir,const std::vector<std::string>& sstables)
// {
// 	auto i = sstables.begin() ; 
// 	auto last = sstables.end() ;
// 	for(; i != last ; i++)
// 	{
// 		if(cache->isfull())
// 			return ; 
// 		// #ifdef DEBUG
// 		// printf("FILEName:%s\n",("./" + dir + "/" +  (*i)).c_str()) ; 
// 		// fflush(stdout) ; 
// 		// #endif

// 		FILE * ss = fopen(("./" + dir+ "/"  + (*i)).c_str(),"r+") ; 
// 		cache->loadCache(ss);
// 		fclose(ss) ; 
// 	}
// }
