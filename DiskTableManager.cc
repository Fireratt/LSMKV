#include "DiskTableManager.h"
// it will pass the directory of the whole database . 
// so we need to scan the levels of it 
DiskTableManager::DiskTableManager(const std::string &dir, const std::string &vlog ,BloomFilter * bf):vlog(vlog)
{
    this->bf = bf ; 
	this->dir = dir ; 
	timeStamp = 0 ; 
	cache = new Cache(MAX_SIZE , bf) ; 
	
    if(utils::dirExists(dir))
	{
		initVlog() ; 
		scanDisk() ; 				// scan the directory 
		return ; 
	}
	utils::mkdir(dir) ;
	char * buf = (char*)malloc(BUFFER_SIZE) ; 
	sprintf(buf,"%s",vlog.c_str()) ; 			// prepare the vlog address
	vlogFile = fopen(buf,"a+") ; 
	
	if(!vlogFile)
	{
		printf("Cant Open:%s\n",buf) ; 
	}
	free(buf) ; 
}

void DiskTableManager::initVlog()
{
	char * buf = (char*)malloc(BUFFER_SIZE) ; 
	sprintf(buf,"%s",vlog.c_str()) ; 			// prepare the vlog address
	vlogFile = fopen(buf,"a+") ; 
	if(!vlogFile)
	{
		printf("Cant Open:%s\n",buf) ; 
	}
	fseek(vlogFile,0,SEEK_END) ; 
	head = ftell(vlogFile) ; 					// push the FILE to end and get the size 
	rewind(vlogFile) ; 
	
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
	rewind(vlogFile) ; 
	free(buf) ; 
	return ; 
}

std::string DiskTableManager::get(uint64_t key)
{
		int offset ;
        std::string result = std::string("") ; 
		int index = cache->access(key,bf,offset) ; 
		#ifdef DEBUG
			printf("Get : find %lu in memory[%d],offset?:%d\n",key , index , offset) ;
		#endif
		if(index!=-1)
		{
			char * sstable = cache->access(index) ; 
			// get the offset in vlog 
			uint64_t vlogOffset = ((uint64_t *)(sstable+offset))[1] ; 
			// get the length of value
			uint32_t length = ((uint32_t *)(sstable+offset))[4] ; 
			#ifdef DEBUG
                printf("DEBUG:: KvStore::get:vlogOffset = %lu length = %u\n"
                ,vlogOffset , length) ; 
            #endif
			char * buffer = (char *)malloc(BUFFER_SIZE) ; 
			// read from the VLOG
			rewind(vlogFile) ; 
			fseek(vlogFile, VLOG_HEAD + vlogOffset , SEEK_SET) ; 
			fread(buffer,length,1,vlogFile) ; 
			// give an end
			buffer[length] = 0 ;
			result = result.assign(buffer) ; 

			free(buffer) ; 
			// find in cache , return 
			return result ; 
		}
		return result ; 		// cant find 
}

void DiskTableManager::write(char * sstable , splayArray * vlog , int mmSize)
{
	// if dont have the directory , create one 
	char * buf = (char*)malloc(BUFFER_SIZE) ; 
	sprintf(buf , "%s/level-%d" , dir.c_str(), 0) ; 
	if(!utils::dirExists(buf))
	{
		utils::mkdir(buf) ; 
	}
	std::string directory(buf) ; 
	int64_t min = GETMIN(sstable) ; 
	int64_t max = GETMAX(sstable) ; 
	sprintf(buf,"%d_%lu-%lu.sst",getNextTimeStamp(),min,max) ; 								// prepare the sstable name ; 
	std::string sstName(buf) ; 

	sprintf(buf,"%s/%s",directory.c_str(),sstName.c_str()) ; 								// prepare the sstable directory ; 

	FILE* F_sstable	= fopen(buf,"wr+");									// find the true sstable's location 
	// int sstableByte = SIZE(memtable->size()) ; 
	if(!F_sstable)
	{
		printf("Cant Open:%s\n",buf) ; 
		free(buf) ; 
		return ; 
	}
	int sstableByte = SIZE(mmSize) ; 
	fwrite(sstable,sstableByte , 1 , F_sstable) ; 
	// #ifdef DEBUG
	// 	printf("DEBUG::vlog::%s vlog.size:%d\n",vlog->access(),vlog->size()) ; 
	// #endif
	fwrite(vlog->access(),vlog->size(),1,vlogFile) ; 
	head += vlog->size() ; 								// update the new location of vlog head after insert new vlogs
	rewind(F_sstable) ; 								// rewind the FILE and load it to cache
	cache->loadCache(F_sstable) ; 						
	timeStamp = timeStamp + 1 ; 						// update the timeStamp 
	insertSS(0,sstName) ; 								// update the manager 
	free(buf) ; 
	fclose(F_sstable) ; 
}

void DiskTableManager::initCache(const std::string & dir , const std::vector<std::string>& sstables) 
{
	auto i = sstables.begin() ; 
	auto last = sstables.end() ;
	for(; i != last ; i++)
	{
		// #ifdef DEBUG
		// printf("FILEName:%s\n",("./" + dir + "/" +  (*i)).c_str()) ; 
		// fflush(stdout) ; 
		// #endif

		FILE * ss = fopen(("./" + dir+ "/"  + (*i)).c_str(),"r+") ; 
		cache->loadCache(ss);
		fclose(ss) ; 
	}
}
void DiskTableManager::scan(uint64_t key1 , uint64_t key2 , 
std::list<std::pair<uint64_t, std::string>> &list , std::unordered_set<uint64_t>& scaned) 
{
	int * lineList = (int*)malloc(sizeof(int)*(cache->size()+1)); 

	cache->scanPossibleLine(key1, key2 , lineList) ;  // get all the cacheLine index that overlap with key1-key2 . linelist ordered , should end with -1
	
	for(int i = 0 ; lineList[i] != -1; i++)				// check all the cacheline
	{
		int index = lineList[i] ;
		char * cacheLine = cache->access(index) ; 
		int offset = cache->arrive(index , key1) ; // get the start location for scan the sstable
		int hand = offset ; 
		for(; hand < MAX_SIZE ; hand = NEXT_KEY(hand))
		{
			uint64_t key = GET_KEY(cacheLine + hand) ; 	

			if(key > key2)											// bigger than max , definitely not have the key , break ; 
			{
				break ; 
			}
			
			if(!scaned.empty()&&scaned.find(key) != scaned.end())					// have the element in the list 
			{
				continue ; 
			}
			if(key >= key1 && key <= key2)							// in the scan range
			{
				std::string ret = getValByCacheLine(cacheLine,hand) ; 				// 
				#ifdef DEBUG
					printf("DiskTableManager : Scan : getRet:%s ,key :%lu\n" , ret.c_str() , key) ; 
				#endif
				scaned.insert(key) ; 
				list.push_back(std::pair<uint64_t, std::string>(key,ret)) ; 
			}
		}
	}
	free(lineList) ; 
}
DiskTableManager::~DiskTableManager() 
{
	fclose(vlogFile) ; 
	delete cache ;
}

void DiskTableManager::reset()
{
    // delete all sstable in the directory
	char * buf = (char *)malloc(BUFFER_SIZE) ; 
	for(int i = 0 ; i < MAX_LEVEL ; i++)
	{
		std::vector<std::string>& ret = levels[i] ; 
		sprintf(buf,"/level-%d",i) ; 					// form the level name 
		std::string dirName = dir +  buf ;
		while(!ret.empty())
		{
			std::string sstableName = ret.back() ; 
			ret.pop_back() ; 
			utils::rmfile(dirName + "/" + sstableName) ; 
		}
		utils::rmdir(dirName) ;
	}
	// must close the vlog first , then we can remove it , and we need to reopen it
	fclose(vlogFile) ; 
	utils::rmfile(vlog) ; 
	sprintf(buf,"%s",vlog.c_str()) ; 			// prepare the vlog address
	vlogFile = fopen(buf,"a+") ; 
	timeStamp = 0 ; 				// reset the global timeStamp
	head = 0  ;
	tail = 0 ; 						// reset the vlog head and tail 

	cache->reset() ; 				// reset the cache
	free(buf) ; 
}

void DiskTableManager::scanDisk()
{
	char * buf = (char *)malloc(BUFFER_SIZE) ; 
	for(int i = 0 ; i < MAX_LEVEL ; i++)
	{
		sprintf(buf,"/level-%d" , i) ; 		// load the directory name 
		std::string dirName(buf) ; 
		dirName = dir + dirName ; 
		if(!utils::dirExists(dirName))		// joint the directory address;
		{
			free(buf) ; 
			return ; 		// Not have the directory ; there will be no levels
		}
		std::vector<std::string> sstables ; 
		utils::scanDir(dirName,sstables) ; 		// scan the directory and get all the sstables in sstables 

		auto end = sstables.end() ; 
		auto begin = sstables.begin() ; 
		for(; begin!=end ; begin++)
		{
			levels[i].push_back(*begin)	;			// push the sstable name into it ;
			sprintf(buf,"%s/%s",dirName.c_str() , (*begin).c_str()) ; 
			FILE * sstable = fopen(buf,"r") ; 
			if(!sstable)
			{
				printf("Cant Read:%s" , buf) ; 
				free(buf) ; 
				return ; 
			}
			cache->loadCache(sstable) ; 
			int currentTimeStamp = cache->getTimeStamp(cache->size()-1) ; 
			if(currentTimeStamp > timeStamp)					// update the global timeStamp
			{
				
				timeStamp = currentTimeStamp ; 
			}
		}
	}
	free(buf) ; 
}

void DiskTableManager::insertSS(int index , std::string& sstableName) 
{
	levels[index].push_back(sstableName) ; 
}

int DiskTableManager::getVlogTail()
{
	return tail ; 
}
int DiskTableManager::getVlogHead()
{
	return head ; 
}

int DiskTableManager::getNextTimeStamp()
{
    return timeStamp + 1 ; 
}

std::string DiskTableManager::getValByCacheLine(char * cacheLine, int offset)
{
	int vlen = GET_VLEN(cacheLine + offset) ; 
	char * val = (char *)malloc(vlen+1) ; 

	int64_t vOffset = GET_OFFSET(cacheLine + offset) ; 

	rewind(vlogFile) ; 
	fseek(vlogFile, VLOG_HEAD + vOffset , SEEK_SET) ; 	// move the ptr to the vOffset
	fread(val,vlen,1,vlogFile) ; 
	val[vlen] = 0 ; 
	// #ifdef DEBUG
	// 	printf("GetValue From CacheLine: %s , vOffset:%d vlen:%d\n" , val , vOffset , vlen) ; 
	// #endif
	rewind(vlogFile) ; 
	std::string backVal = "" ; 
	backVal.assign(val) ; 
	free(val) ; 
	return backVal ; 
}

std::string DiskTableManager::getValByIndex(int index, int offset)
{
	char * cacheLine = cache->access(index) ; 
	return getValByCacheLine(cacheLine,offset) ; 
}