#include "DiskTableManager.h"
// #define STRICT_MODE
// it will pass the directory of the whole database . 
// so we need to scan the levels of it 
DiskTableManager::DiskTableManager(const std::string &dir, const std::string &vlog ,BloomFilter * bf):vlog(vlog)
{
    this->bf = bf ; 
	this->dir = dir ; 
	timeStamp = 0 ; 
	cache = new Cache(MAX_SIZE , bf) ; 
	tail = 0 ; 
	head = 0 ;
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
		if(magic==MAGIC)
		{
			tail = ftell(vlogFile) ; 
			tail = tail-1 ; 			// magic not read in
			break ; 
		}
		magic = fgetc(vlogFile) ; 
	}
	rewind(vlogFile) ; 
	free(buf) ; 
	return ; 
}

std::string DiskTableManager::get(uint64_t key , uint64_t * offset_ret)
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
			// #ifdef GC_DEBUG
			// printf("sstable:%p , offset:%d\n" , sstable , offset) ; 
			// #endif
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
			if(offset_ret)
			{
				*offset_ret = vlogOffset ; 
			}

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
	fseek(vlogFile , 0 , SEEK_END) ; 			// important to maintain the vlogFile's append status . 
	#ifdef STRICT_MODE
		if(ftell(vlogFile) != head)
		{
			printf("write:: , head=%lu , currentSize:%lu\n" , head , ftell(vlogFile)) ; 
			abort() ; 
		}
		
	#endif
	rewind(F_sstable) ; 								// rewind the FILE and load it to cache
	cache->loadCache(F_sstable) ; 						
	timeStamp = timeStamp + 1 ; 						// update the timeStamp 
	insertSS(0,sstName) ; 								// update the manager 
	free(buf) ; 
	fclose(F_sstable) ; 
	// the level 0 is full , will do compaction
	if(levels[0].size() > GET_LEVEL_MAXNUM(0))
	{
		compaction(0) ; 
	}
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
			level = i ; 
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
			cache->loadCache(sstable , i) ; 
			int currentTimeStamp = cache->getTimeStamp(cache->size()-1) ; 
			if(currentTimeStamp > timeStamp)					// update the global timeStamp
			{
				
				timeStamp = currentTimeStamp ; 
			}
			fclose(sstable) ; 
		}
	}
	free(buf) ; 
}

void DiskTableManager::insertSS(int index , std::string& sstableName) 
{
	levels[index].push_back(sstableName) ; 
}

uint64_t DiskTableManager::getVlogTail()
{
	return tail ; 
}
uint64_t DiskTableManager::getVlogHead()
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

void DiskTableManager::compaction(int level)
{
	int size = levels[level].size() ; 
	if(size <= GET_LEVEL_MAXNUM(level))	// legal number 
	{
		return ; 
	}

	std::vector<int> currentLevel ;
	std::vector<int> nextLevel ; 
	std::vector<int> nextLevel_OVERLAP ; 			// overlap with the currentLevel
	cache->scanLevels(level , currentLevel) ; 	
	cache->scanLevels(level+1 , nextLevel) ; 
	if(level != 0)					// not zero , should select the sstable with smallest timestamp . 
	{
		cache->shellSort(currentLevel) ; 
		currentLevel.erase(currentLevel.begin() , currentLevel.begin()+ (GET_LEVEL_MAXNUM(level))) ; 
	}
	uint64_t left = 0 ; 
	uint64_t right = UINT64_MAX ; 
	cache->getRange(currentLevel , left , right) ; 
	cache->getOverlap(left , right , nextLevel ,nextLevel_OVERLAP) ; 

	merge(level + 1 , currentLevel , nextLevel_OVERLAP) ; 
	destroySStable(level,currentLevel , nextLevel_OVERLAP) ; 
	cache->evictTables(currentLevel , nextLevel_OVERLAP) ; 
	#ifdef GC_DEBUG
		PRINT_STATUS() ; 
		cache->PRINT_STATUS() ; 
	#endif
	compaction(level+1) ;			// recursively check if next level need to compaction
}

void DiskTableManager::merge(int insertLevel , const std::vector<int>& first , std::vector<int>& second)
{
	char * singleLine = (char *)malloc(MAX_SIZE) ; 
	int firstSize = first.size() ; 
	int secondSize = second.size() ; 
	int * hands = (int *)calloc(1,sizeof(int)* (firstSize + secondSize)) ; 	// save the merge's progress in each line ; the unit is key index.
	
	int totalKey ;
	int lastLevel = insertLevel > this->level ; 
	while(true)
	{
		int relatedTableCode = -1;
		totalKey = generateLine(first , second , hands , singleLine , relatedTableCode , lastLevel) ; 
		if(totalKey <= 0)
		{
			break ; 
		}
			cache->loadCache(singleLine , insertLevel) ; 
			writeLineToDisk(insertLevel , singleLine) ; 
		if(totalKey < KEY_MAXNUM)
		{
			break ; 
		}
	}

	free(singleLine) ; 
	free(hands) ;
}

void DiskTableManager::writeLineToDisk(int level , char * singleLine)
{
	char * buf = (char *)malloc(BUFFER_SIZE) ; 
	sprintf(buf , "%s/level-%d", dir.c_str() , level) ; 
	std::string directory(buf) ; 
	if(!utils::dirExists(buf))		// not have the unit , so it means not have the directory
	{
		this->level = level ; 
		utils::mkdir(directory) ; 
	}
	char * fileName = (char *)malloc(BUFFER_SIZE) ; 
	formName(singleLine , fileName) ; 
	std::string ssName(fileName) ; 
	insertSS(level , ssName) ; 
	formAddress(fileName , level , buf) ; 
	
	FILE * ssTable = fopen(buf , "w+") ; 
	int size = SIZE(GET_KEY_NUM(singleLine)) ; 
	fwrite(singleLine, size , 1 , ssTable) ; 
	fclose(ssTable) ; 
	free(buf) ; 
	free(fileName) ; 
}

int DiskTableManager::generateLine(const std::vector<int>& first , const std::vector<int>& second 
	, int * hands , char * result , int & relatedTableCode , int lastLevel)
{
	int keyNum = 0 ; 
	int length = first.size() + second.size() ; 
	int firstSize = first.size() ; 
	BloomFilter bf(bloomSize , 4) ; 
	int timeStamp = -1 ;
	while(keyNum < KEY_MAXNUM){
		uint64_t min = UINT64_MAX ; 			// select minimum from all the line
		char * selectedKey = 0 ; 
		int selectedI = -1 ;
		int selectedCacheLine = -1 ; 
		for(int i = 0 ; i < length ; i++){
			if(hands[i] == -1){		// the cacheLine is read over , dont need to access 			
				continue; ; 
			}
			int index ;
			if(i < firstSize){
				index = first[i] ; 
			}
			else{								// read the index from the vector
				index = second[i - firstSize] ; 
			}
			char * keyStart = cache->getKey(hands[i] , index) ; 
			if(keyStart == 0){ 		// the key is use up 
				hands[i] = -1 ; 
				continue ; 
			}
			if(GET_VLEN(keyStart) == 0 && lastLevel)	// deleted signal
			{
				hands[i] ++ ; 
				continue ;
			}
			if(GET_KEY(keyStart) < min){
				min = GET_KEY(keyStart) ; 
				selectedKey = keyStart ; 
				selectedI = i ;
				selectedCacheLine = index ; 
			}
			else if(GET_KEY(keyStart) == min){	// need to check the timeStamp when 2 key is equal
				int selectedTime = cache->getTimeStamp(selectedCacheLine) ;
				int currentTime = cache->getTimeStamp(index) ;  
				int currentVlen = GET_VLEN(keyStart) ;
				if(selectedTime >= currentTime)			// select the higher level's 
				{
					hands[i] ++ ; 			// only one key can be reserved 
					if(selectedTime == currentTime)
					{
						assert(selectedI < firstSize && i >= firstSize) ; 
						#ifdef STRICT_MODE
							if(min == 5552)
							{
								printf("Generate Line key:%lu selectedOffset:%lu , currentOffset:%lu\n" 
								,min , GET_OFFSET(selectedKey) ,GET_OFFSET(keyStart) ) ; 
							}
							assert(GET_OFFSET(selectedKey) > GET_OFFSET(keyStart) ) ; 					
						#endif
					}
				}
				else{
					hands[selectedI] ++ ;
					min = GET_KEY(keyStart) ; 
					selectedKey = keyStart ; 
					selectedI = i ;
					selectedCacheLine = index ; 
				}
			}
		}
		if(selectedKey){

			bf.insert(GET_KEY(selectedKey)) ; 
			
			memcpy(KEY_ADDR(keyNum , result) , selectedKey , keySize) ; 
			hands[selectedI]++ ; 				// move the hand to point at the next element
			keyNum ++ ;

			int insertTimeStamp = cache->getTimeStamp(selectedCacheLine) ; 	// check and update the timeStamp
			if(insertTimeStamp > timeStamp)
			{
				timeStamp = insertTimeStamp ; 
			}
			
		}
		else{	// no more key in the array
			break ; 
		}
	}
	// write the header and bf 
	int min =GET_KEY(KEY_ADDR(0 , result)) ; 
	int max = GET_KEY(KEY_ADDR(keyNum-1 , result)) ; 
	SETMIN(result , min) ; 
	SETMAX(result , max) ; 
	SETKEYNUM(result , keyNum) ; 
	SETTIME(result , timeStamp) ; 
	memcpy(result+headSize , bf.access() , bloomSize) ; 
	relatedTableCode = -1 ;
	return keyNum ; 
}

void DiskTableManager::formName(const char * sstable , char * name)
{
	uint64_t max = GETMAX(sstable) ; 
	uint64_t min = GETMIN(sstable) ; 
	uint64_t timeStamp = GETTIME(sstable) ;


	sprintf(name , "%lu_%lu-%lu.sst" , timeStamp , min , max) ; 
}

void DiskTableManager::formAddress(const char * fileName , int level , char * addr)
{
	sprintf(addr , "%s/level-%d/%s" , dir.c_str() , level , fileName) ; 
}

void DiskTableManager::destroySStable(int level , const std::vector<int>& first , const std::vector<int>& second)
{
	int length1 = first.size() ; 
	int length2 = second.size() ; 
	char * fileName =  (char *)malloc(BUFFER_SIZE) ; 
	char * buf = (char *)malloc(BUFFER_SIZE) ; 
	int currentLevel  ; 
	for(int i = 0 ; i < length1 + length2 ; i++)
	{
		char * cacheLine ; 
		if(i < length1)
		{
			currentLevel = level ; 
			cacheLine = cache->access(first[i]) ; 
		}
		else
		{
			currentLevel = level+1 ; 
			cacheLine = cache->access(second[i - length1]) ; 
		}
		formName(cacheLine , fileName) ; 

		auto toRemove = std::find(levels[currentLevel].begin() , levels[currentLevel].end() , std::string(fileName)) ; 
		auto secondFind = std::find(toRemove+1 , levels[currentLevel].end() , std::string(fileName)) ; 
		#ifdef GC_DEBUG
		printf("DestroySS:FormName:%s\n" , fileName) ; 
			if(toRemove == levels[currentLevel].end())
			{
				PRINT_STATUS() ; 
				cache->PRINT_STATUS() ; 
				assert(0) ;
			}
		#endif
		#ifdef GC_DEBUG
			// if(secondFind != levels[currentLevel].end())
			// {
			// 	PRINT_LEVEL(currentLevel) ; 
			// }
		#endif
		formAddress(fileName , currentLevel , buf) ; 
		if(secondFind == levels[currentLevel].end())		// dont have 2 same sstable name in the levels
		{
			utils::rmfile(std::string(buf)) ; 
			// assert(utils::rmfile(std::string(buf))) ; 
		}
		levels[currentLevel].erase(toRemove) ; 
	}
	free(fileName) ; 
	free(buf) ; 
}

Cache* DiskTableManager::getCache()
{
	return cache ; 
}

int DiskTableManager::readVlogFile(int offset , uint64_t& key , std::string & val) 
{
	char * buf = (char *)malloc(BUFFER_SIZE) ; 
	#ifdef STRICT_MODE
		fseek(vlogFile , offset, SEEK_SET) ; 	// to the key location 
		fread(buf , 1,1,vlogFile) ;
		buf[1] = 0 ;
		// assert(buf[0] == MAGIC) ; 
		if(buf[0]!=MAGIC)
		{
			printf("Error Offset Location:%lu , read:0x%x , tail:%lu , head:%lu\n",offset , buf[0], tail , head) ; 
			assert(0) ; 
		}
	#endif
	fseek(vlogFile , offset+VLOG_KEY_LOC, SEEK_SET) ; 	// to the key location 
	fread(buf , 8 , 1 , vlogFile) ; 
	buf[8] = 0 ; 						// make it a string end 
	key = ((uint64_t*)buf)[0] ; 

	fread(buf , 4 , 1 , vlogFile) ; 	// read the vlen ;
	int vlen = ((int*)buf)[0] ; 
	buf[4] = 0 ; 						// make it a string end 

	fread(buf , vlen , 1 , vlogFile) ; 
	buf[vlen] = 0 ; 						// make it a string end 
	val.assign(buf) ; 
			#ifdef GC_DEBUG
				if(key != val.length()-1)
				{
					printf("key:%d , buf:%s vlen:%d valLen:%d\n" , key , buf, vlen , val.length()) ; // abort
					assert(0) ; 
				}
			#endif
	free(buf) ; 

	return offset + vlen + VLOG_HEAD ; 
}

void DiskTableManager::dealloc(int length)
{
	utils::de_alloc_file(vlog , tail , length) ; 
	tail = tail + length ; 			// update the tail after file location change 
}

uint64_t DiskTableManager::getVlogOffset(uint64_t key)
{
	uint64_t vlogOffset = UINT64_MAX;
	get(key , &vlogOffset) ; 

	return vlogOffset ; 
}

// print the single level status 
void DiskTableManager::PRINT_LEVEL(int i) const
{
	printf("========== PRINT SINGLE LEVEL %d ==========\n" , i) ; 
	auto begin = levels[i].begin() ; 
	auto end = levels[i].end() ; 
	for(; begin != end ; begin++)
	{
		printf("\"%s\" " , (*begin).c_str()); 
	}
	printf("\n========== SINGLE LEVEL %d END ==========\n" , i) ; 
}
// print the whole level status
void DiskTableManager::PRINT_STATUS() const 
{
	printf("========== PRINT LEVELS STATUS ==========\n") ; 
	int length = 0 ; 
	for(int i = 0 ;  i < 5 ; i++)
	{
		printf("Level %d:\n" , i) ; 
		auto begin = levels[i].begin() ; 
		auto end = levels[i].end() ; 
		for(; begin != end ; begin++)
		{
			printf("\"%s\" " , (*begin).c_str()); 
		}
		printf("\n") ; 
	}
	printf("\n========== PRINT LEVEL STATUS END ==========\n") ; 
}