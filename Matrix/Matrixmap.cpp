//---------------------------------------------------------------------------
// PathGraph
// (c) 2013 Massive Data Management Group @ SCTS & CGCL. 
//     Web site: http://grid.hust.edu.cn/triplebit
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------

#include "Matrixmap.h"
#include "OSFile.h"
#include "StatisticsBuffer.h"
#include <dirent.h>
#include "HashID.h"
unsigned BucketManager::pagesize = 65536;

Matrixmap::Matrixmap(const char * upperDir,const char * mapName,vector<ID> preList) {
	// TODO Auto-generated constructor stub
	degree = atoi(mapName +6);
	strcpy(Dir,upperDir);
	matrixPath[110];
	sprintf(matrixPath,"%s/%s/",Dir,mapName);
    cout <<"new matrix:" << matrixPath<< endl;
	if (OSFile::DirectoryExists(matrixPath) == false) {
		OSFile::MkDir(matrixPath);
	}

	isLoad = true;
	isMemory = false;
}

Matrixmap::~Matrixmap() {
	// TODO Auto-generated destructor stub
	hash_map<ID, BucketManager*>::iterator iter = predicate_manager.begin();
	for(;iter != predicate_manager.end(); iter++){
		delete iter->second;
		iter->second = NULL;
	}
	isLoad = false;
}

size_t Matrixmap::getSize(){
	hash_map<ID, BucketManager*>::iterator start = predicate_manager.begin();
	hash_map<ID, BucketManager*>::iterator end = predicate_manager.end();
	size_t size = 0;
	while(start != end){
		size += start->second->getUsedSize();
		start++;
	}
	return size;
}
BucketManager* Matrixmap::getBucketManager(ID id) {
	//there is no predicate_managers[id]
	if(predicate_manager[id] == NULL) {
		predicate_manager[id] = new BucketManager(id, this,matrixPath);
	}
	return predicate_manager[id];
}

Status Matrixmap::insertXY(ID preID,ID xid,EntityIDBuffer*& Y){

	if(Y->getSize() == 0){
		return OK;
	}
	getBucketManager(preID)->insertXY(xid,Y);

	return OK;
}

char* Matrixmap::getNewPage(ID preID) {

	char* rt;
	bool tempresize = false;

	MMapBuffer * tempMap = preFileMap[preID];
	//cout <<"getNewPage:"<< tempMap->get_length() <<" "  <<usedPage[preID] * BucketManager::pagesize<<endl;
	if ((usedPage[preID] + 1) * BucketManager::pagesize >= tempMap->get_length()) {
		size_t newsize = 0;
		if (tempMap->get_length() < (2 << 30))
			newsize = tempMap->get_length() * 2
					+ INCREMENT_PAGE_COUNT * BucketManager::pagesize;
		else
			newsize = tempMap->get_length() + (2 >> 30);

		tempMap->resize(newsize, false);

		if (usedPage[preID] * BucketManager::pagesize >= tempMap->get_length()) {
			cout << "getNewPage error" << endl;
			assert(false);
		}

		tempresize = true;
	}
	rt = tempMap->get_address() + usedPage[preID] * BucketManager::pagesize;
	usedPage[preID]++;

	if (tempresize == true) {
		predicate_manager[preID]->meta = (BucketManagerMeta*) (tempMap->get_address());
		predicate_manager[preID]->meta->startPtr = tempMap->get_address() + sizeof(BucketManagerMeta);
	}

	return rt;
}

Status Matrixmap::endInsertXY(){
	hash_map<ID,BucketManager* >::iterator iter = predicate_manager.begin();
	hash_map<ID,BucketManager* >::iterator iterend = predicate_manager.end();
	while(iter != iterend){
		iter->second->endInsertXY();
		iter++;
	}
	return OK;
}

Status Matrixmap::endInsertXY(ID preID){
	if(predicate_manager[preID] != NULL){
		predicate_manager[preID]->endInsertXY();
	}
	return OK;
}
bool Matrixmap::done(){
	hash_map<ID, BucketManager*>::iterator iter = predicate_manager.begin();
	hash_map<ID, BucketManager*>::iterator iterend = predicate_manager.end();
	while (iter != iterend) {
		if(iter->second != NULL && !iter->second->done())
			return false;
		iter++;
	}
	return true;
}

static bool isMatrixMapFile(const char *in_str) {
	int l_status = 0;
	regex_t l_re;
	char pattern[100];
	strcpy(pattern, "^pre[0-9]+$");

	regcomp(&l_re, pattern, REG_EXTENDED);
	l_status = regexec(&l_re, in_str, (size_t) 0, NULL, 0);
	regfree(&l_re);
	if (0 != l_status) {
		return false;
	}
   return true;
}

Matrixmap* Matrixmap::load(const char* upperDir,const char* Name,bool isMemory){
	struct dirent* ent = NULL;
	DIR *pDir;
	char matrixDir[110];
	sprintf(matrixDir,"%s/%s/",upperDir,Name);
	pDir = opendir(matrixDir);
	//vector<>
	if (pDir == NULL) {
		return NULL;
	}
	Matrixmap* matrixMap = new Matrixmap(upperDir,Name);
	matrixMap->isMemory = isMemory;
	int preID;
	//load startID
	unsigned size = 0;
	const char* startIDPath = (string(upperDir) + "/" + Name + "/startID.0").c_str();
	if (OSFile::FileExists(startIDPath)) {
		char* buf = NULL;
		size = HashID::loadFileinMemory(startIDPath, buf);
		memcpy(matrixMap->start_ID, buf, size);
		free(buf);

	} else {
		cerr << "startID file not exit!" << endl;
		assert(false);
	}
	size = size / sizeof(ID) - 1;
	matrixMap->bucSize = size;
	matrixMap->maxID = matrixMap->start_ID[size] - 1;
	// load pre
	while (NULL != (ent = readdir(pDir))) {
		char mapFile[120];
		strcpy(mapFile, matrixDir);
		if (ent->d_type == 8) {
			if(isMatrixMapFile(ent->d_name)){
				preID = atoi(ent->d_name + 3);
				strcat(mapFile,ent->d_name);
				if(isMemory){
					matrixMap->predicate_manager[preID] = BucketManager::loadMemory(preID,mapFile);
				}else{
					matrixMap->predicate_manager[preID] = BucketManager::load(preID,mapFile);
				}
			}
		}
	}

	return matrixMap;
}

Matrixmap* Matrixmap::parallel_load(const char* upperDir,const char* Name,bool isMemory){
	struct dirent* ent = NULL;
	DIR *pDir;
	char matrixDir[110];
	sprintf(matrixDir,"%s/%s/",upperDir,Name);
	pDir = opendir(matrixDir);
	//vector<>
	if (pDir == NULL) {
		return NULL;
	}
	Matrixmap* matrixMap = new Matrixmap(upperDir,Name);
	matrixMap->isMemory = isMemory;
	int preID;
	//load startID
	unsigned size = 0;
	const char* startIDPath = (string(upperDir) + "/" + Name + "/startID.0").c_str();
	if (OSFile::FileExists(startIDPath)) {
		char* buf = NULL;
		size = HashID::loadFileinMemory(startIDPath, buf);
		memcpy(matrixMap->start_ID, buf, size);
		free(buf);

	} else {
		cerr << "startID file not exit!" << endl;
		assert(false);
	}
	size = size / sizeof(ID) - 1;
	matrixMap->bucSize = size;
	matrixMap->maxID = matrixMap->start_ID[size] - 1;
	// load pre
	string base_dir = string(matrixDir);
	while (NULL != (ent = readdir(pDir))) {
		if (ent->d_type == 8) {
			if(isMatrixMapFile(ent->d_name)){
				preID = atoi(ent->d_name + 3);
				if(isMemory){
					CThreadPool::getInstance().AddTask(boost::bind(&BucketManager::load_memory, preID, base_dir, string(ent->d_name), matrixMap));
				}else{
					CThreadPool::getInstance().AddTask(boost::bind(&BucketManager::load_mmap, preID, base_dir, string(ent->d_name), matrixMap));
				}
			}
		}
	}
	CThreadPool::getInstance().Wait();

	return matrixMap;
}

Status Matrixmap::getAllY(ID xid, EntityIDBuffer* &entBuffer, CycleCache* cache ) {
	if (cache && cache->getValuebyKey(xid, entBuffer) == OK){
			return OK;
	}
	entBuffer->empty();
	if (xid > maxID || xid < start_ID[0])
		return NOT_FOUND;

	unsigned index = 0;
	int begin = 0, mid = 0, end = bucSize - 1;
	while (begin < end) {
		mid = (begin + end) / 2;
		if (start_ID[mid] == xid) {
			begin = mid;
			break;
		} else if (start_ID[mid] < xid)
			begin = mid + 1;
		else
			end = mid;
	}

	if (start_ID[begin] > xid)
		index = begin - 1;
	else
		index = begin;
	//cout <<"index:" <<index<<  endl; 
	getBucketManager(index)->getIndex()->getYByID(xid,entBuffer);

	if (cache && entBuffer->getSize() == 0) {
		cache->insert(xid,NULL);
		return NOT_FOUND;
	}

	
	if(cache){
		EntityIDBuffer* temp  = new EntityIDBuffer(entBuffer);
		cache->insert(xid,temp);
	}

	return OK;
}

Status Matrixmap::getAllY(ID xid, EntityIDBuffer* &entBuffer, bool cacheFlag ) {
	entBuffer->empty();
	if (xid > maxID || xid < start_ID[0])
		return NOT_FOUND;

	unsigned index = 0;
	int begin = 0, mid = 0, end = bucSize - 1;
	while (begin < end) {
		mid = (begin + end) / 2;
		if (start_ID[mid] == xid) {
			begin = mid;
			break;
		} else if (start_ID[mid] < xid)
			begin = mid + 1;
		else
			end = mid;
	}

	if (start_ID[begin] > xid)
		index = begin - 1;
	else
		index = begin;

	getBucketManager(index)->getIndex()->getYByID(xid,entBuffer);
	
	return OK;
}

Status Matrixmap::getAllYByID(ID id,EntityIDBuffer* &entBuffer,ID preID){
	if(!cycCache.count(preID)){
		cycCache[preID] = new CycleCache();
	}
	if (cycCache[preID]->getValuebyKey(id, entBuffer) == OK) {
		return OK;
	}
	hash_map<ID, BucketManager*>::iterator iter = predicate_manager.begin();
	hash_map<ID, BucketManager*>::iterator end = predicate_manager.end();

	EntityIDBuffer* tempBuffer = new EntityIDBuffer();
	EntityIDBuffer* curBuffer = new EntityIDBuffer();
	do {
		iter->second ->getYByID(id, entBuffer);
		iter++;
	} while (entBuffer->getSize() > 0 && iter != end);

	for(; iter != end ; iter++){
		if(iter->second->getYByID(id,curBuffer) == OK){
			EntityIDBuffer::mergeSingleBuffer(tempBuffer,curBuffer,entBuffer);
			EntityIDBuffer::swapBuffer(tempBuffer,entBuffer);
		}
	}

	if(entBuffer->getSize() == 0){
		cycCache[preID]->insert(id,NULL);
	}else{
		EntityIDBuffer* temp  = new EntityIDBuffer(entBuffer);
		cycCache[preID]->insert(id,temp);
	}

	delete tempBuffer;
	delete curBuffer;
	return OK;
}

///////////////////////////////////////////////////////////////////////////////////////////
BucketManager::BucketManager() {
	indexBuffer = NULL;
	bucketFile = NULL;
	ptrs = NULL;
	tempEntBuffer = NULL;
	curEntBuffer = NULL;
	insertEntBuffer = NULL;
    result = NULL;
    meta = NULL;
}

BucketManager::BucketManager(ID id,Matrixmap* matrixMapBuffer,char* Dir):matrixMap(matrixMapBuffer){
	//init list in matrixmap
	char path[100];
	sprintf(path, "%s/pre%d", Dir, id);
	matrixMap->preFileMap[id] = new MMapBuffer(path, INIT_PAGE_COUNT* BucketManager::pagesize);
	matrixMap->usedPage[id] = 0;

	bucketFile = matrixMap->preFileMap[id];
	sprintf(path, "%s_index", path);

	indexBuffer = new PageIndex(*this,path);

	ptrs = matrixMap->getNewPage(id);
//	matrixMap->usedPage[id]++;
	meta = (BucketManagerMeta*)ptrs;
	meta->pid = id;
	meta->lineCount = 0;
	meta->length =  matrixMap->getUsedPage(id)* BucketManager::pagesize;
	meta->usedSpace = sizeof(BucketManagerMeta);
	meta->startPtr = bucketFile->get_address() + sizeof(BucketManagerMeta);
	meta->endPtr = meta->startPtr;


	tempEntBuffer = new EntityIDBuffer();
	tempEntBuffer->setIDCount(1);
	curEntBuffer = new EntityIDBuffer();
	curEntBuffer->setIDCount(1);
	insertEntBuffer = new EntityIDBuffer();
	insertEntBuffer->setIDCount(1);

	result = new MemoryBuffer(BucketManager::pagesize);

//	usedPage = 0;
	lastXID = 0;
	firstCouple = true;
	isDone = false;
//	maxlen = 0;

	coupleNum = 0;
	chunkUsed = 2;
}

BucketManager::~BucketManager(){

	if(indexBuffer != NULL){
        delete indexBuffer;
		indexBuffer = NULL;
	}

	if(bucketFile !=NULL){
    	delete bucketFile;
		bucketFile = NULL;
		ptrs = NULL;
	}else{
    	free((char*)meta);
		meta = NULL;
	}
	if(tempEntBuffer != NULL){
		delete tempEntBuffer;
		tempEntBuffer = NULL;
	}
	if(curEntBuffer != NULL){
		delete curEntBuffer;
		curEntBuffer = NULL;
	}
	if(insertEntBuffer != NULL){
		delete insertEntBuffer;
		insertEntBuffer = NULL;
	}
	if(result != NULL){
		delete result;
		result = NULL;
	}

	int size = edgeBufferFree.size();
	for (int i = 0; i < size; i++) {
		delete edgeBufferFree[i];
		edgeBufferFree[i] = NULL;
	}
	edgeBufferFree.clear();

	size = edgeBufferUsed.size();
	for (int i = 0; i < size; i++) {
		delete edgeBufferUsed[i];
		edgeBufferUsed[i] = NULL;
	}
	edgeBufferUsed.clear();

}

Status BucketManager::insertXY(ID xid, EntityIDBuffer* &Yid){
	if(xid == lastXID || lastXID == 0){
		if(insertEntBuffer->getSize() == 0){
			EntityIDBuffer::swapBuffer(Yid,insertEntBuffer);
		}else{
			EntityIDBuffer::mergeSingleBuffer(tempEntBuffer,insertEntBuffer,Yid);
			EntityIDBuffer::swapBuffer(tempEntBuffer,insertEntBuffer);
		}
	}else{
		//xid > lastXID && lastXID !=0 do insert the last
		insertGatherXY(lastXID,insertEntBuffer);
		insertEntBuffer->empty();
		EntityIDBuffer::swapBuffer(Yid,insertEntBuffer);
	}

	lastXID = xid;
	return OK;
}

bool BucketManager::isPtrFull(unsigned len){
	return meta->usedSpace+ len > meta->length;
}

Status BucketManager::insertGatherXY(ID xid, EntityIDBuffer* Yid){
	unsigned charLen = getChars(result,xid,Yid);
	char * chars = result->getBuffer();
	unsigned offset = 0;

	bool isFirstFull = true;
	while(isPtrFull(charLen) == true) {
		if(isFirstFull){
			offset = meta->length - meta->usedSpace;
			memcpy(meta->endPtr,chars,offset);
			charLen -= offset;
			meta->usedSpace = meta->length;
			isFirstFull = false;
		}
		resize();
	}

	//build the index
	if(!isFirstFull || firstCouple){
		indexBuffer->insertEntries(xid,meta->endPtr -meta->startPtr-offset);
		firstCouple = false;
	}

	assert(meta->endPtr-meta->startPtr + sizeof(BucketManagerMeta) == meta->usedSpace);
	memcpy(meta->endPtr, chars+offset, charLen);
	meta->endPtr = meta->endPtr + charLen;
	meta->usedSpace = meta->usedSpace + charLen;
	meta->lineCount++;
	return OK;
}

Status BucketManager::endInsertXY(){
	if(insertEntBuffer->getSize() !=0)
		insertGatherXY(lastXID,insertEntBuffer);
	insertEntBuffer->empty();
	indexBuffer->endInsert();
	bucketFile->close();
    isDone = true;
	return OK;
}

void BucketManager::flush(){
	bucketFile->flush();
}

Status BucketManager::resize(){
	ptrs = matrixMap->getNewPage(meta->pid);
	meta->length += BucketManager::pagesize;
	meta->endPtr = meta->startPtr - sizeof(BucketManagerMeta)+ meta->usedSpace;
	return OK;
}

unsigned BucketManager::getChars(MemoryBuffer* result, ID Xid,EntityIDBuffer* Yid) {
	bool head = false;
	unsigned length = 0;
	//result->empty();
	char* writer = result->getBuffer();
	memcpy(writer, &Xid, 4);
	writer += 8;
	// left 4 bytes for the length;

	size_t total = Yid->getSize();
	size_t i;
	ID* p = Yid->getBuffer();
	ID x;
	ID y = 0;
	bool isfirst = true;
	for (i = 0; i != total; i++) {
		x = p[i];
		if(isfirst){
			y = x;
			isfirst = false;
		}else{
			x = x -y;
			y = x+y;
		}
		length = writer- result->getBuffer();
		if(length + 5 >= result->getSize()){
			result->resize(BucketManager::pagesize);
			writer = result->getBuffer() + length;
		}

		if (head == true) {		//true 1
			while (x >= 128) {
				unsigned char c = static_cast<unsigned char> (x | 128);
				*writer = c;
				writer++;
				x >>= 7;
			}
			*writer = static_cast<unsigned char> (x | 128);
			++writer;
		} else {				//false 0
			while (x >= 128) {
				unsigned char c = static_cast<unsigned char> (x & 127);
				*writer = c;
				writer++;
				x >>= 7;
			}
			*writer = static_cast<unsigned char> (x & 127);
			++writer;
		}
		head = !head;
	}
	length = writer- result->getBuffer();
	memcpy(result->getBuffer()+4, &length, 4);
	return length;
}

size_t BucketManager::getBody(MemoryBuffer* result, EntityIDBuffer* Yid) {
	bool head = false;
	size_t length = 0;
	char* writer = result->getBuffer();
	size_t total = Yid->getSize();
	size_t i;
	ID* p = Yid->getBuffer();
	ID x;
	ID y = 0;
	bool isfirst = true;

	for (i = 0; i != total; i++) {
		x = p[i];
		if(isfirst){
			y = x;
			isfirst = false;
		}else{
			x = x -y;
			y = x+y;
		}
		length = writer- result->getBuffer();
		if(length + 5 >= result->getSize()){
			result->resize(BucketManager::pagesize);
			writer = result->getBuffer() + length;
		}

		if (head == true) {		//true 1
			while (x >= 128) {
				unsigned char c = static_cast<unsigned char> (x | 128);
				*writer = c;
				writer++;
				x >>= 7;
			}
			*writer = static_cast<unsigned char> (x | 128);
			++writer;
		} else {				//false 0
			while (x >= 128) {
				unsigned char c = static_cast<unsigned char> (x & 127);
				*writer = c;
				writer++;
				x >>= 7;
			}
			*writer = static_cast<unsigned char> (x & 127);
			++writer;
		}
		head = !head;
	}
	length = writer- result->getBuffer();
	return length;
}

size_t BucketManager::getBody1(MemoryBuffer* result, EntityIDBuffer* Yid) {
	bool head = false;
	size_t length = 0;
	char* writer = result->getBuffer();
	size_t total = Yid->getSize();
	size_t i;
	ID* p = Yid->getBuffer();
	ID y0 = 0, yi = 0;
	bool isfirst = true;

	for (i = 0; i != total; i++) {
		yi = p[i];
		if(isfirst){
			y0 = yi;
			isfirst = false;
		}else{
			yi = yi -y0;
		}
		length = writer- result->getBuffer();
		if(length + 5 >= result->getSize()){
			result->resize(BucketManager::pagesize);
			writer = result->getBuffer() + length;
		}

		if (head == true) {		//true 1
			while (yi >= 128) {
				unsigned char c = static_cast<unsigned char> (yi | 128);
				*writer = c;
				writer++;
				yi >>= 7;
			}
			*writer = static_cast<unsigned char> (yi | 128);
			++writer;
		} else {				//false 0
			while (yi >= 128) {
				unsigned char c = static_cast<unsigned char> (yi & 127);
				*writer = c;
				writer++;
				yi >>= 7;
			}
			*writer = static_cast<unsigned char> (yi & 127);
			++writer;
		}
		head = !head;
	}
	length = writer- result->getBuffer();
	return length;
}

const uchar* BucketManager::readChars(const uchar * reader,ID& Xid,EntityIDBuffer* Yid){
	unsigned len = 0;
	memcpy(&Xid,reader,4);
	reader += 4;
	memcpy(&len,reader,4);
	reader +=4;
	Yid->empty();
	return readBody(reader,len,Yid);
}

const uchar* BucketManager::readBody(const uchar* reader, unsigned len,
		EntityIDBuffer* Yid) {
	const uchar* limit = reader + len;
	Yid->empty();

	register unsigned shift = 0;
	ID id = 0;
	ID y = 0;
	register unsigned int c;
	bool isfirst = true;
	while (reader < limit) {
		id = 0;
		shift = 0;
		c = *reader;
		if (!(c & 128)) {
			//head = 0;
			id |= c << shift;
			shift += 7;
			reader++;
			if (reader >= limit)
				break;
			while (true) {
				c = *reader;
				if (!(c & 128)) {
					id |= c << shift;
					shift += 7;
					reader++;
					if (reader >= limit)
						break;
				} else {
					if (isfirst) {
						y = id;
						isfirst = false;
					} else {
						id = id + y;
						y = id;
					}
					Yid->insertID(id);
					break;
				}
			}
		} else {
			id |= (c & 0x7F) << shift;
			shift += 7;
			reader++;
			if (reader >= limit)
				break;
			while (true) {
				c = *reader;
				if (c & 128) {
					id |= (c & 0x7F) << shift;
					shift += 7;
					reader++;
					if (reader >= limit)
						break;
				} else {
					if (isfirst) {
						y = id;
						isfirst = false;
					} else {
						id = id + y;
						y = id;
					}
					Yid->insertID(id);
					break;
				}
			}

		}
	}
	Yid->insertID(y + id);
	return limit;
}

ID BucketManager::readYi(const uchar* &reader, const ID y0, const uchar* limit){
	register unsigned shift = 0, c;
	ID id = 0;

	c = *reader;
	if (!(c & 128)) {
		//head = false;
		id |= c << shift;
		shift += 7;
		reader++;

		while (reader < limit) {
			c = *reader;
			if (!(c & 128)) {
				id |= c << shift;
				shift += 7;
				reader++;
			} else {
				break;
			}
		}
	} else {
		//head = true;
		id |= (c & 0x7F) << shift;
		shift += 7;
		reader++;

		while (reader < limit) {
			c = *reader;
			if (c & 128) {
				id |= (c & 0x7F) << shift;
				shift += 7;
				reader++;
			} else {
				break;
			}
		}
	}

	return (id + y0);
}

Status BucketManager::readHead(const uchar* reader, unsigned& xid, unsigned& len) {
	memcpy(&xid, reader, 4);
	reader += 4;
	memcpy(&len, reader, 4);
	reader += 4;

	return OK;
}

char BucketManager::getIDChar(ID id, unsigned char* idStr){
	if(id < 64){	// 1 byte
		*idStr = id;
		return 1;
	}else if(id < (1<<14)){		// 2 byte
		StatisticsBuffer::writeDelta0(idStr,id | 0x4000);
		return 2;
	}else if(id < (1<<22)){
		StatisticsBuffer::writeDelta0(idStr,id | 0x800000);
		return 3;
	}else {
		StatisticsBuffer::writeDelta0(idStr,id | 0xc0000000);
		return 4;
	}
}

BucketManager* BucketManager::load(unsigned pid, char* fileName){
	if(!OSFile::FileExists(fileName)){
		cerr << "File " << fileName << " not exist!" << endl;
		exit(-1);
	}
	BucketManager* manager = new BucketManager();
	manager->bucketFile = new MMapBuffer(fileName,0);
	char* base = manager->bucketFile->get_address();
	manager->meta = (BucketManagerMeta*)(base);
	manager->meta->startPtr = base + sizeof(BucketManagerMeta);
	manager->meta->endPtr = base + manager->meta->usedSpace ;
	char indexFileName[100];
	sprintf(indexFileName,"%s_index",fileName);
	manager->indexBuffer = PageIndex::loadMemory(*manager,indexFileName);
	manager->isDone = true;

	return manager;
}

void BucketManager::load_mmap(unsigned pid, string base_dir, string fileName, Matrixmap* &matrixMap){

	matrixMap->predicate_manager[pid] = load(pid, (char *)((base_dir+fileName).c_str()));
}

BucketManager* BucketManager::loadMemory(unsigned pid, char* fileName){
	if(!OSFile::FileExists(fileName)){
		cerr << "File " << fileName << " not exist!" << endl;
		exit(-1);
	}

	BucketManager* manager = new BucketManager();
	manager->bucketFile = NULL;
	char* base = NULL;
	HashID::loadFileinMemory(fileName,base);
	manager->meta = (BucketManagerMeta*)(base);
	manager->meta->startPtr = base + sizeof(BucketManagerMeta);
	manager->meta->endPtr = base + manager->meta->usedSpace ;
	char indexFileName[100];
	sprintf(indexFileName,"%s_index",fileName);
	manager->indexBuffer = PageIndex::loadMemory(*manager,indexFileName);
	manager->isDone = true;

	return manager;
}

void BucketManager::load_memory(unsigned pid, string base_dir, string fileName, Matrixmap* &matrixMap){
	matrixMap->predicate_manager[pid] = loadMemory(pid, (char *)((base_dir+fileName).c_str()));
}

Status BucketManager::getOffsetByID(ID id, size_t& offset){
	if( indexBuffer->getOffsetByID(id,offset)==-1)
		return NOT_FOUND;
	else
		return OK;
}

Status BucketManager::getYByID(ID id,EntityIDBuffer* entBuffer){
	size_t offset = 0;
	const uchar* reader = NULL;
	ID x, y;

	if (getOffsetByID(id, offset) == NOT_FOUND)
		return NOT_FOUND;

	reader = getStartPtr() + offset;
	memcpy(&x,reader,sizeof(unsigned));
	if (x != id) {
		return NOT_FOUND;
	}
	readChars(reader,x,entBuffer);
	return OK;
}

BucketManager::BucketManager(ID id,Matrixmap* matrixMapBuffer,char* Dir,bool isstream):matrixMap(matrixMapBuffer){
	//init list in matrixmap
	char path[100];
	sprintf(path, "%s/pre%d", Dir, id);

	tempFile = new TempFile(path,0);
	meta = new BucketManagerMeta();
	memset(meta,0,sizeof(BucketManagerMeta));
	meta->pid = id;
	meta->lineCount = 0;
//	meta->length =  matrixMap->getUsedPage(id)* BucketManager::pagesize;
	meta->usedSpace = sizeof(BucketManagerMeta);

	sprintf(path, "%s_index", path);
	indexBuffer = new PageIndex(*this,path);

	result = new MemoryBuffer(BucketManager::pagesize);

	lastXID = 0;
	firstCouple = true;
	isDone = false;

	tempFile->write(sizeof(meta) ,(char*)meta);
}

Status BucketManager::insertStreamrXY(ID xid, EntityIDBuffer* Yid){
	unsigned charLen = getChars(result,xid,Yid);
	char * chars = result->getBuffer();
	tempFile->write(charLen,chars);
	meta->usedSpace += charLen;
	return OK;
}

Status BucketManager::endStream(){
	FILE* fp = fopen(tempFile->getFile().c_str(), "r+");
//	fseek(fp, 0, SEEK_SET);
	fwrite(&meta, sizeof(meta), 1, fp);
	fclose(fp);

	delete meta;
	meta = NULL;
	delete tempFile;
	tempFile = NULL;
	return OK;
}

Status BucketManager::insertNewXY(ID xid, EntityIDBuffer* Yid) {
	LineChunk* line;
	if (edgeBufferFree.size() == 0) {
		MemoryBuffer* result = new MemoryBuffer(4096);
		line = new LineChunk();
		line->result = result;
	} else {
		line = edgeBufferFree.back();
		edgeBufferFree.pop_back();
	}

	unsigned charLen = getBody(line->result, Yid);
	//unsigned charLen = getBody1( line->result,  Yid);
	line->id = xid;
	line->Len = charLen;

	int size, idoff, edgeoff, offvalue;
	if (charLen + 20 >= BucketManager::pagesize) {
		//current chunk is very big, first save previous chunks
		unsigned left = meta->length - meta->usedSpace;
		size = edgeBufferUsed.size();
		setTwo(meta->endPtr, size);
		idoff = 2;
		edgeoff = idoff + size * (2 + sizeof(ID)); //id and their len_offset
		if (size > 0) {
			indexBuffer->insertEntries(edgeBufferUsed[0]->id, meta->endPtr - meta->startPtr);
			for (int i = 0; i < size; i++) {
				memcpy(meta->endPtr + idoff, &(edgeBufferUsed[i]->id), sizeof(ID));
				idoff += sizeof(ID);
				offvalue = edgeoff + edgeBufferUsed[i]->Len - 1;
				setTwo(meta->endPtr + idoff, offvalue);
				idoff += 2;
				memcpy(meta->endPtr + edgeoff, edgeBufferUsed[i]->result->getBuffer(), edgeBufferUsed[i]->Len);
				edgeoff += edgeBufferUsed[i]->Len;
				edgeBufferFree.push_back(edgeBufferUsed[i]);
			}
			edgeBufferUsed.clear();
			meta->usedSpace = meta->length;
		}

		//then store the current chunk
		unsigned resizetime = 0;
		resizetime = (unsigned) ceil(double(charLen + 10 - (meta->length - meta->usedSpace)) / BucketManager::pagesize);
		for (int i = 0; i < resizetime; i++)resize();
		indexBuffer->insertEntries(xid, meta->endPtr - meta->startPtr);

		size = 0; //when the chunk is big ,set the size zero
		idoff = 2;
		setTwo(meta->endPtr, size); //2byte for num
		memcpy(meta->endPtr + idoff, &xid, sizeof(ID)); //4byte for id
		idoff += sizeof(ID);
		offvalue = charLen;
		memcpy(meta->endPtr + idoff, &offvalue, 4); //4byte for length
		idoff += 4;
		memcpy(meta->endPtr + idoff, line->result->getBuffer(), charLen);

		meta->usedSpace = meta->length;
		resize();
		chunkUsed = 2;
		edgeBufferFree.push_back(line);
	} else if (isPtrFull(30 + chunkUsed + charLen) == false) {
		//not full
		chunkUsed += sizeof(ID) + 2 + charLen;
		edgeBufferUsed.push_back(line);
	} else {
		//full
		size = edgeBufferUsed.size();
		setTwo(meta->endPtr, size);
		idoff = 2;
		edgeoff = idoff + size * (2 + sizeof(ID)); //id and their len_offset
		if (size > 0)indexBuffer->insertEntries(edgeBufferUsed[0]->id, meta->endPtr - meta->startPtr);

		for (int i = 0; i < size; i++) {
			memcpy(meta->endPtr + idoff, &(edgeBufferUsed[i]->id), sizeof(ID));
			idoff += sizeof(ID);
			offvalue = edgeoff + edgeBufferUsed[i]->Len - 1;
			setTwo(meta->endPtr + idoff, offvalue);
			idoff += 2;
			memcpy(meta->endPtr + edgeoff, edgeBufferUsed[i]->result->getBuffer(), edgeBufferUsed[i]->Len);
			edgeoff += edgeBufferUsed[i]->Len;
			if (edgeoff >= BucketManager::pagesize) {
				cout << "error edgeoff:" << edgeoff << endl;
				assert(false);
			}
			edgeBufferFree.push_back(edgeBufferUsed[i]);
		}

		edgeBufferUsed.clear();
		meta->usedSpace = meta->length;
		resize();
		firstCouple = false;
		chunkUsed = 2 + sizeof(ID) + 2 + charLen;
		edgeBufferUsed.push_back(line);
	}

	return OK;
}

Status BucketManager::endNewInsert() {
	int size, idoff, edgeoff, offvalue;
	size = edgeBufferUsed.size();
	setTwo(meta->endPtr, size);
	idoff = 2;
	edgeoff = idoff + size * (2 + sizeof(ID)); //id and their len_offset
	if (size > 0)indexBuffer->insertEntries(edgeBufferUsed[0]->id, meta->endPtr - meta->startPtr);

	for (int i = 0; i < size; i++) {
		memcpy(meta->endPtr + idoff, &(edgeBufferUsed[i]->id), sizeof(ID));
		idoff += sizeof(ID);
		offvalue = edgeoff + edgeBufferUsed[i]->Len - 1;
		setTwo(meta->endPtr + idoff, offvalue);
		idoff += 2;
		memcpy(meta->endPtr + edgeoff, edgeBufferUsed[i]->result->getBuffer(), edgeBufferUsed[i]->Len);
		edgeoff += edgeBufferUsed[i]->Len;
		edgeBufferFree.push_back(edgeBufferUsed[i]);
	}
	meta->usedSpace = meta->length;
	edgeBufferUsed.clear();

	size = edgeBufferFree.size();
	for (int i = 0; i < size; i++) {
		delete edgeBufferFree[i];
		edgeBufferFree[i] = NULL;
	}
	edgeBufferFree.clear();

	insertEntBuffer->empty();
	indexBuffer->endInsert();
	bucketFile->resize(meta->usedSpace, false);
	bucketFile->close();

	delete bucketFile;
	bucketFile = NULL;
	meta = NULL;
	isDone = true;
	return OK;
}
