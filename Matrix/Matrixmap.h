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

#ifndef MATRIXMAP_H_
#define MATRIXMAP_H_

class PageIndex;
class BucketManager;

#include "TripleBit.h"
#include "MMapBuffer.h"
#include "MemoryBuffer.h"
#include "PageIndex.h"
#include "EntityIDBuffer.h"
#include </usr/include/regex.h>
#include <ext/hash_map>
#include <vector>
#include "CycleCache.h"
#include "TempFile.h"

namespace std{
 using namespace __gnu_cxx;
}

class Matrixmap {
	hash_map<ID,MMapBuffer* > preFileMap;
	hash_map<ID,size_t> usedPage;
	char Dir[100];
	char matrixPath[120];

	int degree;
	map<char,CycleCache* > cycCache;
public:
	bool isLoad;
	hash_map<ID, BucketManager*> predicate_manager;
	friend class BucketManager;
	bool isMemory;
	ID start_ID[100];
	ID maxID;
	unsigned bucSize;

	Matrixmap(const char * upperDir,const char* mapName,vector<ID> preList = vector<ID>());
	virtual ~Matrixmap();
	Status insertXY(ID preID,ID xid,EntityIDBuffer* &Y);
	Status endInsertXY();
	Status endInsertXY(ID);
	BucketManager* getBucketManager(ID id);
	char* getNewPage(ID preID);
	Status save();
	unsigned getUsedPage(ID preId){
		return usedPage[preId];
	}

	hash_map<ID, BucketManager*> getPredicate_manager() {
		return predicate_manager;
	}
	static Matrixmap* load(const char* matrixDir,const char * name,bool isMemory);
	static Matrixmap* parallel_load(const char* upperDir,const char* Name,bool isMemory);
	bool done();
	int getDegree(){ return degree;}

	Status getAllY(ID xid, EntityIDBuffer* &entBuffer, CycleCache* cache = NULL);
	Status getAllY(ID xid, EntityIDBuffer* &entBuffer, bool cacheFlag);
	Status getAllYByID(ID id,EntityIDBuffer* &entBuffer,ID preID);
	void clearCache(ID preID){
		cycCache[preID]->clear();
	}
	size_t getSize();

	void initStartID(ID* start,unsigned len){
		memcpy(start_ID,start,len*sizeof(ID));
		bucSize = len -1;
		maxID = start_ID[bucSize]-1;
	}
};

//////////////////////////////////////////////////////////////////////////////////////
struct BucketManagerMeta
{
	size_t length;
	size_t usedSpace;
	int lineCount;
	unsigned pid;
	char* startPtr;
	char* endPtr;
};

//////////////////////////////////////////////////////////////////////////////////////
class BucketManager {

private:
	BucketManagerMeta* meta;
	Matrixmap* matrixMap;
	MMapBuffer* bucketFile;
	EntityIDBuffer* tempEntBuffer, *curEntBuffer, *insertEntBuffer;
	ID lastXID;
	PageIndex* indexBuffer;
	char* ptrs;
	bool firstCouple;
	bool isDone;
	MemoryBuffer* result;

public:
	friend class Matrixmap;
	BucketManager();
	BucketManager(ID, Matrixmap*,char* Dir);
	virtual ~BucketManager();

	Status insertGatherXY(ID xid, EntityIDBuffer* Yid);
	Status insertXY(ID xid, EntityIDBuffer* &Yid);
	static unsigned getChars(MemoryBuffer* result, ID Xid,EntityIDBuffer* Yid); // return the lengh
	static size_t getBody(MemoryBuffer* result, EntityIDBuffer* Yid);
	static size_t getBody1(MemoryBuffer* result, EntityIDBuffer* Yid);
	static const uchar* readChars(const uchar* reader,ID& Xid,EntityIDBuffer* Yid);
	static Status readHead (const uchar* reader,unsigned& xid,unsigned& len);
	static const uchar* readBody(const uchar* reader,unsigned len,EntityIDBuffer* Yid);
	static ID readYi(const uchar* &reader, const ID y0, const uchar* limit);
	char getIDChar(ID id, unsigned char* idStr);
	Status endInsertXY();
	bool isPtrFull(unsigned len);
	bool done(){return isDone;};
	Status getYByID(ID id,EntityIDBuffer* entBuffer);
	Status resize();
	Status save();
	void flush();

	uchar* getStartPtr() {
		return reinterpret_cast<uchar*> (meta->startPtr);	}

	uchar* getEndPtr() {
		return reinterpret_cast<uchar*> (meta->endPtr);
	}
	unsigned getLineCount(){
		return meta->lineCount;
	}
	PageIndex* getIndex(){
		return indexBuffer;
	}
	size_t getUsedSize(){
		return meta->usedSpace;
	}
	Status getOffsetByID(ID id, size_t& offset);
	static BucketManager* load(unsigned pid, char* fileName);
	static void load_mmap(unsigned pid, string base_dir, string fileName, Matrixmap* &matrixMap);
	static BucketManager* loadMemory(unsigned pid, char* fileName);
	static void load_memory(unsigned pid, string base_dir, string fileName, Matrixmap* &matrixMap);

	//-------------------------add by stream--------------------------------
	TempFile* tempFile;
	BucketManager(ID, Matrixmap*,char* Dir,bool);
	Status insertStreamrXY(ID xid, EntityIDBuffer* Yid);
	Status endStream();
	//---------------------------------------------------------------------

public:
	static unsigned pagesize;
	class LineChunk {
	public:
		ID id;
		unsigned Len;
		MemoryBuffer* result;
		virtual ~LineChunk(){
			delete result;
			result = NULL;
		}
	};
	unsigned coupleNum;
	unsigned chunkUsed;
	vector<LineChunk* > edgeBufferUsed;
	vector<LineChunk* > edgeBufferFree;

	Status insertNewXY(ID xid, EntityIDBuffer* Yid);
	Status endNewInsert();

	inline static void setTwo(char* writer,int x){
		unsigned char c = static_cast<unsigned char> (x | 0);
		*(writer++) = c;
		c = static_cast<unsigned char> ((x>>8) | 0);
		*(writer++) = c;
	}

	inline static void getTwo(const uchar* reader,int& x){
		x=0;
		unsigned char c = *reinterpret_cast<const unsigned char*> (reader++);
		x |= static_cast<ID> (c) ;
		c = *reinterpret_cast<const unsigned char*> (reader);
		x |= static_cast<ID> (c) <<8 ;
	}
};
#endif /* MATRIXMAP_H_ */
