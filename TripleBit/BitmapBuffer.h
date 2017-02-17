//---------------------------------------------------------------------------
// TripleBit
// (c) 2011 Massive Data Management Group @ SCTS & CGCL. 
//     Web site: http://grid.hust.edu.cn/triplebit
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------

#ifndef CHUNKMANAGER_H_
#define CHUNKMANAGER_H_

class MemoryBuffer;
class MMapBuffer;
class Chunk;
class ChunkManager;

#include "TripleBit.h"
#include "HashIndex.h"
#include "LineHashIndex.h"
#include "ThreadPool.h"


///////////////////////////////////////////////////////////////////////////////////////////////
///// class BitmapBuffer
///////////////////////////////////////////////////////////////////////////////////////////////
class BitmapBuffer {
public:
	map<ID, ChunkManager*> predicate_managers[2];
	ID startColID;
	const string dir;
	MMapBuffer *temp1, *temp2, *temp3, *temp4;
	size_t usedPage1, usedPage2, usedPage3, usedPage4;
public:
	BitmapBuffer(const string dir);
	BitmapBuffer() : startColID(0), dir("") {}
	~BitmapBuffer();
	/// insert a predicate given specified sorting type and predicate id 
	Status insertPredicate(ID predicateID, unsigned char typeID);
	Status deletePredicate(ID);
	Status insertTriple(ID, ID, unsigned char, ID, unsigned char,bool);
	/// insert a triple;
	Status insertTriple(ID predicateID, ID xID, ID yID, bool isBigger, unsigned char typeID);
	/// get the chunk manager (i.e. the predicate) given the specified type and predicate id
	ChunkManager* getChunkManager(ID, unsigned char);
	/// get the count of chunk manager (i.e. the predicate count) given the specified type

	size_t getSize(unsigned  type);
	Status completeInsert();
	void insert(ID predicateID, ID subjectID, ID objectID);
	void insert(ID predicateID, ID subjectID, ID objectID, bool isBigger, unsigned char flag);
	size_t getTripleCount();
	void flush();
	ID getColCnt() { return startColID; }
	char* getPage(unsigned char type, unsigned char flag, size_t& pageNo);

	void save();
	static BitmapBuffer* load(MMapBuffer* bitmapImage, MMapBuffer*& bitmapIndexImage, MMapBuffer* bitmapPredicateImage);
	static BitmapBuffer* loadPart(MMapBuffer* bitmapImage, MMapBuffer*& bitmapIndexImage, MMapBuffer* bitmapPredicateImage);


	Status getPredicateList(vector<ID>& preList){
		preList.clear();
		map<ID, ChunkManager*>::const_iterator map_it = predicate_managers[0].begin();
		map<ID, ChunkManager*>::const_iterator map_end = predicate_managers[0].end();
		while(map_it != map_end){
			preList.push_back(map_it->first);
			++map_it;
		}
		return OK;
	}
	/// generate the x and y;
	void generateXY(ID& subjectID, ID& objectID);
	/// get the bytes of a id;
	unsigned char getBytes(ID id);
	/// get the storage space (in bytes) of a id;
	static unsigned char getLen(ID id);
};

/////////////////////////////////////////////////////////////////////////////////////////////
///////// class ChunkManager
/////////////////////////////////////////////////////////////////////////////////////////////
struct ChunkManagerMeta
{
	size_t length[2];
	size_t usedSpace[2];
	int tripleCount[2];
	unsigned type;
	unsigned pid;
	char* startPtr[2];
	char* endPtr[2];
};

class ChunkManager {
private:
	char* ptrs[2];

	ChunkManagerMeta* meta;
	///the No. of buffer
	static unsigned int bufferCount;

	///hash index; index the subject and object
	//HashIndex* hashIndex[2];
	//ChunkIndex* chunkIndex[2];
	LineHashIndex* chunkIndex[2];

	BitmapBuffer* bitmapBuffer;
	vector<size_t> usedPage[2];
public:
	friend class BuildSortTask;
	friend class BuildMergeTask;
	friend class HashIndex;
	friend class BitmapBuffer;

	ChunkManager() {}
	ChunkManager(unsigned pid, unsigned _type, BitmapBuffer* bitmapBuffer);
	~ChunkManager();
	Status resize(unsigned char type);
	int findChunkPosByPtr(char* chunkPtr, int& offSet);
	Status insertChunk(unsigned char);
	Status deleteChunk();
	Status optimize();
	Status tripleCountAdd(unsigned char type) {
		meta->tripleCount[type - 1]++;
		return OK;
	};

	LineHashIndex* getChunkIndex(int type) {
		if(type > 2 || type < 1) {
			return NULL;
		}
		return chunkIndex[type - 1];
	}

	int getTrpileCount(unsigned char type) {
		return meta->tripleCount[type - 1];
	}

	bool isPtrFull(unsigned char type, size_t len);

	int getTripleCount() {
		return meta->tripleCount[0] + meta->tripleCount[1];
	}

	unsigned int getPredicateID() const {
		return meta->pid;
	}

	void insertXY(unsigned x, unsigned y, size_t len, unsigned char type);

	uchar* getStartPtr(unsigned char type) {
		return reinterpret_cast<uchar*> (meta->startPtr[type -1]);
	}

	uchar* getEndPtr(unsigned char type) {
		return reinterpret_cast<uchar*> (meta->endPtr[type -1]);
	}

	size_t getUsedSpace(unsigned char type){
		return meta->usedSpace[type-1];
	}



	Status buildChunkIndex();
	//Status getChunkByID(ID id, unsigned int typeID, unsigned& chunkPos);
	Status getChunkPosByID(ID id, unsigned typeID,size_t& offset);
	void setColStartAndEnd(ID& startColID);
	void save(ofstream& ofile);
	Status getAllYByID(ID id,EntityIDBuffer* entBuffer);
	static ChunkManager* load(unsigned pid, unsigned type, char* buffer, size_t& offset);
	static void unload(unsigned pid, unsigned type, char* buffer, size_t& offset);
};

///////////////////////////////////////////////////////////////////////////////////////////////////////

#include "BitVectorWAH.h"
#include "MemoryBuffer.h"

class Chunk {
private:
	unsigned char type;
	unsigned int count;
	ID xMax;
	ID xMin;
	ID yMax;
	ID yMin;
	ID colStart;
	ID colEnd;
	char* startPtr;
	char* endPtr;
	BitVectorWAH * flagVector;
	vector<bool>* soFlags;
public:
//	boost::dynamic_bitset<> flagVector;
	Chunk(unsigned char, ID, ID, ID, ID, char*, char*);
	Chunk(unsigned char, ID, ID, ID, ID, char*, char*, BitVectorWAH*);
	static void writeXId(ID id, char*& ptr);
	static void writeYId(ID id, char*& ptr);
	Status insertXY(ID, ID, unsigned char, unsigned char, bool);
	Status insertXY(ID, ID, bool);
	Status completeInsert(ID& startColID);
	~Chunk();
	unsigned int getCount() { return count; }
	void addCount() { count++; }
	bool getSOFlags(unsigned int pos) {
		return (*soFlags)[pos];
	}
	Status setSOFlags(unsigned int pos, bool value) {
		(*soFlags)[pos] = value;
		return OK;
	}
	vector<bool>* getSOFlagsPtr() {
		return soFlags;
	}
	bool isChunkFull() {
		//unsigned char type = this->type;
		return (unsigned int) (endPtr - startPtr + Type_2_Length(type)) > CHUNK_SIZE * MemoryBuffer::pagesize ? true : false;
	}
	bool isChunkFull(unsigned char len) {
			//unsigned char type = this->type;
			return (unsigned int) (endPtr - startPtr + len) > CHUNK_SIZE * MemoryBuffer::pagesize ? true : false;
	}
	unsigned char getType() {
		return type;
	}
	/// Read a subject id
	static const uchar* readXId(const uchar* reader, register ID& id);
	/// Read an object id
	static const uchar* readYId(const uchar* reader, register ID& id);
	/// Skip a s or o
	static const uchar* skipId(const uchar* reader, unsigned char flag);
	/// Skip backward to s
	static const uchar* skipBackward(const uchar* reader);
	static const uchar* skipForward(const uchar* reader);
	ID getXMax(void) {
		return xMax;
	}
	ID getXMin() {
		return xMin;
	}
	ID getYMax() {
		return yMax;
	}
	ID getYMin() {
		return yMin;
	}
	char* getStartPtr() {
		return startPtr;
	}
	char* getEndPtr() {
		return endPtr;
	}

	ID getColStart() const {
		return colStart;
	}

	ID getColEnd() const {
		return colEnd;
	}

	Status getSO(ID& subjectID, ID& objectID, ID pos);

	BitVectorWAH* getFlagVector() const {
		return flagVector;
	}

	void setColStart(ID _colStart) {
		colStart = _colStart;
	}

	void setColEnd(ID _colEnd) {
		colEnd = _colEnd;
	}

	void setStartPtr(char* ptr){
		startPtr = ptr;
	}

	void setEndPtr(char* ptr) {
		endPtr = ptr;
	}

	void setFlagVector(BitVectorWAH* _flagVector) {
		flagVector = _flagVector;
	}
};
#endif /* CHUNKMANAGER_H_ */
