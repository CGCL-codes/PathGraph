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

#ifndef FOLDLINEINDEX_H_
#define FOLDLINEINDEX_H_

class MemoryBuffer;
class ChunkManager;
class MMapBuffer;

#include "TripleBit.h"
#include "EntityIDBuffer.h"
#include "TempFile.h"

class FoldLineIndex {
public:
	struct Point{
		ID x;
		ID y;
	};
	struct Line{
		double k;
		double b;
		ID startID;
	};

	enum IndexType { SUBJECT_INDEX, OBJECT_INDEX};
private:
	MemoryBuffer* idTable;
	MemoryBuffer* offsetTable;
	ID* idTableEntries;
	ID* offsetTableEntries;
	ChunkManager& chunkManager;
	IndexType indexType;
	unsigned int tableSize;
	unsigned int regionNum;
	unsigned int regionSize;
	unsigned int idRange;
	vector<MemoryBuffer*> regionList;


private:
	void insertEntries(ID id, unsigned offset);
	int searchChunk(ID id);
	bool buildLine(int startEntry, int endEntry, int lineNo);
public:
	FoldLineIndex(ChunkManager& _chunkManager, IndexType type);
	FoldLineIndex();
	Status buildIndex(unsigned chunkType);
	Status buildIndexTriple(const char* ,const char* );
	Status getOffsetByID(ID id, unsigned& offset, unsigned typeID);
	Status getFirstOffsetByID(ID id, unsigned& offset, unsigned typeID);
	Status getYByID(ID id,EntityIDBuffer* entBuffer,unsigned typeID);
	void save(MMapBuffer*& indexBuffer);
	virtual ~FoldLineIndex();
private:
	bool isBufferFull();
public:
	static FoldLineIndex* load(ChunkManager& manager, IndexType type, char* buffer, size_t& offset);
	static void unload( char* buffer, size_t& offset);
};

#endif /* LINEHASHINDEX_H_ */
