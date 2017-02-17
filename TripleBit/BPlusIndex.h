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

#ifndef __BPLUS_INDEX_H__
#define __BPLUS_INDEX_H__

class MMapBuffer;
class ChunkManager;
class MemoryBuffer;

#include "TripleBit.h"

class BPlusIndex {
public:
	enum IndexType {SUBJECT_INDEX, OBJECT_INDEX};
private:
	ChunkManager& manager;
	MemoryBuffer* indexBuffer;
	size_t indexSize;

	unsigned writerIndex;
	unsigned currentIndex;
	unsigned recordCount;
	unsigned pageCount;

	ID* indexEntry;
	IndexType type;
public:
	BPlusIndex(ChunkManager& _manager, IndexType indexType);
	Status buildIndex(char chunkType);
	Status getOffsetByID(ID id, unsigned &offset, unsigned typeId);
	void save(MMapBuffer* & buffer);
	~BPlusIndex();

private:
	int searchChunk(ID id, unsigned& start, unsigned& end);
	void insertEntry(ID id, unsigned offset);
	void packInnerNode();
public:
	static BPlusIndex* load(ChunkManager& manager, IndexType type, char* buffer, size_t& offset);
};

#endif /* __BPLUS_INDEX_H__ */
