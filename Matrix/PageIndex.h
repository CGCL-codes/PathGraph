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

#ifndef PAGEINDEX_H_
#define PAGEINDEX_H_

#include "MemoryBuffer.h"
#include "MMapBuffer.h"
#include "Matrixmap.h"
#include "EntityIDBuffer.h"

struct Point {
	ID x;
	size_t y;
};
class PageIndex {
public:

private:
	MMapBuffer* idOffsetTable;
	Point* idTableEntries;
//	char fileName[100];
	unsigned tableSize;
	BucketManager& manager;
public:
	PageIndex(BucketManager&,char *);
	PageIndex(BucketManager&);
	virtual ~PageIndex();
	void insertEntries(ID id, size_t offset);
	void endInsert();
	int searchChunk(ID id);
	int getOffsetByID(ID id, size_t& offset);
	Status getYByID(ID id,EntityIDBuffer* entBuffer);
	static PageIndex* load(BucketManager& manager,char * fileName);
	static PageIndex* loadMemory(BucketManager& manager,char * fileName);
	void flush();
	Point* getidOffsetTable(){return idTableEntries;}
	void setidOffsetTable(Point *entries){ idTableEntries = entries; }
	unsigned getTableSize(){ return tableSize; }
	void setTableSize(unsigned size){ tableSize = size; }
};

#endif /* PAGEINDEX_H_ */
