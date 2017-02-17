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

#include "PageIndex.h"
#include "sys/mman.h"
#include "OSFile.h"
#include "HashID.h"

PageIndex::PageIndex(BucketManager& _manager ,char *_fileName):manager(_manager) {
	idOffsetTable = NULL;
	idTableEntries = NULL;
	tableSize = 0;

	if (idOffsetTable == NULL) {
		idOffsetTable = MMapBuffer::create(_fileName, MemoryBuffer::pagesize);
		idTableEntries = (Point*) (idOffsetTable->get_address()
				+ sizeof(unsigned));
	}
}

PageIndex::PageIndex(BucketManager& _manager ):manager(_manager) {
	idOffsetTable = NULL;
	idTableEntries = NULL;
	tableSize = 0;
}


PageIndex::~PageIndex() {
	if(idOffsetTable){
    	delete idOffsetTable;
    }
	else if(idTableEntries){
        char* buf = (char*)idTableEntries - sizeof(unsigned);
        free(buf);
    }
	idOffsetTable = NULL;
	idTableEntries = NULL;
}

void PageIndex::insertEntries(ID id, size_t offset){
	// is full ? resize
	if((tableSize +1) * sizeof(Point)+ sizeof(unsigned) > idOffsetTable->get_length()){
		idOffsetTable->resize(idOffsetTable->get_length() + MemoryBuffer::pagesize,false);
		idTableEntries = (Point*)(idOffsetTable->get_address()+ sizeof(unsigned));
	}
	Point idOffset;
	idOffset.x = id;
	idOffset.y = offset;
	idTableEntries[tableSize] = idOffset;
	tableSize++;
}

void PageIndex::endInsert(){
	*(unsigned *)idOffsetTable->get_address() = tableSize;
	idOffsetTable->flush();
}

void PageIndex::flush(){
	idOffsetTable->flush();
}


int PageIndex::searchChunk(ID id){

	int low = 0,mid = 0;
	int high = tableSize-1;

	if(id < idTableEntries[0].x)
		return -1;
	else if(id >= idTableEntries[high].x)
		low = mid = high;

	while (low != high) {
		mid = low + (high - low) / 2;
		if (id > idTableEntries[mid].x) {
			low = mid + 1;
		} else if ((!mid) || id > idTableEntries[mid - 1].x) {
			break;
		} else {
			high = mid;
		}
	}
	int res = mid;
	if (idTableEntries[res].x > id && res > 0) {
		res--;
		while (idTableEntries[res].x > id && res > 0)
			res--;
	} else {
		while (res + 1 < tableSize && idTableEntries[res].x < id
				&& idTableEntries[res + 1].x < id)
			res++;
	}
	return res;
}

PageIndex* PageIndex::load(BucketManager& _manager,char* fileName){
	if (!OSFile::FileExists(fileName)) {
		cerr << "File " << fileName << " not exist!" << endl;
		exit(-1);
	}
	PageIndex* index = new PageIndex(_manager,fileName);
	index->idOffsetTable =  new MMapBuffer(fileName, 0);
	memcpy(&index->tableSize,index->idOffsetTable->get_address(),sizeof(unsigned));
	index->idTableEntries = (Point*)(index->idOffsetTable->get_address()+sizeof(unsigned));
	return index;
}

PageIndex* PageIndex::loadMemory(BucketManager& _manager,char* fileName){
	if (!OSFile::FileExists(fileName)) {
		cerr << "File " << fileName << " not exist!" << endl;
		exit(-1);
	}
	PageIndex* index = new PageIndex(_manager);
	char * buf = NULL;
	HashID::loadFileinMemory(fileName,buf);
	memcpy(&index->tableSize, buf,
			sizeof(unsigned));
	index->idTableEntries = (Point*) (buf
			+ sizeof(unsigned));
    return index;
}

int PageIndex::getOffsetByID(ID id, size_t& offset){
	if(tableSize == 0)
		return NOT_FOUND;

	int offsetId = this->searchChunk(id);
	if (offsetId == -1) {
		return NOT_FOUND;
	}

	size_t pBegin = idTableEntries[offsetId].y;
	uchar* reader = manager.getStartPtr() + pBegin;
	int size,low,high,mid,midvalue,len,edgeoff;
	BucketManager::getTwo(reader,size);
	reader +=2;
	if(size == 0){
		midvalue = *(ID*)(reader);
		if(midvalue== id){
			offset = pBegin + 10;
			len = *(ID*)(reader+4);
			return len;
		}else
			return -1;
	}
	low = 0;
	high = size-1;
	while(low <= high){
		mid = (low + high) / 2;
		midvalue = *(ID*)(reader+mid*6);
		if (midvalue == id) {
			low = mid;
			break;
		} else if (midvalue< id)
			low = mid + 1;
		else
			high = mid-1;
	}

	reader +=  mid * 6;
	if(midvalue == id){
		BucketManager::getTwo((reader +4), len);
		if (mid != 0) {
			BucketManager::getTwo((reader - 2), edgeoff);
			offset = pBegin + edgeoff+1;
			len = len -edgeoff;
		}else{
			edgeoff = 2+size*6;
			offset= pBegin+edgeoff;
			len = len+1 -edgeoff;
		}
		return len;
	}else
		return -1;
}

Status PageIndex::getYByID(ID id,EntityIDBuffer* entBuffer){
	size_t offset = 0;
	const uchar* reader = NULL;
	ID x, y;
	int len = getOffsetByID(id, offset);

	if (len == -1)
		return NOT_FOUND;

	reader = manager.getStartPtr() + offset;
	BucketManager::readBody(reader,len,entBuffer);

	return OK;
}
