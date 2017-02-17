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

#include "FoldLineIndex.h"
#include "MMapBuffer.h"
#include "MemoryBuffer.h"
#include "BitmapBuffer.h"
#include "float.h"
#include "TempFile.h"

#include <math.h>


unsigned LINE_POINT_NUM = 16;
unsigned XDifference = 6;
/**
 * linear fit;
 * f(x)=kx + b;
 * used to calculate the parameter k and b;
 */
static bool calculateLineKB(vector<FoldLineIndex::Point>& a, double& k, double& b, int pointNo)
{
	if (pointNo < 2){
		k = 0;
		b = a[0].y;
		return false;
	}

	double mX, mY, mXX, mXY;
	mX = mY = mXX = mXY = 0;
	int i;
	for (i = 0; i < pointNo; i++) {
		mX += a[i].x;
		mY += a[i].y;
		mXX += (double)a[i].x * a[i].x;
		mXY += (double)a[i].x * a[i].y;
	}

	if (mX * mX - mXX * pointNo == 0){
		k = DBL_MAX;
		b = a[0].y;
		return false;
	}

	k = (mY * mX - mXY * pointNo) / (mX * mX - mXX * pointNo);
	b = (mXY * mX - mY * mXX) / (mX * mX - mXX * pointNo);
	return true;
}

FoldLineIndex::FoldLineIndex(ChunkManager& _chunkManager, IndexType type) : chunkManager(_chunkManager), indexType(type){
	// TODO Auto-generated constructor stub
	idTable = NULL;
	offsetTable = NULL;
	idTableEntries = NULL;
	offsetTableEntries = NULL;

}

FoldLineIndex::FoldLineIndex():chunkManager(*(new ChunkManager())) {
	// TODO Auto-generated constructor stub
	idTable = NULL;
	offsetTable = NULL;
	idTableEntries = NULL;
	offsetTableEntries = NULL;

}

FoldLineIndex::~FoldLineIndex() {
	// TODO Auto-generated destructor stub
//	delete idTable;
	idTable = NULL;
//	delete offsetTable;
	offsetTable = NULL;
	idTableEntries = NULL;
	offsetTableEntries = NULL;

	for(unsigned i = 0; i < regionList.size(); i++)	{
		/*if(regionList[i] != NULL)
			delete regionList[i];*/
		regionList[i] = NULL;
	}
	regionList.clear();
}

/**
 * From startEntry to endEntry in idtableEntries build a line;
 * @param lineNo: the lineNo-th line to be build;
 */
bool FoldLineIndex::buildLine(int startEntry, int endEntry, int lineNo)
{
	vector<Point> vpt;
	Point pt;
	Line line;
	int i;
	double ktemp, btemp;
	Line* lineTable = (Line*)(regionList[lineNo]->getBuffer()+sizeof(unsigned));
	unsigned lineTableSize = 0;
	static int k = 0;

	ID lastX = idTableEntries[startEntry];
	for (i = startEntry; i < endEntry; i++) {
		pt.x = idTableEntries[i];
		pt.y = i;

		if(vpt.size() <LINE_POINT_NUM || pt.x - lastX <XDifference)
			vpt.push_back(pt);
		else{

			calculateLineKB(vpt, ktemp, btemp, vpt.size());
			line.startID = vpt[0].x;
			line.k = ktemp;
			line.b = btemp;
			if((4+ (lineTableSize+1)* sizeof(Line) ) >= regionList[lineNo]->getSize() ){
				regionList[lineNo]->resize(HASH_CAPACITY);
				lineTable = (Line*)(regionList[lineNo]->getBuffer()+sizeof(unsigned));
			}
			lineTable[lineTableSize] = line;
			lineTableSize++;

			vpt.clear();
			vpt.push_back(pt);
			lastX = pt.x;
		}
	}
	if(vpt.size() > 0){
		calculateLineKB(vpt, ktemp, btemp, vpt.size());
		line.startID = vpt[0].x;
		line.k = ktemp;
		line.b = btemp;

		if (sizeof(unsigned) + (lineTableSize + 1) * sizeof(Line) >= regionList[lineNo]->getSize()) {
			regionList[lineNo]->resize(HASH_CAPACITY);
			lineTable = (Line*)(regionList[lineNo]->getBuffer()+sizeof(unsigned));
		}
		lineTable[lineTableSize] = line;
		lineTableSize++;
	}
	memcpy(regionList[lineNo]->getBuffer(),&lineTableSize,sizeof(unsigned));
	regionSize = regionSize + sizeof(unsigned) + lineTableSize * sizeof(Line);
	return true;
}
Status FoldLineIndex::buildIndexTriple(const char* begin,const char* limit){
	if (idTable == NULL) {
		idTable = new MemoryBuffer(HASH_CAPACITY);
		tableSize = idTable->getSize() / sizeof(unsigned);
		offsetTable = new MemoryBuffer(HASH_CAPACITY);
		idTableEntries = (ID*) idTable->getBuffer();
		offsetTableEntries = (ID*) offsetTable->getBuffer();
		tableSize = 0;
		idRange = 0;
		regionNum = 0;
		regionSize = 0;
	}

	const char *reader;
	ID x, y;
	ID lastX = 0;
	ID maxX = 0;

	int startEntry = 0, endEntry = 0;

	reader = begin;
	if (begin == limit)
		return OK;

	//get the idRange

	TempFile::readId(limit-2*sizeof(ID),maxX);
	int chunkNum = (int) ::ceil((double) (limit-begin) / MemoryBuffer::pagesize);
	regionNum = 10 * (int) log10(chunkNum) + 1;

	idRange = maxX / regionNum + 1;
	//		cout << "idRange:" << idRange << endl;
	//regionList.clear();
	assert(regionList.size() == 0);

	x = 0;
	TempFile::readId(reader,x);
	insertEntries(x, 0);
	reader = reader + (int) MemoryBuffer::pagesize;

	while (reader < limit) {
		x = 0;
		TempFile::readId(reader,x);
		if (x / idRange > regionList.size()) { // x/idRange > regionCount
			startEntry = endEntry;
			endEntry = tableSize;
			regionList.push_back(new MemoryBuffer(HASH_CAPACITY));
			assert(lastX/idRange +1 == regionList.size());
			buildLine(startEntry, endEntry, regionList.size() - 1);
			for (unsigned i = regionList.size(); i < x / idRange; i++) {
				regionList.push_back(NULL);
				regionSize += sizeof(unsigned);

			}
			lastX = x;
		}
		insertEntries(x, reader - begin);
		reader = reader + (int) MemoryBuffer::pagesize;

	}

	insertEntries(maxX, limit-2*sizeof(ID)-begin);

	startEntry = endEntry;
	endEntry = tableSize;

	regionList.push_back(new MemoryBuffer(HASH_CAPACITY));
	assert(lastX/idRange +1 == regionList.size());
	buildLine(startEntry, endEntry, regionList.size() - 1);


	regionNum = regionList.size();
	//	cout << "maxX:"<< maxX<< "  idRange:" <<idRange <<"  regionSize:" <<regionSize << " regionNum:" << regionNum << "  tableSize:"<< tableSize<< endl;
	//	if(regionNum !=1)
	assert((int) ::ceil(maxX/idRange +1 )== regionNum);
	return OK;
}

Status FoldLineIndex::buildIndex(unsigned chunkType)
{
	if (idTable == NULL) {
		idTable = new MemoryBuffer(HASH_CAPACITY);
		tableSize = idTable->getSize() / sizeof(unsigned);
		offsetTable = new MemoryBuffer(HASH_CAPACITY);
		idTableEntries = (ID*) idTable->getBuffer();
		offsetTableEntries = (ID*) offsetTable->getBuffer();
		tableSize = 0;
		idRange = 0;
		regionNum = 0;
		regionSize = 0;
	}

	const uchar* begin, *limit, *reader;
	ID x, y;
	ID lastX = 0;
	ID maxX = 0;

	int startEntry = 0, endEntry = 0;

	if (chunkType == 1) {
		reader = chunkManager.getStartPtr(1);
		begin = reader;
		if (chunkManager.getStartPtr(chunkType) == chunkManager.getEndPtr(chunkType))
			return OK;

		limit = chunkManager.getEndPtr(1);

		//get the idRange
		Chunk::readXId(Chunk::skipBackward(limit), maxX);
		int chunkNum = (int)::ceil((double)(chunkManager.getUsedSpace(chunkType)+ sizeof(ChunkManagerMeta))/MemoryBuffer::pagesize);
		regionNum = 10*(int)log10(chunkNum) +1;

		idRange = maxX/regionNum+1;
//		cout << "idRange:" << idRange << endl;
		//regionList.clear();
		assert(regionList.size() == 0);

		x = 0;
		reader = Chunk::readXId(reader, x);
		insertEntries(x, 0);
		reader = reader + (int) MemoryBuffer::pagesize;

		while (reader < limit) {
			x = 0;
			const uchar* temp = Chunk::skipBackward(reader);
			Chunk::readXId(temp, x);
			if (x / idRange > regionList.size()) {	// x/idRange > regionCount
				/*for(unsigned i = regionCount+1; i< x/idRange; i++)
					regionList.push_back(NULL);*/
				startEntry = endEntry;
				endEntry = tableSize;
				regionList.push_back(new MemoryBuffer(HASH_CAPACITY));
				assert(lastX/idRange +1 == regionList.size());
				buildLine(startEntry, endEntry, regionList.size()-1) ;
				for (unsigned i = regionList.size(); i < x / idRange; i++){
					regionList.push_back(NULL);
					regionSize += sizeof(unsigned);

				}
				lastX = x;
			}
			insertEntries(x, temp - begin);
			reader = reader + (int) MemoryBuffer::pagesize;
			maxX = x;
		}

		reader = Chunk::skipBackward(limit);
		x = 0;
		Chunk::readXId(reader, x);
		insertEntries(x, reader - begin);

		startEntry = endEntry; endEntry = tableSize;

		regionList.push_back(new MemoryBuffer(HASH_CAPACITY));
		assert(lastX/idRange +1 == regionList.size());
		buildLine(startEntry, endEntry, regionList.size()-1);

	}


	if (chunkType == 2) {
		reader = chunkManager.getStartPtr(2);
		begin = reader;
		if (chunkManager.getStartPtr(chunkType) == chunkManager.getEndPtr(
				chunkType))
			return OK;

		limit = chunkManager.getEndPtr(2);
		//get the idRange
		Chunk::readYId(Chunk::readXId(Chunk::skipBackward(limit), x), y);
		maxX = x+y;
		int chunkNum =	(int) ::ceil((double) (chunkManager.getUsedSpace(chunkType)	+ sizeof(ChunkManagerMeta)) / MemoryBuffer::pagesize);
		regionNum = 10 * (int)log10(chunkNum);
		if (regionNum == 0)
			regionNum = 1;
		idRange = (maxX) / regionNum + 1;
//		cout << "idRange:" << idRange << endl;
		x = 0;
		y = 0;
		Chunk::readYId(Chunk::readXId(reader, x), y);
		insertEntries(x + y, 0);
		reader = reader + (int) MemoryBuffer::pagesize;

		while (reader < limit) {
			x = 0;
			y = 0;
			const uchar* temp = Chunk::skipBackward(reader);
			Chunk::readYId(Chunk::readXId(temp, x), y);
			insertEntries(x + y, temp - begin);

			if((x + y) / idRange > regionList.size()) {

				startEntry = endEntry; endEntry = tableSize;
				regionList.push_back(new MemoryBuffer(HASH_CAPACITY));
				assert(lastX/idRange +1 == regionList.size());
				buildLine(startEntry, endEntry, regionList.size()-1);
				for (unsigned i = regionList.size(); i < (x+y) / idRange; i++){
					regionList.push_back(NULL);
					regionSize += sizeof(unsigned);
				}
				lastX = x+y;
			}
			reader = reader + (int) MemoryBuffer::pagesize;
			maxX = x+y;
		}

		x = y = 0;
		reader = Chunk::skipBackward(limit);
		Chunk::readYId(Chunk::readXId(reader, x), y);
		insertEntries(x + y, reader - begin);

		startEntry = endEntry; endEntry = tableSize;
		regionList.push_back(new MemoryBuffer(HASH_CAPACITY));
		assert(lastX/idRange +1 == regionList.size());
		buildLine(startEntry, endEntry, regionList.size()-1);
	}
	regionNum = regionList.size();
//	cout << "maxX:"<< maxX<< "  idRange:" <<idRange <<"  regionSize:" <<regionSize << " regionNum:" << regionNum << "  tableSize:"<< tableSize<< endl;
//	if(regionNum !=1)
		assert((int) ::ceil(maxX/idRange +1 )== regionNum);
	return OK;
}

bool FoldLineIndex::isBufferFull()
{
	return tableSize >= idTable->getSize() / 4;
}

void FoldLineIndex::insertEntries(ID id, unsigned offset)
{
	if (isBufferFull() == true) {
		idTable->resize(HASH_CAPACITY);
		idTableEntries = (ID*) idTable->get_address();
		offsetTable->resize(HASH_CAPACITY);
		offsetTableEntries = (ID*) offsetTable->get_address();
	}
	idTableEntries[tableSize] = id;
	offsetTableEntries[tableSize] = offset;

	tableSize++;
}

int FoldLineIndex::searchChunk(ID id)
{
	int res;
	unsigned regionNo;

	if(idRange == 0)
		return -1;

	regionNo = id/idRange;
	if(regionNo >= regionList.size()){
		return tableSize-1;
	}
	if(regionList[regionNo] == NULL)
		return -1;
	Line* lineTable = (Line*)(regionList[regionNo]->getBuffer()+4);
	unsigned lineTableSize = 0;
	memcpy(&lineTableSize,regionList[regionNo]->getBuffer(),4);
	Line* startLine = lineTable;
	//cout << "regionNo:" << regionNo << "  lineTableSize:" << lineTableSize << "  startID:"<< startLine->startID << endl;

	//get the rignt line
	int low = 0, high = lineTableSize, mid=0;
	if(id < startLine->startID)
			high = 0;
	while (low <= high) {
		mid = low + ((high - low) / 2);
		if (startLine[mid].startID == id)
			break;
		if (startLine[mid].startID > id)
			high = mid - 1;
		else
			low = mid + 1;
	}
	if(startLine[mid].startID > id && mid >0){
		mid--;
	}else if(mid +1< lineTableSize && startLine[mid].startID < id && startLine[mid+1].startID < id ){
		mid++;
	}

	//get the right chunk
	if(startLine[mid].k== 0  || startLine[mid].k == DBL_MAX){
		res = startLine[mid].b;
	}else
 		res = startLine[mid].k * id + startLine[mid].b;

	if(res > tableSize){
		res = tableSize-1;
	}

	int oldres = res;
	if (idTableEntries[res] >= id && res > 0) {
		res--;
		while (idTableEntries[res] >= id && res > 0)
			res--;
	} else {
		while (res + 1 < tableSize && idTableEntries[res] < id && idTableEntries[res + 1] < id)
			res++;
	}
//	cout << "cha zhi:" << oldres -res << endl;

	return res;
}

Status FoldLineIndex::getOffsetByID(ID id, unsigned& offset, unsigned typeID)
{
	if (chunkManager.getTrpileCount(typeID) == 0) {
		//		cerr << "id: " << id << " not found! 1" << endl;
		return NOT_FOUND;
	}
	int offsetId = this->searchChunk(id);
//	cout <<"id" << id<< "  res chunk:" << offsetId << endl;
	if (offsetId == -1) {
		//cout<<"id: "<<id<<endl;
		if (tableSize > 0 && id > idTableEntries[tableSize - 1]) {
			return NOT_FOUND;
		} else {
			offset = 0;
			return OK;
		}
	}
	/*if (offsetId == -1){
		return NOT_FOUND;
	}*/

	unsigned pBegin = offsetTableEntries[offsetId];
	unsigned pEnd = offsetTableEntries[offsetId + 1];

	const uchar* beginPtr = NULL, *reader = NULL;
	int low, high, mid = 0, lastmid = 0;
	ID x, y;

	if (chunkManager.getTrpileCount(typeID) == 0)
		return NOT_FOUND;

	if (typeID == 1) {
		low = pBegin;
		high = pEnd;

		reader = chunkManager.getStartPtr(1) + low;
		beginPtr = chunkManager.getStartPtr(1);
		Chunk::readXId(reader, x);

		if (x == id) {
			offset = low;
			lastmid = low;
			//cout<<__FUNCTION__<<"x==id"<<endl;
			while (low > 0) {
				//x = 0;
				reader = Chunk::skipBackward(beginPtr + low);
				Chunk::readXId(reader, x);
				if (x < id) {
					offset = lastmid;
					return OK;
				}
				lastmid = reader - beginPtr;
				low = lastmid - 1;
			}
			offset = lastmid;
			return OK;
		} else if (x > id)
			return OK;

		//cout<<__FUNCTION__<<"low<=high typeID == 1"<<endl;
		while (low <= high) {
			//x = 0;
			mid = low + (high - low) / 2;
			if (lastmid == mid)
				break;
			lastmid = mid;
			reader = Chunk::skipBackward(beginPtr + mid);
			mid = reader - beginPtr;
			Chunk::readXId(reader, x);

			if (x == id) {
				lastmid = mid;
				while (mid > 0) {
					//x = 0;
					reader = Chunk::skipBackward(beginPtr + mid);
					Chunk::readXId(reader, x);
					if (x < id) {
						offset = lastmid;
						return OK;
					}
					lastmid = reader - beginPtr;
					mid = lastmid - 1;
				}
				offset = lastmid;
				return OK;
			} else if (x > id) {
				high = mid - 1;
			} else {
				low = mid + 1;
			}
		}
	}

	if (typeID == 2) {
		low = pBegin;
		high = pEnd;

		reader = chunkManager.getStartPtr(2) + low;
		beginPtr = chunkManager.getStartPtr(2);

		reader = Chunk::readXId(reader, x);
		reader = Chunk::readYId(reader, y);
		if (x + y == id) {
			offset = low;
			lastmid = low;
			//cout<<__FUNCTION__<<"x + y == id typeID == 2"<<endl;
			while (low > 0) {
				//x = 0;
				//y = 0;
				reader = Chunk::skipBackward(beginPtr + low);
				Chunk::readYId(Chunk::readXId(reader, x), y);
				if (x + y < id) {
					offset = lastmid;
					return OK;
				}
				lastmid = reader - beginPtr;
				low = lastmid - 1;
			}
			offset = lastmid;
			return OK;
		}

		if (x + y > id)
			return OK;
		//cout<<__FUNCTION__<<"low<=high"<<endl;
		while (low <= high) {
			//x = 0;
			mid = (low + high) / 2;
			reader = Chunk::skipBackward(beginPtr + mid);
			mid = reader - beginPtr;
			if (lastmid == mid)
				break;
			lastmid = mid;
			reader = Chunk::readXId(reader, x);
			reader = Chunk::readYId(reader, y);
			if (x + y == id) {
				lastmid = mid;
				while (mid > 0) {
					//x = y = 0;
					reader = Chunk::skipBackward(beginPtr + mid);
					Chunk::readYId(Chunk::readXId(reader, x), y);
					if (x + y < id) {
						offset = lastmid;
						return OK;
					}
					lastmid = reader - beginPtr;
					mid = lastmid - 1;
					//mid = reader - beginPtr;
				}
				offset = lastmid;
				return OK;
			} else if (x + y > id) {
				high = mid - 1;
			} else {
				low = mid + 1;
			}
		}
	}
	if (mid <= 0)
		offset = 0;
	else
		//if not found, offset is the first id which is bigger than the given id.
		offset = Chunk::skipBackward(beginPtr + mid) - beginPtr;

	return OK;
}

Status FoldLineIndex::getFirstOffsetByID(ID id, unsigned& offset, unsigned typeID)
{

	if (chunkManager.getTrpileCount(typeID) == 0) {
//		cerr << "id: " << id << " not found! 1" << endl;
		return NOT_FOUND;
	}
	int offsetId = this->searchChunk(id);
//	cout << "offsetID:" <<  offsetId << endl;
	if (offsetId == -1) {
//		cerr << "id: " << id << " not found! 2" << endl;
		return NOT_FOUND;

	}

	unsigned pBegin = offsetTableEntries[offsetId];
	unsigned pEnd;
	if(offsetId+1 < tableSize)
		pEnd = offsetTableEntries[offsetId + 1];
	else
		pEnd = chunkManager.getEndPtr(typeID) - chunkManager.getStartPtr(typeID);

//	cout << "pbegin:" << pBegin << " pend:" << pEnd << endl;
	/*if(pEnd < pBegin)
		printf("%u  %u\n", pBegin,pEnd);*/
	assert(pEnd >= pBegin);
	/*if(pEnd < pBegin) {
		pEnd = chunkManager.getEndPtr(typeID) - chunkManager.getStartPtr(typeID);
	}*/

	const uchar* beginPtr = NULL, *reader = NULL;
	int low, high, mid = 0, lastmid = 0;
	ID x, y;



	if (typeID == 1) {
		low = pBegin;
		high = pEnd;

		reader = chunkManager.getStartPtr(1) + low;
		beginPtr = chunkManager.getStartPtr(1);
		Chunk::readXId(reader, x);

		if (x == id) {
			offset = low;
			lastmid = low;
			//cout<<__FUNCTION__<<"x==id"<<endl;
			while (low > 0) {
				//x = 0;
				reader = Chunk::skipBackward(beginPtr + low);
				Chunk::readXId(reader, x);
				if (x < id|| reader < beginPtr) {
					offset = lastmid;
					return OK;
				}
				lastmid = reader - beginPtr;
				low = lastmid - 1;
			}
			offset = lastmid;
			return OK;
		} else if (x > id) {
			return NOT_FOUND;
		}

		//cout<<__FUNCTION__<<"low<=high typeID == 1"<<endl;
		while (low <= high) {
			//x = 0;
			mid = low + (high - low) / 2;

	//		lastmid = mid;
			reader = Chunk::skipBackward(beginPtr + mid);
			//mid = reader - beginPtr;
			Chunk::readXId(reader, x);

			if (x == id) {
				lastmid = mid;
				while (mid > 0) {
					//x = 0;
					reader = Chunk::skipBackward(beginPtr + mid);
					Chunk::readXId(reader, x);
					if (x < id || reader < beginPtr) {
						offset = lastmid;
						return OK;
					}
					lastmid = reader - beginPtr;
					mid = lastmid - 1;
				}
				offset = lastmid;
				return OK;
			} else if (x > id) {
				high = mid - 1;
			} else {
				low = mid + 1;
			}
			if (lastmid == mid)
				break;
			lastmid = mid;
		}
	}

	if (typeID == 2) {
		low = pBegin;
		high = pEnd;

		reader = chunkManager.getStartPtr(2) + low;
		beginPtr = chunkManager.getStartPtr(2);

		reader = Chunk::readXId(reader, x);
		reader = Chunk::readYId(reader, y);
		if (x + y == id) {
			offset = low;
			lastmid = low;
			//cout<<__FUNCTION__<<"x + y == id typeID == 2"<<endl;
			while (low > 0) {
				//x = 0;
				//y = 0;
				reader = Chunk::skipBackward(beginPtr + low);
				Chunk::readYId(Chunk::readXId(reader, x), y);
				if (reader < beginPtr || x + y < id) {
					offset = lastmid;
					return OK;
				}
				lastmid = reader - beginPtr;
				low = lastmid - 1;
			}
			offset = lastmid;
			return OK;
		}else if (x + y > id) {
			return NOT_FOUND;
		}
		while (low <= high) {
			//x = 0;
			mid = (low + high) / 2;
			reader = Chunk::skipBackward(beginPtr + mid);
			//mid = reader - beginPtr;

		//	lastmid = mid;
			reader = Chunk::readXId(reader, x);
			reader = Chunk::readYId(reader, y);
			if (x + y == id) {
				lastmid = mid;
				while (mid > 0) {
					//x = y = 0;
					reader = Chunk::skipBackward(beginPtr + mid);
					//if(reader < beginPtr)

					Chunk::readYId(Chunk::readXId(reader, x), y);
					if (reader < beginPtr || x + y < id) {
						offset = lastmid;
						return OK;
					}
					lastmid = reader - beginPtr;
					mid = lastmid - 1;
					//mid = reader - beginPtr;
				}
				offset = lastmid;
				return OK;
			} else if (x + y > id) {
				high = mid - 1;
			} else {
				low = mid + 1;
			}
			if (lastmid == mid)
				break;
			lastmid = mid;
		}
	}
	if (mid <= 0)
		offset = 0;
	else
		//if not found, offset is the first id which is bigger than the given id.
		offset = Chunk::skipBackward(beginPtr + mid) - beginPtr;
	return OK;
}

Status FoldLineIndex::getYByID(ID id,EntityIDBuffer* entBuffer,unsigned typeID){
	unsigned offset = 0;
	const uchar* reader = NULL;
	ID x,y;

	if(getFirstOffsetByID(id,offset,typeID) == NOT_FOUND )
		return NOT_FOUND;

	if(typeID == 1){
		reader = chunkManager.getStartPtr(1) + offset;
		reader = Chunk::readXId(reader, x);
		reader = Chunk::readYId(reader, y);
		while(x == id){
			entBuffer->insertID(x+y);
			reader = Chunk::readXId(reader, x);
			reader = Chunk::readYId(reader, y);
		}
	}

	if (typeID == 2) {
		reader = chunkManager.getStartPtr(2) + offset;
		reader = Chunk::readXId(reader, x);
		reader = Chunk::readYId(reader, y);
		while (x+y == id) {
			entBuffer->insertID(x);
			reader = Chunk::readXId(reader, x);
			reader = Chunk::readYId(reader, y);
		}
	}
	return OK;
}

void FoldLineIndex::save(MMapBuffer*& indexBuffer)
{
	char* writeBuf;
	size_t size = 0;
	if (indexBuffer == NULL) {
		indexBuffer = MMapBuffer::create(string(string(DATABASE_PATH) + "/BitmapBuffer_index").c_str(), tableSize * sizeof(ID) * 2 + sizeof(ID) + 3* sizeof(unsigned) + regionSize);
		writeBuf = indexBuffer->get_address();
	} else {
		size = indexBuffer->get_length();
		indexBuffer->resize(size+tableSize * sizeof(ID) * 2 + sizeof(ID) + 3* sizeof(unsigned) + regionSize,false) ;
		writeBuf = indexBuffer->get_address() + size;
	}

	*(ID*) writeBuf = tableSize;
	writeBuf = writeBuf + 4;
	memcpy(writeBuf, (char*) idTableEntries, tableSize * 4);
	writeBuf = writeBuf + tableSize * 4;
	memcpy(writeBuf, (char*) offsetTableEntries, tableSize * 4);
	writeBuf = writeBuf + tableSize * 4;

	*(unsigned *) writeBuf = idRange;
	writeBuf = writeBuf + sizeof(unsigned);
	*(unsigned *) writeBuf = regionSize;
	writeBuf = writeBuf + sizeof(unsigned);
	*(unsigned *) writeBuf = regionNum;
	writeBuf = writeBuf + sizeof(unsigned);

//	cout << "  idRange:" <<idRange <<"  regionSize:" <<regionSize << " regionNum:" << regionNum << "  tableSize:"<< tableSize <<endl;

	unsigned lineCount = 0;
	for(unsigned i=0; i < regionList.size(); i++){
		if(regionList[i] == NULL){
			memset(writeBuf,0,sizeof(unsigned));
			writeBuf += sizeof(unsigned);
		} else {
			memcpy(&lineCount, regionList[i]->getBuffer(), sizeof(unsigned));
			memcpy(writeBuf, regionList[i]->getBuffer(), lineCount* sizeof(Line) + sizeof(unsigned));
			writeBuf = writeBuf + lineCount * sizeof(Line) + sizeof(unsigned);
		}
	}

	indexBuffer->flush();

//	cout << "offset:"<<(indexBuffer->getSize() - size) << " new size:" << indexBuffer->getSize() << " old size:" << size<< endl;
//	delete idTable;
	idTable = NULL;
//	delete offsetTable;
	offsetTable = NULL;
}

FoldLineIndex* FoldLineIndex::load(ChunkManager& manager, IndexType type, char* buffer, size_t& offset)
{

	FoldLineIndex* index = new FoldLineIndex(manager, type);
	char* base = buffer + offset;
	index->tableSize = *((ID*)base);
	index->idTableEntries = (ID*)(base + sizeof(ID));
	index->offsetTableEntries = (ID*)(index->idTableEntries + index->tableSize);
	offset = offset + 4 + 4 * 2 * index->tableSize;
	base = buffer + offset;

	index->idRange = *((ID*)base);
	index->regionSize = *((ID*)(base + sizeof(unsigned)));
	index->regionNum = *((ID*)(base + 2*sizeof(unsigned)));
	offset = offset + 3*sizeof(unsigned);
	base = buffer + offset;
/*	cout << "  idRange:" << index->idRange << "  regionSize:"
			<< index->regionSize << " regionNum:" << index->regionNum
			<< "  tableSize:" << index->tableSize << endl;*/
	index->regionList.clear();

	unsigned lineCount = 0;

	for(unsigned i = 0; i < index->regionNum ; i++) {
		memcpy(&lineCount,base, sizeof(unsigned));
		if(lineCount == 0)
			index->regionList.push_back(NULL);
		else
			index->regionList.push_back(new MemoryBuffer(sizeof(unsigned) + lineCount * sizeof(Line),base));
		base = base + sizeof(unsigned) + lineCount * sizeof(Line);
	}

	offset = offset + index->regionSize ;

	return index;
}

void FoldLineIndex::unload(char* buffer, size_t& offset)
{
	char* base = buffer + offset;
		unsigned tableSize = *((ID*)base);
		/*index->idTableEntries = (ID*)(base + sizeof(ID));
		index->offsetTableEntries = (ID*)(index->idTableEntries + index->tableSize);*/
		offset = offset + 4 + 4 * 2 * tableSize;
		base = buffer + offset;

		//index->idRange = *((ID*)base);
		unsigned regionSize = *((ID*)(base + sizeof(unsigned)));
		//index->regionNum = *((ID*)(base + 2*sizeof(unsigned)));
		offset = offset + 3*sizeof(unsigned);
		base = buffer + offset;
	/*	cout << "  idRange:" << index->idRange << "  regionSize:"
				<< index->regionSize << " regionNum:" << index->regionNum
				<< "  tableSize:" << index->tableSize << endl;*/
		//index->regionList.clear();

		//unsigned lineCount = 0;

		/*for(unsigned i = 0; i < index->regionNum ; i++) {
			memcpy(&lineCount,base, sizeof(unsigned));
			if(lineCount == 0)
				index->regionList.push_back(NULL);
			else
				index->regionList.push_back(new MemoryBuffer(sizeof(unsigned) + lineCount * sizeof(Line),base));
			base = base + sizeof(unsigned) + lineCount * sizeof(Line);
		}*/

		offset = offset + regionSize ;
	//	cout << "offset:"<<(offset - oldoff)<< endl;

}
