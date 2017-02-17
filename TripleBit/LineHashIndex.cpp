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

#include "LineHashIndex.h"
#include "MMapBuffer.h"
#include "MemoryBuffer.h"
#include "BitmapBuffer.h"

#include <math.h>
/**
 * linear fit;
 * f(x)=kx + b;
 * used to calculate the parameter k and b;
 */
static bool calculateLineKB(vector<LineHashIndex::Point>& a, double& k, double& b, int pointNo)
{
	if (pointNo < 2)
		return false;

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
		k = 0;
		b = 0;
//		cout << "vertical line" << endl;
		return true;
	}
	k = 0;b= 0;
	k = (mY * mX - mXY * pointNo) / (mX * mX - mXX * pointNo);
	b = (mXY * mX - mY * mXX) / (mX * mX - mXX * pointNo);

	return true;
}

LineHashIndex::LineHashIndex(ChunkManager& _chunkManager, IndexType type) : chunkManager(_chunkManager), indexType(type){
	// TODO Auto-generated constructor stub
	idTable = NULL;
	offsetTable = NULL;
	idTableEntries = NULL;
	offsetTableEntries = NULL;

	startID[0] = startID[1] = startID[2] = startID[3] = UINT_MAX;
}

LineHashIndex::~LineHashIndex() {
	// TODO Auto-generated destructor stub
	idTable = NULL;
	offsetTable = NULL;
	idTableEntries = NULL;
	offsetTableEntries = NULL;
}

/**
 * From startEntry to endEntry in idtableEntries build a line;
 * @param lineNo: the lineNo-th line to be build;
 */
bool LineHashIndex::buildLine(int startEntry, int endEntry, int lineNo)
{
	vector<Point> vpt;
	Point pt;
	int i;
//	cout << "startEntry:" << startEntry << "    endEntry:" << endEntry << endl;
	//build lower limit line;
	for (i = startEntry; i < endEntry; i++) {
		pt.x = idTableEntries[i];
		pt.y = i;
		vpt.push_back(pt);
	//	cout << "x:" << pt.x  << " y:" << pt.y << "   " << flush;
	}

	//cout << endl;
	double ktemp, btemp;
	int size = vpt.size();
	if (calculateLineKB(vpt, ktemp, btemp, size) == false){
		return false;
	}
	double difference = btemp;//(vpt[0].y - (ktemp * vpt[0].x + btemp));
	double difference_final = difference;

	for (i = 0; i < size; i++) {
		difference = vpt[i].y - ktemp * vpt[i].x;//vpt[0].y - (ktemp * vpt[0].x + btemp);
//		cout<<"differnce: "<<difference<<endl;
		if ((difference > difference_final) == true)
			difference_final = difference;
	}
	btemp = difference_final;

	upperk[lineNo] = ktemp;
	upperb[lineNo] = btemp;
	startID[lineNo] = vpt[0].x;

//	cout << lineNo<<" upperk:" << ktemp << "   upperb:"<< btemp << "  startID:" << vpt[0].x << endl;

	vpt.resize(0);
	//build upper limit line;
	for (i = startEntry; i < endEntry-1; i++) {
		pt.x = idTableEntries[i + 1];
		pt.y = i;
		vpt.push_back(pt);
//		cout << "x:" << pt.x << " y:" << pt.y << "   " << flush;
	}
//	cout << endl;



	size = vpt.size();
	if(size >1)
		calculateLineKB(vpt, ktemp, btemp, size);

	difference = btemp;//(vpt[0].y - (ktemp * vpt[0].x + btemp));
	difference_final = difference;

	for (i = 0; i < size; i++) {
		difference = vpt[i].y - ktemp * vpt[i].x; //vpt[0].y - (ktemp * vpt[0].x + btemp);
		if (difference < difference_final)
			difference_final = difference;
	}
	btemp = difference_final;

	lowerk[lineNo] = ktemp;
	lowerb[lineNo] = btemp;
//	cout <<lineNo <<" lowerk:" << ktemp << "   lowerb:"<< btemp << endl<< endl<< endl;

	return true;
}

static ID splitID[3] = {255, 65535, 16777215};

Status LineHashIndex::buildIndex(unsigned chunkType)
{
	if (idTable == NULL) {
		idTable = new MemoryBuffer(HASH_CAPACITY);
		tableSize = idTable->getSize() / sizeof(unsigned);
		offsetTable = new MemoryBuffer(HASH_CAPACITY);
		idTableEntries = (ID*) idTable->getBuffer();
		offsetTableEntries = (ID*) offsetTable->getBuffer();
		tableSize = 0;
	}

	const uchar* begin, *limit, *reader;
	ID x, y;

	lineNo = 0;
	int startEntry = 0, endEntry = 0;

	if (chunkType == 1) {
		reader = chunkManager.getStartPtr(1);
		begin = reader;
		if (chunkManager.getStartPtr(chunkType) == chunkManager.getEndPtr(chunkType))
			return OK;

		x = 0;
		reader = Chunk::readXId(reader, x);
		insertEntries(x, 0);

		reader = reader + (int) MemoryBuffer::pagesize;

		limit = chunkManager.getEndPtr(1);
		while (reader < limit) {
			x = 0;
			const uchar* temp = Chunk::skipBackward(reader);
			Chunk::readXId(temp, x);
			insertEntries(x, temp - begin);

			if(x > splitID[lineNo] && tableSize -endEntry >1) {
				startEntry = endEntry; endEntry = tableSize;
				if(buildLine(startEntry, endEntry, lineNo) == true) {
					lineNo++;
				}
			}
			reader = reader + (int) MemoryBuffer::pagesize;
		}

		reader = Chunk::skipBackward(limit);
		x = 0;
		Chunk::readXId(reader, x);
		insertEntries(x, reader - begin);

		startEntry = endEntry; endEntry = tableSize;
		if(buildLine(startEntry, endEntry, lineNo) == true) {
			lineNo++;
		}
//		cout << "type 1 lineNo:" << lineNo << endl;
	}


	if (chunkType == 2) {
		reader = chunkManager.getStartPtr(2);
		begin = reader;
		if (chunkManager.getStartPtr(chunkType) == chunkManager.getEndPtr(
				chunkType))
			return OK;

		x = 0;
		y = 0;
		Chunk::readYId(Chunk::readXId(reader, x), y);
		insertEntries(x + y, 0);

		reader = reader + (int) MemoryBuffer::pagesize;

		limit = chunkManager.getEndPtr(2);
		while (reader < limit) {
			x = 0;
			y = 0;
			const uchar* temp = Chunk::skipBackward(reader);
			Chunk::readYId(Chunk::readXId(temp, x), y);
			insertEntries(x + y, temp - begin);

			if((x + y) > splitID[lineNo] && tableSize -endEntry >1) {

				startEntry = endEntry; endEntry = tableSize;
				if(buildLine(startEntry, endEntry, lineNo) == true) {
					lineNo++;
				}
			}
			reader = reader + (int) MemoryBuffer::pagesize;
		}

		x = y = 0;
		reader = Chunk::skipBackward(limit);
		Chunk::readYId(Chunk::readXId(reader, x), y);
		insertEntries(x + y, reader - begin);

		startEntry = endEntry; endEntry = tableSize;
		if(buildLine(startEntry, endEntry, lineNo) == true) {
			lineNo++;
		}
//		cout << "type 2 lineNo:" << lineNo << endl;
	}
/*
	if (chunkType ==2)
	cout << "chunk num:"<< (int)::ceil((float)chunkManager.getUsedSpace(chunkType)/4096.0) << "  tablesize:"<<tableSize-1<< endl;
	else if (chunkType ==1)
		cout << "chunk num:"<< (int)::ceil(((float)chunkManager.getUsedSpace(chunkType)+ sizeof(ChunkManagerMeta))/4096.0) << "  tablesize:"<<tableSize-1<< endl;
*/

	return OK;
}

bool LineHashIndex::isBufferFull()
{
	return tableSize >= idTable->getSize() / 4;
}

void LineHashIndex::insertEntries(ID id, size_t offset)
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

int LineHashIndex::searchChunk(ID id)
{
	int lowerchunk, upperchunk;


//	cout << "id:" << id << "  startID:" << startID[0] << "  " << startID[1] << "  " << startID[2] << "  " << startID[3] << endl;

	int lineNo;
	if(id < startID[0]) {
		return -1;
	} else if(id < startID[1]) {
		lineNo = 0;
	} else if(id < startID[2]) {
		lineNo = 1;
	} else if(id < startID[3]) {
		lineNo = 2;
	} else {
		lineNo = 3;
	}

/*
	for(unsigned i = 0;i < tableSize;i++)
		cout << idTableEntries[i] << "--" << flush;
	cout << endl<< endl;

	cout << lowerk[lineNo] << "  " << lowerb[lineNo] << "  " << upperk[lineNo] << "  " << upperb[lineNo] << endl;
*/


 	lowerchunk = (int)::floor(lowerk[lineNo] * id + lowerb[lineNo]);
 	upperchunk = (int)::ceil(upperk[lineNo] * id + upperb[lineNo]);
 //	cout << "lineNo:"<< lineNo << " lowerchunk:"<< lowerchunk << "  upperchunk:" << upperchunk << endl;

 	if(upperchunk >= (int)tableSize || upperchunk < 0) upperchunk = tableSize - 1;
 	if(lowerchunk < 0 || lowerchunk >= (int)tableSize) lowerchunk = 0;

 	int low =lowerchunk;
 	int high = upperchunk;

 	assert(low <= high);
 	/*if(low <= high){
 		cout << "low <= high: id:" << id << endl;
 	}*/
//	assert(idTableEntries[low] <= id && idTableEntries[high] >= id);
 //	cout << id << "  " << idTableEntries[low] << "  " << idTableEntries[high]  << idTableEntries[high +1]<< endl;

 	int mid;

 	if(low == high)
 		return low;
 	//find the first entry >= id;
 	while (low != high) {
		mid = low + (high - low) / 2;

		if(id > idTableEntries[mid]) {
			low = mid + 1;
		} else if((!mid) || id > idTableEntries[mid - 1]) {
			break;
		} else {
			high = mid;
		}
	}

 	int res = -1;

 	if(low == high) {
 		//return ;
 		if(low > 0 &&idTableEntries[low] > id) res  = low -1;
 		else res =  low;
 	} else if(idTableEntries[mid] == id) {
 		res = mid;
 	} else if(mid > 0) {
 		res = mid - 1;
 	} else {
 		res = mid;
 	}
	if (idTableEntries[res] >= id && res >0) {
		res--;
		while (idTableEntries[res] >= id && res > 0)
			res--;
	} else {
		while (res+1 < tableSize && idTableEntries[res] < id && idTableEntries[res + 1] < id)
			res++;
	}
	return res;
}

Status LineHashIndex::getOffsetByID(ID id, size_t& offset, unsigned typeID)
{

	if (chunkManager.getTrpileCount(typeID) == 0) {
		return NOT_FOUND;
	}
	int offsetId = this->searchChunk(id);
	if (offsetId == -1) {
		//cerr<<"id: "<<id<<endl;
		if (tableSize > 0 && id > idTableEntries[tableSize - 1]) {
			return NOT_FOUND;
		} else {
			offset = 0;
			return OK;
		}
	}

	unsigned pBegin = offsetTableEntries[offsetId];
	unsigned pEnd;
	if(offsetId+1 < tableSize)
		pEnd = offsetTableEntries[offsetId + 1];
	else
		pEnd = chunkManager.getEndPtr(typeID) - chunkManager.getStartPtr(typeID);

	if(pEnd < pBegin) {
		pEnd = chunkManager.getEndPtr(typeID) - chunkManager.getStartPtr(typeID);
	}

	const uchar* beginPtr = NULL, *reader = NULL;
	size_t low, high, mid = 0, lastmid = 0;
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
				if (reader < beginPtr || x < id) {
					offset = lastmid;
					return OK;
				}
				lastmid = reader - beginPtr;
				low = lastmid - 1;
			}
			offset = lastmid;
			return OK;
		} else if (x > id) {
			return OK;
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
					if (reader < beginPtr || x < id) {
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
		}

		if (x + y > id) {
			return OK;
		}
		//cout<<__FUNCTION__<<"low<=high"<<endl;
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


Status LineHashIndex::getFirstOffsetByID(ID id, size_t& offset, unsigned typeID)
{

	if (chunkManager.getTrpileCount(typeID) == 0) {
//		cerr << "id: " << id << " not found! 1" << endl;
		return NOT_FOUND;
	}
	int offsetId = this->searchChunk(id);
//	cout << "offsetID:" <<  offsetId << endl;
	if (offsetId == -1) {
		//cerr << "preID:"<< preID << "  id: " << id << "  type:"<< typeID<<" not found! 2" << endl;
		return NOT_FOUND;

	}

	unsigned pBegin = offsetTableEntries[offsetId];
	unsigned pEnd;
	if(offsetId+1 < tableSize)
		pEnd = offsetTableEntries[offsetId + 1];
	else
		pEnd = chunkManager.getEndPtr(typeID) - chunkManager.getStartPtr(typeID);

	if(pEnd < pBegin) {
		pEnd = chunkManager.getEndPtr(typeID) - chunkManager.getStartPtr(typeID);
	}

	const uchar* beginPtr = NULL, *reader = NULL;
	size_t low, high, mid = 0, lastmid = 0;
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

Status LineHashIndex::getYByID(ID id,EntityIDBuffer* entBuffer,unsigned typeID){
	size_t offset = 0;
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

/*
Status LineHashIndex::getAllYByID(ID id,EntityIDBuffer* entBuffer){
	EntityIDBuffer* entBuffer1 = new EntityIDBuffer();
	if((getYByID(id, entBuffer1,1) == OK) && (getYByID(id, entBuffer,2) == OK)){
		entBuffer->appendBuffer(entBuffer1);
		return OK;
	}
	return NOT_FOUND;
}*/

string LineHashIndex::dir = "";
void LineHashIndex::save(MMapBuffer*& indexBuffer)
{
	char* writeBuf;

	if (indexBuffer == NULL) {
		if(DATABASE_PATH == NULL)
			DATABASE_PATH = (char *)dir.c_str();
		indexBuffer = MMapBuffer::create(string(string(DATABASE_PATH) + "/BitmapBuffer_index").c_str(), tableSize * 4 * 2 + 4 + 16 * sizeof(double) + 4 * sizeof(ID));
		writeBuf = indexBuffer->get_address();
	} else {
		size_t size = indexBuffer->get_length();
		indexBuffer->resize(indexBuffer->get_length() + tableSize * 4 * 2 + 4 + 16 * sizeof(double) + 4 * sizeof(ID), false);
		writeBuf = indexBuffer->get_address() + size;
	}

	*(ID*) writeBuf = tableSize;
	writeBuf = writeBuf + 4;
	memcpy(writeBuf, (char*) idTableEntries, tableSize * 4);
	writeBuf = writeBuf + tableSize * 4;
	memcpy(writeBuf, (char*) offsetTableEntries, tableSize * 4);
	writeBuf = writeBuf + tableSize * 4;

	for(int i = 0; i < 4; i++) {
		*(ID*)writeBuf = startID[i];
		writeBuf = writeBuf + sizeof(ID);

		*(double*)writeBuf = lowerk[i];
		writeBuf = writeBuf + sizeof(double);
		*(double*)writeBuf = lowerb[i];
		writeBuf = writeBuf + sizeof(double);

		*(double*)writeBuf = upperk[i];
		writeBuf = writeBuf + sizeof(double);
		*(double*)writeBuf = upperb[i];
		writeBuf = writeBuf + sizeof(double);
	}

	indexBuffer->flush();
	delete idTable;
	idTable = NULL;
	delete offsetTable;
	offsetTable = NULL;
}

LineHashIndex* LineHashIndex::load(ChunkManager& manager, IndexType type, char* buffer, size_t& offset)
{
	LineHashIndex* index = new LineHashIndex(manager, type);
	char* base = buffer + offset;
	index->tableSize = *((ID*)base);
	index->idTableEntries = (ID*)(base + 4);
	index->offsetTableEntries = (ID*)(index->idTableEntries + index->tableSize);
	offset = offset + 4 + 4 * 2 * index->tableSize;

	base = buffer + offset;
	for(int i = 0; i < 4; i++) {
		//base = buffer + offset;
		index->startID[i] = *(ID*)base;
		base = base + sizeof(ID);

		index->lowerk[i] = *(double*)base;
		base = base + sizeof(double);
		index->lowerb[i] = *(double*)base;
		base = base + sizeof(double);

		index->upperk[i] = *(double*)base;
		base = base + sizeof(double);
		index->upperb[i] = *(double*)base;
		base = base + sizeof(double);
	}

	offset = offset + 16 * sizeof(double) + 4 * sizeof(ID);
	//cout <<"tablesize:" << index->tableSize << endl;
	return index;
}

void LineHashIndex::unload(char* buffer, size_t& offset)
{
//	cout << "offset:"<< offset << " ~ " << flush;

	offset = offset + 4 + 4 * 2 *  *((ID*)(buffer + offset));;

	offset = offset + 16 * sizeof(double) + 4 * sizeof(ID);

//	cout << offset << endl;

}

/*void LineHashIndex::save(MMapBuffer*& indexBuffer)
{
	char* writeBuf;

	if (indexBuffer == NULL) {
		indexBuffer = MMapBuffer::create(string(string(DATABASE_PATH) + "/BitmapBuffer_index").c_str(), tableSize * 4 * 2 + 4 + 16 * sizeof(double) + 4 * sizeof(ID));
		writeBuf = indexBuffer->get_address();
	} else {
		size_t size = indexBuffer->get_length();
		indexBuffer->resize(indexBuffer->get_length() + tableSize * 4 * 2 + 4 + 16 * sizeof(double) + 4 * sizeof(ID), false);
		writeBuf = indexBuffer->get_address() + size;
	}

	*(ID*) writeBuf = tableSize;
	writeBuf = writeBuf + 4;
	memcpy(writeBuf, (char*) idTableEntries, tableSize * 4);
	writeBuf = writeBuf + tableSize * 4;
	memcpy(writeBuf, (char*) offsetTableEntries, tableSize * 4);
	writeBuf = writeBuf + tableSize * 4;

	*(ID*) writeBuf = lineNo;
	writeBuf = writeBuf + 4;

	for(unsigned i = 0; i < lineNo; i++) {
		*(ID*)writeBuf = startID[i];
		writeBuf = writeBuf + sizeof(ID);

		*(double*)writeBuf = lowerk[i];
		writeBuf = writeBuf + sizeof(double);
		*(double*)writeBuf = lowerb[i];
		writeBuf = writeBuf + sizeof(double);

		*(double*)writeBuf = upperk[i];
		writeBuf = writeBuf + sizeof(double);
		*(double*)writeBuf = upperb[i];
		writeBuf = writeBuf + sizeof(double);
	}

	indexBuffer->flush();
	delete idTable;
	idTable = NULL;
	delete offsetTable;
	offsetTable = NULL;
}

LineHashIndex* LineHashIndex::load(ChunkManager& manager, IndexType type, char* buffer, size_t& offset)
{
	LineHashIndex* index = new LineHashIndex(manager, type);
	char* base = buffer + offset;
	index->tableSize = *((ID*)base);
	index->idTableEntries = (ID*)(base + 4);
	index->offsetTableEntries = (ID*)(index->idTableEntries + index->tableSize);
	offset = offset + 4 + 4 * 2 * index->tableSize;

	base = buffer + offset;
	index->lineNo = *((ID*)base);
	offset += sizeof(unsigned);
	base += sizeof(unsigned);


	for(unsigned i = 0; i < index->lineNo; i++) {
		//base = buffer + offset;
		index->startID[i] = *(ID*)base;
		base = base + sizeof(ID);

		index->lowerk[i] = *(double*)base;
		base = base + sizeof(double);
		index->lowerb[i] = *(double*)base;
		base = base + sizeof(double);

		index->upperk[i] = *(double*)base;
		base = base + sizeof(double);
		index->upperb[i] = *(double*)base;
		base = base + sizeof(double);
	}

	offset = offset + index->lineNo * (4*sizeof(double) + sizeof(ID));
	return index;
}*/
