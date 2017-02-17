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

#include "StatisticsBuffer.h"
#include "BitmapBuffer.h"
#include "MMapBuffer.h"
#include "HashIndex.h"
#include "EntityIDBuffer.h"
#include "URITable.h"
#include "MemoryBuffer.h"
#include<unistd.h>
#include<sys/mman.h>

extern char* writeData(char* writer, unsigned data);
extern const char* readData(const char* reader, unsigned int& data);

static inline unsigned readDelta1(const unsigned char* pos) { return pos[0]; }
static unsigned readDelta2(const unsigned char* pos) { return (pos[0]<<8)|pos[1]; }
static unsigned readDelta3(const unsigned char* pos) { return (pos[0]<<16)|(pos[1]<<8)|pos[2]; }
static unsigned readDelta4(const unsigned char* pos) { return (pos[0]<<24)|(pos[1]<<16)|(pos[2]<<8)|pos[3]; }

static void writeUint32(unsigned char* target,unsigned value)
   // Write a 32bit value
{
   target[0]=value>>24;
   target[1]=(value>>16)&0xFF;
   target[2]=(value>>8)&0xFF;
   target[3]=value&0xFF;
}

unsigned char* StatisticsBuffer::writeDelta0(unsigned char* buffer, unsigned value)
   // Write an integer with varying size
{
	if (value >= (1 << 24)) {
		writeUint32(buffer, value);
		return buffer + 4;
	} else if (value >= (1 << 16)) {
		buffer[0] = value >> 16;
		buffer[1] = (value >> 8) & 0xFF;
		buffer[2] = value & 0xFF;
		return buffer + 3;
	} else if (value >= (1 << 8)) {
		buffer[0] = value >> 8;
		buffer[1] = value & 0xFF;
		return buffer + 2;
	} else if (value > 0) {
		buffer[0] = value;
		return buffer + 1;
	} else
		return buffer;
}

StatisticsBuffer::StatisticsBuffer() : HEADSPACE(2) {
	// TODO Auto-generated constructor stub
	
}

StatisticsBuffer::~StatisticsBuffer() {
	// TODO Auto-generated destructor stub
}

/////////////////////////////////////////////////////////////////

OneConstantStatisticsBuffer::OneConstantStatisticsBuffer(const string path, StatisticsType type) : StatisticsBuffer(), type(type), reader(NULL), ID_HASH(50)
{
	buffer = new MMapBuffer(path.c_str(), STATISTICS_BUFFER_INIT_PAGE_COUNT * MemoryBuffer::pagesize);
	writer = (unsigned char*)buffer->get_address();
//	reader = NULL;
	index.resize(2000);
	nextHashValue = 0;
	lastId = 0;
	usedSpace = 0;
	reader = NULL;

	triples = new Triple[ID_HASH];
	first = true;
}

OneConstantStatisticsBuffer::~OneConstantStatisticsBuffer()
{
	if(buffer != NULL) {
		delete buffer;
	}

	if(triples != NULL) {
		delete[] triples;
		triples = NULL;
	}
	buffer = NULL;
}

void OneConstantStatisticsBuffer::writeId(unsigned id, char*& ptr, bool isID)
{
	if ( isID == true ) {
		while (id >= 128) {
			unsigned char c = static_cast<unsigned char> (id & 127);
			*ptr = c;
			ptr++;
			id >>= 7;
		}
		*ptr = static_cast<unsigned char> (id & 127);
		ptr++;
	} else {
		while (id >= 128) {
			unsigned char c = static_cast<unsigned char> (id | 128);
			*ptr = c;
			ptr++;
			id >>= 7;
		}
		*ptr = static_cast<unsigned char> (id | 128);
		ptr++;
	}
}

bool OneConstantStatisticsBuffer::isPtrFull(unsigned len) 
{
	return (unsigned int) ( writer - (unsigned char*)buffer->get_address() + len ) > buffer->get_length() ? true : false;
}

unsigned OneConstantStatisticsBuffer::getLen(unsigned v)
{
	if (v >= (1 << 24))
		return 4;
	else if (v >= (1 << 16))
		return 3;
	else if (v >= (1 << 8))
		return 2;
	else if(v > 0)
		return 1;
	else
		return 0;
}

static unsigned int countEntity(const unsigned char* begin, const unsigned char* end)
{
	if(begin >= end) 
		return 0;

	//cout<<"begin - end: "<<end - begin<<endl;

	unsigned int entityCount = 0;
	entityCount = 1;
	begin = begin + 8;

	while(begin < end) {
		// Decode the header byte
		unsigned info = *(begin++);
		// Small gap only?
		if (info < 0x80) {
			if (!info)
				break;
			/*
			count = (info >> 4) + 1;
			value1 += (info & 15);
			(*writer).value1 = value1;
			(*writer).count = count;
			++writer;
			*/
			entityCount++ ;
			continue;
		}
		// Decode the parts
		//value1 += 1;
		switch (info & 127) {
			case 0: break;
			case 1: begin += 1; break;
			case 2: begin += 2;break;
			case 3: begin += 3;break;
			case 4: begin += 4;break;
			case 5: begin += 1;break;
			case 6: begin += 2;break;
			case 7: begin += 3;break;
			case 8: begin += 4;break;
			case 9: begin += 5; break;
			case 10: begin += 2; break;
			case 11: begin += 3; break;
			case 12: begin += 4; break;
			case 13: begin += 5; break;
			case 14: begin += 6; break;
			case 15: begin += 3; break;
			case 16: begin += 4; break;
			case 17: begin += 5; break;
			case 18: begin += 6; break;
			case 19: begin += 7;break;
			case 20: begin += 4;break;
			case 21: begin += 5;break;
			case 22: begin += 6;break;
			case 23: begin += 7;break;
			case 24: begin += 8;break;
		}
		entityCount++;
	}

	return entityCount;
}

const unsigned char* OneConstantStatisticsBuffer::decode(const unsigned char* begin, const unsigned char* end)
{
	Triple* writer = triples;
	unsigned value1, count;
	value1 = readDelta4(begin);
	begin += 4;
	count = readDelta4(begin);
	begin += 4;

	(*writer).value1 = value1;
	(*writer).count = count;
	writer++;

	while (begin < end) {
		// Decode the header byte

		unsigned info = *(begin++);
		// Small gap only?
		if (info < 0x80) {
			if (!info)
				break;
			count = (info >> 4) + 1;
			value1 += (info & 15);
			(*writer).value1 = value1;
			(*writer).count = count;
			++writer;
			continue;
		}
		// Decode the parts
		value1 += 1;
		switch (info & 127) {
			case 0: count = 1;break;
			case 1: count = readDelta1(begin) + 1; begin += 1; break;
			case 2: count = readDelta2(begin) + 1;begin += 2;break;
			case 3: count = readDelta3(begin) + 1;begin += 3;break;
			case 4: count = readDelta4(begin) + 1;begin += 4;break;
			case 5: value1 += readDelta1(begin);count = 1;begin += 1;break;
			case 6: value1 += readDelta1(begin);count = readDelta1(begin + 1) + 1;begin += 2;break;
			case 7: value1 += readDelta1(begin);count = readDelta2(begin + 1) + 1;begin += 3;break;
			case 8: value1 += readDelta1(begin);count = readDelta3(begin + 1) + 1;begin += 4;break;
			case 9: value1 += readDelta1(begin); count = readDelta4(begin + 1) + 1; begin += 5; break;
			case 10: value1 += readDelta2(begin); count = 1; begin += 2; break;
			case 11: value1 += readDelta2(begin); count = readDelta1(begin + 2) + 1; begin += 3; break;
			case 12: value1 += readDelta2(begin); count = readDelta2(begin + 2) + 1; begin += 4; break;
			case 13: value1 += readDelta2(begin); count = readDelta3(begin + 2) + 1; begin += 5; break;
			case 14: value1 += readDelta2(begin); count = readDelta4(begin + 2) + 1; begin += 6; break;
			case 15: value1 += readDelta3(begin); count = 1; begin += 3; break;
			case 16: value1 += readDelta3(begin); count = readDelta1(begin + 3) + 1; begin += 4; break;
			case 17: value1 += readDelta3(begin); count = readDelta2(begin + 3) + 1; begin += 5; break;
			case 18: value1 += readDelta3(begin); count = readDelta3(begin + 3) + 1; begin += 6; break;
			case 19: value1 += readDelta3(begin);count = readDelta4(begin + 3) + 1;begin += 7;break;
			case 20: value1 += readDelta4(begin);count = 1;begin += 4;break;
			case 21: value1 += readDelta4(begin);count = readDelta1(begin + 4) + 1;begin += 5;break;
			case 22: value1 += readDelta4(begin);count = readDelta2(begin + 4) + 1;begin += 6;break;
			case 23: value1 += readDelta4(begin);count = readDelta3(begin + 4) + 1;begin += 7;break;
			case 24: value1 += readDelta4(begin);count = readDelta4(begin + 4) + 1;begin += 8;break;
		}
		(*writer).value1 = value1;
		(*writer).count = count;
		++writer;
	}

	pos = triples;
	posLimit = writer;

	return begin;
}

unsigned int OneConstantStatisticsBuffer::getEntityCount()
{
	unsigned int entityCount = 0;
	unsigned i = 0;

	const unsigned char* begin, *end;
	unsigned beginChunk = 0, endChunk = 0;

#ifdef DEBUG
	cout<<"indexSize: "<<indexSize<<endl;
#endif
	for(i = 1; i <= indexSize; i++) {
		if(i < indexSize)
			endChunk = index[i];

		while(endChunk == 0 && i < indexSize) {
			i++;
			endChunk = index[i];
		}
		
		if(i == indexSize) { 
			endChunk = usedSpace;
		}
			
		if(endChunk != 0) {
			begin = (const unsigned char*)(buffer->get_address()) + beginChunk;
			end = (const unsigned char*)(buffer->get_address()) + endChunk;
			entityCount = entityCount + countEntity(begin, end);

			beginChunk = endChunk;
		}

		//beginChunk = endChunk;
	}

	return entityCount;
}

Status OneConstantStatisticsBuffer::addStatis(unsigned v1, unsigned v2, unsigned v3 /* = 0 */)
{

//	static bool first = true;
	unsigned interVal;
	if ( v1 >= nextHashValue ) {
		interVal = v1;
	} else {
		interVal = v1 - lastId;
	}

	unsigned len;
	if(v1 >= nextHashValue) {
		len = 8;
	} else if(interVal < 16 && v2 <= 8) {
		len = 1;
	} else {
		len = 1 + getLen(interVal - 1) + getLen(v2 - 1);
	}

	if ( isPtrFull(len) == true ) {
		//MessageEngine::showMessage("OneConstantStatisticsBuffer::addStatis()", MessageEngine::INFO);
		usedSpace = writer - (unsigned char*)buffer->get_address();
		buffer->resize(buffer->get_length() + STATISTICS_BUFFER_INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize, false);
		writer = (unsigned char*)buffer->get_address() + usedSpace;
	}

	if ( first || (v1 >= nextHashValue) ) {
		unsigned offset = writer - (uchar*)buffer->get_address();
		while (index.size() <= (v1 / ID_HASH)) {
			index.resize(index.size() + 2000, 0);
#ifdef DEBUG
			cout<<"index size"<<index.size()<<" v1 / ID_HASH: "<<(v1 / ID_HASH)<<endl;
#endif
		}

		index[v1 / ID_HASH] = offset;
		indexSize = v1 / ID_HASH +1;
//		cout << "v1:" <<v1 << "   offset:" <<offset << "  v1/ID_HASH:" << v1/ID_HASH << "  nextHashValue:" << nextHashValue<<endl;
		while(nextHashValue <= v1) nextHashValue += ID_HASH;

		writeUint32(writer, v1);
		writer += 4;
		writeUint32(writer, v2);
		writer += 4;

		first = false;
	} else {
		if(len == 1) {
			*writer = ((v2 - 1) << 4) | (interVal);
			writer++;
		} else {
			*writer = 0x80|((getLen(interVal-1)*5)+getLen(v2 - 1));
			writer++;
			writer = writeDelta0(writer,interVal - 1);
			writer = writeDelta0(writer, v2 - 1);
		}
	}

	lastId = v1;
	usedSpace = writer - (uchar*)buffer->get_address();

	return OK;
}

bool OneConstantStatisticsBuffer::find(unsigned value)
{
	//const Triple* l = pos, *r = posLimit;
	int l = 0, r = posLimit - pos;
	int m;
	while (l != r) {
		m = l + ((r - l) / 2);
		if (value > pos[m].value1) {
			l = m + 1;
		} else if ((!m) || value > pos[m - 1].value1) {
			break;
		} else {
			r = m;
		}
	}

	if(l == r)
		return false;
	else {
		pos = &pos[m];
		return true;
	}

}

bool OneConstantStatisticsBuffer::find_last(unsigned value)
{
	//const Triple* l = pos, *r = posLimit;
	int left = 0, right = posLimit - pos;
	int middle = 0;

	while (left < right) {
		middle = left + ((right - left) / 2);
		if (value < pos[middle].value1) {
			right = middle;
		} else if ((!middle) || value < pos[middle + 1].value1) {
			break;
		} else {
			left = middle + 1;
		}
	}

	if(left == right) {
		return false;
	} else {
		pos = &pos[middle];
		return true;
	}
}

Status OneConstantStatisticsBuffer::getStatis(unsigned& v1, unsigned v2 /* = 0 */)
{
	unsigned i;
	unsigned begin = index[ v1 / ID_HASH];
	unsigned end = 0;

	i = v1 / ID_HASH + 1;
	while(i < indexSize) { // get next chunk start offset;
		if(index[i] != 0) {
			end = index[i];
			break;
		}
		i++;
	}

	if(i == indexSize)
		end = usedSpace;

	reader = (unsigned char*)buffer->get_address() + begin;

	lastId = readDelta4(reader);
	reader += 4;

	//reader = readId(lastId, reader, true);
	if ( lastId == v1 ) {
		//reader = readId(v1, reader, false);
		v1 = readDelta4(reader);
		return OK;
	}

	const uchar* limit = (uchar*)buffer->get_address() + end;
	this->decode(reader - 4, limit);
	if(this->find(v1)) {
		if(pos->value1 == v1) {
			v1 = pos->count;
			return OK;
		}
	}

	return ERR;
}

Status OneConstantStatisticsBuffer::save(MMapBuffer*& indexBuffer)
{
#ifdef DEBUG
	cout<<"index size: "<<index.size()<<endl;
#endif
	char * writer;
	if(indexBuffer == NULL) {
		indexBuffer = MMapBuffer::create(string(string(DATABASE_PATH) + "/statIndex").c_str(), (index.size() + 2) * 4);
		writer = indexBuffer->get_address();
	} else {
		size_t size = indexBuffer->get_length();
		indexBuffer->resize(indexBuffer->get_length() + (index.size() + 2) * 4, false);
		writer = indexBuffer->get_address() + size;
	}
	writer = writeData(writer, usedSpace);
	writer = writeData(writer, index.size());
//	cout << "one: usedspace:" << usedSpace << "   indexsize:" <<  index.size() << endl;
	vector<unsigned>::iterator iter, limit;

	for(iter = index.begin(), limit = index.end(); iter != limit; iter++) {
		writer = writeData(writer, *iter);
	}
	//memcpy(writer, index, indexSize * sizeof(unsigned));

	return OK;
}

OneConstantStatisticsBuffer* OneConstantStatisticsBuffer::load(StatisticsType type,const string path, char*& indexBuffer)
{
	OneConstantStatisticsBuffer* statBuffer = new OneConstantStatisticsBuffer(path, type);

	unsigned size, first;
	indexBuffer = (char*)readData(indexBuffer, statBuffer->usedSpace);
	indexBuffer = (char*)readData(indexBuffer, size);
	cout << "one: usedspace:" << statBuffer->usedSpace << "   indexsize:" <<  size << endl;
	statBuffer->index.resize(0);

	statBuffer->indexSize = size;

	for( unsigned i = 0; i < size; i++ ) {
		indexBuffer = (char*)readData(indexBuffer, first);
		statBuffer->index.push_back(first);
	}

	return statBuffer;
}

Status OneConstantStatisticsBuffer::getIDs(EntityIDBuffer* entBuffer, ID minID, ID maxID)
{
	unsigned i, endEntry;
	unsigned begin = index[ minID / ID_HASH], end=0;
	reader = (uchar*)buffer->get_address() + begin;

	i = minID / ID_HASH;
	while(i < indexSize) {
		if(index[i] != 0) {
			end = index[i];
			break;
		}
		i++;
	}
	if(i == indexSize)
		end = usedSpace;
	endEntry = i;

	const uchar* limit = (uchar*)buffer->get_address() + end;

	lastId = readDelta4(reader);
	decode(reader, limit);
	if ( lastId != minID ) {
		find(minID);
	}

	i = maxID / ID_HASH + 1;
	unsigned end1;
	while(i < indexSize && index[i] == 0) {
		i++;
	}
	if(i >= indexSize)
		end1 = usedSpace;
	else
		end1 = index[i];

	while(true) {
		if(end == end1) {
			Triple* temp = pos;
			if(find(maxID) == true)
				posLimit = pos + 1;
			pos = temp;
		}

		while(pos < posLimit) {
			entBuffer->insertID(pos->value1);
			pos++;
		}

		begin = end;
		if(begin == end1)
			break;

		endEntry = endEntry + 1;
		while(endEntry < indexSize && index[endEntry] == 0) {
			endEntry++;
		}
		if(endEntry == indexSize) {
			end = usedSpace;
		} else {
			end = index[endEntry];
		}

		reader = (const unsigned char*)buffer->get_address() + begin;
		limit = (const unsigned char*)buffer->get_address() + end;
		decode(reader, limit);
	}

	return OK;
}

//////////////////////////////////////////////////////////////////////

TwoConstantStatisticsBuffer::TwoConstantStatisticsBuffer(const string path, StatisticsType type) : StatisticsBuffer(), type(type), reader(NULL)
{
	buffer = new MMapBuffer(path.c_str(), STATISTICS_BUFFER_INIT_PAGE_COUNT * MemoryBuffer::pagesize);
	//index = (Triple*)malloc(MemoryBuffer::pagesize * sizeof(Triple));
	writer = (uchar*)buffer->get_address();
	lastId = 0; lastPredicate = 0;
	usedSpace = 0;
	indexPos = 0;
	indexSize = 0; //MemoryBuffer::pagesize;
	index = NULL;

	first = true;
}

TwoConstantStatisticsBuffer::~TwoConstantStatisticsBuffer()
{
	if(buffer != NULL) {
		delete buffer;
	}
	buffer = NULL;
	index = NULL;
}

const uchar* TwoConstantStatisticsBuffer::decode(const uchar* begin, const uchar* end)
{
//	printf("decode   %x  %x\n",begin,end);
	unsigned value1 = readDelta4(begin); begin += 4;
	unsigned value2 = readDelta4(begin); begin += 4;
	unsigned count = readDelta4(begin); begin += 4;
	Triple* writer = &triples[0];
	(*writer).value1 = value1;
	(*writer).value2 = value2;
	(*writer).count = count;
//	cout << "value1:" << value1 << "   value2:" << value2 << "   count:" << count<<endl;
	++writer;

	// Decompress the remainder of the page
	while (begin < end) {
		// Decode the header byte
		unsigned info = *(begin++);
		// Small gap only?
		if (info < 0x80) {
			if (!info){
				break;
			}
			count = (info >> 5) + 1;
			value2 += (info & 31);
			(*writer).value1 = value1;
			(*writer).value2 = value2;
			(*writer).count = count;
//			cout << "value1:" << value1 << "   value2:" << value2 << "   count:" << count<<endl;
			++writer;
			continue;
		}
	      // Decode the parts
 		switch (info&127) {
		case 0: count=1; break;
		case 1: count=readDelta1(begin)+1; begin+=1; break;
		case 2: count=readDelta2(begin)+1; begin+=2; break;
		case 3: count=readDelta3(begin)+1; begin+=3; break;
		case 4: count=readDelta4(begin)+1; begin+=4; break;
		case 5: value2 += readDelta1(begin); count=1; begin+=1; break;
		case 6: value2 += readDelta1(begin); count=readDelta1(begin+1)+1; begin+=2; break;
		case 7: value2 += readDelta1(begin); count=readDelta2(begin+1)+1; begin+=3; break;
		case 8: value2 += readDelta1(begin); count=readDelta3(begin+1)+1; begin+=4; break;
		case 9: value2 += readDelta1(begin); count=readDelta4(begin+1)+1; begin+=5; break;
		case 10: value2 += readDelta2(begin); count=1; begin+=2; break;
		case 11: value2 += readDelta2(begin); count=readDelta1(begin+2)+1; begin+=3; break;
		case 12: value2 += readDelta2(begin); count=readDelta2(begin+2)+1; begin+=4; break;
		case 13: value2 += readDelta2(begin); count=readDelta3(begin+2)+1; begin+=5; break;
		case 14: value2 += readDelta2(begin); count=readDelta4(begin+2)+1; begin+=6; break;
		case 15: value2 += readDelta3(begin); count=1; begin+=3; break;
		case 16: value2 += readDelta3(begin); count=readDelta1(begin+3)+1; begin+=4; break;
		case 17: value2 += readDelta3(begin); count=readDelta2(begin+3)+1; begin+=5; break;
		case 18: value2 += readDelta3(begin); count=readDelta3(begin+3)+1; begin+=6; break;
		case 19: value2 += readDelta3(begin); count=readDelta4(begin+3)+1; begin+=7; break;
		case 20: value2 += readDelta4(begin); count=1; begin+=4; break;
		case 21: value2 += readDelta4(begin); count=readDelta1(begin+4)+1; begin+=5; break;
		case 22: value2 += readDelta4(begin); count=readDelta2(begin+4)+1; begin+=6; break;
		case 23: value2 += readDelta4(begin); count=readDelta3(begin+4)+1; begin+=7; break;
		case 24: value2 += readDelta4(begin); count=readDelta4(begin+4)+1; begin+=8; break;
		case 25: value1 += readDelta1(begin); value2=0; count=1; begin+=1; break;
		case 26: value1 += readDelta1(begin); value2=0; count=readDelta1(begin+1)+1; begin+=2; break;
		case 27: value1 += readDelta1(begin); value2=0; count=readDelta2(begin+1)+1; begin+=3; break;
		case 28: value1 += readDelta1(begin); value2=0; count=readDelta3(begin+1)+1; begin+=4; break;
		case 29: value1 += readDelta1(begin); value2=0; count=readDelta4(begin+1)+1; begin+=5; break;
		case 30: value1 += readDelta1(begin); value2=readDelta1(begin+1); count=1; begin+=2; break;
		case 31: value1 += readDelta1(begin); value2=readDelta1(begin+1); count=readDelta1(begin+2)+1; begin+=3; break;
		case 32: value1 += readDelta1(begin); value2=readDelta1(begin+1); count=readDelta2(begin+2)+1; begin+=4; break;
		case 33: value1+=readDelta1(begin); value2=readDelta1(begin+1); count=readDelta3(begin+2)+1; begin+=5; break;
		case 34: value1+=readDelta1(begin); value2=readDelta1(begin+1); count=readDelta4(begin+2)+1; begin+=6; break;
		case 35: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=1; begin+=3; break;
		case 36: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=readDelta1(begin+3)+1; begin+=4; break;
		case 37: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=readDelta2(begin+3)+1; begin+=5; break;
		case 38: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=readDelta3(begin+3)+1; begin+=6; break;
		case 39: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=readDelta4(begin+3)+1; begin+=7; break;
		case 40: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=1; begin+=4; break;
		case 41: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=readDelta1(begin+4)+1; begin+=5; break;
		case 42: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=readDelta2(begin+4)+1; begin+=6; break;
		case 43: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=readDelta3(begin+4)+1; begin+=7; break;
		case 44: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=readDelta4(begin+4)+1; begin+=8; break;
		case 45: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=1; begin+=5; break;
		case 46: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=readDelta1(begin+5)+1; begin+=6; break;
		case 47: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=readDelta2(begin+5)+1; begin+=7; break;
		case 48: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=readDelta3(begin+5)+1; begin+=8; break;
		case 49: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=readDelta4(begin+5)+1; begin+=9; break;
		case 50: value1+=readDelta2(begin); value2=0; count=1; begin+=2; break;
		case 51: value1+=readDelta2(begin); value2=0; count=readDelta1(begin+2)+1; begin+=3; break;
		case 52: value1+=readDelta2(begin); value2=0; count=readDelta2(begin+2)+1; begin+=4; break;
		case 53: value1+=readDelta2(begin); value2=0; count=readDelta3(begin+2)+1; begin+=5; break;
		case 54: value1+=readDelta2(begin); value2=0; count=readDelta4(begin+2)+1; begin+=6; break;
		case 55: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=1; begin+=3; break;
		case 56: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=readDelta1(begin+3)+1; begin+=4; break;
		case 57: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=readDelta2(begin+3)+1; begin+=5; break;
		case 58: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=readDelta3(begin+3)+1; begin+=6; break;
		case 59: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=readDelta4(begin+3)+1; begin+=7; break;
		case 60: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=1; begin+=4; break;
		case 61: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=readDelta1(begin+4)+1; begin+=5; break;
		case 62: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=readDelta2(begin+4)+1; begin+=6; break;
		case 63: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=readDelta3(begin+4)+1; begin+=7; break;
		case 64: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=readDelta4(begin+4)+1; begin+=8; break;
		case 65: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=1; begin+=5; break;
		case 66: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=readDelta1(begin+5)+1; begin+=6; break;
		case 67: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=readDelta2(begin+5)+1; begin+=7; break;
		case 68: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=readDelta3(begin+5)+1; begin+=8; break;
		case 69: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=readDelta4(begin+5)+1; begin+=9; break;
		case 70: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=1; begin+=6; break;
		case 71: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=readDelta1(begin+6)+1; begin+=7; break;
		case 72: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=readDelta2(begin+6)+1; begin+=8; break;
		case 73: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=readDelta3(begin+6)+1; begin+=9; break;
		case 74: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=readDelta4(begin+6)+1; begin+=10; break;
		case 75: value1+=readDelta3(begin); value2=0; count=1; begin+=3; break;
		case 76: value1+=readDelta3(begin); value2=0; count=readDelta1(begin+3)+1; begin+=4; break;
		case 77: value1+=readDelta3(begin); value2=0; count=readDelta2(begin+3)+1; begin+=5; break;
		case 78: value1+=readDelta3(begin); value2=0; count=readDelta3(begin+3)+1; begin+=6; break;
		case 79: value1+=readDelta3(begin); value2=0; count=readDelta4(begin+3)+1; begin+=7; break;
		case 80: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=1; begin+=4; break;
		case 81: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=readDelta1(begin+4)+1; begin+=5; break;
		case 82: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=readDelta2(begin+4)+1; begin+=6; break;
		case 83: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=readDelta3(begin+4)+1; begin+=7; break;
		case 84: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=readDelta4(begin+4)+1; begin+=8; break;
		case 85: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=1; begin+=5; break;
		case 86: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=readDelta1(begin+5)+1; begin+=6; break;
		case 87: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=readDelta2(begin+5)+1; begin+=7; break;
		case 88: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=readDelta3(begin+5)+1; begin+=8; break;
		case 89: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=readDelta4(begin+5)+1; begin+=9; break;
		case 90: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=1; begin+=6; break;
		case 91: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=readDelta1(begin+6)+1; begin+=7; break;
		case 92: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=readDelta2(begin+6)+1; begin+=8; break;
		case 93: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=readDelta3(begin+6)+1; begin+=9; break;
		case 94: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=readDelta4(begin+6)+1; begin+=10; break;
		case 95: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=1; begin+=7; break;
		case 96: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=readDelta1(begin+7)+1; begin+=8; break;
		case 97: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=readDelta2(begin+7)+1; begin+=9; break;
		case 98: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=readDelta3(begin+7)+1; begin+=10; break;
		case 99: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=readDelta4(begin+7)+1; begin+=11; break;
		case 100: value1+=readDelta4(begin); value2=0; count=1; begin+=4; break;
		case 101: value1+=readDelta4(begin); value2=0; count=readDelta1(begin+4)+1; begin+=5; break;
		case 102: value1+=readDelta4(begin); value2=0; count=readDelta2(begin+4)+1; begin+=6; break;
		case 103: value1+=readDelta4(begin); value2=0; count=readDelta3(begin+4)+1; begin+=7; break;
		case 104: value1+=readDelta4(begin); value2=0; count=readDelta4(begin+4)+1; begin+=8; break;
		case 105: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=1; begin+=5; break;
		case 106: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=readDelta1(begin+5)+1; begin+=6; break;
		case 107: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=readDelta2(begin+5)+1; begin+=7; break;
		case 108: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=readDelta3(begin+5)+1; begin+=8; break;
		case 109: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=readDelta4(begin+5)+1; begin+=9; break;
		case 110: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=1; begin+=6; break;
		case 111: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=readDelta1(begin+6)+1; begin+=7; break;
		case 112: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=readDelta2(begin+6)+1; begin+=8; break;
		case 113: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=readDelta3(begin+6)+1; begin+=9; break;
		case 114: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=readDelta4(begin+6)+1; begin+=10; break;
		case 115: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=1; begin+=7; break;
		case 116: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=readDelta1(begin+7)+1; begin+=8; break;
		case 117: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=readDelta2(begin+7)+1; begin+=9; break;
		case 118: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=readDelta3(begin+7)+1; begin+=10; break;
		case 119: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=readDelta4(begin+7)+1; begin+=11; break;
		case 120: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=1; begin+=8; break;
		case 121: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=readDelta1(begin+8)+1; begin+=9; break;
		case 122: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=readDelta2(begin+8)+1; begin+=10; break;
		case 123: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=readDelta3(begin+8)+1; begin+=11; break;
		case 124: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=readDelta4(begin+8)+1; begin+=12; break;
		}
		(*writer).value1=value1;
		(*writer).value2=value2;
		(*writer).count=count;
//		cout << "value1:" << value1 << "   value2:" << value2 << "   count:" << count<<endl;
		++writer;
	}

	// Update the entries
	pos=triples;
	posLimit=writer;

	return begin;
}

/*const uchar* TwoConstantStatisticsBuffer::decodeIdAndPredicate(const uchar* begin, const uchar* end)
{
	cout << "decodeIdAndPredicate" << endl;
	unsigned value1 = readDelta4(begin); begin += 4;
	unsigned value2 = readDelta4(begin); begin += 4;
	unsigned count = readDelta4(begin); begin += 4;
	Triple* writer = &triples[0];
	(*writer).value1 = value1;
	(*writer).value2 = value2;
	(*writer).count = count;
	++writer;

	// Decompress the remainder of the page
	while (begin < end) {
		// Decode the header byte
		unsigned info = *(begin++);
		// Small gap only?
		if (info < 0x80) {
			if (!info)
				break;
			//count = (info >> 5) + 1;
			value2 += (info & 31);
			(*writer).value1 = value1;
			(*writer).value2 = value2;
			(*writer).count = count;
			++writer;
			continue;
		}
	      // Decode the parts
		switch (info&127) {
		case 0: break;
		case 1: begin+=1; break;
		case 2: begin+=2; break;
		case 3: begin+=3; break;
		case 4: begin+=4; break;
		case 5: value2 += readDelta1(begin); begin+=1; break;
		case 6: value2 += readDelta1(begin); begin+=2; break;
		case 7: value2 += readDelta1(begin); begin+=3; break;
		case 8: value2 += readDelta1(begin); begin+=4; break;
		case 9: value2 += readDelta1(begin); begin+=5; break;
		case 10: value2 += readDelta2(begin); begin+=2; break;
		case 11: value2 += readDelta2(begin); begin+=3; break;
		case 12: value2 += readDelta2(begin); begin+=4; break;
		case 13: value2 += readDelta2(begin); begin+=5; break;
		case 14: value2 += readDelta2(begin); begin+=6; break;
		case 15: value2 += readDelta3(begin); begin+=3; break;
		case 16: value2 += readDelta3(begin); begin+=4; break;
		case 17: value2 += readDelta3(begin); begin+=5; break;
		case 18: value2 += readDelta3(begin); begin+=6; break;
		case 19: value2 += readDelta3(begin); begin+=7; break;
		case 20: value2 += readDelta4(begin); begin+=4; break;
		case 21: value2 += readDelta4(begin); begin+=5; break;
		case 22: value2 += readDelta4(begin); begin+=6; break;
		case 23: value2 += readDelta4(begin); begin+=7; break;
		case 24: value2 += readDelta4(begin); begin+=8; break;
		case 25: value1 += readDelta1(begin); value2=0; begin+=1; break;
		case 26: value1 += readDelta1(begin); value2=0; begin+=2; break;
		case 27: value1 += readDelta1(begin); value2=0; begin+=3; break;
		case 28: value1 += readDelta1(begin); value2=0; begin+=4; break;
		case 29: value1 += readDelta1(begin); value2=0; begin+=5; break;
		case 30: value1 += readDelta1(begin); value2=readDelta1(begin+1); begin+=2; break;
		case 31: value1 += readDelta1(begin); value2=readDelta1(begin+1); begin+=3; break;
		case 32: value1 += readDelta1(begin); value2=readDelta1(begin+1); begin+=4; break;
		case 33: value1+=readDelta1(begin); value2=readDelta1(begin+1); begin+=5; break;
		case 34: value1+=readDelta1(begin); value2=readDelta1(begin+1); begin+=6; break;
		case 35: value1+=readDelta1(begin); value2=readDelta2(begin+1); begin+=3; break;
		case 36: value1+=readDelta1(begin); value2=readDelta2(begin+1); begin+=4; break;
		case 37: value1+=readDelta1(begin); value2=readDelta2(begin+1); begin+=5; break;
		case 38: value1+=readDelta1(begin); value2=readDelta2(begin+1); begin+=6; break;
		case 39: value1+=readDelta1(begin); value2=readDelta2(begin+1); begin+=7; break;
		case 40: value1+=readDelta1(begin); value2=readDelta3(begin+1); begin+=4; break;
		case 41: value1+=readDelta1(begin); value2=readDelta3(begin+1); begin+=5; break;
		case 42: value1+=readDelta1(begin); value2=readDelta3(begin+1); begin+=6; break;
		case 43: value1+=readDelta1(begin); value2=readDelta3(begin+1); begin+=7; break;
		case 44: value1+=readDelta1(begin); value2=readDelta3(begin+1); begin+=8; break;
		case 45: value1+=readDelta1(begin); value2=readDelta4(begin+1); begin+=5; break;
		case 46: value1+=readDelta1(begin); value2=readDelta4(begin+1); begin+=6; break;
		case 47: value1+=readDelta1(begin); value2=readDelta4(begin+1); begin+=7; break;
		case 48: value1+=readDelta1(begin); value2=readDelta4(begin+1); begin+=8; break;
		case 49: value1+=readDelta1(begin); value2=readDelta4(begin+1); begin+=9; break;
		case 50: value1+=readDelta2(begin); value2=0; begin+=2; break;
		case 51: value1+=readDelta2(begin); value2=0; begin+=3; break;
		case 52: value1+=readDelta2(begin); value2=0; begin+=4; break;
		case 53: value1+=readDelta2(begin); value2=0; begin+=5; break;
		case 54: value1+=readDelta2(begin); value2=0; begin+=6; break;
		case 55: value1+=readDelta2(begin); value2=readDelta1(begin+2); begin+=3; break;
		case 56: value1+=readDelta2(begin); value2=readDelta1(begin+2); begin+=4; break;
		case 57: value1+=readDelta2(begin); value2=readDelta1(begin+2); begin+=5; break;
		case 58: value1+=readDelta2(begin); value2=readDelta1(begin+2); begin+=6; break;
		case 59: value1+=readDelta2(begin); value2=readDelta1(begin+2); begin+=7; break;
		case 60: value1+=readDelta2(begin); value2=readDelta2(begin+2); begin+=4; break;
		case 61: value1+=readDelta2(begin); value2=readDelta2(begin+2); begin+=5; break;
		case 62: value1+=readDelta2(begin); value2=readDelta2(begin+2); begin+=6; break;
		case 63: value1+=readDelta2(begin); value2=readDelta2(begin+2); begin+=7; break;
		case 64: value1+=readDelta2(begin); value2=readDelta2(begin+2); begin+=8; break;
		case 65: value1+=readDelta2(begin); value2=readDelta3(begin+2); begin+=5; break;
		case 66: value1+=readDelta2(begin); value2=readDelta3(begin+2); begin+=6; break;
		case 67: value1+=readDelta2(begin); value2=readDelta3(begin+2); begin+=7; break;
		case 68: value1+=readDelta2(begin); value2=readDelta3(begin+2); begin+=8; break;
		case 69: value1+=readDelta2(begin); value2=readDelta3(begin+2); begin+=9; break;
		case 70: value1+=readDelta2(begin); value2=readDelta4(begin+2); begin+=6; break;
		case 71: value1+=readDelta2(begin); value2=readDelta4(begin+2); begin+=7; break;
		case 72: value1+=readDelta2(begin); value2=readDelta4(begin+2); begin+=8; break;
		case 73: value1+=readDelta2(begin); value2=readDelta4(begin+2); begin+=9; break;
		case 74: value1+=readDelta2(begin); value2=readDelta4(begin+2); begin+=10; break;
		case 75: value1+=readDelta3(begin); value2=0; begin+=3; break;
		case 76: value1+=readDelta3(begin); value2=0; begin+=4; break;
		case 77: value1+=readDelta3(begin); value2=0; begin+=5; break;
		case 78: value1+=readDelta3(begin); value2=0; begin+=6; break;
		case 79: value1+=readDelta3(begin); value2=0; begin+=7; break;
		case 80: value1+=readDelta3(begin); value2=readDelta1(begin+3); begin+=4; break;
		case 81: value1+=readDelta3(begin); value2=readDelta1(begin+3); begin+=5; break;
		case 82: value1+=readDelta3(begin); value2=readDelta1(begin+3); begin+=6; break;
		case 83: value1+=readDelta3(begin); value2=readDelta1(begin+3); begin+=7; break;
		case 84: value1+=readDelta3(begin); value2=readDelta1(begin+3); begin+=8; break;
		case 85: value1+=readDelta3(begin); value2=readDelta2(begin+3); begin+=5; break;
		case 86: value1+=readDelta3(begin); value2=readDelta2(begin+3); begin+=6; break;
		case 87: value1+=readDelta3(begin); value2=readDelta2(begin+3); begin+=7; break;
		case 88: value1+=readDelta3(begin); value2=readDelta2(begin+3); begin+=8; break;
		case 89: value1+=readDelta3(begin); value2=readDelta2(begin+3); begin+=9; break;
		case 90: value1+=readDelta3(begin); value2=readDelta3(begin+3); begin+=6; break;
		case 91: value1+=readDelta3(begin); value2=readDelta3(begin+3); begin+=7; break;
		case 92: value1+=readDelta3(begin); value2=readDelta3(begin+3); begin+=8; break;
		case 93: value1+=readDelta3(begin); value2=readDelta3(begin+3); begin+=9; break;
		case 94: value1+=readDelta3(begin); value2=readDelta3(begin+3); begin+=10; break;
		case 95: value1+=readDelta3(begin); value2=readDelta4(begin+3); begin+=7; break;
		case 96: value1+=readDelta3(begin); value2=readDelta4(begin+3); begin+=8; break;
		case 97: value1+=readDelta3(begin); value2=readDelta4(begin+3); begin+=9; break;
		case 98: value1+=readDelta3(begin); value2=readDelta4(begin+3); begin+=10; break;
		case 99: value1+=readDelta3(begin); value2=readDelta4(begin+3); begin+=11; break;
		case 100: value1+=readDelta4(begin); value2=0; begin+=4; break;
		case 101: value1+=readDelta4(begin); value2=0; begin+=5; break;
		case 102: value1+=readDelta4(begin); value2=0; begin+=6; break;
		case 103: value1+=readDelta4(begin); value2=0; begin+=7; break;
		case 104: value1+=readDelta4(begin); value2=0; begin+=8; break;
		case 105: value1+=readDelta4(begin); value2=readDelta1(begin+4); begin+=5; break;
		case 106: value1+=readDelta4(begin); value2=readDelta1(begin+4); begin+=6; break;
		case 107: value1+=readDelta4(begin); value2=readDelta1(begin+4); begin+=7; break;
		case 108: value1+=readDelta4(begin); value2=readDelta1(begin+4); begin+=8; break;
		case 109: value1+=readDelta4(begin); value2=readDelta1(begin+4); begin+=9; break;
		case 110: value1+=readDelta4(begin); value2=readDelta2(begin+4); begin+=6; break;
		case 111: value1+=readDelta4(begin); value2=readDelta2(begin+4); begin+=7; break;
		case 112: value1+=readDelta4(begin); value2=readDelta2(begin+4); begin+=8; break;
		case 113: value1+=readDelta4(begin); value2=readDelta2(begin+4); begin+=9; break;
		case 114: value1+=readDelta4(begin); value2=readDelta2(begin+4); begin+=10; break;
		case 115: value1+=readDelta4(begin); value2=readDelta3(begin+4); begin+=7; break;
		case 116: value1+=readDelta4(begin); value2=readDelta3(begin+4); begin+=8; break;
		case 117: value1+=readDelta4(begin); value2=readDelta3(begin+4); begin+=9; break;
		case 118: value1+=readDelta4(begin); value2=readDelta3(begin+4); begin+=10; break;
		case 119: value1+=readDelta4(begin); value2=readDelta3(begin+4); begin+=11; break;
		case 120: value1+=readDelta4(begin); value2=readDelta4(begin+4); begin+=8; break;
		case 121: value1+=readDelta4(begin); value2=readDelta4(begin+4); begin+=9; break;
		case 122: value1+=readDelta4(begin); value2=readDelta4(begin+4); begin+=10; break;
		case 123: value1+=readDelta4(begin); value2=readDelta4(begin+4); begin+=11; break;
		case 124: value1+=readDelta4(begin); value2=readDelta4(begin+4); begin+=12; break;
		}
		(*writer).value1=value1;
		(*writer).value2=value2;
		(*writer).count=count;
		++writer;
	}

	// Update the entries
	pos=triples;
	posLimit=writer;

	return begin;
}*/

static inline bool greater(unsigned a1,unsigned a2,unsigned b1,unsigned b2) {
   return (a1>b1)||((a1==b1)&&(a2>b2));
}

static inline bool less(unsigned a1, unsigned a2, unsigned b1, unsigned b2) {
	return (a1 < b1) || ((a1 == b1) && (a2 < b2));
}
/*
 * find the first entry >= (value1, value2);
 * pos: the start address of the first triple;
 * posLimit: the end address of last triple;
 */
bool TwoConstantStatisticsBuffer::find(unsigned value1, unsigned value2)
{
	//const Triple* l = pos, *r = posLimit;
	int left = 0, right = posLimit - pos;
	int middle=0;
	while (left < right) {
		middle = left + ((right - left) / 2);
		if (::greater(value1, value2, pos[middle].value1, pos[middle].value2)) {
			left = middle +1;
		} else if ((!middle) || ::greater(value1, value2, pos[middle - 1].value1, pos[middle -1].value2)) {
			break;
		} else {
			right = middle;
		}
	}
//	cout << "left:" << left << "  right:" << right << "  middle:" << middle << endl;
	if(left == right) {
		return false;
	} else {
		pos = &pos[middle];
//		cout << "pos[middle]:" << pos[middle].value1 << "  " << pos[middle].value2 << endl;
		return true;
	}
}

bool TwoConstantStatisticsBuffer::find(unsigned value1)
{//find by the value1
	//const Triple* l = pos, *r = posLimit;
	int left = 0, right = posLimit - pos;
//	cout << "right:" << right << endl;
	int middle=0;

	while (left < right) {
		middle = left + ((right - left) / 2);
//		cout << "first:" << pos[middle].value1 << "  " << value1 << "  "<< pos[middle - 1].value1 << endl;
		if (value1 > pos[middle].value1) {
			left = middle +1;
		} else if ((!middle) || value1 > pos[middle - 1].value1) {
//			cout << "break1:" << pos[middle].value1 << "  " << value1 << "  "<< pos[middle - 1].value1 << endl;
			break;
		} else {
			right = middle;
		}
	}

	if(left == right) {
		pos = &pos[middle];
		return false;
	} else {
		pos = &pos[middle];
//		cout << "pos[middle]:" << pos[middle].value1 << "  " << pos[middle].value2 << endl;
		return true;
	}
}

/*
 * find the last entry <= (value1, value2);
 * pos: the start address of the first triple;
 * posLimit: the end address of last triple;
 */
bool TwoConstantStatisticsBuffer::find_last(unsigned value1, unsigned value2)
{
	//const Triple* l = pos, *r = posLimit;
	int left = 0, right = posLimit - pos;
	int middle = 0;

	while (left < right) {
		middle = left + ((right - left) / 2);
		if (::less(value1, value2, pos[middle].value1, pos[middle].value2)) {
			right = middle;
		} else if ((!middle) || ::less(value1, value2, pos[middle + 1].value1, pos[middle + 1].value2)) {
			break;
		} else {
			left = middle + 1;
		}
	}

	if(left == right) {
		return false;
	} else {
		pos = &pos[middle];
		return true;
	}
}

int TwoConstantStatisticsBuffer::findPredicate(unsigned value1,Triple*pos,Triple* posLimit){
	int low = 0, high= posLimit - pos,mid;
	while (low <= high) { //当前查找区间R[low..high]非空
		mid = low + ((high - low)/2);
		if (pos[mid].value1 == value1)
			return mid; //查找成功返回
		if (pos[mid].value1 > value1)
			high = mid - 1; //继续在R[low..mid-1]中查找
		else
			low = mid + 1; //继续在R[mid+1..high]中查找
	}
	return -1; //当low>high时表示查找区间为空，查找失败

}

Status TwoConstantStatisticsBuffer::getStatis(unsigned& v1, unsigned v2)
{
	pos = index, posLimit = index + indexPos;
	find(v1, v2);
	if(::greater(pos->value1, pos->value2, v1, v2))
		pos--;

	unsigned start = pos->count; pos++;
	unsigned end = pos->count;
	if(pos == (index + indexPos))
		end = usedSpace;

	const unsigned char* begin = (uchar*)buffer->get_address() + start, *limit = (uchar*)buffer->get_address() + end;
	decode(begin, limit);
	find(v1, v2);
	if(pos->value1 == v1 && pos->value2 == v2) {
		v1 = pos->count;
		return OK;
	}
	v1 = 0;
	return NOT_FOUND;
}

Status TwoConstantStatisticsBuffer::addStatis(unsigned v1, unsigned v2, unsigned v3)
{
//	static bool first = true;
	unsigned len = 0;
//	cout << "value1:" << v1 << "   value2:" << v2 << "   count:" << v3<<endl;

	// otherwise store the compressed value
	if ( v1 == lastId && (v2 - lastPredicate) < 32 && v3 < 5) {
		len = 1;
	} else if(v1 == lastId) {
		len = 1 + getLen(v2 - lastPredicate) + getLen(v3 - 1);
	} else {
		len = 1+ getLen(v1-lastId)+ getLen(v2)+getLen(v3 - 1);
	}

	if ( first == true || ( usedSpace + len ) > buffer->get_length() ) {
		usedSpace = writer - (uchar*)buffer->get_address();
		buffer->resize(buffer->get_length() + STATISTICS_BUFFER_INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize, false);
		writer = (uchar*)buffer->get_address() + usedSpace;

		writeUint32(writer, v1); writer += 4;
		writeUint32(writer, v2); writer += 4;
		writeUint32(writer, v3); writer += 4;

		if((indexPos + 1) >= indexSize) {
#ifdef DEBUF
			cout<<"indexPos: "<<indexPos<<" indexSize: "<<indexSize<<endl;
#endif
			index = (Triple*)realloc(index, indexSize * sizeof(Triple) + MemoryBuffer::pagesize * sizeof(Triple));
			indexSize += MemoryBuffer::pagesize;
		}

		index[indexPos].value1 = v1;
		index[indexPos].value2 = v2;
		index[indexPos].count = usedSpace; //record offset
		indexPos++;

//		cout << "v1:" << v1 << "  v2:" << v2 <<"  usedspace:" << usedSpace << "  indexPos:" << indexPos << "   indexSize:"<< indexSize << endl;

		first = false;
	} else {
		if (v1 == lastId && v2 - lastPredicate < 32 && v3 < 5) {
			*writer++ = ((v3 - 1) << 5) | (v2 - lastPredicate);
		} else if (v1 == lastId) {
			*writer++ = 0x80 | (getLen(v2 - lastPredicate) * 5 + getLen(v3 - 1));
			writer = writeDelta0(writer, v2 - lastPredicate);
			writer = writeDelta0(writer, v3 - 1);
		} else {
			*writer++ = 0x80 | (getLen(v1 - lastId) * 25 + getLen(v2) * 5 + getLen(v3 - 1));
			writer = writeDelta0(writer, v1 - lastId);
			writer = writeDelta0(writer, v2);
			writer = writeDelta0(writer, v3 - 1);
		}
	}

	lastId = v1; lastPredicate = v2;

	usedSpace = writer - (uchar*)buffer->get_address();
	return OK;
}

Status TwoConstantStatisticsBuffer::save(MMapBuffer*& indexBuffer)
{
	char* writer;
	if(indexBuffer == NULL) {
		indexBuffer = MMapBuffer::create(string(string(DATABASE_PATH) + "/statIndex").c_str(), indexPos * sizeof(Triple) + 2 * sizeof(unsigned));
		writer = indexBuffer->get_address();
	} else {
		size_t size = indexBuffer->get_length();
		indexBuffer->resize(indexBuffer->get_length() + indexPos * sizeof(Triple) + 2 * sizeof(unsigned), false);
		writer = indexBuffer->get_address() + size;
	}

	writer = writeData(writer, usedSpace);
	writer = writeData(writer, indexPos);
//	cout << "two usedSpace:" << usedSpace << "  indexPos:" << indexPos << endl;
	memcpy(writer, (char*)index, indexPos * sizeof(Triple));
#ifdef DEBUG
	for(int i = 0; i < 3; i++)
	{
		cout<<index[i].value1<<" : "<<index[i].value2<<" : "<<index[i].count<<endl;
	}

	cout<<"indexPos: "<<indexPos<<endl;
#endif
	free(index);

	return OK;
}
//#define DEBUG 1
TwoConstantStatisticsBuffer* TwoConstantStatisticsBuffer::load(StatisticsType type, const string path, char*& indexBuffer)
{
	TwoConstantStatisticsBuffer* statBuffer = new TwoConstantStatisticsBuffer(path, type);

	indexBuffer = (char*)readData(indexBuffer, statBuffer->usedSpace);
	indexBuffer = (char*)readData(indexBuffer, statBuffer->indexPos);
	cout << "two usedSpace:" << statBuffer->usedSpace << "  indexPos:" << statBuffer->indexPos << endl;
#ifdef DEBUG
	cout<<__FUNCTION__<<"indexPos: "<<statBuffer->indexPos<<endl;
#endif
	// load index;
	statBuffer->index = (Triple*)indexBuffer;
	indexBuffer = indexBuffer + statBuffer->indexPos * sizeof(Triple);

#ifdef DEBUG
	for(int i = 0; i < 3; i++)
	{
		cout<<statBuffer->index[i].value1<<" : "<<statBuffer->index[i].value2<<" : "<<statBuffer->index[i].count<<endl;
	}
#endif

	return statBuffer;
}

TwoConstantStatisticsBuffer* TwoConstantStatisticsBuffer::loadone(StatisticsType type, const string path, char*& indexBuffer){
	unsigned  size;
	char* begin = indexBuffer;
	indexBuffer += 4;

	indexBuffer = (char*) readData(indexBuffer, size);
	indexBuffer = indexBuffer + size * 4;
	indexBuffer += 4;
	indexBuffer = (char*) readData(indexBuffer, size);
	indexBuffer = indexBuffer + size * 4;
	if (type == OBJECTPREDICATE_STATIS) {
		indexBuffer += 4;
		indexBuffer = (char*) readData(indexBuffer, size);
		indexBuffer = indexBuffer + size * sizeof(Triple);
	}
	//munmap the useless part
	munmap(begin,((int)(indexBuffer-begin)/4096)*4096);

	TwoConstantStatisticsBuffer* statBuffer = new TwoConstantStatisticsBuffer(
			path, type);
	indexBuffer = (char*) readData(indexBuffer, statBuffer->usedSpace);
	indexBuffer = (char*) readData(indexBuffer, statBuffer->indexPos);
//	cout << "two usedSpace:" << statBuffer->usedSpace << "  indexPos:"
//			<< statBuffer->indexPos << endl;

	// load index;
	statBuffer->index = (Triple*) indexBuffer;
	indexBuffer = indexBuffer + statBuffer->indexPos * sizeof(Triple);

	return statBuffer;
}




unsigned TwoConstantStatisticsBuffer::getLen(unsigned v)
{
	if (v>=(1<<24))
		return 4; 
	else if (v>=(1<<16))
		return 3;
	else if (v>=(1<<8)) 
		return 2;
	else if(v > 0)
		return 1;
	else
		return 0;
}
Status TwoConstantStatisticsBuffer::getPredicatesByID(unsigned id,
		EntityIDBuffer* entBuffer, ID minID, ID maxID) {
	Triple* pos, *posLimit;
	pos = index;
	posLimit = index + indexPos;
	find(id, pos, posLimit);
	//	cout << "findchunk:" << pos->value1 << "  " << pos->value2 << endl;
	assert(pos >= index && pos < posLimit);
	Triple* startChunk = NULL;
	Triple* endChunk = NULL;
	if (pos == index) {
		assert(id >= pos->value1);
		startChunk = pos;
		endChunk = pos + 1;
	} else if (pos->value1 > id) {
		startChunk = pos - 1;
		endChunk = pos;
	} else if (pos->value1 == id) {
		//		cout << "pos:" << pos->value1 << "   " <<pos->value2 << endl;
		if (pos->value2 == 1) {
			//			cout << "here" << endl;
			startChunk = pos;
		} else
			startChunk = pos - 1;
		endChunk = pos + 1;
	} else if (pos->value1 < id) {
		startChunk = pos;
		endChunk = pos + 1;
	}

	const unsigned char* begin, *limit;
	Triple* chunkIter = startChunk;

	while (chunkIter < endChunk) {
		//		cout << "------------------------------------------------" << endl;
		begin = (uchar*) buffer->get_address() + chunkIter->count;
		//		printf("1: %x  %x  %u\n",begin, buffer->get_address() ,chunkIter->count);
		chunkIter++;
		if (chunkIter == index + indexPos)
			limit = (uchar*) buffer->get_address() + usedSpace;
		else
			limit = (uchar*) buffer->get_address() + chunkIter->count;
		//		printf("2: %x  %x  %u\n",limit, buffer->get_address() ,chunkIter->count);

		Triple triples[3 * MemoryBuffer::pagesize];
		decode(begin, limit, triples, pos, posLimit);

		int mid = findPredicate(id, pos, posLimit), loc = mid;
		//		cout << mid << "  " << loc << endl;


		if (loc == -1)
			continue;
		entBuffer->insertID(pos[loc].value2);
		//		cout << "result:" << pos[loc].value2<< endl;
		while (pos[--loc].value1 == id && loc >= 0) {
			entBuffer->insertID(pos[loc].value2);
			//			cout << "result:" << pos[loc].value2<< endl;
		}
		loc = mid;
		while (pos[++loc].value1 == id && loc < posLimit - pos) {
			entBuffer->insertID(pos[loc].value2);
			//			cout << "result:" << pos[loc].value2<< endl;
		}
	}

	return OK;
}
//-------------------------------add for matrix---------------------------------------------------

Status TwoConstantStatisticsBuffer::getAllPredicatesByID(unsigned id, ID resultBuffer[18],int& resultBufferlen)
{
	Triple* pos,*posLimit;
	resultBufferlen = 0;
	pos = index;
	posLimit = index + indexPos;
	find(id,pos,posLimit);
//	cout << "findchunk:" << pos->value1 << "  " << pos->value2 << endl;
	assert(pos >=index && pos < posLimit);
	Triple* startChunk = NULL;
	Triple* endChunk = NULL;
	if(pos == index){
		assert(id >= pos->value1);
		startChunk = pos;
		endChunk = pos + 1;
	}else if(pos->value1> id) {
		startChunk = pos-1;
		endChunk = pos;
	}else if (pos->value1 == id) {
//		cout << "pos:" << pos->value1 << "   " <<pos->value2 << endl;
		if (pos->value2 ==1){
//			cout << "here" << endl;
			startChunk = pos;
		}
		else
			startChunk = pos - 1;
		endChunk = pos + 1;
	}else if (pos->value1 < id) {
		startChunk = pos;
		endChunk = pos + 1;
	}
//	cout << "start chunk pos:" << startChunk->value1 << "  " << startChunk->value2 << "  "<< startChunk->count << endl;
//	cout << "end chunk pos:" << endChunk->value1 << "  " << endChunk->value2 << "   " <<   endChunk->count<< endl;

	const unsigned char* begin, *limit;
	Triple* chunkIter = startChunk;

	while(chunkIter < endChunk) {
//		cout << "------------------------------------------------" << endl;
		begin = (uchar*)buffer->get_address() + chunkIter->count;
//		printf("1: %x  %x  %u\n",begin, buffer->get_address() ,chunkIter->count);
		chunkIter++;
		if(chunkIter == index + indexPos)
			limit = (uchar*)buffer->get_address() + usedSpace;
		else
			limit = (uchar*)buffer->get_address() + chunkIter->count;
//		printf("2: %x  %x  %u\n",limit, buffer->get_address() ,chunkIter->count);

		Triple triples[3*MemoryBuffer::pagesize];
		decode(begin, limit,triples,pos,posLimit);

		int mid = findPredicate(id,pos,posLimit),loc = mid;
//		cout << mid << "  " << loc << endl;


		if(loc == -1) continue;
//		cout << pos[loc].value2 << endl;
		resultBuffer[resultBufferlen++] = pos[loc].value2;
//		cout << "result:" << pos[loc].value2<< endl;
		while(pos[--loc].value1 == id && loc >=0){
			resultBuffer[resultBufferlen++] = pos[loc].value2;
//			cout << "result:" << pos[loc].value2<< endl;
		}
		loc = mid;
		while(pos[++loc].value1 == id && loc < posLimit - pos){
			resultBuffer[resultBufferlen++] = pos[loc].value2;
//			cout << "result:" << pos[loc].value2<< endl;
		}
	}

	return OK;
}



bool TwoConstantStatisticsBuffer::find(unsigned value1,Triple*& pos,Triple*& posLimit)
{//find by the value1
	//const Triple* l = pos, *r = posLimit;
	int left = 0, right = posLimit - pos;
//	cout << "right:" << right << endl;
	int middle=0;

	while (left < right) {
		middle = left + ((right - left) / 2);
//		cout << "first:" << pos[middle].value1 << "  " << value1 << "  "<< pos[middle - 1].value1 << endl;
		if (value1 > pos[middle].value1) {
			left = middle +1;
		} else if ((!middle) || value1 > pos[middle - 1].value1) {
//			cout << "break1:" << pos[middle].value1 << "  " << value1 << "  "<< pos[middle - 1].value1 << endl;
			break;
		} else {
			right = middle;
		}
	}

	if(left == right) {
		pos = &pos[middle];
		return false;
	} else {
		pos = &pos[middle];
//		cout << "pos[middle]:" << pos[middle].value1 << "  " << pos[middle].value2 << endl;
		return true;
	}
}


const uchar* TwoConstantStatisticsBuffer::decode(const uchar* begin, const uchar* end,Triple*triples,Triple* &pos,Triple* &posLimit)
{
//	printf("decode   %x  %x\n",begin,end);
	unsigned value1 = readDelta4(begin); begin += 4;
	unsigned value2 = readDelta4(begin); begin += 4;
	unsigned count = readDelta4(begin); begin += 4;
	Triple* writer = &triples[0];
	(*writer).value1 = value1;
	(*writer).value2 = value2;
	(*writer).count = count;
//	cout << "value1:" << value1 << "   value2:" << value2 << "   count:" << count<<endl;
	++writer;

	// Decompress the remainder of the page
	while (begin < end) {
		// Decode the header byte
		unsigned info = *(begin++);
		// Small gap only?
		if (info < 0x80) {
			if (!info){
				break;
			}
			count = (info >> 5) + 1;
			value2 += (info & 31);
			(*writer).value1 = value1;
			(*writer).value2 = value2;
			(*writer).count = count;
//			cout << "value1:" << value1 << "   value2:" << value2 << "   count:" << count<<endl;
			++writer;
			continue;
		}
	      // Decode the parts
 		switch (info&127) {
		case 0: count=1; break;
		case 1: count=readDelta1(begin)+1; begin+=1; break;
		case 2: count=readDelta2(begin)+1; begin+=2; break;
		case 3: count=readDelta3(begin)+1; begin+=3; break;
		case 4: count=readDelta4(begin)+1; begin+=4; break;
		case 5: value2 += readDelta1(begin); count=1; begin+=1; break;
		case 6: value2 += readDelta1(begin); count=readDelta1(begin+1)+1; begin+=2; break;
		case 7: value2 += readDelta1(begin); count=readDelta2(begin+1)+1; begin+=3; break;
		case 8: value2 += readDelta1(begin); count=readDelta3(begin+1)+1; begin+=4; break;
		case 9: value2 += readDelta1(begin); count=readDelta4(begin+1)+1; begin+=5; break;
		case 10: value2 += readDelta2(begin); count=1; begin+=2; break;
		case 11: value2 += readDelta2(begin); count=readDelta1(begin+2)+1; begin+=3; break;
		case 12: value2 += readDelta2(begin); count=readDelta2(begin+2)+1; begin+=4; break;
		case 13: value2 += readDelta2(begin); count=readDelta3(begin+2)+1; begin+=5; break;
		case 14: value2 += readDelta2(begin); count=readDelta4(begin+2)+1; begin+=6; break;
		case 15: value2 += readDelta3(begin); count=1; begin+=3; break;
		case 16: value2 += readDelta3(begin); count=readDelta1(begin+3)+1; begin+=4; break;
		case 17: value2 += readDelta3(begin); count=readDelta2(begin+3)+1; begin+=5; break;
		case 18: value2 += readDelta3(begin); count=readDelta3(begin+3)+1; begin+=6; break;
		case 19: value2 += readDelta3(begin); count=readDelta4(begin+3)+1; begin+=7; break;
		case 20: value2 += readDelta4(begin); count=1; begin+=4; break;
		case 21: value2 += readDelta4(begin); count=readDelta1(begin+4)+1; begin+=5; break;
		case 22: value2 += readDelta4(begin); count=readDelta2(begin+4)+1; begin+=6; break;
		case 23: value2 += readDelta4(begin); count=readDelta3(begin+4)+1; begin+=7; break;
		case 24: value2 += readDelta4(begin); count=readDelta4(begin+4)+1; begin+=8; break;
		case 25: value1 += readDelta1(begin); value2=0; count=1; begin+=1; break;
		case 26: value1 += readDelta1(begin); value2=0; count=readDelta1(begin+1)+1; begin+=2; break;
		case 27: value1 += readDelta1(begin); value2=0; count=readDelta2(begin+1)+1; begin+=3; break;
		case 28: value1 += readDelta1(begin); value2=0; count=readDelta3(begin+1)+1; begin+=4; break;
		case 29: value1 += readDelta1(begin); value2=0; count=readDelta4(begin+1)+1; begin+=5; break;
		case 30: value1 += readDelta1(begin); value2=readDelta1(begin+1); count=1; begin+=2; break;
		case 31: value1 += readDelta1(begin); value2=readDelta1(begin+1); count=readDelta1(begin+2)+1; begin+=3; break;
		case 32: value1 += readDelta1(begin); value2=readDelta1(begin+1); count=readDelta2(begin+2)+1; begin+=4; break;
		case 33: value1+=readDelta1(begin); value2=readDelta1(begin+1); count=readDelta3(begin+2)+1; begin+=5; break;
		case 34: value1+=readDelta1(begin); value2=readDelta1(begin+1); count=readDelta4(begin+2)+1; begin+=6; break;
		case 35: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=1; begin+=3; break;
		case 36: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=readDelta1(begin+3)+1; begin+=4; break;
		case 37: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=readDelta2(begin+3)+1; begin+=5; break;
		case 38: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=readDelta3(begin+3)+1; begin+=6; break;
		case 39: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=readDelta4(begin+3)+1; begin+=7; break;
		case 40: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=1; begin+=4; break;
		case 41: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=readDelta1(begin+4)+1; begin+=5; break;
		case 42: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=readDelta2(begin+4)+1; begin+=6; break;
		case 43: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=readDelta3(begin+4)+1; begin+=7; break;
		case 44: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=readDelta4(begin+4)+1; begin+=8; break;
		case 45: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=1; begin+=5; break;
		case 46: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=readDelta1(begin+5)+1; begin+=6; break;
		case 47: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=readDelta2(begin+5)+1; begin+=7; break;
		case 48: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=readDelta3(begin+5)+1; begin+=8; break;
		case 49: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=readDelta4(begin+5)+1; begin+=9; break;
		case 50: value1+=readDelta2(begin); value2=0; count=1; begin+=2; break;
		case 51: value1+=readDelta2(begin); value2=0; count=readDelta1(begin+2)+1; begin+=3; break;
		case 52: value1+=readDelta2(begin); value2=0; count=readDelta2(begin+2)+1; begin+=4; break;
		case 53: value1+=readDelta2(begin); value2=0; count=readDelta3(begin+2)+1; begin+=5; break;
		case 54: value1+=readDelta2(begin); value2=0; count=readDelta4(begin+2)+1; begin+=6; break;
		case 55: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=1; begin+=3; break;
		case 56: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=readDelta1(begin+3)+1; begin+=4; break;
		case 57: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=readDelta2(begin+3)+1; begin+=5; break;
		case 58: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=readDelta3(begin+3)+1; begin+=6; break;
		case 59: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=readDelta4(begin+3)+1; begin+=7; break;
		case 60: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=1; begin+=4; break;
		case 61: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=readDelta1(begin+4)+1; begin+=5; break;
		case 62: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=readDelta2(begin+4)+1; begin+=6; break;
		case 63: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=readDelta3(begin+4)+1; begin+=7; break;
		case 64: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=readDelta4(begin+4)+1; begin+=8; break;
		case 65: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=1; begin+=5; break;
		case 66: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=readDelta1(begin+5)+1; begin+=6; break;
		case 67: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=readDelta2(begin+5)+1; begin+=7; break;
		case 68: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=readDelta3(begin+5)+1; begin+=8; break;
		case 69: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=readDelta4(begin+5)+1; begin+=9; break;
		case 70: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=1; begin+=6; break;
		case 71: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=readDelta1(begin+6)+1; begin+=7; break;
		case 72: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=readDelta2(begin+6)+1; begin+=8; break;
		case 73: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=readDelta3(begin+6)+1; begin+=9; break;
		case 74: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=readDelta4(begin+6)+1; begin+=10; break;
		case 75: value1+=readDelta3(begin); value2=0; count=1; begin+=3; break;
		case 76: value1+=readDelta3(begin); value2=0; count=readDelta1(begin+3)+1; begin+=4; break;
		case 77: value1+=readDelta3(begin); value2=0; count=readDelta2(begin+3)+1; begin+=5; break;
		case 78: value1+=readDelta3(begin); value2=0; count=readDelta3(begin+3)+1; begin+=6; break;
		case 79: value1+=readDelta3(begin); value2=0; count=readDelta4(begin+3)+1; begin+=7; break;
		case 80: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=1; begin+=4; break;
		case 81: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=readDelta1(begin+4)+1; begin+=5; break;
		case 82: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=readDelta2(begin+4)+1; begin+=6; break;
		case 83: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=readDelta3(begin+4)+1; begin+=7; break;
		case 84: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=readDelta4(begin+4)+1; begin+=8; break;
		case 85: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=1; begin+=5; break;
		case 86: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=readDelta1(begin+5)+1; begin+=6; break;
		case 87: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=readDelta2(begin+5)+1; begin+=7; break;
		case 88: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=readDelta3(begin+5)+1; begin+=8; break;
		case 89: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=readDelta4(begin+5)+1; begin+=9; break;
		case 90: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=1; begin+=6; break;
		case 91: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=readDelta1(begin+6)+1; begin+=7; break;
		case 92: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=readDelta2(begin+6)+1; begin+=8; break;
		case 93: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=readDelta3(begin+6)+1; begin+=9; break;
		case 94: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=readDelta4(begin+6)+1; begin+=10; break;
		case 95: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=1; begin+=7; break;
		case 96: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=readDelta1(begin+7)+1; begin+=8; break;
		case 97: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=readDelta2(begin+7)+1; begin+=9; break;
		case 98: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=readDelta3(begin+7)+1; begin+=10; break;
		case 99: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=readDelta4(begin+7)+1; begin+=11; break;
		case 100: value1+=readDelta4(begin); value2=0; count=1; begin+=4; break;
		case 101: value1+=readDelta4(begin); value2=0; count=readDelta1(begin+4)+1; begin+=5; break;
		case 102: value1+=readDelta4(begin); value2=0; count=readDelta2(begin+4)+1; begin+=6; break;
		case 103: value1+=readDelta4(begin); value2=0; count=readDelta3(begin+4)+1; begin+=7; break;
		case 104: value1+=readDelta4(begin); value2=0; count=readDelta4(begin+4)+1; begin+=8; break;
		case 105: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=1; begin+=5; break;
		case 106: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=readDelta1(begin+5)+1; begin+=6; break;
		case 107: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=readDelta2(begin+5)+1; begin+=7; break;
		case 108: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=readDelta3(begin+5)+1; begin+=8; break;
		case 109: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=readDelta4(begin+5)+1; begin+=9; break;
		case 110: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=1; begin+=6; break;
		case 111: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=readDelta1(begin+6)+1; begin+=7; break;
		case 112: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=readDelta2(begin+6)+1; begin+=8; break;
		case 113: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=readDelta3(begin+6)+1; begin+=9; break;
		case 114: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=readDelta4(begin+6)+1; begin+=10; break;
		case 115: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=1; begin+=7; break;
		case 116: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=readDelta1(begin+7)+1; begin+=8; break;
		case 117: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=readDelta2(begin+7)+1; begin+=9; break;
		case 118: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=readDelta3(begin+7)+1; begin+=10; break;
		case 119: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=readDelta4(begin+7)+1; begin+=11; break;
		case 120: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=1; begin+=8; break;
		case 121: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=readDelta1(begin+8)+1; begin+=9; break;
		case 122: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=readDelta2(begin+8)+1; begin+=10; break;
		case 123: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=readDelta3(begin+8)+1; begin+=11; break;
		case 124: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=readDelta4(begin+8)+1; begin+=12; break;
		}
		(*writer).value1=value1;
		(*writer).value2=value2;
		(*writer).count=count;
//		cout << "value1:" << value1 << "   value2:" << value2 << "   count:" << count<<endl;
		++writer;
	}

	// Update the entries
	pos=triples;
	posLimit=writer;

	return begin;
}
