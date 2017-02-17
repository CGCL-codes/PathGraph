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

#include "MemoryBuffer.h"
#include "BitmapBuffer.h"
#include "BitVectorWAH.h"
#include "MMapBuffer.h"
#include "TempFile.h"
#include<unistd.h>
#include<sys/mman.h>

unsigned int ChunkManager::bufferCount = 0;

//#define WORD_ALIGN 1

BitmapBuffer::BitmapBuffer(const string _dir) : dir(_dir) {
	// TODO Auto-generated constructor stub
	startColID = 1;
	string filename(dir);
	filename.append("/temp1");
	temp1 = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);

	filename.assign(dir.begin(), dir.end());
	filename.append("/temp2");
	temp2 = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);

	filename.assign(dir.begin(), dir.end());
	filename.append("/temp3");
	temp3 = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT* MemoryBuffer::pagesize);

	filename.assign(dir.begin(), dir.end());
	filename.append("/temp4");
	temp4 = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);

	usedPage1 = usedPage2 = usedPage3 = usedPage4 = 0;
}

BitmapBuffer::~BitmapBuffer() {
	// TODO Auto-generated destructor stub
	for(map<ID, ChunkManager*>::iterator iter = predicate_managers[0].begin(); iter != predicate_managers[0].end(); iter++) {
		if(iter->second != NULL) {
			delete iter->second;
			iter->second = NULL;
		}
	}

	for (map<ID, ChunkManager*>::iterator iter = predicate_managers[1].begin(); iter != predicate_managers[1].end(); iter++) {
		if (iter->second != NULL) {
			delete iter->second;
			iter->second = NULL;
		}
	}
}

Status BitmapBuffer::insertPredicate(ID id,unsigned char type) {
	predicate_managers[type][id] = new ChunkManager(id, type, this);
	return OK;
}

Status BitmapBuffer::deletePredicate(ID id) {
	//TODO
	return OK;

}

size_t BitmapBuffer::getTripleCount()
{
	size_t tripleCount = 0;
	map<ID, ChunkManager*>::iterator begin, limit;
	for(begin = predicate_managers[0].begin(), limit = predicate_managers[0].end(); begin != limit; begin++) {
		tripleCount = tripleCount + begin->second->getTripleCount();
	}
	cout<<"triple count: "<<tripleCount<<endl;

	tripleCount = 0;
	for(begin = predicate_managers[1].begin(), limit = predicate_managers[1].end(); begin != limit; begin++) {
		tripleCount = tripleCount + begin->second->getTripleCount();
	}
	cout<<"triple count: "<<tripleCount<<endl;

	return tripleCount;
}

/*
 *	@param id: the chunk manager id ( predicate id );
 *       type: the predicate_manager type;
 */

ChunkManager* BitmapBuffer::getChunkManager(ID id, unsigned char type) {
	//there is no predicate_managers[id]
	if(!predicate_managers[type].count(id)) {
		//the first time to insert
		insertPredicate(id, type);
	}
	return predicate_managers[type][id];
}


/*
 *	@param f: 0 for triple being sorted by subject; 1 for triple being sorted by object
 *         flag: indicate whether x is bigger than y;
 */
Status BitmapBuffer::insertTriple(ID predicateId, ID xId, ID yId, bool flag, unsigned char f) {
	unsigned char len;

	len = getLen(xId);
	len += getLen(yId);

	if ( flag == false){
		getChunkManager(predicateId, f)->insertXY(xId, yId, len, 1);
	}else {
		getChunkManager(predicateId, f)->insertXY(xId, yId, len, 2);
	}

//	cout<<getChunkManager(1, 0)->meta->length[0]<<" "<<getChunkManager(1, 0)->meta->tripleCount[0]<<endl;
	return OK;
}

Status BitmapBuffer::completeInsert()
{
	//TODO do something after complete;
	startColID = 1;

	for (map<ID, ChunkManager*>::iterator iter = predicate_managers[0].begin(); iter != predicate_managers[0].end(); iter++ ){
		if(iter->second != 0) {
			iter->second->setColStartAndEnd(startColID);
		}
	//	cout<<startColID<<endl;
	}

	startColID = 1;
	for (map<ID, ChunkManager*>::iterator iter = predicate_managers[1].begin(); iter != predicate_managers[1].end(); iter++ ){
		if(iter->second != 0) {
			iter->second->setColStartAndEnd(startColID);
		}
	//	cout<<startColID<<endl;
	}

	return OK;
}

void BitmapBuffer::flush()
{
	temp1->flush();
	temp2->flush();
	temp3->flush();
	temp4->flush();
}

void BitmapBuffer::generateXY(ID& subjectID, ID& objectID)
{
	ID temp;

	if(subjectID > objectID)
	{
		temp = subjectID;
		subjectID = objectID;
		objectID = temp - objectID;
	}else{
		objectID = objectID - subjectID;
	}
}

unsigned char BitmapBuffer::getBytes(ID id)
{
	if(id <=0xFF){
		return 1;
	}else if(id <= 0xFFFF){
		return 2;
	}else if(id <= 0xFFFFFF){
		return 3;
	}else if(id <= 0xFFFFFFFF){
		return 4;
	}else{
		return 0;
	}
}

char* BitmapBuffer::getPage(unsigned char type, unsigned char flag, size_t& pageNo)
{
	char* rt;
	bool tempresize = false;
	MMapBuffer *temp = NULL;
//	cout<<__FUNCTION__<<" begin"<<endl;
	if(type == 0 ) {
		if(flag == 0) {
           // printf("temp1: %x  length:%zu usedPage:%u\n",temp1->get_length(),temp1->get_length(),usedPage1);
			if(usedPage1 * MemoryBuffer::pagesize >= temp1->get_length()) {
				temp1->resize(temp1->get_length() + INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize, false);
				temp = temp1;
				tempresize = true;
			}
			pageNo = usedPage1;
			rt = temp1->get_address() + usedPage1 * MemoryBuffer::pagesize;
			usedPage1++;
           // printf("temp1: %x  length:%zu usedPage:%u  rt:%x\n",temp1->get_length(),temp1->get_length(),usedPage1,rt);
		} else {
          //  printf("temp2: %p  length:%zu usedPage:%u\n",temp2->get_address(),temp2->get_length(),usedPage2);
			if(usedPage2 * MemoryBuffer::pagesize >= temp2->get_length()) {
				temp2->resize(temp2->get_length() + INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize, false);
				temp = temp2;
				tempresize = true;
			}
			pageNo = usedPage2;
			rt = temp2->get_address() + usedPage2 * MemoryBuffer::pagesize;
			usedPage2++;
            //printf("temp2: %p  length:%zu usedPage:%u  rt:%p\n",temp2->get_address(),temp2->get_length(),usedPage2,rt);
		}
	} else {
		if(flag == 0) {
			if(usedPage3 * MemoryBuffer::pagesize >= temp3->get_length()) {
				temp3->resize(temp3->get_length() + INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize, false);
				temp = temp3;
				tempresize = true;
			}
			pageNo = usedPage3;
			rt = temp3->get_address() + usedPage3 * MemoryBuffer::pagesize;
			usedPage3++;
		} else {
			if(usedPage4 * MemoryBuffer::pagesize >= temp4->get_length()) {
				temp4->resize(temp4->get_length() + INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize, false);
				temp = temp4;
				tempresize = true;
			}
			pageNo = usedPage4;
			rt = temp4->get_address() + usedPage4 * MemoryBuffer::pagesize;
			usedPage4++;
		}
	}
//	tempresize = false;

	if(tempresize == true ) {
		if(type == 0) {
			if(flag == 0) {
				map<ID, ChunkManager*>::iterator iter, limit;
				iter = predicate_managers[0].begin(); limit = predicate_managers[0].end();
				for(; iter != limit; iter++) {
					if(iter->second == NULL)
						continue;
					iter->second->meta = (ChunkManagerMeta*)(temp1->get_address() + iter->second->usedPage[0][0] * MemoryBuffer::pagesize);

						iter->second->meta->endPtr[0] = temp1->get_address() + iter->second->usedPage[0].back() * MemoryBuffer::pagesize +
								MemoryBuffer::pagesize - (iter->second->meta->length[0] - iter->second->meta->usedSpace[0] - sizeof(ChunkManagerMeta));

					}
			} else {
//	            cout<<"tempresize begin"<< endl;
    			map<ID, ChunkManager*>::iterator iter, limit;
				iter = predicate_managers[0].begin(); limit = predicate_managers[0].end();
				for(; iter != limit; iter++) {
					if(iter->second == NULL)
						continue;
//                    printf("<");
//                    printf("temp2 addr:%p ",temp2->get_address());
//                    printf("usedPage[1].back():%zu %zu  ", iter->second->usedPage[1].back() * MemoryBuffer::pagesize);
//                    printf("+:%p",temp2->get_address() + iter->second->usedPage[1].back() * MemoryBuffer::pagesize);
                    iter->second->meta->endPtr[1] = temp2->get_address() + iter->second->usedPage[1].back() * MemoryBuffer::pagesize +
							MemoryBuffer::pagesize - (iter->second->meta->length[1] - iter->second->meta->usedSpace[1]);
//                    cout<<">" <<endl;
				}
  //              cout<<"tempresize begin"<< endl;
			}
		} else if(type == 1) {
			if(flag == 0) {
				map<ID, ChunkManager*>::iterator iter, limit;
				iter = predicate_managers[1].begin(); limit = predicate_managers[1].end();
				for(; iter != limit; iter++) {
					if(iter->second == NULL)
						continue;
					iter->second->meta = (ChunkManagerMeta*)(temp3->get_address() + iter->second->usedPage[0][0] * MemoryBuffer::pagesize);

						iter->second->meta->endPtr[0] = temp3->get_address() + iter->second->usedPage[0].back() * MemoryBuffer::pagesize +
								MemoryBuffer::pagesize - (iter->second->meta->length[0] - iter->second->meta->usedSpace[0] - sizeof(ChunkManagerMeta));

				}
			} else {
				map<ID, ChunkManager*>::iterator iter, limit;
				iter = predicate_managers[1].begin(); limit = predicate_managers[1].end();
				for(; iter != limit; iter++) {
					if(iter->second == NULL)
						continue;
					iter->second->meta->endPtr[1] = temp4->get_address() + iter->second->usedPage[1].back() * MemoryBuffer::pagesize +
							MemoryBuffer::pagesize - (iter->second->meta->length[1] - iter->second->meta->usedSpace[1]);
				}
			}
		}
	}

//	cout<<__FUNCTION__<<" end"<<endl;

	return rt;
}

unsigned char BitmapBuffer::getLen(ID id) {
	unsigned char len = 0;
	while(id >= 128) {
		len++;
		id >>= 7;
	}
	return len + 1;
}

void BitmapBuffer::save()
{
	//loadfromtemp();
	static bool first = true;
	string filename = dir + "/BitmapBuffer";
	MMapBuffer * buffer; //new MMapBuffer(filename.c_str(), 0);
	string predicateFile(filename);
	predicateFile.append("_predicate");
	unsigned preSize = max(predicate_managers[0].size(),predicate_managers[1].size());
	MMapBuffer * predicateBuffer =new MMapBuffer(predicateFile.c_str(), preSize * (sizeof(ID) + sizeof(size_t)) * 2);

	char* predicateWriter = predicateBuffer->get_address();
	char* bufferWriter = NULL;

	map<ID, ChunkManager*>::const_iterator iter = predicate_managers[0].begin();
	char* startPtr;
	size_t offset = 0;
	startPtr = iter->second->ptrs[0];

	if(first == true) {
		buffer = new MMapBuffer(filename.c_str(), iter->second->meta->length[0]);
		//offset = buffer->get_offset();
		//first = false;
	} else {
		//buffer = new MMapBuffer(filename.c_str(), iter->second->meta->length[0], true);
		//offset = buffer->get_offset();
	}

	predicateWriter = predicateBuffer->get_address();
	bufferWriter = buffer->get_address();
	vector<size_t>::iterator pageNoIter = iter->second->usedPage[0].begin(),
			limit = iter->second->usedPage[0].end();
//	cout <<"01 pre:" << iter->first <<"  ";
	for(; pageNoIter != limit; pageNoIter++ ) {
		size_t pageNo = *pageNoIter;
//		cout << pageNo <<"  ";
		memcpy(bufferWriter, temp1->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
		bufferWriter = bufferWriter + MemoryBuffer::pagesize;
	}
//	cout<< endl;

	*((ID*)predicateWriter) = iter->first; predicateWriter = predicateWriter + sizeof(ID);
	*((size_t*)predicateWriter) = offset; predicateWriter = predicateWriter + sizeof(size_t);
	//cout << iter->first << "  " << offset << endl;
	offset = offset + iter->second->meta->length[0];

	buffer->resize(buffer->get_length() + iter->second->meta->length[1], false);
	bufferWriter = buffer->get_address();
	char* startPos = bufferWriter + offset;
//	cout <<"02 pre:" << iter->first <<"  ";
	pageNoIter = iter->second->usedPage[1].begin(); limit = iter->second->usedPage[1].end();
	for(; pageNoIter != limit; pageNoIter++ ) {
		size_t pageNo = *pageNoIter;
//		cout << pageNo <<"  ";
		memcpy(startPos, temp2->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
		startPos = startPos + MemoryBuffer::pagesize;
	}
//	cout<< endl;
	assert(iter->second->meta->length[1] == iter->second->usedPage[1].size() * MemoryBuffer::pagesize);
	offset = offset + iter->second->meta->length[1];

	iter++;
	for(; iter != predicate_managers[0].end(); iter++) {
		buffer->resize(buffer->get_length() + iter->second->meta->length[0], false);
		bufferWriter = buffer->get_address();
		startPos = bufferWriter + offset;

		pageNoIter = iter->second->usedPage[0].begin(); limit = iter->second->usedPage[0].end();
//		cout <<"1 pre:" << iter->first <<"  ";
		for(; pageNoIter != limit; pageNoIter++) {
			size_t pageNo = *pageNoIter;
//			cout << pageNo <<"  ";
			memcpy(startPos, temp1->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
			startPos = startPos + MemoryBuffer::pagesize;
		}
//		cout << endl;
		//cout<<"used page count: "<<iter->second->usedPage[0].size()<<endl;

		//iter->second->meta->endPtr[0] = startPos + iter->second->meta->usedSpace[0];  //used to build index;

		*((ID*)predicateWriter) = iter->first; predicateWriter = predicateWriter + sizeof(ID);
		*((size_t*)predicateWriter) = offset; predicateWriter = predicateWriter + sizeof(size_t);
		offset += iter->second->meta->length[0];

		assert(iter->second->usedPage[0].size() * MemoryBuffer::pagesize == iter->second->meta->length[0]);

		buffer->resize(buffer->get_length() + iter->second->meta->length[1], false);
		bufferWriter = buffer->get_address();
		startPos = bufferWriter + offset;
		//iter->second->meta->startPtr[1] = startPos; //used to build index;
		//iter->second->meta->endPtr[1] = startPos + iter->second->meta->usedSpace[1];
//		cout <<"2 pre:" << iter->first <<"  ";
		pageNoIter = iter->second->usedPage[1].begin(); limit = iter->second->usedPage[1].end();
		for(; pageNoIter != limit; pageNoIter++) {
			size_t pageNo = *pageNoIter;
//			cout << pageNo <<"  ";
			memcpy(startPos, temp2->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
			startPos = startPos + MemoryBuffer::pagesize;
		}
//		cout << endl;
		offset += iter->second->meta->length[1];
		assert(iter->second->usedPage[1].size() * MemoryBuffer::pagesize == iter->second->meta->length[1]);
	}

	buffer->flush();
//	temp1->remove();
//	temp2->remove();

	iter = predicate_managers[1].begin();
	for(; iter != predicate_managers[1].end(); iter++) {
		buffer->resize(buffer->get_length() + iter->second->meta->length[0], false);
		bufferWriter = buffer->get_address();
		startPos = bufferWriter + offset;
//		cout <<"3 pre:" << iter->first <<"  ";
		pageNoIter = iter->second->usedPage[0].begin(); limit = iter->second->usedPage[0].end();
		for(; pageNoIter != limit; pageNoIter++) {
			size_t pageNo = *pageNoIter;
//			cout << pageNo <<"  ";
			memcpy(startPos, temp3->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
			startPos = startPos + MemoryBuffer::pagesize;
		}
//		cout << endl;
		*((ID*)predicateWriter) = iter->first; predicateWriter = predicateWriter + sizeof(ID);
		*((size_t*)predicateWriter) = offset; predicateWriter = predicateWriter + sizeof(size_t);
		offset += iter->second->meta->length[0];

		assert(iter->second->usedPage[0].size() * MemoryBuffer::pagesize == iter->second->meta->length[0]);

		buffer->resize(buffer->get_length() + iter->second->usedPage[1].size() * MemoryBuffer::pagesize, false);
		bufferWriter = buffer->get_address();
		startPos = bufferWriter + offset;
//		cout <<"4 pre:" << iter->first <<"  ";
		pageNoIter = iter->second->usedPage[1].begin(); limit = iter->second->usedPage[1].end();
		for(; pageNoIter != limit; pageNoIter++) {
			size_t pageNo = *pageNoIter;
//			cout << pageNo <<"  ";
			memcpy(startPos, temp4->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
			startPos = startPos + MemoryBuffer::pagesize;
		}
//		cout<< endl;
		offset += iter->second->meta->length[1];
		assert(iter->second->usedPage[1].size() * MemoryBuffer::pagesize == iter->second->meta->length[1]);
	}
	buffer->flush();
	predicateBuffer->flush();

	predicateWriter = predicateBuffer->get_address();
	int i = 0;

	ID id;
	for(iter = predicate_managers[0].begin(); iter != predicate_managers[0].end(); iter++, i++) {
		id = *((ID*)predicateWriter);
		assert(iter->first == id);
		predicateWriter = predicateWriter + sizeof(ID);
		offset = *((size_t*)predicateWriter);
		predicateWriter = predicateWriter + sizeof(size_t);

		char* base = buffer->get_address() + offset;
		iter->second->meta = (ChunkManagerMeta*)base;
		iter->second->meta->startPtr[0] = base + sizeof(ChunkManagerMeta);
		iter->second->meta->endPtr[0] = iter->second->meta->startPtr[0] + iter->second->meta->usedSpace[0];
		iter->second->meta->startPtr[1] = base + iter->second->meta->length[0];
		iter->second->meta->endPtr[1] = iter->second->meta->startPtr[1] + iter->second->meta->usedSpace[1];
		//::printMeta(*(iter->second->meta));
	}

	for(iter = predicate_managers[1].begin(); iter != predicate_managers[1].end(); iter++, i++) {
		id = *((ID*)predicateWriter);
		//assert(iter->first == id);
		predicateWriter = predicateWriter + sizeof(ID);
		offset = *((size_t*)predicateWriter);
		predicateWriter = predicateWriter + sizeof(size_t);

		char* base = buffer->get_address() + offset;
		iter->second->meta = (ChunkManagerMeta*)base;
		iter->second->meta->startPtr[0] = base + sizeof(ChunkManagerMeta);
		iter->second->meta->endPtr[0] = iter->second->meta->startPtr[0] + iter->second->meta->usedSpace[0];
		iter->second->meta->startPtr[1] = base + iter->second->meta->length[0];
		iter->second->meta->endPtr[1] = iter->second->meta->startPtr[1] + iter->second->meta->usedSpace[1];
		//::printMeta(*(iter->second->meta));
	}

//	temp3->remove();
//	temp4->remove();

	//build index;
	MMapBuffer* bitmapIndex = NULL;
#ifdef DEBUG
	cout<<"build hash index for subject"<<endl;
#endif
	for ( map<ID,ChunkManager*>::iterator iter = predicate_managers[0].begin(); iter != predicate_managers[0].end(); iter++ ) {
		if ( iter->second != NULL ) {
#ifdef DEBUG
			cout<<iter->first<<endl;
#endif
			iter->second->buildChunkIndex();
			iter->second->getChunkIndex(1)->save(bitmapIndex);
			iter->second->getChunkIndex(2)->save(bitmapIndex);

		}
	}

#ifdef DEBUG
	cout<<"build hash index for object"<<endl;
#endif
	for ( map<ID, ChunkManager*>::iterator iter = predicate_managers[1].begin(); iter != predicate_managers[1].end(); iter++ ) {
		if ( iter->second != NULL ) {
#ifdef DEBUF
			cout<<iter->first<<endl;
#endif
			iter->second->buildChunkIndex();
			iter->second->getChunkIndex(1)->save(bitmapIndex);
			iter->second->getChunkIndex(2)->save(bitmapIndex);
		}
	}

	delete bitmapIndex;
	delete buffer;
	delete predicateBuffer;
}

BitmapBuffer*  BitmapBuffer::load(MMapBuffer* bitmapImage, MMapBuffer*& bitmapIndexImage, MMapBuffer* bitmapPredicateImage)
{
	//TODO load objects from image file;
	BitmapBuffer* buffer = new BitmapBuffer();

	char* predicateReader = bitmapPredicateImage->get_address();

	size_t predicateSize = bitmapPredicateImage->get_length() / ((sizeof(ID) + sizeof(size_t)) * 2);
	ID id;
	size_t offset = 0, indexOffset = 0;
	for(size_t i = 0; i < predicateSize; i++) {
		id = *((ID*)predicateReader); predicateReader = predicateReader + sizeof(ID);
		predicateReader = predicateReader + sizeof(size_t);
		ChunkManager* manager = ChunkManager::load(id, 0, bitmapImage->get_address(), offset);
		manager->chunkIndex[0] = LineHashIndex::load(*manager, LineHashIndex::SUBJECT_INDEX, bitmapIndexImage->get_address(), indexOffset);
		manager->chunkIndex[1] = LineHashIndex::load(*manager, LineHashIndex::SUBJECT_INDEX, bitmapIndexImage->get_address(), indexOffset);

		buffer->predicate_managers[0][id] = manager;

/*
		manager->otherIndex[0] = new BPlusIndex(*manager, BPlusIndex::SUBJECT_INDEX);
		manager->otherIndex[0]->buildIndex(1);
		manager->otherIndex[1] = new BPlusIndex(*manager, BPlusIndex::SUBJECT_INDEX);
		manager->otherIndex[1]->buildIndex(2);
*/
	}

	for(size_t i = 0; i < predicateSize; i++) {
		id = *((ID*)predicateReader); predicateReader = predicateReader + sizeof(ID);
		offset = *((size_t*)predicateReader);
		predicateReader = predicateReader + sizeof(size_t);

		ChunkManager* manager = ChunkManager::load(id, 1, bitmapImage->get_address(), offset);
		manager->chunkIndex[0] = LineHashIndex::load(*manager, LineHashIndex::OBJECT_INDEX, bitmapIndexImage->get_address(), indexOffset);
		manager->chunkIndex[1] = LineHashIndex::load(*manager, LineHashIndex::OBJECT_INDEX, bitmapIndexImage->get_address(), indexOffset);

		buffer->predicate_managers[1][id] = manager;

/*
		manager->otherIndex[0] = new BPlusIndex(*manager, BPlusIndex::OBJECT_INDEX);
		manager->otherIndex[0]->buildIndex(1);
		manager->otherIndex[1] = new BPlusIndex(*manager, BPlusIndex::OBJECT_INDEX);
		manager->otherIndex[1]->buildIndex(2);
*/
	}

	return buffer;
}

BitmapBuffer*  BitmapBuffer::loadPart(MMapBuffer* bitmapImage, MMapBuffer*& bitmapIndexImage, MMapBuffer* bitmapPredicateImage)
{
	//TODO load objects from image file;
	BitmapBuffer* buffer = new BitmapBuffer();

	char* predicateReader = bitmapPredicateImage->get_address();

	size_t predicateSize = bitmapPredicateImage->get_length() / ((sizeof(ID) + sizeof(size_t)) * 2);
	ID id;
	size_t offset = 0, indexOffset = 0,offsetStart = 0, indexOffsetStart = 0;
	size_t len1 = 0,len2 = 0;
	for(size_t i = 0; i < predicateSize; i++) {
		id = *((ID*)predicateReader); predicateReader = predicateReader + sizeof(ID);
		predicateReader = predicateReader + sizeof(size_t);
		ChunkManager* manager = ChunkManager::load(id, 0, bitmapImage->get_address(), offset);
		manager->chunkIndex[0] = LineHashIndex::load(*manager, LineHashIndex::SUBJECT_INDEX, bitmapIndexImage->get_address(), indexOffset);
		manager->chunkIndex[1] = LineHashIndex::load(*manager, LineHashIndex::SUBJECT_INDEX, bitmapIndexImage->get_address(), indexOffset);
		manager->bitmapBuffer = buffer;
		buffer->predicate_managers[0][id] = manager;
	}
//	cout << "=====================================================================================" << endl;

	offsetStart = offset;
	indexOffsetStart = indexOffset;

	for(size_t i = 0; i < predicateSize; i++) {
		id = *((ID*)predicateReader); predicateReader = predicateReader + sizeof(ID);
		offset = *((size_t*)predicateReader);
		predicateReader = predicateReader + sizeof(size_t);

		ChunkManager::unload(id, 1, bitmapImage->get_address(), offset);
		LineHashIndex::unload( bitmapIndexImage->get_address(), indexOffset);
		LineHashIndex::unload( bitmapIndexImage->get_address(), indexOffset);

	}

//	munmap the useless part
	offsetStart = (int)::ceil((double)offset/MemoryBuffer::pagesize) * MemoryBuffer::pagesize;
	indexOffsetStart = (int)::ceil((double)indexOffset/MemoryBuffer::pagesize) * MemoryBuffer::pagesize;
	len1 = (int)::ceil((double)(offset - offsetStart)/MemoryBuffer::pagesize) * MemoryBuffer::pagesize;
	len2 = (int)::ceil((double)(indexOffset - indexOffsetStart)/MemoryBuffer::pagesize) * MemoryBuffer::pagesize;
	munmap(bitmapImage->get_address()+ offsetStart,len1);
	munmap(bitmapIndexImage->get_address() +indexOffsetStart,len2 );
	return buffer;
}


size_t BitmapBuffer::getSize(unsigned  type) {
	map<ID, ChunkManager*>::iterator iter, limit;
	iter = predicate_managers[type].begin();
	limit = predicate_managers[type].end();
	size_t size = 0;
	for (; iter != limit; iter++) {
		size += iter->second->getTripleCount();
		cout << "rowcount " << iter->second->getPredicateID() << ": " << iter->second->getTripleCount() << endl;
	}
	cout << predicate_managers[type].size() << endl;
	return size;
//	return predicate_managers[type].size();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*
void getTempFilename(string& filename, unsigned pid, unsigned _type) {
	filename.clear();
	filename.append(DATABASE_PATH);
	filename.append("temp_");
	char temp[5];
	sprintf(temp, "%d", pid);
	filename.append(temp);
	sprintf(temp, "%d", _type);
	filename.append(temp);
}*/

ChunkManager::ChunkManager(unsigned pid, unsigned _type, BitmapBuffer* _bitmapBuffer) : bitmapBuffer(_bitmapBuffer) {
	/*string filename;
	getTempFilename(filename, pid, _type);
	filename.append("_0");
	ptrs[0] = new MMapBuffer(filename.c_str(), sizeof(ChunkManagerMeta) + INIT_PAGE_COUNT * MemoryBuffer::pagesize);
	getTempFilename(filename, pid, _type);
	filename.append("_1");
	ptrs[1] = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);*/
	usedPage[0].resize(0); usedPage[1].resize(0);
	size_t pageNo = 0;
	meta = NULL;
	ptrs[0] = bitmapBuffer->getPage(_type, 0, pageNo);
	usedPage[0].push_back(pageNo);
	ptrs[1] = bitmapBuffer->getPage(_type, 1, pageNo);
	usedPage[1].push_back(pageNo);

	assert(ptrs[1] != ptrs[0]);

	meta = (ChunkManagerMeta*)ptrs[0];
	memset((char*)meta, 0, sizeof(ChunkManagerMeta));
	meta->endPtr[0] = meta->startPtr[0] = ptrs[0] + sizeof(ChunkManagerMeta);
	meta->endPtr[1] = meta->startPtr[1] = ptrs[1];
	meta->length[0] = usedPage[0].size() * MemoryBuffer::pagesize;
	meta->length[1] = usedPage[1].size() * MemoryBuffer::pagesize;
	meta->usedSpace[0] = 0;
	meta->usedSpace[1] = 0;
	meta->tripleCount[0] = meta->tripleCount[1] = 0;
	meta->pid = pid;
	meta->type = _type;

	//need to modify!
	if( meta->type == 0) {
		chunkIndex[0] = new LineHashIndex(*this, LineHashIndex::SUBJECT_INDEX);
		chunkIndex[1] = new LineHashIndex(*this, LineHashIndex::SUBJECT_INDEX);
	} else {
		chunkIndex[0] = new LineHashIndex(*this, LineHashIndex::OBJECT_INDEX);
		chunkIndex[1] = new LineHashIndex(*this, LineHashIndex::OBJECT_INDEX);
	}

	for (int i = 0; i < 2; i++)
		meta->tripleCount[i] = 0;

}

ChunkManager::~ChunkManager() {
	// TODO Auto-generated destructor stub
	///free the buffer;
	ptrs[0] = ptrs[1] = NULL;

	if(chunkIndex[0] != NULL)
		delete chunkIndex[0];
	chunkIndex[0] = NULL;
	if(chunkIndex[1] != NULL)
		delete chunkIndex[1];
	chunkIndex[1] = NULL;
}

Status ChunkManager::deleteChunk() {
	// TODO
	return OK;
}

static void getInsertChars(char* temp, unsigned x, unsigned y) {
	char* ptr = temp;

	while (x >= 128) {
		unsigned char c = static_cast<unsigned char> (x & 127);
		*ptr = c;
		ptr++;
		x >>= 7;
	}
	*ptr = static_cast<unsigned char> (x & 127);
	ptr++;

	while (y >= 128) {
		unsigned char c = static_cast<unsigned char> (y | 128);
		*ptr = c;
		ptr++;
		y >>= 7;
	}
	*ptr = static_cast<unsigned char> (y | 128);
	ptr++;
}

void ChunkManager::insertXY(unsigned x, unsigned y, size_t len, unsigned char type)
{
	char temp[12];
	getInsertChars(temp, x, y);

	size_t offset = 0;
	size_t remain = len;
	if(isPtrFull(type, len) == true) {
		if(type == 1) {
			offset = meta->length[0] - meta->usedSpace[0] - sizeof(ChunkManagerMeta);
		} else {
			offset = meta->length[1] - meta->usedSpace[1];
		}

		memcpy(meta->endPtr[type - 1], temp, offset);
		remain = len - offset;
		//cout<<"resize"<<endl;
		resize(type);
		//cout<<"resize"<<endl;
	}

	memcpy(meta->endPtr[type - 1], temp + offset, remain);

	meta->endPtr[type - 1] = meta->endPtr[type - 1] + remain;
	meta->usedSpace[type - 1] = meta->usedSpace[type - 1] + len;
	tripleCountAdd(type);
}

Status ChunkManager::resize(unsigned char type) {
	// TODO
	size_t pageNo = 0;
	ptrs[type - 1] = bitmapBuffer->getPage(meta->type, type - 1, pageNo);
    usedPage[type - 1].push_back(pageNo);
	meta->length[type - 1] = usedPage[type - 1].size() * MemoryBuffer::pagesize;
	meta->endPtr[type - 1] = ptrs[type - 1];

	bufferCount++;
	return OK;
}

Status ChunkManager::optimize() {
	// TODO
	return OK;
}

/// build the hash index for query;
Status ChunkManager::buildChunkIndex()
{
	chunkIndex[0]->buildIndex(1);
	chunkIndex[1]->buildIndex(2);
	return OK;
}

Status ChunkManager::getChunkPosByID(ID id, unsigned typeID, size_t& offset)
{
	if(typeID == 1) {
		return chunkIndex[0]->getOffsetByID(id, offset, typeID);
	}else if(typeID  == 2) {
		return chunkIndex[1]->getOffsetByID(id, offset, typeID);
	}

	cerr<<"unknown type id"<<endl;
	return ERR;
}

void ChunkManager::setColStartAndEnd(ID& startColID)
{
}

int ChunkManager::findChunkPosByPtr(char* chunkPtr, int& offSet)
{
	return -1;
}

bool ChunkManager::isPtrFull(unsigned char type, size_t len)
{
	if(type == 1) {
		len = len + sizeof(ChunkManagerMeta);
	}
	return meta->usedSpace[type - 1] + len >= meta->length[type - 1];
}

void ChunkManager::save(ofstream& ofile)
{

}

Status ChunkManager::getAllYByID(ID id, EntityIDBuffer* entBuffer) {
	chunkIndex[1]->getYByID(id, entBuffer, 2);
	chunkIndex[0]->getYByID(id, entBuffer, 1) ;
	if(entBuffer->getSize() == 0)
		return NOT_FOUND;
	else
		return OK;

}

void ChunkManager::unload(unsigned pid, unsigned type, char* buffer, size_t& offset)
{
//	cout << "offset:"<< offset << " ~ " << flush;
	ChunkManagerMeta * meta = (ChunkManagerMeta*)(buffer + offset);
	if(meta->pid != pid || meta->type != type) {
		MessageEngine::showMessage("load chunkmanager error: check meta info", MessageEngine::ERR);
		cout<<meta->pid<<": "<<meta->type<<endl;
		return ;
	}
	offset = offset + meta->length[0] + meta->length[1];
//	cout << offset << endl;
}

ChunkManager* ChunkManager::load(unsigned pid, unsigned type, char* buffer, size_t& offset)
{
	ChunkManagerMeta * meta = (ChunkManagerMeta*)(buffer + offset);
	if(meta->pid != pid || meta->type != type) {
		MessageEngine::showMessage("load chunkmanager error: check meta info", MessageEngine::ERR);
		cout<<meta->pid<<": "<<meta->type<<endl;
		return NULL;
	}

	ChunkManager* manager = new ChunkManager();
	char* base = buffer + offset + sizeof(ChunkManagerMeta);
	manager->meta = meta;
	manager->meta->startPtr[0] = base; manager->meta->startPtr[1] = buffer + offset + manager->meta->length[0];
	manager->meta->endPtr[0] = manager->meta->startPtr[0] + manager->meta->usedSpace[0];
	manager->meta->endPtr[1] = manager->meta->startPtr[1] + manager->meta->usedSpace[1];

	offset = offset + manager->meta->length[0] + manager->meta->length[1];

	return manager;
}
/////////////////////////////////////////////////////////////////////////////////////////////////////////

Chunk::Chunk(unsigned char type, ID xMax, ID xMin, ID yMax, ID yMin, char* startPtr, char* endPtr) {
	// TODO Auto-generated constructor stub
	this->type = type;
	this->xMax = xMax;
	this->xMin = xMin;
	this->yMax = yMax;
	this->yMin = yMin;
	count = 0;
	this->startPtr = startPtr;
	this->endPtr = endPtr;
	this->flagVector = new BitVectorWAH;
}

Chunk::Chunk(unsigned char type,ID xMax, ID xMin, ID yMax, ID yMin, char* startPtr, char* endPtr, BitVectorWAH* flagVector)
{
	this->type = type;
	this->xMax = xMax;
	this->xMin = xMin;
	this->yMax = yMax;
	this->yMin = yMin;
	this->startPtr = startPtr;
	this->endPtr = endPtr;
	this->flagVector = flagVector;
}

Chunk::~Chunk() {
	// TODO Auto-generated destructor stub
	this->startPtr = 0;
	this->endPtr = 0;
	delete flagVector;
	flagVector = NULL;
}

/*
 *	write x id; set the 7th bit to 0 to indicate it is a x byte;
 */
void Chunk::writeXId(ID id, char*& ptr) {
// Write a id
	while (id >= 128) {
		unsigned char c = static_cast<unsigned char> (id & 127);
		*ptr = c;
		ptr++;
		id >>= 7;
	}
	*ptr = static_cast<unsigned char> (id & 127);
	ptr++;
}

/*
 *	write y id; set the 7th bit to 1 to indicate it is a y byte;
 */
void Chunk::writeYId(ID id, char*& ptr) {
	while (id >= 128) {
		unsigned char c = static_cast<unsigned char> (id | 128);
		*ptr = c;
		ptr++;
		id >>= 7;
	}
	*ptr = static_cast<unsigned char> (id | 128);
	ptr++;
}

Status Chunk::insertXY(ID xId, ID yId, unsigned char xLen, unsigned char yLen,bool flag) {
	if(isChunkFull())
		return CHUNK_IS_FULL;
	//mem write xId yId by xLen and yLen
	memcpy(endPtr,&xId,xLen);
	endPtr += xLen;
	memcpy(endPtr,&yId,yLen);
	endPtr += yLen;
	//modify xMin xMax yMax yMin
	//maybe there is useless
	xMax = xId > xMax ? xId : xMax;
	yMax = yId > yMax ? yId : yMax;
	xMin = xId < xMin ? xId : xMin;
	yMin = yId < yMin ? yId : yMin;

	soFlags->push_back(flag);

	return OK;
}

Status Chunk::insertXY(ID xId, ID yId, bool flag) {
	//if(isChunkFull())
	//	return CHUNK_IS_FULL;

	//modify xMin xMax yMax yMin
	//maybe there is useless
	xMax = xId > xMax ? xId : xMax;
	yMax = yId > yMax ? yId : yMax;
	xMin = xId < xMin ? xId : xMin;
	yMin = yId < yMin ? yId : yMin;

	writeXId(xId, endPtr);
	writeYId(yId, endPtr);

	addCount();

	return OK;
}

Status Chunk::completeInsert(ID& startColID)
{
//	flagVector->completeInsert();

	//set the start and end column ID

	//this->colStart = startColID;
	//this->colEnd = startColID + (endPtr - startPtr) / Type_2_Length(this->type) - 1;

	//startColID = this->colEnd + 1;
/*
	if ( flagVector != NULL)
		delete flagVector;

	flagVector = BitVectorWAH::convertVector(*soFlags);

	delete soFlags;
*/
	return OK;
}

/*
 * pos 从1开始算起
 *
 */
Status Chunk::getSO(ID& subjectID,ID& objectID, ID pos)
{
	if( pos > (colEnd - colStart + 1))
		return ERR;
	unsigned char xLen, yLen;
	ID x = 0,y = 0;

	char* p = startPtr + (pos - 1) * Type_2_Length(this->type);
	Type_2_Length(type,xLen,yLen);

	memcpy(&x,p,xLen);
	p = p + xLen;
	memcpy(&y,p,yLen);

	bool flag = flagVector->getValue(pos);

//	bool flag = flagVector[pos-1];

	if(flag == true){
		subjectID = x+y;
		objectID = x;
	}else{
		objectID = x+y;
		subjectID = x;
	}

	return OK;
}

static inline unsigned int readUInt(const uchar* reader) {
	return (reader[0]<<24 | reader[1] << 16 | reader[2] << 8 | reader[3]);
}

const uchar* Chunk::readXId(const uchar* reader, register ID& id) {
#ifdef WORD_ALIGN
	id = 0;
	register unsigned int c = *((unsigned int*)reader);
	register unsigned int flag = c & 0x80808080;       	/* get the first bit of every byte. */
	switch(flag) {
		case 0:		//reads 4 or more bytes;
			id = *reader;
			reader++;
			id = id | ((*reader) << 7);
			reader++;
			id = id | ((*reader) << 14);
			reader++;
			id = id | ((*reader) << 21);
			reader++;
			if(*reader < 128) {
				id = id | ((*reader) << 28);
				reader++;
			}
			break;
		case 0x80000080:
		case 0x808080:
		case 0x800080:
		case 0x80008080:
		case 0x80:
		case 0x8080:
		case 0x80800080:
		case 0x80808080:
			break;

		case 0x80808000://reads 1 byte;
		case 0x808000:
		case 0x8000:
		case 0x80008000:
			id = *reader;
			reader++;
			break;
		case 0x800000: //read 2 bytes;
		case 0x80800000:
			id = *reader;
			reader++;
			id = id | ((*reader) << 7);
			reader++;
			break;
		case 0x80000000: //reads 3 bytes;
			id = *reader;
			reader++;
			id = id | ((*reader) << 7);
			reader++;
			id = id | ((*reader) << 14);
			reader++;
			break;
	}
	return reader;
#else
// Read an x id
	register unsigned shift = 0;
	id = 0;
	register unsigned int c;

	while (true) {
		c = *reader;
		if (!(c & 128)) {
			id |= c << shift;
			shift += 7;
		} else {
			break;
		}
		reader++;
	}
	return reader;
#endif /* end for WORD_ALIGN */
}

const uchar* Chunk::readYId(const uchar* reader, register ID& id) {
// Read an y id
#ifdef WORD_ALIGN
	id = 0;
	register unsigned int c = *((unsigned int*)reader);
	register unsigned int flag = c & 0x80808080;       /* get the first bit of every byte. */
	switch(flag) {
		case 0: //no byte;
		case 0x8000:
		case 0x808000:
		case 0x80008000:
		case 0x80800000:
		case 0x800000:
		case 0x80000000:
		case 0x80808000:
			break;
		case 0x80:
		case 0x80800080:
		case 0x80000080:
		case 0x800080: //one byte
			id = (*reader)& 0x7F;
			reader++;
			break;
		case 0x8080:
		case 0x80008080: // two bytes
			id = (*reader)& 0x7F;
			reader++;
			id = id | (((*reader) & 0x7F) << 7);
			reader++;
			break;
		case 0x808080: //three bytes;
			id = (*reader) & 0x7F;
			reader++;
			id = id | (((*reader) & 0x7F) << 7);
			reader++;
			id = id | (((*reader) & 0x7F) << 14);
			reader++;
			break;
		case 0x80808080: //reads 4 or 5 bytes;
			id = (*reader) & 0x7F;
			reader++;
			id = id | (((*reader) & 0x7F) << 7);
			reader++;
			id = id | (((*reader) & 0x7F) << 14);
			reader++;
			id = id | (((*reader) & 0x7F) << 21);
			reader++;
			if(*reader >= 128) {
				id = id | (((*reader) & 0x7F) << 28);
				reader++;
			}
			break;
	}
	return reader;
#else
	register unsigned shift = 0;
	id = 0;
	register unsigned int c;

	while (true) {
		c = *reader;
		if (c & 128) {
			id |= (c & 0x7F) << shift;
			shift += 7;
		} else {
			break;
		}
		reader++;
	}
	return reader;
#endif /* END FOR WORD_ALIGN */
}

const uchar* Chunk::skipId(const uchar* reader, unsigned char flag) {
// Skip an id
	if(flag == 1) {
		while ((*reader) & 128)
			++reader;
	//	return reader;
	} else {
		while (!((*reader) & 128))
			++reader;
	//	return reader;
	}

	return reader;
}

const uchar* Chunk::skipForward(const uchar* reader) {
// skip a x,y forward;
	return skipId(skipId(reader, 0), 1);
}

const uchar* Chunk::skipBackward(const uchar* reader) {
// skip backward to the last x,y;
	while ((*reader) == 0)
		--reader;
	while ((*reader) & 128)
		--reader;
	while (!((*reader) & 128))
		--reader;
	return ++reader;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////

