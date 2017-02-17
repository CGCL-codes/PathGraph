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

#include "BPlusIndex.h"
#include "MemoryBuffer.h"
#include "BitmapBuffer.h"

BPlusIndex::BPlusIndex(ChunkManager& _manager, IndexType _type) : manager(_manager), type(_type){
	// TODO Auto-generated constructor stub
	indexBuffer = NULL;
	indexEntry = NULL;
	indexSize = 0;
	writerIndex = 0;
	currentIndex = 0;
	recordCount = 0;
}


BPlusIndex::~BPlusIndex() {
	// TODO Auto-generated destructor stub
	if(indexBuffer != NULL) {
		delete indexBuffer;
	}
	indexBuffer = NULL;
	indexEntry = NULL;
	indexSize = 0;
}

int BPlusIndex::searchChunk(ID id, unsigned& start, unsigned& end)
{
	ID* ref; //= (ID*)indexBuffer->getBuffer();
	int pageNo = pageCount - 1;
	ID low, high, mid;
	bool leafnode = false; // is a leaf node?

	while(!leafnode) {
		// TODO read the tree node.
		ref = (ID*)(indexBuffer->getBuffer() + pageNo * MemoryBuffer::pagesize);

		if(ref[0] == 0xFFFFFFFF) {
			low = ref[2]; high = ref[3];
			leafnode = true;
		} else {
			low = ref[0]; high = ref[1];
		}

		// TODO use the binary search to find the node.
		while(low <= high) {
			mid = (low + high) >> 1;
			if(id < ref[2 * mid]) high = mid  - 1;
			else if(mid >= high || id <= ref[2 * (mid + 1)]) break;
			else low = mid + 1;
		}

		if(ref[2 * mid] > id)
			mid--;
		pageNo = ref[2 * mid + 1];
	}

	if(mid > ref[2] && ref[2 * mid] >= id)
		mid--;

	if(mid != -1) {
		start = ref[2 * mid + 1];
		end = ref[2 * mid + 3] == 0xFFFFFFFF ? ref[2 * mid + 5] : ref[2 * mid + 3];
		return 0;
	} else {
		return -1;
	}
}

void BPlusIndex::insertEntry(ID id, unsigned offset)
{
	if((writerIndex + currentIndex) >= indexBuffer->getSize() / (sizeof(ID))) {
		// filling the header 
		indexEntry[writerIndex] = 0xFFFFFFFF;
		indexEntry[writerIndex+1] = 0;
		indexEntry[writerIndex+2] = 2;
		indexEntry[writerIndex+3] = currentIndex / 2;
		// resize the data
		indexBuffer->resize(MemoryBuffer::pagesize);
		indexEntry = (ID*)indexBuffer->getBuffer();
		// increase index
		writerIndex += currentIndex;
		currentIndex = 4;
	}

	indexEntry[writerIndex + currentIndex] = id;
	currentIndex++;
	indexEntry[writerIndex + currentIndex] = offset;
	currentIndex++;
}

void BPlusIndex::packInnerNode() {
	// filling the last leaf node
	indexEntry[writerIndex] = 0xFFFFFFFF;
	indexEntry[writerIndex+1] = 0;
	indexEntry[writerIndex+2] = 2;
	indexEntry[writerIndex+3] = currentIndex / 2;

	//increase the index
	writerIndex += currentIndex;
	indexSize = writerIndex;

	int lastPageNo = (int)ceil((double)( writerIndex * sizeof(ID)) / MemoryBuffer::pagesize);
	int pageNo;
	pageCount = lastPageNo;

	ID i;
	ID* temp;

	while(lastPageNo > 1) {
		writerIndex = 0;
//		cout<<"page NO1: "<<pageNo<<endl;
		pageNo = (int)ceil((double)lastPageNo / (MemoryBuffer::pagesize - 2 * sizeof(ID)));
//		cout<<"page No2: "<<pageNo<<endl;
		indexBuffer->resize(pageNo * MemoryBuffer::pagesize);
		indexEntry = (ID*)(indexBuffer->getBuffer() + pageCount * MemoryBuffer::pagesize);

		indexBuffer->get_length();

		writerIndex = 0;
		currentIndex = 2;

		for(i = pageCount - lastPageNo; i < pageCount; i++) {
			if(currentIndex % MemoryBuffer::pagesize == 0) {
				indexEntry[writerIndex] = 1;
				indexEntry[writerIndex+1] = currentIndex / 2;

				writerIndex += currentIndex;
				currentIndex = 2;
			}

			temp = (ID*)(indexBuffer->getBuffer() + i * MemoryBuffer::pagesize);
			if(temp[0] == 0xFFFFFFFF) {
				indexEntry[writerIndex + currentIndex] = temp[4];
				currentIndex++;
				indexEntry[writerIndex + currentIndex] = i;
				currentIndex++;
			} else {
				indexEntry[writerIndex+ currentIndex] = temp[2];
				currentIndex++;
				indexEntry[writerIndex + currentIndex] = i;
				currentIndex++;
			}
		}
		// filling the padding space.
		indexEntry[writerIndex] = 1;
		indexEntry[writerIndex+1] = currentIndex / 2;

		lastPageNo = pageNo;
		pageCount += lastPageNo;
	}
}

Status BPlusIndex::buildIndex(char chunkType) {
	if(indexBuffer == NULL) {
		indexBuffer = new MemoryBuffer(MemoryBuffer::pagesize);
		indexEntry = (ID*)indexBuffer->getBuffer();
		currentIndex = 4;
		//indexEntry[writerIndex++] = 0xFFFFFFFF;
		//++writerIndex;
	}

	const uchar *begin, *limit, *reader;
	ID x, y;

	//      int startEntry = 0, endEntry = 0;

	if(chunkType == 1) {
//		if(manager.getPredicateID() == 4) {
//			cout<<"asdfasdf"<<endl;
//		}

		reader = manager.getStartPtr(1);
		begin = reader;
		if(manager.getStartPtr(chunkType) == manager.getEndPtr(chunkType))
		{
			return OK;
		}

		x = 0;
		reader = Chunk::readXId(reader, x);
		// TODO insert entry
		insertEntry(x, 0);

		reader = reader + (int)MemoryBuffer::pagesize;
		limit = manager.getEndPtr(chunkType);

		while(reader < limit) {
			x = 0;
			const uchar *temp = Chunk::skipBackward(reader);
			Chunk::readXId(temp, x);
			// TODO insert entry
			insertEntry(x, temp - begin);

			reader = reader + (int)MemoryBuffer::pagesize;
		}

		reader = Chunk::skipBackward(limit);
		x = 0;
		Chunk::readXId(reader, x);
		//TODO insert entry
		insertEntry(x, reader - begin);
	}

	if(chunkType == 2) {
		begin = manager.getStartPtr(chunkType);
		reader = begin;
		if(manager.getStartPtr(chunkType) == manager.getEndPtr(chunkType)) {
			return OK;
		}

		x = 0;
		y = 0;
		Chunk::readYId(Chunk::readXId(reader, x), y);
		// TODO insert entry
		insertEntry(x + y, 0);

		reader = reader + (int)MemoryBuffer::pagesize;

		limit = manager.getEndPtr(chunkType);
		while(reader < limit) {
			x = 0;
			y = 0;

			const uchar *temp = Chunk::skipBackward(reader);
			Chunk::readYId(Chunk::readXId(reader, x), y);
			// TODO insert entry
			insertEntry(x + y, temp - begin);

			reader = reader + (int)MemoryBuffer::pagesize;
		}

		x = y = 0;
		reader = Chunk::skipBackward(limit);
		Chunk::readYId(Chunk::readXId(reader, x), y);
		// TODO insert entry
		insertEntry(x + y, reader - begin);
		x = 0;
                Chunk::readXId(reader, x);
                //TODO insert entry
                insertEntry(x, reader - begin);
        }

        if(chunkType == 2) {
                begin = manager.getStartPtr(chunkType);
                reader = begin;
                if(manager.getStartPtr(chunkType) == manager.getEndPtr(chunkType)) {
                        return OK;
                }

                x = 0;
                y = 0;
                Chunk::readYId(Chunk::readXId(reader, x), y);
                // TODO insert entry
                insertEntry(x + y, 0);

                reader = reader + (int)MemoryBuffer::pagesize;

                limit = manager.getEndPtr(chunkType);
                while(reader < limit) {
                        x = 0;
                        y = 0;

                        const uchar *temp = Chunk::skipBackward(reader);
                        Chunk::readYId(Chunk::readXId(reader, x), y);
                        // TODO insert entry
                        insertEntry(x + y, temp - begin);

                        reader = reader + (int)MemoryBuffer::pagesize;
                }

                x = y = 0;
                reader = Chunk::skipBackward(limit);
                Chunk::readYId(Chunk::readXId(reader, x), y);
                // TODO insert entry
                insertEntry(x + y, reader - begin);
		}

		packInnerNode();

		return OK;
}

Status BPlusIndex::getOffsetByID(ID id, unsigned &offset, unsigned typeID)
{
//	cout<<__FUNCTION__<<endl;

	if(pageCount == 0) 
		return NOT_FOUND;

	unsigned pStart, pEnd;

#if (defined(TRIPLEBIT_UNIX) && defined(TEST_TIME))
	struct timeval start_time, end_time;
	gettimeofday(&start_time, NULL);
#endif

	int offsetId = searchChunk(id, pStart, pEnd);
	if(offsetId == -1) {
		// TODO check the buffer size;
		if(indexBuffer && indexBuffer->getSize() > 0 && id > indexEntry[indexSize - 1] ) {
#if (defined(TRIPLEBIT_UNIX) && defined(TEST_TIME))
			gettimeofday(&end_time, NULL);
			// printf the time;
			printf("time used: %lf ms\n", ((end_time.tv_sec - start_time.tv_sec) * 1000000.0 + end_time.tv_usec - start_time.tv_usec) / 1000);
#endif
			return NOT_FOUND;
		} else {
			offset = 0;
#if (defined(TRIPLEBIT_UNIX) && defined(TEST_TIME))
			gettimeofday(&end_time, NULL);
			printf("time used: %lf ms\n", ((end_time.tv_sec - start_time.tv_sec) * 1000000.0 + end_time.tv_usec - start_time.tv_usec) / 1000);
#endif
			return OK;
		}
	}

	// TODO get the start and end offset of the chunk;
	const uchar *beginPtr = NULL, *reader = NULL;
	int low, high, mid = 0, lastmid = 0;
	ID x, y;

	// TODO binary search in the chunk;
	if(manager.getTrpileCount(typeID) == 0) {
#if (defined(TRIPLEBIT_UNIX) && defined(TEST_TIME))
		gettimeofday(&end_time, NULL);
		printf("time used: %f ms\n", ((end_time.tv_sec - start_time.tv_sec) * 1000000.0 + end_time.tv_usec - start_time.tv_usec) / 1000);
#endif
		return NOT_FOUND;
	}

	if(typeID == 1) {
		low = pStart;
		high = pEnd;

		reader = manager.getStartPtr(1) + low;
		beginPtr = manager.getStartPtr(1);
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
#if (defined(TRIPLEBIT_UNIX) && defined(TEST_TIME))
					gettimeofday(&end_time, NULL);
					printf("time used: %f ms\n", ((end_time.tv_sec - start_time.tv_sec) * 1000000.0 + end_time.tv_usec - start_time.tv_usec) / 1000);
#endif
					return OK;
				}
				lastmid = reader - beginPtr;
				low = lastmid - 1;
			}
			offset = lastmid;
#if (defined(TRIPLEBIT_UNIX) && defined(TEST_TIME))
			gettimeofday(&end_time, NULL);
			printf("time used: %f ms\n", ((end_time.tv_sec - start_time.tv_sec) * 1000000.0 + end_time.tv_usec - start_time.tv_usec) / 1000);
#endif
			return OK;
		} else if (x > id) {
#if (defined(TRIPLEBIT_UNIX) && defined(TEST_TIME))
			gettimeofday(&end_time, NULL);
			printf("time used: %f ms\n", ((end_time.tv_sec - start_time.tv_sec) * 1000000.0 + end_time.tv_usec - start_time.tv_usec) / 1000);
#endif
			return OK;
		}

		//cout<<__FUNCTION__<<"low<=high typeID == 1"<<endl;
		while (low <= high) {
			//x = 0;
			mid = low + (high - low) / 2;
			reader = Chunk::skipBackward(beginPtr + mid);
			Chunk::readXId(reader, x);

			if (x == id) {
				lastmid = mid;
				while (mid > 0) {
					//x = 0;
					reader = Chunk::skipBackward(beginPtr + mid);
					Chunk::readXId(reader, x);
					if (reader < beginPtr || x < id) {
						offset = lastmid;
#if (defined(TRIPLEBIT_UNIX) && defined(TEST_TIME))
						gettimeofday(&end_time, NULL);
						printf("time used: %f ms\n", ((end_time.tv_sec - start_time.tv_sec) * 1000000.0 + end_time.tv_usec - start_time.tv_usec) / 1000);
#endif
						return OK;
					}
					lastmid = reader - beginPtr;
					mid = lastmid - 1;
				}
				offset = lastmid;
#if (defined(TRIPLEBIT_UNIX) && defined(TEST_TIME))
				gettimeofday(&end_time, NULL);
				printf("time used: %f ms\n", ((end_time.tv_sec - start_time.tv_sec) * 1000000.0 + end_time.tv_usec - start_time.tv_usec) / 1000);
#endif
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
		low = pStart;
		high = pEnd;

		reader = manager.getStartPtr(2) + low;
		beginPtr = manager.getStartPtr(2);

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
#if (defined(TRIPLEBIT_UNIX) && defined(TEST_TIME))
					gettimeofday(&end_time, NULL);
					printf("time used: %f ms\n", ((end_time.tv_sec - start_time.tv_sec) * 1000000.0 + end_time.tv_usec - start_time.tv_usec) / 1000);
#endif
					return OK;
				}
				lastmid = reader - beginPtr;
				low = lastmid - 1;
			}
			offset = lastmid;
#if (defined(TRIPLEBIT_UNIX) && defined(TEST_TIME))
			gettimeofday(&end_time, NULL);
			printf("time used: %f ms\n", ((end_time.tv_sec - start_time.tv_sec) * 1000000.0 + end_time.tv_usec - start_time.tv_usec) / 1000);
#endif
			return OK;
		}

		if (x + y > id) {
#if (defined (TRIPLEBIT_UNIX) && defined(TEST_TIME))
			gettimeofday(&end_time, NULL);
			printf("time used: %f ms\n", ((end_time.tv_sec - start_time.tv_sec) * 1000000.0 + end_time.tv_usec - start_time.tv_usec) / 1000);
#endif
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
#if (defined(TRIPLEBIT_UNIX) && defined(TEST_TIME))
						gettimeofday(&end_time, NULL);
						printf("time used: %f ms\n", ((end_time.tv_sec - start_time.tv_sec) * 1000000.0 + end_time.tv_usec - start_time.tv_usec) / 1000);
#endif
						return OK;
					}
					lastmid = reader - beginPtr;
					mid = lastmid - 1;
					//mid = reader - beginPtr;
				}
				offset = lastmid;
#if (defined(TRIPLEBIT_UNIX) && defined(TEST_TIME))
				gettimeofday(&end_time, NULL);
				printf("time used: %f ms\n", ((end_time.tv_sec - start_time.tv_sec) * 1000000.0 + end_time.tv_usec - start_time.tv_usec) / 1000);
#endif
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
#if (defined(TRIPLEBIT_UNIX) && defined(TEST_TIME))
	gettimeofday(&end_time, NULL);
	printf("time used: %f ms\n", ((end_time.tv_sec - start_time.tv_sec) * 1000000.0 + end_time.tv_usec - start_time.tv_usec) / 1000);
#endif

	return OK;
}

void BPlusIndex::save(MMapBuffer* & buffer) 
{

}

BPlusIndex* BPlusIndex::load(ChunkManager& manager, IndexType type, char* buffer, size_t& offset) 
{
	return NULL;
}