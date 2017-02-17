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

#include "FindEntityID.h"
#include "SortMergeJoin.h"
#include "MemoryBuffer.h"
#include "URITable.h"
#include "PredicateTable.h"
#include "TripleBitRepository.h"
#include "BitmapBuffer.h"
#include "EntityIDBuffer.h"
#include "HashIndex.h"
#include "StatisticsBuffer.h"

FindEntityID::FindEntityID(TripleBitRepository* repo) {
	// TODO Auto-generated constructor stub
	bitmap = repo->getBitmapBuffer();
	UriTable = repo->getURITable();
	preTable = repo->getPredicateTable();
	spStatBuffer = (TwoConstantStatisticsBuffer*)repo->getStatisticsBuffer(StatisticsBuffer::SUBJECTPREDICATE_STATIS);
	opStatBuffer = (TwoConstantStatisticsBuffer*)repo->getStatisticsBuffer(StatisticsBuffer::OBJECTPREDICATE_STATIS);
	sStatBuffer = (OneConstantStatisticsBuffer*)repo->getStatisticsBuffer(StatisticsBuffer::SUBJECT_STATIS);
	oStatBuffer = (OneConstantStatisticsBuffer*)repo->getStatisticsBuffer(StatisticsBuffer::OBJECT_STATIS);

	XTemp = new EntityIDBuffer;
	XYTemp = new EntityIDBuffer;
}

FindEntityID::~FindEntityID() {
	// TODO Auto-generated destructor stub
	if(XTemp != NULL)
		delete XTemp;
	XTemp = NULL;

	if(XYTemp != NULL)
		delete XYTemp;
	XYTemp = NULL;
}

Status FindEntityID::findSubjectIDByPredicate(ID predicateID, EntityIDBuffer* buffer, ID minID, ID maxID)
{
	if(minID == 0 && maxID == UINT_MAX)
		return this->findSubjectIDByPredicate(predicateID, buffer);
#ifdef DEBUG
	cout<<__FUNCTION__<<endl;
#endif
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(1);
	XYTemp->setSortKey(0);

	register ID x, y;
	register ID key;
	const uchar* startPtr, *limit, *reader;

	size_t offset;
	Status s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(minID, 1, offset);
	if(s == OK) {
		startPtr =  bitmap->getChunkManager(predicateID, 0)->getStartPtr(1) + offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(1);
		reader = startPtr;
		for (; startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);

			if(x < minID) {
				continue;
			} else if( x <= maxID ) {
				XTemp->insertID(x);
			} else {
				break;
			}
			startPtr = Chunk::skipId(startPtr, 1); //skip y;
		}
	}

	s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(minID, 2, offset);
	if(s == OK) {
		startPtr =  bitmap->getChunkManager(predicateID, 0)->getStartPtr(2) + offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(2);
		reader = startPtr;
		for (; reader < limit;) {
			reader = Chunk::readXId(reader, x);
			reader = Chunk::readYId(reader, y);
			key = x + y;
			if(key < minID) {
				continue;
			} else if( key <= maxID) {
				XYTemp->insertID(key);
			} else {
				break;
			}
		}
	}

	buffer->mergeBuffer(XTemp, XYTemp);

	return OK;
}

Status FindEntityID::findSubjectIDByPredicate(ID predicateID, EntityIDBuffer* buffer)
{
#ifdef DEBUG
	cout << __FUNCTION__ << endl;
#endif
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(1);
	XYTemp->setSortKey(0);

	register ID x, y;
	const uchar* startPtr, *limit, *reader;

	startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(1);
	limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(1);
	reader = startPtr;
	for (; startPtr < limit;) {
		startPtr = Chunk::readXId(startPtr, x);
		XTemp->insertID(x);
		startPtr = Chunk::skipId(startPtr, 1); //skip y;
	}

	startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(2);
	limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(2);
	reader = startPtr;
	for (; reader < limit;) {
		reader = Chunk::readXId(reader, x);
		reader = Chunk::readYId(reader, y);
		XYTemp->insertID(x + y);
	}

	buffer->mergeBuffer(XTemp, XYTemp);

	return OK;
}

Status FindEntityID::findObjectIDAndSubjectIDByPredicate(ID predicateID, EntityIDBuffer* buffer)
{
#ifdef DEBUG
	cout<<__FUNCTION__<<endl;
#endif
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	register ID x, y;
	const uchar* startPtr, *limit;

	startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(1);
	limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(1);
	for (;startPtr < limit;) {
		startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
		XTemp->insertID(x);
		XTemp->insertID(x + y);
	}

	startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(2);
	limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(2);
	for (; startPtr < limit; ) {
		startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
		XYTemp->insertID(x + y);
		XYTemp->insertID(x);
	}
	buffer->mergeBuffer(XTemp, XYTemp);

	return OK;
}

Status FindEntityID::findSubjectIDAndObjectIDByPredicate(ID predicateID, EntityIDBuffer* buffer)
{
#ifdef DEBUG
	cout<<__FUNCTION__<<endl;
#endif
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	register ID x, y;
	const uchar* startPtr, *limit;

	startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(1);
	limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(1);
	for (;startPtr < limit;) {
		startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
		XTemp->insertID(x);
		XTemp->insertID(x + y);
	}
	startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(2);
	limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(2);

	for (; startPtr < limit; ) {
		startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
		XYTemp->insertID(x + y);
		XYTemp->insertID(x);
	}

	buffer->mergeBuffer(XTemp, XYTemp);

	return OK;
}

Status FindEntityID::findObjectIDByPredicate(ID predicateID, EntityIDBuffer* buffer, ID minID, ID maxID)
{
	if(minID == 0 && maxID == UINT_MAX)
		return this->findObjectIDByPredicate(predicateID, buffer);
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(1);
	XYTemp->setSortKey(0);

	register ID x, y;
	const uchar* startPtr, *limit;

	size_t offset;

	Status s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(minID, 1, offset);
	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(1) + offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(1);

		for (; startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);
			if (x > maxID)
				break;
			else if (x <= maxID)
				XTemp->insertID(x);
			startPtr = Chunk::skipId(startPtr, 1); //skip y;
		}
	}

	s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(minID, 2, offset);
	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(2) + offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);
			startPtr = Chunk::readYId(startPtr, y);
			if (x + y > maxID)
				break;
			else if (x + y <= maxID)
				XYTemp->insertID(x + y);
		}
	}

	buffer->mergeBuffer(XTemp, XYTemp);

	return OK;
}

Status FindEntityID::findObjectIDByPredicate(ID predicateID, EntityIDBuffer* buffer)
{
	buffer->setIDCount(1);
	buffer->setSortKey(0);
	XTemp->empty();
	XYTemp->empty();

	XTemp->setIDCount(1);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(1);
	XYTemp->setSortKey(0);

	register ID x, y;
	const uchar* startPtr, *limit;

	startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(1);
	limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(1);

	for (; startPtr < limit;) {
		startPtr = Chunk::readXId(startPtr, x);
		XTemp->insertID(x);
		startPtr = Chunk::skipId(startPtr, 1); //skip y;
	}

	startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(2);
	limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(2);

	for (; startPtr < limit;) {
		startPtr = Chunk::readXId(startPtr, x);
		startPtr = Chunk::readYId(startPtr, y);
		XYTemp->insertID(x + y);
	}

	buffer->mergeBuffer(XTemp, XYTemp);

	return OK;
}

Status FindEntityID::findSubjectIDByPredicateAndObject(ID predicateID, ID objectID, EntityIDBuffer* buffer, ID minID, ID maxID)
{
#ifdef DEBUG
	cout<<__FUNCTION__<<endl;
#endif
	Status s;
	register ID x, y;
	register ID key;
	const uchar* startPtr, *limit;
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XYTemp->setIDCount(1);

	size_t offset;
	s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(objectID, 1, offset);

	ID tmp;

	tmp = 0;

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(1) + offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(1);

		for(;startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);

			if(x != objectID)
				break;
			//TODO need to add a range
			startPtr = Chunk::readYId(startPtr, y);
			key = x + y;
			if(key < minID) {
				continue;
			}else if (key <= maxID) {
				XYTemp->insertID(key);
			} else if (key > maxID) {
				break;
			}
		}
	}

	s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(objectID, 2, offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(2) + offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
			if(x + y != objectID)
				break;
			//TODO need to add a range
			if(x < minID) {
				continue;
			} else if (x <= maxID) {
				XTemp->insertID(x);
			} else if (x > maxID) {
				break;
			}
		}
	}

	buffer->mergeBuffer(XTemp, XYTemp);

	return OK;
}

Status FindEntityID::findObjectIDByPredicateAndSubject(ID predicateID, ID subjectID, EntityIDBuffer* buffer, ID minID, ID maxID)
{
	register ID x, y, key;
	const uchar* startPtr, *limit;
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(1);
	XYTemp->setSortKey(0);

	Status s;

	unsigned int typeID;
	size_t offset;

	typeID = 1;

	s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(subjectID, typeID, offset);
	ID tmp;

	tmp = 0;

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(1) + offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(1);
		for(;startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);

			if(x != subjectID)
				break;
			//TODO need to add a range
			startPtr = Chunk::readYId(startPtr, y);
			key = x + y;
			if (key <  minID) {
				continue;
			} else if (key <= maxID) {
				XYTemp->insertID(x + y);
			} else {
				break;
			}
		}
	}

	typeID = 2;
	s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(subjectID, typeID, offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(2) + offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);

			if(x + y != subjectID)
				break;
			//TODO need to add a range
			if (x < minID) {
				continue;
			} else if ( x <= maxID) {
				XTemp->insertID(x);
			} else {
				break;
			}
		}
	}

	buffer->mergeBuffer(XTemp, XYTemp);

	return OK;
}

Status FindEntityID::findSubjectIDAndObjectIDByPredicate(ID predicateID, EntityIDBuffer* buffer, ID min, ID max)
{
	if(min == 0 && max == UINT_MAX)
		return this->findSubjectIDAndObjectIDByPredicate(predicateID, buffer);
#ifdef DEBUG
	cout<<__FUNCTION__<<endl;
#endif
	XTemp->empty();
	XYTemp->empty();

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	register ID x, y;
	const uchar* startPtr, *limit;

	unsigned int typeIDMin;
	size_t offsetMin;
	Status s;

	ChunkManager *manager;
	typeIDMin = 1;
	manager = bitmap->getChunkManager(predicateID, 0);

	s = manager->getChunkPosByID(min, typeIDMin, offsetMin);

	if (s == OK) {
		startPtr = manager->getStartPtr(1) + offsetMin;
		limit = manager->getEndPtr(1);
		for (; startPtr < limit; ) {
			startPtr = Chunk::readXId(startPtr, x);
			if (x > max)
				goto END;

			startPtr = Chunk::readYId(startPtr, y);
			XTemp->insertID(x);
			XTemp->insertID(x + y);
		}
	}

END:
	typeIDMin = 2;

	s = manager->getChunkPosByID(min, typeIDMin, offsetMin);
	if (s == OK) {
		startPtr = manager->getStartPtr(2) + offsetMin;
		limit = manager->getEndPtr(2);
		for (; startPtr < limit; ) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
			if (x + y > max)
				goto END1;

			XYTemp->insertID(x + y);
			XYTemp->insertID(x);
		}
	}
END1:
	buffer->mergeBuffer(XTemp, XYTemp);

	return OK;
}

Status FindEntityID::findObjectIDAndSubjectIDByPredicate(ID predicateID, EntityIDBuffer* buffer, ID minID, ID maxID)
{
	if(minID == 0 && maxID == UINT_MAX)
		return this->findObjectIDAndSubjectIDByPredicate(predicateID, buffer);
#ifdef DEBUG
	cout<<__FUNCTION__<<endl;
#endif
	XTemp->empty();
	XYTemp->empty();

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	register ID x, y;
	const uchar* startPtr, *limit;

	unsigned int typeIDMin;
	size_t offsetMin;
	Status s;

	ChunkManager *manager;
	typeIDMin = 1;
	manager = bitmap->getChunkManager(predicateID, 1);

	s = manager->getChunkPosByID(minID, typeIDMin, offsetMin);

	if (s == OK) {
		startPtr = manager->getStartPtr(1) + offsetMin;
		limit = manager->getEndPtr(1);
		for (; startPtr < limit; ) {
			startPtr = Chunk::readXId(startPtr, x);
			if (x > maxID)
				goto END;

			startPtr = Chunk::readYId(startPtr, y);
			XTemp->insertID(x);
			XTemp->insertID(x + y);
		}
	}

END:
	typeIDMin = 2;

	s = manager->getChunkPosByID(minID, typeIDMin, offsetMin);
	if (s == OK) {
		startPtr = manager->getStartPtr(2) + offsetMin;
		limit = manager->getEndPtr(2);

		for (; startPtr < limit; ) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
			if (x + y > maxID)
				goto END1;

			XYTemp->insertID(x + y);
			XYTemp->insertID(x);
		}
	}
END1:
	buffer->mergeBuffer(XTemp, XYTemp);
	return OK;
}

Status FindEntityID::findSubjectIDAndPredicateIDByPredicate(ID predicateID, EntityIDBuffer* buffer)
{
#ifdef DEBUG
	cout<<__FUNCTION__<<endl;
#endif
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	register ID x, y;
	register ID key;
	const uchar* startPtr, *limit, *reader;

	startPtr =  bitmap->getChunkManager(predicateID, 0)->getStartPtr(1);
	limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(1);

	reader = startPtr;
	for (; startPtr < limit;) {
		startPtr = Chunk::readXId(startPtr, x);
		XTemp->insertID(x);
		XTemp->insertID(predicateID);

		startPtr = Chunk::skipId(startPtr, 1); //skip y;
	}

	startPtr =  bitmap->getChunkManager(predicateID, 0)->getStartPtr(2);
	limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(2);

	reader = startPtr;
	for (; reader < limit;) {
		reader = Chunk::readXId(reader, x);
		reader = Chunk::readYId(reader, y);
		key = x + y;
		XYTemp->insertID(key);
		XYTemp->insertID(predicateID);
	}

	buffer->appendBuffer(XTemp);
	buffer->appendBuffer(XYTemp);

	return OK;
}

Status FindEntityID::findSubjectIDAndPredicateIDByPredicate(ID predicateID, EntityIDBuffer* buffer, ID minID, ID maxID)
{
#ifdef DEBUG
	cout<<__FUNCTION__<<endl;
#endif
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	register ID x, y;
	register ID key;
	const uchar* startPtr, *limit, *reader;

	size_t offset;
	Status s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(minID, 1, offset);

	if(s == OK) {
		startPtr =  bitmap->getChunkManager(predicateID, 0)->getStartPtr(1) + offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(1);

		reader = startPtr;
		for (; startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);

			if( x <= maxID ) {
				XTemp->insertID(x);
				XTemp->insertID(predicateID);
			} else {
				break;
			}
			startPtr = Chunk::skipId(startPtr, 1); //skip y;
		}
	}

	s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(minID, 2, offset);

	if(s == OK) {
		startPtr =  bitmap->getChunkManager(predicateID, 0)->getStartPtr(2) + offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(2);

		reader = startPtr;
		for (; reader < limit;) {
			reader = Chunk::readXId(reader, x);
			reader = Chunk::readYId(reader, y);
			key = x + y;
			if( key <= maxID) {
				XYTemp->insertID(key);
				XYTemp->insertID(predicateID);
			} else {
				break;
			}
		}
	}

	buffer->appendBuffer(XTemp);
	buffer->appendBuffer(XYTemp);

	return OK;
}

Status FindEntityID::findPredicateIDAndSubjectIDByPredicate(ID predicateID, EntityIDBuffer* buffer)
{
#ifdef DEBUG
	cout<<__FUNCTION__<<endl;
#endif
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	register ID x, y;
	register ID key;
	const uchar* startPtr, *limit, *reader;

	startPtr =  bitmap->getChunkManager(predicateID, 0)->getStartPtr(1);
	limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(1);

	reader = startPtr;
	for (; startPtr < limit;) {
		startPtr = Chunk::readXId(startPtr, x);
		XTemp->insertID(predicateID);
		XTemp->insertID(x);
		startPtr = Chunk::skipId(startPtr, 1); //skip y;
	}

	startPtr =  bitmap->getChunkManager(predicateID, 0)->getStartPtr(2);
	limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(2);

	reader = startPtr;
	for (; reader < limit;) {
		reader = Chunk::readXId(reader, x);
		reader = Chunk::readYId(reader, y);
		key = x + y;
		XYTemp->insertID(predicateID);
		XYTemp->insertID(key);
	}

	buffer->appendBuffer(XTemp);
	buffer->appendBuffer(XYTemp);

	return OK;
}

Status FindEntityID::findObjectIDAndPredicateIDByPredicate(ID predicateID, EntityIDBuffer* buffer)
{
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	register ID x, y;
	const uchar* startPtr, *limit;

	startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(1);
	limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(1);

	for (; startPtr < limit;) {
		startPtr = Chunk::readXId(startPtr, x);
		XTemp->insertID(x);
		XTemp->insertID(predicateID);
		startPtr = Chunk::skipId(startPtr, 1); //skip y;
	}

	startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(2);
	limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(2);

	for (; startPtr < limit;) {
		startPtr = Chunk::readXId(startPtr, x);
		startPtr = Chunk::readYId(startPtr, y);

		XYTemp->insertID(x + y);
		XYTemp->insertID(predicateID);
	}

	buffer->appendBuffer(XTemp);
	buffer->appendBuffer(XYTemp);

	return OK;
}

Status FindEntityID::findObjectIDAndPredicateIDByPredicate(ID predicateID, EntityIDBuffer* buffer, ID minID, ID maxID)
{
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	register ID x, y;
	const uchar* startPtr, *limit;

	size_t offset;
	Status s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(minID, 1, offset);
	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(1) + offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(1);

		for (; startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);
			if (x > maxID)
				break;
			else if (x <= maxID) {
				XTemp->insertID(x);
				XTemp->insertID(predicateID);
			}
			startPtr = Chunk::skipId(startPtr, 1); //skip y;
		}
	}

	s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(minID, 2, offset);
	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(2) + offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);
			startPtr = Chunk::readYId(startPtr, y);
			if (x + y > maxID)
				break;
			else if (x + y <= maxID) {
				XYTemp->insertID(x + y);
				XYTemp->insertID(predicateID);
			}
		}
	}

	buffer->appendBuffer(XTemp);
	buffer->appendBuffer(XYTemp);

	return OK;
}

Status FindEntityID::findPredicateIDAndObjectIDByPredicate(ID predicateID, EntityIDBuffer* buffer)
{
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	register ID x, y;
	const uchar* startPtr, *limit;

	startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(1);
	limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(1);

	for (; startPtr < limit;) {
		startPtr = Chunk::readXId(startPtr, x);
		XTemp->insertID(predicateID);
		XTemp->insertID(x);
		startPtr = Chunk::skipId(startPtr, 1); //skip y;
	}

	startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(2);
	limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(2);

	for (; startPtr < limit;) {
		startPtr = Chunk::readXId(startPtr, x);
		startPtr = Chunk::readYId(startPtr, y);
		XYTemp->insertID(predicateID);
		XYTemp->insertID(x + y);
	}

	buffer->appendBuffer(XTemp);
	buffer->appendBuffer(XYTemp);

	return OK;
}

Status FindEntityID::findSubjectIDAndPredicateIDByObject(ID objectID, EntityIDBuffer* buffer)
{
	EntityIDBuffer preBuffer;
	opStatBuffer->getPredicatesByID(objectID, &preBuffer, 0, UINT_MAX);
	size_t size = preBuffer.getSize();
	for(size_t i = 0; i != size; i++) {
		this->findSubjectIDAndPredicateIDByPredicate(preBuffer[1], buffer);
	}

	buffer->sort(1);
	return OK;
}

Status FindEntityID::findSubjectIDAndPredicateIDByObject(ID objectID, EntityIDBuffer* buffer, ID minID, ID maxID)
{
	if(minID == 0 && maxID == UINT_MAX)
		return findSubjectIDAndPredicateIDByObject(objectID, buffer);

	EntityIDBuffer preBuffer;
	opStatBuffer->getPredicatesByID(objectID, &preBuffer, 0, UINT_MAX);
	size_t size = preBuffer.getSize();
	for(size_t i = 0; i != size; i++) {
		this->findSubjectIDAndPredicateIDByPredicate(preBuffer[1], buffer, minID, maxID);
	}

	buffer->sort(1);
	return OK;
}

Status FindEntityID::findPredicateIDAndSubjectIDByObject(ID objectID, EntityIDBuffer* buffer, ID minID, ID maxID)
{
	EntityIDBuffer preBuffer;
	opStatBuffer->getPredicatesByID(objectID, &preBuffer, minID, maxID);
	size_t size = preBuffer.getSize();
	for(size_t i = 0; i != size; i++) {
		this->findPredicateIDAndObjectIDByPredicate(preBuffer[i], buffer);
	}

	buffer->sort(1);
	return OK;
}

Status FindEntityID::findPredicateIDAndObjectIDBySubject(ID subjectID, EntityIDBuffer* buffer, ID minID, ID maxID)
{
	EntityIDBuffer preBuffer;
	spStatBuffer->getPredicatesByID(subjectID, &preBuffer, minID, maxID);
	size_t size = preBuffer.getSize();
	for(size_t i = 0; i != size; i++) {
		this->findPredicateIDAndObjectIDByPredicate(preBuffer[i], buffer);
	}

	buffer->sort(1);
	return OK;
}

Status FindEntityID::findObjectIDAndPredicateIDBySubject(ID subjectID, EntityIDBuffer *buffer)
{
	EntityIDBuffer preBuffer;
	spStatBuffer->getPredicatesByID(subjectID, &preBuffer, 0, UINT_MAX);
	size_t size = preBuffer.getSize();
	for(size_t i = 0; i != size; i++) {
		this->findObjectIDAndPredicateIDByPredicate(preBuffer[i], buffer);
	}

	buffer->sort(1);
	return OK;
}

Status FindEntityID::findObjectIDAndPredicateIDBySubject(ID subjectID, EntityIDBuffer *buffer, ID minID, ID maxID)
{
	if(minID == 0 && maxID == UINT_MAX)
		return findObjectIDAndPredicateIDBySubject(subjectID, buffer);

	EntityIDBuffer preBuffer;
	spStatBuffer->getPredicatesByID(subjectID, &preBuffer, 0, UINT_MAX);
	size_t size = preBuffer.getSize();
	for(size_t i = 0; i != size; i++) {
		this->findObjectIDAndPredicateIDByPredicate(preBuffer[i], buffer, minID, maxID);
	}

	buffer->sort(1);
	return OK;
}

Status FindEntityID::findPredicateIDBySubjectAndObject(ID subjectID, ID objectID, EntityIDBuffer* buffer)
{
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);
	Status s;
	if( (s = spStatBuffer->getPredicatesByID(subjectID, XTemp, 0, UINT_MAX)) != OK )
		return s;
	if((s = opStatBuffer->getPredicatesByID(objectID, XYTemp, 0, UINT_MAX)) != OK)
		return s;

	SortMergeJoin join;
	join.Join(XTemp, XYTemp, 1, 1, false);
	buffer = XTemp;

	return s;
}

Status FindEntityID::findPredicateIDByObject(ID objectID, EntityIDBuffer* buffer, ID minID, ID maxID)
{
	buffer->setIDCount(1);
	buffer->setSortKey(0);
	Status s = opStatBuffer->getPredicatesByID(objectID, buffer, minID, maxID);
	return s;
}

Status FindEntityID::findPredicateIDBySubject(ID subjectID, EntityIDBuffer* buffer, ID minID, ID maxID)
{
	buffer->setIDCount(1);
	buffer->setSortKey(0);
	return spStatBuffer->getPredicatesByID(subjectID, buffer, minID, maxID);
}

Status FindEntityID::findObjectIDBySubject(ID subjectID, EntityIDBuffer *buffer)
{
	buffer->setIDCount(1);
	buffer->setSortKey(0);
	EntityIDBuffer preBuffer, temp;
	spStatBuffer->getPredicatesByID(subjectID, &preBuffer, 0 , UINT_MAX);

	Status s;
	size_t size = preBuffer.getSize();
	for(size_t i = 0; i != size; i++) {
		this->findObjectIDByPredicate(preBuffer[i], &temp);
		if((s = buffer->appendBuffer(&temp)) != OK)
			return s;
		temp.empty();
	}

	buffer->sort(1);
	buffer->uniqe();
	return OK;
}
Status FindEntityID::findObjectIDBySubject(ID subjectID, EntityIDBuffer* buffer, ID minID, ID maxID)
{
	buffer->setIDCount(1);
	buffer->setSortKey(0);
	EntityIDBuffer preBuffer;
	spStatBuffer->getPredicatesByID(subjectID, &preBuffer, 0, UINT_MAX);

	Status s;
	EntityIDBuffer temp;
	size_t size = preBuffer.getSize();
	for(size_t i = 0; i != size; i++) {
		this->findObjectIDByPredicate(preBuffer[i], &temp, minID, maxID);
		if(( s = buffer->appendBuffer(&temp)) != OK)
			return s;
		temp.empty();
	}

	buffer->sort(1);
	buffer->uniqe();

	return OK;
}

Status FindEntityID::findSubjectIDByObject(ID objectID, EntityIDBuffer* buffer)
{
	buffer->setIDCount(1);
	buffer->setSortKey(0);

	Status s;
	EntityIDBuffer preBuffer, temp;
	opStatBuffer->getPredicatesByID(objectID, &preBuffer, 0, UINT_MAX);

	size_t size = preBuffer.getSize();
	for(size_t i = 0; i != size; i++) {
		this->findSubjectIDByPredicate(preBuffer[i], &temp);
		if((s = buffer->appendBuffer(&temp)) != OK)
			return s;
		temp.empty();
	}

	buffer->sort(1);
	buffer->uniqe();

	return OK;
}

Status FindEntityID::findSubjectIDByObject(ID objectID, EntityIDBuffer* buffer, ID minID, ID maxID)
{
	buffer->setIDCount(1);
	buffer->setSortKey(0);

	Status s;
	EntityIDBuffer preBuffer, temp;
	opStatBuffer->getPredicatesByID(objectID, &preBuffer, 0, UINT_MAX);

	size_t size = preBuffer.getSize();
	for(size_t i = 0; i != size; i++) {
		this->findSubjectIDByPredicate(preBuffer[i], &temp, minID, maxID);
		if((s = buffer->appendBuffer(&temp)) != OK)
			return s;
		temp.empty();
	}

	buffer->sort(1);
	buffer->uniqe();
	return OK;
}

Status FindEntityID::findSubject(EntityIDBuffer* buffer, ID minID, ID maxID)
{
	buffer->setIDCount(1);
	buffer->setSortKey(0);

	return sStatBuffer->getIDs(buffer, minID, maxID);
}

/**
 * the predicate's id is continuous.
 */
Status FindEntityID::findPredicate(EntityIDBuffer* buffer, ID minID, ID maxID)
{
	for(ID id = minID; id <= maxID; id++)
		buffer->insertID(id);
	return OK;
}

Status FindEntityID::findObject(EntityIDBuffer* buffer, ID minID, ID maxID)
{
	buffer->setIDCount(1);
	buffer->setSortKey(0);

	return oStatBuffer->getIDs(buffer, minID, maxID);
}
