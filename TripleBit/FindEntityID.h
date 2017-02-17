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

#ifndef FINDENTITYID_H_
#define FINDENTITYID_H_

class BitmapBuffer;
class URITable;
class PredicateTable;
class ColumnBuffer;
class TripleBitRepository;
class EntityIDBuffer;
class Chunk;
class TwoConstantStatisticsBuffer;
class OneConstantStatisticsBuffer;

#include "TripleBit.h"
#include "ThreadPool.h"

#ifdef TEST_TIME
#include "TimeStamp.h"
#endif

class FindEntityID {
private:
	BitmapBuffer* bitmap;
	URITable* UriTable;
	PredicateTable* preTable;
	
	TwoConstantStatisticsBuffer* spStatBuffer, *opStatBuffer;
	OneConstantStatisticsBuffer* sStatBuffer, *oStatBuffer;

	EntityIDBuffer *XTemp, *XYTemp;
#ifdef TEST_TIME
	TimeStamp indexTimer, readTimer;
#endif

public:
	FindEntityID(TripleBitRepository* repo);
	virtual ~FindEntityID();
	Status findSubjectIDByPredicate(ID predicateID, EntityIDBuffer* buffer, ID minID, ID maxID);

	Status findObjectIDByPredicate(ID predicateID, EntityIDBuffer* buffer, ID minID, ID maxID);

	Status findObjectIDAndSubjectIDByPredicate(ID predicate, EntityIDBuffer* buffer, ID min, ID max);
	Status findSubjectIDAndObjectIDByPredicate(ID predicate, EntityIDBuffer* buffer, ID min, ID max);

	Status findSubjectIDByPredicateAndObject(ID predicateID, ID objectID, EntityIDBuffer* buffer, ID minID, ID maxID);
	Status findObjectIDByPredicateAndSubject(ID predicateID, ID subjectID, EntityIDBuffer* buffer, ID minID, ID maxID);

	Status findObjectIDAndPredicateIDBySubject(ID subjectID, EntityIDBuffer* buffer, ID minID, ID maxID);
	Status findPredicateIDAndObjectIDBySubject(ID subjectID, EntityIDBuffer* buffer, ID minID, ID maxID);

	Status findPredicateIDAndSubjectIDByObject(ID objectID, EntityIDBuffer* buffer, ID minID, ID maxID);
	Status findSubjectIDAndPredicateIDByObject(ID objectID, EntityIDBuffer* buffer, ID minID, ID maxID);

	Status findPredicateIDBySubjectAndObject(ID subject, ID object, EntityIDBuffer* buffer);

	Status findPredicateIDBySubject(ID subject, EntityIDBuffer* buffer, ID minID, ID maxID);

	Status findPredicateIDByObject(ID object, EntityIDBuffer* buffer, ID minID, ID maxID);

	Status findSubjectIDByObject(ID object, EntityIDBuffer* buffer, ID minID, ID maxID);
	Status findObjectIDBySubject(ID subject, EntityIDBuffer* buffer, ID minID, ID maxID);

	Status findSubject(EntityIDBuffer* buffer, ID minID, ID maxID);
	Status findPredicate(EntityIDBuffer* buffer, ID minID, ID maxID);
	Status findObject(EntityIDBuffer* buffer, ID minID, ID maxID);
private:
	Status findSubjectIDByPredicate(ID predicateID, EntityIDBuffer* buffer);

	Status findObjectIDByPredicate(ID predicateID, EntityIDBuffer* buffer);

	Status findObjectIDAndSubjectIDByPredicate(ID predicate, EntityIDBuffer* buffer);
	Status findSubjectIDAndObjectIDByPredicate(ID predicate, EntityIDBuffer* buffer);

	Status findSubjectIDAndPredicateIDByPredicate(ID predicate, EntityIDBuffer* buffer);
	Status findSubjectIDAndPredicateIDByPredicate(ID predicate, EntityIDBuffer* buffer, ID min, ID max);

	Status findPredicateIDAndObjectIDByPredicate(ID predicate, EntityIDBuffer* buffer);
	Status findPredicateIDAndSubjectIDByPredicate(ID predicate, EntityIDBuffer* buffer);

	Status findObjectIDAndPredicateIDByPredicate(ID predicate, EntityIDBuffer* buffer);
	Status findObjectIDAndPredicateIDByPredicate(ID predicate, EntityIDBuffer* buffer, ID min, ID max);

	Status findSubjectIDAndPredicateIDByObject(ID objectID, EntityIDBuffer* buffer);
	Status findPredicateIDAndSubjectIDByObject(ID objectID, EntityIDBuffer* buffer);

	Status findPredicateIDAndObjectIDBySubject(ID subjectID, EntityIDBuffer* buffer);
	Status findObjectIDAndPredicateIDBySubject(ID subjectID, EntityIDBuffer *buffer);

	Status findSubjectIDByObject(ID object, EntityIDBuffer* buffer);
	Status findObjectIDBySubject(ID subject, EntityIDBuffer* buffer);
#ifdef TEST_TIME 
	void printTime() {
		indexTimer.printTime("index timer");
		readTimer.printTime("read time");
	}

#endif
};
#endif /* FINDENTITYID_H_ */
