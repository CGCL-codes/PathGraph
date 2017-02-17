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
#include "TripleBitQuery.h"
#include "TripleBitRepository.h"
#include "URITable.h"
#include "PredicateTable.h"
#include "FindEntityID.h"
#include "TripleBitQueryGraph.h"
#include "EntityIDBuffer.h"
#include "BufferManager.h"

#include <algorithm>
#include <math.h>
#include <set>
#ifdef TRIPLEBIT_UNIX
#include <sys/time.h>
#endif

//#define PRINT_BUFFERSIZE 1
//#define PRINT_RESULT 1

using namespace std;

TripleBitQuery::TripleBitQuery(TripleBitRepository& repo) {
	bitmap = repo.getBitmapBuffer();
	UriTable = repo.getURITable();
	preTable = repo.getPredicateTable();

	entityFinder = new FindEntityID(&repo);
}

TripleBitQuery::~TripleBitQuery() {
	if(entityFinder != NULL)
		delete entityFinder;
	entityFinder = NULL;

	size_t i;

	for( i = 0; i < EntityIDList.size(); i++)
	{
		if(EntityIDList[i] != NULL)
			BufferManager::getInstance()->freeBuffer(EntityIDList[i]);
	}

	EntityIDList.clear();
}

void TripleBitQuery::releaseBuffer()
{
	idTreeBFS.clear();
	leafNode.clear();
	varVec.clear();

	EntityIDListIterType iter = EntityIDList.begin();

	for( ; iter != EntityIDList.end(); iter++)
	{
		if(iter->second != NULL)
			BufferManager::getInstance()->freeBuffer(iter->second);
	}

	BufferManager::getInstance()->reserveBuffer();
	EntityIDList.clear();
}

Status TripleBitQuery::query(TripleBitQueryGraph* queryGraph, vector<string>& resultSet)
{
	this->_queryGraph = queryGraph;
	this->_query = &(queryGraph->getQuery());
	this->resultPtr = &resultSet;

	if(_query->joinVariables.size() == 1){
		// has only one join variable
		singleVariableJoin();
	}else {
		if(_query->joinGraph == TripleBitQueryGraph::ACYCLIC){
			// has multiple join variable but not cyclic
			acyclicJoin();
		}else if(_query->joinGraph == TripleBitQueryGraph::CYCLIC){
			cyclicJoin();
		}
	}
#ifdef TEST_TIME
	entityFinder->printTime();
#endif
	return OK;
}

static void generateProjectionBitVector(uint& bitv, std::vector<ID>& project)
{
	bitv = 0;
	for(size_t i = 0; i != project.size(); i++) {
		bitv |= 1 << project[i];
	}
}

static void generateTripleNodeBitVector(uint& bitv, TripleBitQueryGraph::TripleNode& node)
{
	bitv = 0;
	if(!node.constSubject)
		bitv = bitv | (1 << node.subject);
	if(!node.constPredicate)
		bitv = bitv | (1 << node.predicate);
	if(!node.constObject)
		bitv = bitv | (1 << node.object);
}

static size_t countOneBits(uint bitv)
{
	size_t count = 0;
	while(bitv) {
		bitv = bitv & (bitv - 1);
		count++;
	}

	return count;
}

static ID bitVtoID(uint bitv)
{
	uint mask = 0x1;
	ID count = 0;
	while(true) {
		if((mask & bitv) == mask)
			break;
		bitv = bitv>>1;
		count++;
	}

	return count;
}

/*
 * Generate the result-variable vector and the id of sortKey according to the current query pattern and joinKey of last query pattern.
 * input:
 *		key -- joinKey of last query pattern
 *		idvec- result-variable vector
 *		node - current query pattern
 *		sortID - the id of sort key
 * output:
 *		the position of joinKey in current query pattern's variables.
 */
static int insertVarID(ID key, std::vector<ID>& idvec, TripleBitQueryGraph::TripleNode& node, ID& sortID)
{
	int ret = 0;
	switch(node.scanOperation) {
	case TripleBitQueryGraph::TripleNode::FINDO:
	case TripleBitQueryGraph::TripleNode::FINDOBYP:
	case TripleBitQueryGraph::TripleNode::FINDOBYS:
	case TripleBitQueryGraph::TripleNode::FINDOBYSP:
		if(key != node.object ) idvec.push_back(node.object);
		sortID = node.object;
		break;
	case TripleBitQueryGraph::TripleNode::FINDOPBYS:
		if(key != node.object)
			idvec.push_back(node.object);
		else {
			sortID = node.object;
			ret = 0;
		}
		if(key != node.predicate)
			idvec.push_back(node.predicate);
		else {
			sortID = node.predicate;
			ret = 1;
		}
		break;
	case TripleBitQueryGraph::TripleNode::FINDOSBYP:
		if(key != node.object)
			idvec.push_back(node.object);
		else {
			sortID = node.object;
			ret = 0;
		}
		if(key != node.subject)
			idvec.push_back(node.subject);
		else {
			sortID = node.subject;
			ret = 1;
		}
		break;
	case TripleBitQueryGraph::TripleNode::FINDP:
	case TripleBitQueryGraph::TripleNode::FINDPBYO:
	case TripleBitQueryGraph::TripleNode::FINDPBYS:
	case TripleBitQueryGraph::TripleNode::FINDPBYSO:
		if(key != node.predicate) idvec.push_back(node.predicate);
		sortID = node.predicate;
		break;
	case TripleBitQueryGraph::TripleNode::FINDPOBYS:
		if(key != node.predicate)
			idvec.push_back(node.predicate);
		else {
			sortID = node.predicate;
			ret = 0;
		}

		if(key != node.object)
			idvec.push_back(node.object);
		else {
			sortID = node.object;
			ret = 1;
		}
		break;
	case TripleBitQueryGraph::TripleNode::FINDPSBYO:
		if(key != node.predicate)
			idvec.push_back(node.predicate);
		else {
			sortID = node.predicate;
			ret = 0;
		}

		if(key != node.subject)
			idvec.push_back(node.subject);
		else {
			sortID = node.subject;
			ret = 1;
		}
		break;
	case TripleBitQueryGraph::TripleNode::FINDS:
	case TripleBitQueryGraph::TripleNode::FINDSBYO:
	case TripleBitQueryGraph::TripleNode::FINDSBYP:
	case TripleBitQueryGraph::TripleNode::FINDSBYPO:
		if(key != node.subject) idvec.push_back(node.subject);
		sortID = node.subject;
		break;
	case TripleBitQueryGraph::TripleNode::FINDSOBYP:
		if(key != node.subject)
			idvec.push_back(node.subject);
		else {
			sortID = node.subject;
			ret = 0;
		}

		if(key != node.object)
			idvec.push_back(node.object);
		else {
			sortID = node.object;
			ret = 1;
		}
		break;
	case TripleBitQueryGraph::TripleNode::FINDSPBYO:
		if(key != node.subject)
			idvec.push_back(node.subject);
		else {
			sortID = node.subject;
			ret = 0;
		}
		if(key != node.predicate)
			idvec.push_back(node.predicate);
		else {
			sortID = node.predicate;
			ret = 1;
		}
		break;
	case TripleBitQueryGraph::TripleNode::NOOP:
		break;
	}

	return ret;
}

static void generateResultPos(std::vector<ID>& idVec, std::vector<ID>& projection, std::vector<int>& resultPos)
{
	resultPos.clear();

	std::vector<ID>::iterator iter;
	for(size_t i = 0; i != projection.size(); i++) {
		iter = find(idVec.begin(), idVec.end(), projection[i]);
		resultPos.push_back(iter - idVec.begin());
	}
}

/*
 * Generate the verify-variable's position of result-variable vector
 * input:
 *		idVec -- result-variable vector
 */
static void generateVerifyPos(std::vector<ID>& idVec, std::vector<int>& verifyPos)
{
	verifyPos.clear();
	size_t i, j;
	size_t size = idVec.size();
	for(i = 0; i != size; i++) {
		for(j = i + 1; j != size; j++) {
			if(idVec[i] == idVec[j]) {
				verifyPos.push_back(i);
				verifyPos.push_back(j);
				return;
			}
		}
	}
}

Status TripleBitQuery::singleVariableJoin()
{
	vector<TripleBitQueryGraph::JoinVariableNodeID>::iterator joinNodeIter = _query->joinVariables.begin();
	vector<pair<TripleBitQueryGraph::TripleNodeID,TripleBitQueryGraph::JoinVariableNode::DimType> >::iterator nodePatternIter;

	TripleBitQueryGraph::JoinVariableNode* node;
	EntityIDBuffer *buffer;
	TripleBitQueryGraph::TripleNode* triple;

	//TODO Initialize the first query pattern's triple of the pattern group which has the same variable;
	getVariableNodeByID(node, *joinNodeIter);
	nodePatternIter = node->appear_tpnodes.begin();

	buffer = BufferManager::getInstance()->getNewBuffer();
	getTripleNodeByID(triple, nodePatternIter->first);
	findEntityIDByTriple(triple, buffer, 0, INT_MAX);
	EntityIDList[nodePatternIter->first] = buffer;
	nodePatternIter++;

	EntityIDBuffer* tempBuffer;
	ID minID, maxID;
	if(_queryGraph->getProjection().size() == 1) {
		tempBuffer = BufferManager::getInstance()->getNewBuffer();
		for( ; nodePatternIter != node->appear_tpnodes.end(); nodePatternIter++ )
		{
			tempBuffer->empty();
			buffer->getMinMax(minID, maxID);
			getTripleNodeByID(triple, nodePatternIter->first);
			findEntityIDByTriple(triple,tempBuffer, minID, maxID);
			mergeJoin.Join(buffer,tempBuffer,1,1,false);
			if(buffer->getSize() == 0) {
#ifdef PRINT_RESULT
				cout<<"empty result"<<endl;
#else
				resultPtr->push_back("-1");
				resultPtr->push_back("Empty Result");
#endif
				return OK;
			}
		}
	} else {
		for( ; nodePatternIter != node->appear_tpnodes.end(); nodePatternIter++ ) {
			tempBuffer = BufferManager::getInstance()->getNewBuffer();
			tempBuffer->empty();
			buffer->getMinMax(minID, maxID);
			getTripleNodeByID(triple, nodePatternIter->first);
			findEntityIDByTriple(triple,tempBuffer, minID, maxID);
			mergeJoin.Join(buffer,tempBuffer,1,1,true);
			if(buffer->getSize() == 0) {
#ifdef PRINT_RESULT
				cout<<"empty result"<<endl;
#else
				resultPtr->push_back("-1");
				resultPtr->push_back("Empty Result");
#endif
				return OK;
			}
			EntityIDList[nodePatternIter->first] = tempBuffer;
		}
	}

	//TODO materialization the result;
	size_t i;
	size_t size = buffer->getSize();

	std::string URI;

	ID* p = buffer->getBuffer();

	size_t projectNo = _queryGraph->getProjection().size();
#ifndef PRINT_RESULT
	char temp[2];
	sprintf(temp, "%d", projectNo);
	resultPtr->push_back(temp);
#endif

	if(projectNo == 1) {
		for (i = 0; i < size; i++) {
			if (UriTable->getURIById(URI, p[i]) == OK)
#ifdef PRINT_RESULT
				cout << URI << endl;
#else
				resultPtr->push_back(URI);
#endif
			else
#ifdef PRINT_RESULT
				cout << p[i] << " " << "not found" << endl;
#else
				resultPtr->push_back("NULL");
#endif
		}
	} else {
		std::vector<EntityIDBuffer*> bufferlist;
		std::vector<ID> resultVar;
		resultVar.resize(0);
		uint projBitV, nodeBitV, resultBitV, tempBitV;
		resultBitV = 0;
		ID sortID;
		int keyp;
		keyPos.clear();

		generateProjectionBitVector(projBitV, _queryGraph->getProjection());
		for( i = 0; i != _query->tripleNodes.size(); i++) {
			// generate the bit vector of query pattern.
			generateTripleNodeBitVector(nodeBitV, _query->tripleNodes[i]);
			// get the common bit
			tempBitV = projBitV & nodeBitV;
			if(tempBitV == nodeBitV) {
				// the query pattern which contains two or more variables is better.
				if(EntityIDList.count(_query->tripleNodes[i].tripleNodeID) == 0 || (EntityIDList[_query->tripleNodes[i].tripleNodeID]->getIDCount() == 1))
					continue;
				bufferlist.push_back(EntityIDList[_query->tripleNodes[i].tripleNodeID]);
				if(countOneBits(resultBitV) == 0)
					// the first time, last joinKey should be set as UINT_MAX
					keyp = insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID); 
				else {
					keyp = insertVarID(*joinNodeIter, resultVar, _query->tripleNodes[i], sortID);
					keyPos.push_back(keyp);
				}
				resultBitV = resultBitV | nodeBitV;
				// the buffer of query pattern is enough.
				if(countOneBits(resultBitV) == projectNo)
					break;
			}
		}

		if(projectNo == 2) {
			generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);

			EntityIDBuffer* buf = bufferlist[0];
			size_t bufsize = buf->getSize(); ID* ids = buf->getBuffer();
			int IDCount = buf->getIDCount();
			for(i = 0; i < bufsize; i++) {
				for(int j = 0; j < IDCount; j++) {
					if (UriTable->getURIById(URI, ids[i * IDCount + resultPos[j]]) == OK) {
#ifdef PRINT_RESULT
						cout<<URI<<" ";
#else
						resultPtr->push_back(URI);
#endif
					} else
						cout<<"not found"<<endl;
				}
				cout<<endl;
			}
		} else {
			generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);
			needselect = false;

			EntityIDBuffer* buf = bufferlist[0];
			size_t bufsize = buf->getSize();
			bufPreIndexs.clear();
			bufPreIndexs.resize(bufferlist.size(), 0);
			ID key; int IDCount = buf->getIDCount(); ID* ids = buf->getBuffer(); int sortKey = buf->getSortKey();
			for(i = 0; i != bufsize; i++) {
				resultVec.resize(0);
				key = ids[i * IDCount + sortKey];
				for(int j = 0; j < IDCount ; j++)
					resultVec.push_back(ids[i * IDCount + j]);

				bool ret = getResult(key, bufferlist, 1);
				if(ret == false) {
					while(i < bufsize && ids[i * IDCount + sortKey] == key) {
						i++;
					}
					i--;
				}
			}
		}
	}

	return OK;
}

bool TripleBitQuery::getResult(ID key, std::vector<EntityIDBuffer*>& bufferlist, size_t buflist_index) {
	if(buflist_index == bufferlist.size())
		return true;

	EntityIDBuffer* entBuf = bufferlist[buflist_index];
	size_t currentIndex = bufPreIndexs[buflist_index];
	size_t bufsize = entBuf->getSize();
	while(currentIndex < bufsize && (*entBuf)[currentIndex] < key) {
		currentIndex++;
	}
	bufPreIndexs[buflist_index] = currentIndex;
	if(currentIndex >= bufsize || (*entBuf)[currentIndex] > key)
		return false;

	bool ret;
	ID* buf = entBuf->getBuffer();
	int IDCount = entBuf->getIDCount();
	int sortKey = entBuf->getSortKey();

	ret = true;
	size_t resultVecSize = resultVec.size();
	std::string URI;
	while(currentIndex < bufsize && buf[currentIndex * IDCount + sortKey] == key) {
		for(int i = 0; i < IDCount; i++) {
			if(i != sortKey)
				resultVec.push_back(buf[currentIndex * IDCount + i]);
		}
		if(buflist_index == (bufferlist.size() - 1)) {
			if(needselect == true) {
				if(resultVec[verifyPos[0]] != resultVec[verifyPos[1]]) {
					currentIndex++;
					continue;
				}
			}
			for(size_t j = 0; j != resultPos.size(); j++) {
				UriTable->getURIById(URI, resultVec[resultPos[j]]);
#ifdef PRINT_RESULT
				std::cout<<URI<<" ";
#else
				resultPtr->push_back(URI);
#endif
			}
#ifdef PRINT_RESULT
			std::cout<<std::endl;
#endif
		} else {
			ret = getResult(resultVec[keyPos[buflist_index]], bufferlist, buflist_index + 1);
			if(ret != true)
				break;
		}

		currentIndex++;
		resultVec.resize(resultVecSize);
	}

	return ret;
}

void TripleBitQuery::getResult_join(ID key, std::vector<EntityIDBuffer*>& bufferlist, size_t buflist_index)
{
	if (buflist_index == bufferlist.size())
		return;

	EntityIDBuffer* entBuf = bufferlist[buflist_index];
	size_t currentIndex = entBuf->getEntityIDPos(key);//bufPreIndexs[buflist_index];
	if(currentIndex == size_t(-1))
		return;
	size_t bufsize = entBuf->getSize();

	ID* buf = entBuf->getBuffer();
	int IDCount = entBuf->getIDCount();
	int sortKey = entBuf->getSortKey();

	size_t resultVecSize = resultVec.size();
	std::string URI;
	while (currentIndex < bufsize && buf[currentIndex * IDCount + sortKey] == key) {
		for (int i = 0; i < IDCount; i++) {
			if (i != sortKey)
				resultVec.push_back(buf[currentIndex * IDCount + i]);
		}
		if (buflist_index == (bufferlist.size() - 1)) {
			if (needselect == true) {
				if (resultVec[verifyPos[0]] != resultVec[verifyPos[1]]) {
					currentIndex++;
					continue;
				}
			}
			for (size_t j = 0; j != resultPos.size(); j++) {
				UriTable->getURIById(URI, resultVec[resultPos[j]]);
#ifdef PRINT_RESULT
				std::cout << URI << " ";
#else
				resultPtr->push_back(URI);
#endif
			}
#ifdef PRINT_RESULT
			std::cout << std::endl;
#endif
		} else {
			getResult_join(resultVec[keyPos[buflist_index]], bufferlist, buflist_index + 1);
		}

		currentIndex++;
		resultVec.resize(resultVecSize);
	}
}

Status TripleBitQuery::findEntityIDByTriple(TripleBitQueryGraph::TripleNode* triple, EntityIDBuffer* buffer, ID minID, ID maxID)
{
	switch(triple->scanOperation)
	{
	case TripleBitQueryGraph::TripleNode::FINDSBYPO:
		entityFinder->findSubjectIDByPredicateAndObject(triple->predicate,triple->object,buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDOBYSP:
		entityFinder->findObjectIDByPredicateAndSubject(triple->predicate,triple->subject,buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDSOBYP:
		entityFinder->findSubjectIDAndObjectIDByPredicate(triple->predicate, buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDOSBYP:
		entityFinder->findObjectIDAndSubjectIDByPredicate(triple->predicate, buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDS:
		entityFinder->findObject(buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDSBYP:
		entityFinder->findSubjectIDByPredicate(triple->predicate, buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDSBYO:
		entityFinder->findSubjectIDByObject(triple->object, buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDSPBYO:
		entityFinder->findSubjectIDAndPredicateIDByObject(triple->object, buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDPSBYO:
		entityFinder->findPredicateIDAndSubjectIDByObject(triple->object, buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDO:
		entityFinder->findObject(buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDOBYP:
		entityFinder->findObjectIDByPredicate(triple->predicate, buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDOBYS:
		entityFinder->findObjectIDBySubject(triple->subject, buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDP:
		entityFinder->findPredicate(buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDPBYO:
		entityFinder->findPredicateIDByObject(triple->object, buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDPBYS:
		entityFinder->findPredicateIDBySubject(triple->subject, buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDPBYSO:
		entityFinder->findPredicateIDBySubjectAndObject(triple->subject, triple->object, buffer);
		break;
	case TripleBitQueryGraph::TripleNode::FINDPOBYS:
		entityFinder->findPredicateIDAndObjectIDBySubject(triple->subject, buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDOPBYS:
		entityFinder->findObjectIDAndPredicateIDBySubject(triple->subject, buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::NOOP:
		break;
	default:
		cerr<<"unsupported operation"<<endl;
	}
	return OK;
}

Status TripleBitQuery::acyclicJoin()
{
#ifdef DEBUG
	cout<<__FUNCTION__<<endl;
#endif
	vector<TripleBitQueryGraph::JoinVariableNodeID>::iterator joinNodeIter = _query->joinVariables.begin();
	vector<pair<TripleBitQueryGraph::TripleNodeID,TripleBitQueryGraph::JoinVariableNode::DimType> >::iterator nodePatternIter;

	TripleBitQueryGraph::JoinVariableNode* node;
	EntityIDBuffer *buffer;
	TripleBitQueryGraph::TripleNode* triple;

	// initialize the patterns which are related to the first variable.
	getVariableNodeByID(node, *joinNodeIter);
	nodePatternIter = node->appear_tpnodes.begin();
	buffer = BufferManager::getInstance()->getNewBuffer();
	EntityIDList[nodePatternIter->first] = buffer;
	getTripleNodeByID(triple, nodePatternIter->first);
	findEntityIDByTriple(triple, buffer, 0, INT_MAX);

	buffer = EntityIDList[nodePatternIter->first];
	if ( this->findEntitiesAndJoin(*joinNodeIter, node->appear_tpnodes,true) == NULL_RESULT) {
#ifdef PRINT_RESULT
		cout<<"empty result"<<endl;
#else
		resultPtr->push_back("-1");
		resultPtr->push_back("Empty Result");
#endif
		return OK;
	}

	joinNodeIter++;
	//iterate the join variables.
	for (; joinNodeIter != _query->joinVariables.end(); joinNodeIter++) {
		getVariableNodeByID(node, *joinNodeIter);
		if ( node->appear_tpnodes.size() > 1 ) {
			if ( this->findEntitiesAndJoin(*joinNodeIter, node->appear_tpnodes,true) == NULL_RESULT) {
#ifdef PRINT_RESULT
				cout<<"empty result"<<endl;
#else
				resultPtr->push_back("-1");
				resultPtr->push_back("Empty Result");
#endif
				return OK;
			}
		}
	}

	/// iterate reverse.
	TripleBitQueryGraph::JoinVariableNodeID varID;
	bool isLeafNode;
	size_t size = _query->joinVariables.size();
	size_t i;

	for( i = 0; i <size; i++)
	{
		varID = _query->joinVariables[i];
		isLeafNode = nodeIsLeaf(varID);
		if( isLeafNode == false){
			getVariableNodeByID(node,varID);
			if ( this->findEntitiesAndJoin(varID,node->appear_tpnodes,false) == NULL_RESULT) {
#ifdef PRINT_RESULT
				cout<<"empty result"<<endl;
#else
				resultPtr->push_back("-1");
				resultPtr->push_back("Empty Result");
#endif
				return OK;
			}
		}
	}

	//TODO materialize.
	size_t projectionSize = _queryGraph->getProjection().size();
	if(projectionSize == 1) {
		uint projBitV, nodeBitV, tempBitV;
		std::vector<ID> resultVar;
		resultVar.resize(0);
		ID sortID;

		generateProjectionBitVector(projBitV, _queryGraph->getProjection());
		TripleBitQueryGraph::TripleNodeID tid;
		size_t bufsize = UINT_MAX;
		for( i = 0; i != _query->tripleNodes.size(); i++) {
			if(EntityIDList.count(_query->tripleNodes[i].tripleNodeID) == 0)
				continue;
			generateTripleNodeBitVector(nodeBitV, _query->tripleNodes[i]);
			tempBitV = projBitV | nodeBitV;
			if(tempBitV == projBitV) {
				//TODO output the result.
				if(EntityIDList[_query->tripleNodes[i].tripleNodeID]->getIDCount() == 1) {
					insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
					tid = _query->tripleNodes[i].tripleNodeID;
					break;
				}else {
					if(EntityIDList[_query->tripleNodes[i].tripleNodeID]->getSize() < bufsize) {
						insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
						bufsize = EntityIDList[_query->tripleNodes[i].tripleNodeID]->getSize();
						tid = _query->tripleNodes[i].tripleNodeID;
					}
				}
			}
		}

		std::vector<EntityIDBuffer*> bufferlist;
		bufferlist.push_back(EntityIDList[_query->tripleNodes[i].tripleNodeID]);
		generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);
		needselect = false;

		std::string URI;
		ID* p = EntityIDList[tid]->getBuffer();
		int IDCount = EntityIDList[tid]->getIDCount();
		for(i = 0; i != bufsize; i++) {
			UriTable->getURIById(URI, p[i * IDCount + resultPos[0]]);
#ifdef PRINT_RESULT
			std::cout<<URI<<std::endl;
#else
			resultPtr->push_back(URI);
#endif
		}
	} else {
		std::vector<EntityIDBuffer*> bufferlist;
		std::vector<ID> resultVar;
		ID sortID;
		resultVar.resize(0);
		uint projBitV, nodeBitV, resultBitV, tempBitV;
		resultBitV = 0;
		generateProjectionBitVector(projBitV, _queryGraph->getProjection());
		i = 0;
		int sortKey;
		size_t tnodesize = _query->tripleNodes.size();
		while(true) {
			if(EntityIDList.count(_query->tripleNodes[i].tripleNodeID) == 0 || (EntityIDList[_query->tripleNodes[i].tripleNodeID]->getIDCount() == 1)) {
				i++;
				i = i % tnodesize;
				continue;
			}
			generateTripleNodeBitVector(nodeBitV, _query->tripleNodes[i]);
			tempBitV = projBitV & nodeBitV;
			if(tempBitV == nodeBitV) {
				if(countOneBits(resultBitV) == 0) {
					insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
					sortKey = EntityIDList[_query->tripleNodes[i].tripleNodeID]->getSortKey();
				} else {
					tempBitV = nodeBitV & resultBitV;
					if(countOneBits(tempBitV) == 1) {
						ID key = ID(log((double)tempBitV) / log(2.0));
						sortKey = insertVarID(key, resultVar, _query->tripleNodes[i], sortID);
					} else{
						i++;
						i = i % tnodesize;
						continue;
					}
				}

				resultBitV = resultBitV | nodeBitV;
				EntityIDList[_query->tripleNodes[i].tripleNodeID]->setSortKey(sortKey);
				bufferlist.push_back(EntityIDList[_query->tripleNodes[i].tripleNodeID]);

				if(countOneBits(resultBitV) == projectionSize)
					break;
			}

			i++;
			i = i % tnodesize;
		}

		for(i = 0; i < bufferlist.size(); i++) {
			bufferlist[i]->sort();
		}

		generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);
		needselect = false;

		std::string URI;
		if(projectionSize == 2) {
			EntityIDBuffer* buf = bufferlist[0];
			size_t bufsize = buf->getSize(); ID* ids = buf->getBuffer();
			int IDCount = buf->getIDCount();
			for(i = 0; i < bufsize; i++) {
				for(int j = 0; j < IDCount; j++) {
					if (UriTable->getURIById(URI, ids[i * IDCount + resultPos[j]]) == OK) {
#ifdef PRINT_RESULT
						cout<<URI<<" ";
#else
						resultPtr->push_back(URI);
#endif
					} else {
						cout<<"not found"<<endl;
					}
				}
				cout<<endl;
			}
		} else {
			EntityIDBuffer* buf = bufferlist[0];
			size_t bufsize = buf->getSize();
			bufPreIndexs.resize(bufferlist.size(), 0);
			ID key; int IDCount = buf->getIDCount(); ID* ids = buf->getBuffer(); int sortKey = buf->getSortKey();
			for(i = 0; i != bufsize; i++) {
				resultVec.resize(0);
				key = ids[i * IDCount + sortKey];
				for(int j = 0; j < IDCount ; j++)
					resultVec.push_back(ids[i * IDCount + j]);

				getResult_join(resultVec[keyPos[0]], bufferlist, 1);
			}
		}
	}
	return OK;
}

bool TripleBitQuery::nodeIsLeaf(TripleBitQueryGraph::JoinVariableNodeID varID)
{
	vector<TripleBitQueryGraph::JoinVariableNodeID>::iterator iter;
	iter = find(_query->leafNodes.begin(),_query->leafNodes.end(),varID);
	if( iter != _query->leafNodes.end())
		return true;
	else
		return false;
}


Status TripleBitQuery::cyclicJoin() {
	vector<TripleBitQueryGraph::JoinVariableNodeID>::iterator joinNodeIter = _query->joinVariables.begin();
	vector<pair<TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> >::iterator nodePatternIter;

	TripleBitQueryGraph::JoinVariableNode* node;
	EntityIDBuffer *buffer;
	TripleBitQueryGraph::TripleNode* triple;

	// initialize the patterns which are related to the first variable.
	getVariableNodeByID(node, *joinNodeIter);
	nodePatternIter = node->appear_tpnodes.begin();
	buffer = BufferManager::getInstance()->getNewBuffer();
	EntityIDList[nodePatternIter->first] = buffer;
	getTripleNodeByID(triple, nodePatternIter->first);
	findEntityIDByTriple(triple, buffer, 0, INT_MAX);

	if ( this->findEntitiesAndJoin(*joinNodeIter, node->appear_tpnodes, true) == NULL_RESULT ) {
#ifdef PRINT_RESULT
		cout<<"empty result"<<endl;
#else
		resultPtr->push_back("-1");
		resultPtr->push_back("Empty Result");
#endif
		return OK;
	}

	joinNodeIter++;
	// iterate the pattern groups.
	for (; joinNodeIter != _query->joinVariables.end(); joinNodeIter++) {
		getVariableNodeByID(node, *joinNodeIter);
		if (node->appear_tpnodes.size() > 1) {
			if ( this->findEntitiesAndJoin(*joinNodeIter, node->appear_tpnodes, true) == NULL_RESULT ) {
#ifdef PRINT_RESULT
				cout<<"empty result"<<endl;
#else
				resultPtr->push_back("-1");
				resultPtr->push_back("Empty Result");
#endif
				return OK;
			}
		}
	}

	// iterate reverse.
	bool isLeafNode;
	TripleBitQueryGraph::JoinVariableNodeID varID;
	size_t size = this->_query->joinVariables.size();
	size_t i = 0;

	for (i = size - 1; i != (size_t(-1)); i--) {
		varID = _query->joinVariables[i];
		isLeafNode = nodeIsLeaf(varID);
		if (isLeafNode == false) {
			getVariableNodeByID(node, varID);
			if ( this->findEntitiesAndJoin(varID, node->appear_tpnodes, false) == NULL_RESULT ) {
#ifdef PRINT_RESULT
				cout<<"empty result"<<endl;
#else
				resultPtr->push_back("-1");
				resultPtr->push_back("Empty Result");
#endif
				return OK;
			}
		}
	}

	for( i = 0; i < size; i++)
	{
		varID = _query->joinVariables[i];
		isLeafNode = nodeIsLeaf(varID);
		getVariableNodeByID(node, varID);
		if ( this->findEntitiesAndJoin(varID, node->appear_tpnodes, false) == NULL_RESULT ) {
#ifdef PRINT_RESULT
			cout<<"empty result"<<endl;
#else
			resultPtr->push_back("-1");
			resultPtr->push_back("Empty Result");
#endif
			return OK;
		}
	}

	for (i = size - 1; i != (size_t(-1)); i--) {
		varID = _query->joinVariables[i];
		isLeafNode = nodeIsLeaf(varID);
		if (isLeafNode == false) {
			getVariableNodeByID(node, varID);
			if (  this->findEntitiesAndJoin(varID, node->appear_tpnodes, false) == NULL_RESULT ) {
#ifdef PRINT_RESULT
				cout<<"empty result"<<endl;
#else
				resultPtr->push_back("-1");
				resultPtr->push_back("Empty Result");
#endif
				return OK;
			}
		}
	}

	//TODO materialize
	std::vector<EntityIDBuffer*> bufferlist;
	std::vector<ID> resultVar;
	ID sortID;

	resultVar.resize(0);
	uint projBitV, nodeBitV, resultBitV, tempBitV;
	resultBitV = 0;
	generateProjectionBitVector(projBitV, _queryGraph->getProjection());

	int sortKey;
	size_t tnodesize = _query->tripleNodes.size();
	std::set<ID> tids;
	ID tid;
	bool complete = true;
	i = 0;
	vector<ID>::iterator iter;

	keyPos.clear();
	resultPos.clear();
	verifyPos.clear();

	while(true) {
		//if the pattern has no buffer, then skip it;
		if(EntityIDList.count(_query->tripleNodes[i].tripleNodeID) == 0) {
			i++;
			i = i % tnodesize;
			continue;
		}
		generateTripleNodeBitVector(nodeBitV, _query->tripleNodes[i]);
		if(countOneBits(nodeBitV) == 1) {
			i++;
			i = i % tnodesize;
			continue;
		}

		tid = _query->tripleNodes[i].tripleNodeID;
		if(tids.count(tid) == 0) {
			if(countOneBits(resultBitV) == 0) {
				insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
				sortKey = EntityIDList[tid]->getSortKey();
			} else {
				tempBitV = nodeBitV & resultBitV;
				if(countOneBits(tempBitV) == 1) {
					ID key = bitVtoID(tempBitV);//ID(log((double)tempBitV) / log(2.0));
					sortKey = insertVarID(key, resultVar, _query->tripleNodes[i], sortID);
					iter = find(resultVar.begin(), resultVar.end(), sortID);
					keyPos.push_back(iter - resultVar.begin());
				} else if(countOneBits(tempBitV) == 2){
					// verify buffers;
					ID key = bitVtoID(tempBitV); //ID(log((double)tempBitV) / log(2.0));
					sortKey = insertVarID(key, resultVar, _query->tripleNodes[i], sortID);
					iter = find(resultVar.begin(), resultVar.end(), sortID);
					keyPos.push_back(iter - resultVar.begin());
				} else {
					complete = false;
					i++;
					i = i % tnodesize;
					continue;
				}
			}
			resultBitV = resultBitV | nodeBitV;
			EntityIDList[tid]->setSortKey(sortKey);
			bufferlist.push_back(EntityIDList[tid]);
			tids.insert(tid);
		}

		i++;
		if( i == tnodesize) {
			if( complete == true)
				break;
			else {
				complete = true;
				i = i % tnodesize;
			}
		}
	}

	for (i = 0; i < bufferlist.size(); i++) {
		bufferlist[i]->sort();
	}

	generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);
	/// generate verify pos vector;
	generateVerifyPos(resultVar, verifyPos);
	needselect = true;

	EntityIDBuffer* buf = bufferlist[0];
	size_t bufsize = buf->getSize();

	ID key; int IDCount = buf->getIDCount(); ID* ids = buf->getBuffer(); sortKey = buf->getSortKey();
	for(i = 0; i != bufsize; i++) {
		resultVec.resize(0);
		key = ids[i * IDCount + sortKey];
		for(int j = 0; j < IDCount ; j++)
			resultVec.push_back(ids[i * IDCount + j]);

		getResult_join(resultVec[keyPos[0]], bufferlist, 1);
//		if(ret == false) {
//			while(i < bufsize && ids[i * IDCount + sortKey] == key) {
//				i++;
//			}
//			i--;
//		}
	}
	return OK;
}

int TripleBitQuery::getVariablePos(TripleBitQueryGraph::JoinVariableNodeID id, TripleBitQueryGraph::TripleNode* triple)
{
	int pos;

	switch(triple->scanOperation) {
	case TripleBitQueryGraph::TripleNode::FINDO:
	case TripleBitQueryGraph::TripleNode::FINDOBYP:
	case TripleBitQueryGraph::TripleNode::FINDOBYS:
	case TripleBitQueryGraph::TripleNode::FINDOBYSP:
		pos = 1;
		break;
	case TripleBitQueryGraph::TripleNode::FINDOPBYS:
		if(id == triple->object) pos = 1;
		else pos = 2;
		break;
	case TripleBitQueryGraph::TripleNode::FINDOSBYP:
		if(id == triple->object) pos = 1;
		else pos = 2;
		break;
	case TripleBitQueryGraph::TripleNode::FINDP:
	case TripleBitQueryGraph::TripleNode::FINDPBYO:
	case TripleBitQueryGraph::TripleNode::FINDPBYS:
	case TripleBitQueryGraph::TripleNode::FINDPBYSO:
		pos = 1;
		break;
	case TripleBitQueryGraph::TripleNode::FINDPOBYS:
		if(id == triple->predicate) pos = 1;
		else pos = 2;
		break;
	case TripleBitQueryGraph::TripleNode::FINDPSBYO:
		if(id == triple->predicate) pos = 1;
		else pos =2 ;
		break;
	case TripleBitQueryGraph::TripleNode::FINDS:
	case TripleBitQueryGraph::TripleNode::FINDSBYO:
	case TripleBitQueryGraph::TripleNode::FINDSBYP:
	case TripleBitQueryGraph::TripleNode::FINDSBYPO:
		pos = 1;
		break;
	case TripleBitQueryGraph::TripleNode::FINDSOBYP:
		if(id == triple->subject) pos = 1;
		else pos = 2;
		break;
	case TripleBitQueryGraph::TripleNode::FINDSPBYO:
		if(id == triple->subject) pos = 1;
		else pos = 2;
		break;
	case TripleBitQueryGraph::TripleNode::NOOP:
		pos = -1;
		break;
	}
	return pos;
}

Status TripleBitQuery::getTripleNodeByID(TripleBitQueryGraph::TripleNode*& triple, TripleBitQueryGraph::TripleNodeID nodeID)
{
	vector<TripleBitQueryGraph::TripleNode>::iterator iter = _query->tripleNodes.begin();

	for(; iter != _query->tripleNodes.end(); iter++)
	{
		if(iter->tripleNodeID == nodeID){
			triple = &(*iter);
			return OK;
		}
	}

	return NOT_FOUND;
}

/*
 * find the candidate result and join with the minimum intermediate result buffer.
 *
 * @param:
 * id: the id of variable node
 * tpnodes: query patterns which belongs to variable node.
 * firstTime: identify whether it is the first of the variable to do join.
 */
Status TripleBitQuery::findEntitiesAndJoin(TripleBitQueryGraph::JoinVariableNodeID id,
		vector< pair < TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> >& tpnodes,bool firstTime)
{
	size_t minSize;		// minimum record size.
	ID minSizeID;		// the bufferer No. which contains the minimum records;
	size_t size;
	EntityIDBuffer* buffer;
	map<ID,bool> firstInsertFlag;	//flag the first inserted pattern buffer;
	Status s;

	minSize = INT_MAX;
	minSizeID = 0;

	size = EntityIDList.size();

	// TODO get the buffer id which contains minimum candidate result.
	EntityIDListIterType iter;
	size_t i;
	if( firstTime == true){
		for (i = 0; i < tpnodes.size(); i++) {
			firstInsertFlag[tpnodes[i].first] = false;
			iter = EntityIDList.find(tpnodes[i].first);
			if (iter != EntityIDList.end()) {
				if ((size = iter->second->getSize()) < minSize) {
					minSize = size;
					minSizeID = tpnodes[i].first;
				}
			} else if (getVariableCount(tpnodes[i].first) >= 2) {
				//TODO if the number of variable in a pattern is greater than 1, then allocate a buffer for the pattern.
				buffer = BufferManager::getInstance()->getNewBuffer();
				EntityIDList[tpnodes[i].first] = buffer;
				firstInsertFlag[tpnodes[i].first] = true;
			}
		}
	}else{
		for(i = 0; i < tpnodes.size(); i++) {
			firstInsertFlag[tpnodes[i].first] = false;
			if(getVariableCount(tpnodes[i].first) >= 2){
				if(EntityIDList[tpnodes[i].first]->getSize() < minSize) {
					minSize = EntityIDList[tpnodes[i].first]->getSize();
					minSizeID = tpnodes[i].first;
				}
			}
		}
	}

	//if the most selective pattern has not been initialized, then initialize it.
	iter = EntityIDList.find(minSizeID);
	if( iter == EntityIDList.end())
	{
		TripleBitQueryGraph::TripleNode* triple;
		getTripleNodeByID(triple,minSizeID);
		buffer = BufferManager::getInstance()->getNewBuffer();
		EntityIDList[minSizeID] = buffer;
		findEntityIDByTriple(triple,buffer,0, INT_MAX);
	}

	if(firstTime == true)
		s = findEntitiesAndJoinFirstTime(tpnodes,minSizeID,firstInsertFlag, id);
	else
		s = modifyEntitiesAndJoin(tpnodes, minSizeID, id);

	return s;
}

EntityType TripleBitQuery::getDimInTriple(vector< pair < TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> >& tpnodes,
			TripleBitQueryGraph::TripleNodeID tripleID)
{
	size_t i;
	TripleBitQueryGraph::JoinVariableNode::DimType type = TripleBitQueryGraph::JoinVariableNode::SUBJECT;

	for( i = 0 ; i < tpnodes.size(); i++)
	{
		if ( tpnodes[i].first == tripleID)
		{
			type = tpnodes[i].second;
			break;
		}
	}
	if(type == TripleBitQueryGraph::JoinVariableNode::SUBJECT)
		return SUBJECT;
	else if(type == TripleBitQueryGraph::JoinVariableNode::PREDICATE)
		return PREDICATE;
	else
		return OBJECT;
}

Status TripleBitQuery::findEntitiesAndJoinFirstTime(vector< pair < TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> >& tpnodes,
			ID tripleID, map<ID,bool>& firstInsertFlag, TripleBitQueryGraph::JoinVariableNodeID id)
{
	// use merge join operator to to the joins.
	EntityIDBuffer* buffer = EntityIDList[tripleID];
#ifdef PRINT_BUFFERSIZE
	cout<<__FUNCTION__<<endl;
#endif

	EntityIDBuffer* temp;
	TripleBitQueryGraph::TripleNode * triple;
	int joinKey , joinKey2;
	bool insertFlag;
	size_t i;
	ID tripleNo;
	int varCount;

	ID maxID,minID;

	if( tpnodes.size() == 1 ) {
		return OK;
	}

	joinKey = this->getVariablePos(id,tripleID);
	buffer->sort(joinKey);

	for ( i = 0; i < tpnodes.size(); i++)
	{
		tripleNo = tpnodes[i].first;

		if (tripleNo != tripleID) {
			joinKey2 = this->getVariablePos(id, tripleNo);
			insertFlag = firstInsertFlag[tripleNo];
			getTripleNodeByID(triple, tripleNo);
			varCount = this->getVariableCount(triple);

			if (insertFlag == false && varCount == 1) {
				buffer->getMinMax(minID, maxID);
				temp = BufferManager::getInstance()->getNewBuffer();
				findEntityIDByTriple(triple, temp, minID, maxID);
#ifdef PRINT_BUFFERSIZE
				cout<<"case 1"<<endl;
				cout<<"pattern "<<tripleNo<<" temp buffer size: "<<temp->getSize()<<endl;
#endif
				mergeJoin.Join(buffer,temp,joinKey,1,false);
#ifdef PRINT_BUFFERSIZE
				cout<<"pattern "<<tripleID<<" buffer size: "<<buffer->getSize()<<endl;
#endif
				BufferManager::getInstance()->freeBuffer(temp);
			} else if (insertFlag == false && varCount == 2) {
				temp = EntityIDList[tripleNo];
				mergeJoin.Join(buffer,temp,joinKey,joinKey2,true);
#ifdef PRINT_BUFFERSIZE
				cout<<"case 2"<<endl;
				cout<<"pattern "<<tripleID<<" buffer size: "<<buffer->getSize()<<endl;
				cout<<"pattern "<<tripleNo<<" buffer size: "<<temp->getSize()<<endl;
#endif
			} else {
				buffer->getMinMax(minID, maxID);
				temp = EntityIDList[tripleNo];
				this->findEntityIDByTriple(triple, temp, minID, maxID);
#ifdef PRINT_BUFFERSIZE
				cout<<"case 3"<<endl;
				cout<<"pattern "<<tripleNo<<" buffer size: "<<temp->getSize()<<endl;
#endif
				//hashJoin.Join(buffer,temp,joinKey, joinKey2);
				mergeJoin.Join(buffer,temp,joinKey,joinKey2,true);
#ifdef PRINT_BUFFERSIZE
				cout<<"pattern "<<tripleNo<<" buffer size: "<<temp->getSize()<<endl;
				cout<<"pattern "<<tripleID<<" buffer size: "<<buffer->getSize()<<endl;
#endif
			}
		}

		if ( buffer->getSize() == 0)
			return NULL_RESULT;
	}

	if ( buffer->getSize() == 0 ) {
		return NULL_RESULT;
	}
	return OK;
}


int TripleBitQuery::getVariablePos(TripleBitQueryGraph::JoinVariableNodeID id,
		TripleBitQueryGraph::TripleNodeID tripleID)
{
	TripleBitQueryGraph::TripleNode * triple;
	getTripleNodeByID(triple,tripleID);
	return getVariablePos(id, triple);
}


Status TripleBitQuery::modifyEntitiesAndJoin(vector< pair < TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> >& tpnodes,
		ID tripleID, TripleBitQueryGraph::JoinVariableNodeID id)
{
	// use hash join operator to do the joins.
	EntityIDBuffer* buffer = EntityIDList[tripleID];
	EntityIDBuffer* temp;
	TripleBitQueryGraph::TripleNode * triple;
	int joinKey, joinKey2;
	size_t i;
	ID tripleNo;
	int varCount;
	bool sizeChanged = false;

#ifdef PRINT_BUFFERSIZE
	cout<<__FUNCTION__<<endl;
#endif
	joinKey = this->getVariablePos(id, tripleID);
	buffer->setSortKey(joinKey - 1);
	size_t size = buffer->getSize();
	
	for (i = 0; i < tpnodes.size(); i++) {
		tripleNo = tpnodes[i].first;
		this->getTripleNodeByID(triple, tripleNo);
		if (tripleNo != tripleID) {
			varCount = getVariableCount(triple);
			if ( varCount == 2) {
				joinKey2 = this->getVariablePos(id, tripleNo);
				temp = EntityIDList[tripleNo];
#ifdef PRINT_BUFFERSIZE
				cout<<"--------------------------------------"<<endl;
				cout<<"pattern "<<tripleNo<<" buffer size: "<<temp->getSize()<<endl;
				cout<<"pattern "<<tripleID<<" buffer size: "<<buffer->getSize()<<endl;
#endif
				hashJoin.Join(buffer,temp,joinKey, joinKey2);
#ifdef PRINT_BUFFERSIZE
				cout<<"pattern "<<tripleNo<<" buffer size: "<<temp->getSize()<<endl;
				cout<<"pattern "<<tripleID<<" buffer size: "<<buffer->getSize()<<endl;
#endif
			}
		}
		if( buffer->getSize() == 0 ) {
			return NULL_RESULT;
		}
	}

	if ( size != buffer->getSize() || sizeChanged == true )
		return BUFFER_MODIFIED;
	else
		return OK;
}

Status TripleBitQuery::getVariableNodeByID(TripleBitQueryGraph::JoinVariableNode*& node, TripleBitQueryGraph::JoinVariableNodeID id)
{
	int i, size = _query->joinVariableNodes.size();

	for( i = 0; i < size; i++)
	{
		if( _query->joinVariableNodes[i].value == id){
			node = &(_query->joinVariableNodes[i]);
			break;
		}
	}

	return OK;
}

int TripleBitQuery::getVariableCount(TripleBitQueryGraph::TripleNode* node)
{
	switch(node->scanOperation) {
	case TripleBitQueryGraph::TripleNode::FINDO:
		return 1;
	case TripleBitQueryGraph::TripleNode::FINDOBYP:
		return 1;
	case TripleBitQueryGraph::TripleNode::FINDOBYS:
		return 1;
	case TripleBitQueryGraph::TripleNode::FINDOBYSP:
		return 1;
	case TripleBitQueryGraph::TripleNode::FINDP:
		return 1;
	case TripleBitQueryGraph::TripleNode::FINDPBYO:
		return 1;
	case TripleBitQueryGraph::TripleNode::FINDPBYS:
		return 1;
	case TripleBitQueryGraph::TripleNode::FINDPBYSO:
		return 1;
	case TripleBitQueryGraph::TripleNode::FINDPOBYS:
		return 2;
	case TripleBitQueryGraph::TripleNode::FINDS:
		return 1;
	case TripleBitQueryGraph::TripleNode::FINDSBYO:
		return 1;
	case TripleBitQueryGraph::TripleNode::FINDSBYP:
		return 1;
	case TripleBitQueryGraph::TripleNode::FINDSBYPO:
		return 1;
	case TripleBitQueryGraph::TripleNode::FINDSOBYP:
	case TripleBitQueryGraph::TripleNode::FINDOSBYP:
	case TripleBitQueryGraph::TripleNode::FINDOPBYS:
	case TripleBitQueryGraph::TripleNode::FINDPSBYO:
		return 2;
	case TripleBitQueryGraph::TripleNode::FINDSPBYO:
		return 2;
	case TripleBitQueryGraph::TripleNode::NOOP:
		return -1;
	}

	return -1;
}

int TripleBitQuery::getVariableCount(TripleBitQueryGraph::TripleNodeID id)
{
	TripleBitQueryGraph::TripleNode* triple;
	getTripleNodeByID(triple,id);
	return getVariableCount(triple);
}
