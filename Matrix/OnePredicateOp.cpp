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

#include "OnePredicateOp.h"
#include "MMapBuffer.h"
#include "OSFile.h"
#include "HashID.h"

OnePredicateOp::OnePredicateOp() {
	qlock.init();
	clock.init();
}

OnePredicateOp::~OnePredicateOp() {
	// TODO Auto-generated destructor stub
	map<char, Matrixmap*>::iterator iterm = matrixMap.begin();
	for (; iterm != matrixMap.end(); iterm++) {
		delete iterm->second;
		iterm->second = NULL;
	}
	matrixMap.clear();
}

Status OnePredicateOp::init(const char* outDir, const char* outName,
		bool isMemory, size_t maxMemory) {
	this->Dir = string(outDir);
	this->Name = string(outName);
	this->isMemory = isMemory;
	unsigned leftMemory = maxMemory;
	if (matrixMap[1] == NULL) {
		matrixMap[1] = Matrixmap::load(outDir, outName, isMemory);
	}
	cout<<"load matrix"<<endl;

	unsigned size = 0;
	const char* startIDPath = (Dir + "/" + outName + "/startID.0").c_str();
	if (OSFile::FileExists(startIDPath)) {
		char* buf = NULL;
		size = HashID::loadFileinMemory(startIDPath, buf);
		memcpy(start_ID, buf, size);
		free(buf);
	} else {
		cerr << "startID file not exit!" << startIDPath << endl;
		assert(false);
	}

	size = size / sizeof(ID) - 1;
	bucSize = size;
	maxID = start_ID[size] - 1;

	cout << "load complete!" << endl;
	return OK;
}

Status OnePredicateOp::loadMatrix(int small, int big, bool isMemory) {
	char outName[100];
	sprintf(outName, "Matrix%d", small);
	if (matrixMap[small] == NULL) {
		matrixMap[small] = Matrixmap::load(Dir.c_str(), outName, true);
	}
	sprintf(outName, "Matrix%d", big);
	if (big != small) {
		if (matrixMap[big] == NULL) {
			matrixMap[big] = Matrixmap::load(Dir.c_str(), outName, false);
		}
	}

	return OK;
}

Status OnePredicateOp::clearMatrix(int small, int big) {
	if (matrixMap[small] != NULL) {
		delete matrixMap[small];
		matrixMap[small] = NULL;
	}
	if (matrixMap[big] != NULL) {
		delete matrixMap[big];
		matrixMap[big] = NULL;
	}

	return OK;
}

Status OnePredicateOp::quadraticMultiply(ID preID) {
	cout << "quadraticMultiply preID:" << preID << "  begin" << endl;
	qlock.lock();
	if (!matrixMap.count(2)) {
		matrixMap[2] = new Matrixmap(Dir.c_str(), (char *) "Matrix2", preList);
		matrixMap[2]->initStartID(start_ID, bucSize + 1);
		//copy startID
		TempFile* temp = new TempFile(Dir + "Matrix2/startID", 0);
		temp->write((bucSize + 1) * sizeof(ID), (const char*) start_ID);
		delete temp;
	}
	CycleCache* cache = new CycleCache();
	qlock.unLock();

	const uchar *reader, *limit, *base;
	ID keyValue = 0, x = 0, y = 0;

	BucketManager* bucManager1 = matrixMap[1]->getBucketManager(preID);
	BucketManager* bucManager2 = matrixMap[2]->getBucketManager(preID);

	reader = bucManager1->getStartPtr();
	limit = bucManager1->getEndPtr();
	base = reader - sizeof(BucketManagerMeta);

	EntityIDBuffer* ent = new EntityIDBuffer();
	ent->setIDCount(1);
	EntityIDBuffer* entbuffer = new EntityIDBuffer();
	entbuffer->setIDCount(1);
	EntityIDBuffer* tempbuffer = new EntityIDBuffer();
	tempbuffer->setIDCount(1);
	EntityIDBuffer* finalbuffer = new EntityIDBuffer();
	finalbuffer->setIDCount(1);
	ID* p;
	int size = 0, entsize = 0;
	int len, idoff, edgeoff, lastedgeoff;
	while (reader < limit) {
		BucketManager::getTwo(reader, size);
		if (size == 0) {
			idoff = 2;
			x = *(ID*) (reader + idoff);
			idoff += sizeof(ID);
			len = *(unsigned*) (reader + idoff);
			idoff += 4;
			if (len != 0)
				BucketManager::readBody(reader + idoff, len, ent);
			//insert
			entsize = ent->getSize();
			p = ent->getBuffer();
			for (int j = 0; j < entsize; j++) {
				matrixMap[1]->getAllY(p[j], entbuffer, cache);
				if (entbuffer->getSize() == 0)continue;
				EntityIDBuffer::mergeSingleBuffer(tempbuffer, entbuffer, finalbuffer);
				EntityIDBuffer::swapBuffer(finalbuffer, tempbuffer);
			}

			if (finalbuffer->getSize() > 0) {
				bucManager2->insertNewXY(x, finalbuffer);
				finalbuffer->empty();
			}

			base += (size_t) ceil((double) (len + 10) / BucketManager::pagesize) * BucketManager::pagesize;
		} else {
			idoff = 2;
			lastedgeoff = 2 + size * (2 + sizeof(ID));
			for (int i = 0; i < size; i++) {
				x = *(ID*) (reader + idoff);
				if (x == 0)
					break;
				idoff += sizeof(ID);
				BucketManager::getTwo(reader + idoff, edgeoff);
				idoff += 2;
				if (edgeoff + 1 <= lastedgeoff) {
					cout << edgeoff + 1 << "  " << lastedgeoff << endl;
					exit(-1);
				}

				BucketManager::readBody(reader + lastedgeoff, edgeoff + 1 - lastedgeoff, ent);
				lastedgeoff = edgeoff + 1;
				entsize = ent->getSize();
				p = ent->getBuffer();
				for (int j = 0; j < entsize; j++) {
					matrixMap[1]->getAllY(p[j], entbuffer, cache);
					if (entbuffer->getSize() == 0)
						continue;
					EntityIDBuffer::mergeSingleBuffer(tempbuffer, entbuffer, finalbuffer);
					EntityIDBuffer::swapBuffer(finalbuffer, tempbuffer);
				}

				if (finalbuffer->getSize() > 0) {
					bucManager2->insertNewXY(x, finalbuffer);
					finalbuffer->empty();
				}
			}
			base += BucketManager::pagesize;
		}
		reader = base;
	}
	bucManager2->endNewInsert();
	delete bucManager2;
	matrixMap[2]->predicate_manager[preID] = NULL;

	delete cache;
	delete entbuffer;
	delete finalbuffer;
	delete tempbuffer;
	delete ent;

	cout << "end quadraticMultiply  " << preID << endl;
	return OK;
}

Status OnePredicateOp::matrixMultiply(int big, int small, int preID) {
	// offen matrix1 is bigger than matrix2
	cout << "matrixMultiply preID:" << preID << endl;
	if (big < small) {
		big ^= small;
		small ^= big;
		big ^= small;
	}
	int now = big + small;
	char name[100];
	sprintf(name, "Matrix%d", now);
	qlock.lock();
	BucketManager* bucManager1,* bucManager2;
	if (!matrixMap.count(now)) {
		matrixMap[now] = new Matrixmap(Dir.c_str(), name, preList);
		matrixMap[now]->initStartID(start_ID, bucSize + 1);
		//copy startID
		string startName = Dir + "/" + name + "/" + "startID";
		TempFile* temp = new TempFile(startName, 0);
		temp->write((bucSize + 1) * sizeof(ID), (const char*) start_ID);
		delete temp;
	}
	CycleCache* cache = new CycleCache();
	bucManager2 = matrixMap[now]->getBucketManager(preID);
	bucManager1 = matrixMap[big]->getBucketManager(preID);
	qlock.unLock();
	const uchar *reader, *limit, *base;
	ID keyValue = 0, x = 0, y = 0;

	reader = bucManager1->getStartPtr();
	limit = bucManager1->getEndPtr();
	base = reader - sizeof(BucketManagerMeta);

	EntityIDBuffer* ent = new EntityIDBuffer();
	ent->setIDCount(1);
	EntityIDBuffer* entbuffer = new EntityIDBuffer();
	entbuffer->setIDCount(1);
	EntityIDBuffer* tempbuffer = new EntityIDBuffer();
	tempbuffer->setIDCount(1);
	EntityIDBuffer* finalbuffer = new EntityIDBuffer();
	finalbuffer->setIDCount(1);

	ID* p;
	int size = 0, entsize = 0;
	int len, idoff, edgeoff, lastedgeoff;
	while (reader < limit) {
		BucketManager::getTwo(reader, size);
		if (size == 0) {
			idoff = 2;
			x = *(ID*) (reader + idoff);
			if(x == 0) break;
			idoff += sizeof(ID);
			len = *(unsigned*) (reader + idoff);
			idoff += 4;
			if (len != 0)
				BucketManager::readBody(reader + idoff, len, ent);
			//insert
			entsize = ent->getSize();
			p = ent->getBuffer();
			size_t test = 0;
			for (int j = entsize; j >=0; j--) {
//				if(j%10000 == 0)cout <<"run:" << (double)(entsize-j)*100/entsize<<"%  size:" << test<<endl;
				test += entbuffer->getSize();
				matrixMap[small]->getAllY(p[j], entbuffer);
			
				if (entbuffer->getSize() == 0)
					continue;

				EntityIDBuffer::mergeSingleBuffer(tempbuffer, entbuffer, finalbuffer);
				EntityIDBuffer::swapBuffer(finalbuffer, tempbuffer);
			}

			if (finalbuffer->getSize() > 0) {
				bucManager2->insertNewXY(x, finalbuffer);
				finalbuffer->empty();
			}
			base += (size_t) ceil((double) (len + 10) / BucketManager::pagesize) * BucketManager::pagesize;
		} else {
			idoff = 2;
			lastedgeoff = 2 + size * (2 + sizeof(ID));
			for (int i = 0; i < size; i++) {
				x = *(ID*) (reader + idoff);
				if (x == 0)
					break;
				idoff += sizeof(ID);
				BucketManager::getTwo(reader + idoff, edgeoff);
				idoff += 2;
				if (edgeoff + 1 <= lastedgeoff) {
					cout << edgeoff + 1 << "  " << lastedgeoff << endl;
					exit(-1);
				}

				BucketManager::readBody(reader + lastedgeoff, edgeoff + 1 - lastedgeoff, ent);
				lastedgeoff = edgeoff + 1;
				entsize = ent->getSize();
				p = ent->getBuffer();
				for (int j = entsize-1; j >=0; j--) {
					matrixMap[small]->getAllY(p[j], entbuffer, cache);
					if (entbuffer->getSize() == 0)
						continue;
					EntityIDBuffer::mergeSingleBuffer(tempbuffer, entbuffer, finalbuffer);
					EntityIDBuffer::swapBuffer(finalbuffer, tempbuffer);

				}
				if (finalbuffer->getSize() > 0) {
					bucManager2->insertNewXY(x, finalbuffer);
					finalbuffer->empty();
				}
			}
			base += BucketManager::pagesize;
		}
		reader = base;
	}
	bucManager2->endNewInsert();
	delete bucManager2;
	matrixMap[now]->predicate_manager[preID] = NULL;

	delete cache;
	delete entbuffer;
	delete finalbuffer;
	delete tempbuffer;
	delete ent;

	cout << "end quadraticMultiply  " << preID << endl;
	return OK;
}

Status OnePredicateOp::initRoot(ID*& root, unsigned& rootNum) {
	const char* rootPath = (Dir + "/new_root.forward.0").c_str();
	size_t size;
	if (OSFile::FileExists(rootPath)) {
		char* buf = NULL;
		size = HashID::loadFileinMemory(rootPath, buf);
		size = size/sizeof(ID);
		root = (ID*)buf;
	} else {
		cerr << "root file not exit!" << rootPath << endl;
		assert(false);
	}
	return OK;
}

Status OnePredicateOp::initID(vector<unsigned*>& idValue){
	if (isMemory && idValue.size() == 0) {
		for (int i = 0; i < bucSize; i++) {
			idValue.push_back((unsigned*) calloc((start_ID[i + 1] - start_ID[i]),sizeof(unsigned)));
		}
	}
	return OK;
}

ID OnePredicateOp::getNewID(ID id){
	MemoryMappedFile temp;
	string path = Dir + "/old_mapto_new.forward.0";
	if(!OSFile::FileExists(path.c_str()))path = Dir + "/old_mapto_new.0";
	temp.open(path.c_str());
	ID* start = (ID*)temp.getBegin();
	ID* end = (ID*)temp.getEnd();
	ID count = 0;
	for(; start != end; start++){
		count++;
		if(id == *(start))
			break;
	}
	return count;
}

Status OnePredicateOp::bfs(ID id, char* ispush) {
	deque<ID>* que = new deque<ID>();
	ID* p;
	int entsize = 0;
	unsigned long findnodes = 0, findedges = 0;
	EntityIDBuffer* ent = new EntityIDBuffer();
	cout <<"old ID:" << id<< endl;
	id = getNewID(id);
	cout <<"new ID:" << id << endl;

	if (ispush[id] == 0) {
		que->push_back(id);
		ispush[id] = 255;

		while (!que->empty()) {
			id = que->front();
			que->pop_front();
			//visit a node
			findnodes++;
			matrixMap[1]->getAllY(id, ent);
			entsize = ent->getSize();
			p = ent->getBuffer();
			for (int i = 0; i < entsize; i++) {
				if (ispush[p[i]] == 0) {
					//visit a edge
					findedges++;
					que->push_back(p[i]);
					ispush[p[i]] = 255;
				}
			}
		}
	}

	delete ent;
	delete que;
	if(findnodes || findedges)cout <<"find nodes:" << findnodes <<"   find edges:" << findedges << endl;
	return OK;
}

Status OnePredicateOp::bfs_forest(int preID, char* ispush,unsigned& treeNum) {
	deque<ID>* que = new deque<ID>();
	ID* p;
	int entsize = 0;
	unsigned long findnodes = 0, findedges = 0;
	EntityIDBuffer* ent = new EntityIDBuffer();
	
	for (ID id = start_ID[preID]; id < start_ID[preID + 1]; id++) {
		if (ispush[id] == 0) {
			treeNum++;
			que->push_back(id);
			ispush[id] = 255;

			while (!que->empty()) {
				id = que->front();
				que->pop_front();
				//visit a node
				findnodes++;
				matrixMap[1]->getAllY(id, ent);
				entsize = ent->getSize();
				p = ent->getBuffer();
				for (int i = 0; i < entsize; i++) {
					if (ispush[p[i]] == 0) {
						//visit a edge
						findedges++;
						que->push_back(p[i]);
						ispush[p[i]] = 255;
					}
				}
			}
		}
	}

	delete ent;
	delete que;
	if(findnodes || findedges)cout <<"find nodes:" << findnodes <<", find edges:" << findedges << endl;
	return OK;
}
