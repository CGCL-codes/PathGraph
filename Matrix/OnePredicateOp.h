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

#ifndef MATRIXREPOSITORY_H_
#define MATRIXREPOSITORY_H_

#include "TripleBit.h"
#include "StatisticsBuffer.h"
#include "BitmapBuffer.h"
#include "Matrixmap.h"
#include "EntityIDBuffer.h"
#include "CycleCache.h"
#include "MutexLock.h"
#include <vector>
#include <queue>

class vec{
	ID id;
	float value;
};

class OnePredicateOp {
	map<char,Matrixmap*> matrixMap;
	vector<ID> preList;

	ID start_ID[100];
	unsigned bucSize;
	ID maxID;
	vector<MMapBuffer*> idValueMap;
	vector<float*> idValue;
	bool isMemory;
	MMapBuffer* degreeMap;

	string Dir;
	string Name;
    MutexLock clock;
    MutexLock qlock;

public:
	OnePredicateOp();
	virtual ~OnePredicateOp();
	Status init(const char* outDir, const char* outName, bool isMemory,
			size_t maxMemory);
	Status loadMatrix(int small,int big,bool isMemory);
	Status clearMatrix(int small,int big);
	Status quadraticMultiply(ID preid);
	Status matrixMultiply(int small,int big,int preID);

	ID getNewID(ID id);
	Status bfs(ID id, char* ispush);
	Status bfs_forest(int preID,char* ispush,unsigned & test);
	Status initRoot(ID*& root, ID& rootNum);
	Status initID(vector<unsigned*>& idValue);

	ID* getStartID(){
		return start_ID;
	}
	unsigned getBucSize(){
		return bucSize;
	}
	unsigned getMaxID(){
		return (start_ID[bucSize]-1);
	}
};
#endif /* MATRIXREPOSITORY_H_ */
