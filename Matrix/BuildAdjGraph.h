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

#ifndef BUILDADJGRAPH_H_
#define BUILDADJGRAPH_H_

#include "Matrixmap.h"

class BuildAdjGraph {
	Matrixmap* matrixMap;
	ID start_ID[100];
	unsigned bucSize;
	ID maxID;
	string Dir;
	string Name;

public:
	static int flag;
	static const unsigned maxBucketSize;
    //static const unsigned maxThreadSize;

	BuildAdjGraph();
	virtual ~BuildAdjGraph();
	unsigned convertToAdj(const char* inFile, const char* outDir,const char* outName, int flag);
	static void insertTask(const ID* begin, const ID* limit, BucketManager* buc);
};

struct InsertTaskWrapperArgs {
	pthread_t m_thread;
	const ID* m_begin;
	const ID* m_limit;
	BucketManager* m_buc;
	InsertTaskWrapperArgs(const ID* begin, const ID* limit, BucketManager* buc) :
		m_thread(-1), m_begin(begin), m_limit(limit), m_buc(buc) {
		pthread_create(&m_thread, NULL, (void*(*)(void*)) InsertTask, this);
	}

	~InsertTaskWrapperArgs() {
		pthread_join(m_thread, NULL);
		m_thread = -1;
	}

	static inline void InsertTask(InsertTaskWrapperArgs* args) {
		BuildAdjGraph::insertTask(args->m_begin, args->m_limit, args->m_buc);
	}
};
#endif /* BUILDADJGRAPH_H_ */
