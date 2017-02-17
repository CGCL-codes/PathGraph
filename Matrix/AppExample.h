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

#ifndef APPEXAMPLE_H_
#define APPEXAMPLE_H_

#include "TripleBit.h"
#include "Matrixmap.h"
#include "EntityIDBuffer.h"
#include "HashID.h"
#include "BuildAdjGraph.h"
#include "Scheduler.h"
#include "ThreadPool.h"
#include <vector>
#include <queue>

//#define TASK_ANALYSIS 1
#define RANDOMRESETPROB 0.15
#define SPECIAL_ROOT_SIZE 1024

typedef struct ValueID {
	float first;
	ID second;
} VV;

class AppExample;

typedef struct _return_of_task{
	AppExample *generator;
	size_t startChunk;
	size_t endChunk;
	unsigned index;
	double *result; //used by spmv
	unsigned itera; //used by pagerank
}return_value;

class AppExample {
public:
	AppExample(bool is_pagerank_not_spmv, bool is_bfs_not_wcc, bool scheduler_choice);
	virtual ~AppExample();

	static const unsigned maxRunChunk;

	Status init(const char* outDir, const char* outName, bool isMemory);
	Status initRoot(ID*& root, ID& rootNum);
	Status initID(vector<unsigned*>& idValue);

	//Status loadMatrix(int small,int big,bool isMemory);
	//Status clearMatrix(int small,int big);
	//Status quadraticMultiply(ID preid);
	//Status matrixMultiply(int small,int big,int preID);

	bool small_graph(){return matrixMap[1]->getBucketManager(0)->getUsedSize() <= 5 * BucketManager::pagesize;}

	Status pagerank(unsigned index, unsigned itera);
	void endComputePageRank();
	Status save_rank();
	void save_task(char *dst, char *src, size_t copysize);
	Status save(string path, char *data, size_t size);
	double sum_of_changes();
	void getMaxValue(unsigned num);
	static void getMaxValue(float* start, size_t size, unsigned num, VV*& runs,ID startID);

	Status spmv(int preID, double *result);
	Status bfs_forest(ID rootID, bool *&isVisited);
	Status wcc(unsigned rootID, bool *&isVisited, ID *&idValue, ID *&changes);
	Status wcc_apply_changes(ID *&idValue, ID *changes, size_t size, bool isParallel);
	void wcc_apply_changes_task(ID *&idValue, ID *changes, size_t begin, size_t end);

	//-----------------------work stealing application--------------------------------
	Status pagerank(unsigned itera);
	return_value* pagerank_task(size_t startNum, size_t endNum, unsigned index, unsigned itera);
	Status spmv(double *result);
	return_value* spmv_task(size_t startNum, size_t endNum, unsigned index, double* result);
	return_value* pagerank_or_spmv_task(size_t startNum, size_t endNum, unsigned index, unsigned itera, double* result);

	Status bfs_forest(unsigned max_index, ID *root_arr, bool *&isVisited);
	return_value* bfs_forest_task(unsigned begin, unsigned end, unsigned max_index);
	Status wcc(unsigned max_index, ID *root_arr, bool *&isVisited, ID *&idValue, ID *&changes);
	return_value* wcc_task(unsigned begin, unsigned end, unsigned max_index);
	return_value* bfs_forest_or_wcc_task(unsigned begin, unsigned end, unsigned max_index);
	//--------------------------------------------------------------------------------

	ID* getStartID(){ return start_ID; }
	unsigned getBucSize(){ return bucSize; }
	unsigned getMaxID(){ return (start_ID[bucSize]-1); }
	char* get_vertices_rank(){ return (char *)idValueIn; }

private:
	map<char,Matrixmap*> matrixMap;
	ID start_ID[100];
	unsigned bucSize;
	ID maxID;
	bool isMemory;
	bool is_pagerank_not_spmv;
	bool is_bfs_not_wcc;

	MMapBuffer* idValueMap;
	float* idValueOut;
	float* idValueIn;
	Degree* degree;
	MMapBuffer* degreeMap;

	ID *root_array;
	bool *isVisited;
	ID *idValue;
	ID *changes;

	string Dir;
	string Name;
	Scheduler *scheduler;
#ifdef TASK_ANALYSIS
	pthread_mutex_t print_mutex;
	map<unsigned long, unsigned long> thread_info;
#endif
};

struct TopValue{
	pthread_t m_thread;
	float* m_start;
	size_t m_size;
	unsigned m_num;
	VV*& m_runs;
	ID m_startID;
	TopValue(float* start,ID startID, size_t size, unsigned num,VV*& runs) :
		m_thread(-1), m_start(start), m_size(size), m_num(num),m_runs(runs),m_startID(startID){
		pthread_create(&m_thread, NULL, (void*(*)(void*))getTopTask, this);
	}
	~TopValue() {
		pthread_join(m_thread, NULL);
		m_thread = -1;
	}
	static inline void getTopTask(TopValue* args){
		AppExample::getMaxValue(args->m_start,args->m_size,args->m_num,args->m_runs,args->m_startID);
	}
};
#endif /* APPEXAMPLE_H_ */
