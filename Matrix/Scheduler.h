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

#ifndef SCHEDULER_H_
#define SCHEDULER_H_

#include "TaskQueue.h"
#include <vector>
#include <boost/bind.hpp>

//#define TASK_SIZE 2000       /* simulate the task granularity, just for test */
//#define CHILDREN_SIZE 20     /* simulate the children number, just for test */
//#define MAX_TREE_LEVEL 5     /* used for terminate the sub-tasks generator, just for test */
//#define SCHEDULER_ANALYSIS 1

using namespace std;
typedef boost::function<struct _return_of_task* ()> Task;

class TaskQueue;
class Scheduler;

typedef struct _worker {
	int id;
	cpu_set_t cpu;
	int logged_worker;
	int *victim_set;  //exclude the local task queue itself
	int victim_num;
} worker;

typedef struct _thread_arguments{
	Scheduler *sched;
	worker *thread_worker;
} ThreadArg;

class Scheduler {
public:
	Scheduler(bool scheduler_choice);
	virtual ~Scheduler();
	//static Scheduler* getInstance();
	//static void destroyInstance();
	void firstAddTask(const Task& task);
	void add_task_by_local_thread_with_lock(const Task& task, int workerId);
	void add_task_by_local_thread_without_lock(const Task& task, int workerId);
	void add_task_by_main_thread(const Task& task, int workerId);
	void set_lock_free(){lock_free = true;}
	void waitForComplete();  //wait for the tasks to be finished
private:
	vector<TaskQueue *> threadQueue;  //task info
	worker *workers;  //thread info
	int m_threadNum;
	volatile unsigned long m_sleep_threadNum;  //cache_line_size = 64, so use 'unsigned long'
	volatile unsigned long m_activate_threadNum;  //use to check the finish signal(m_sleep_threadNum == m_activate_threadNum) of a round
	//bool set_activate_thread_num;  //set 'm_activate_threadNum'
	bool lock_free;
	//vector<bool> firstTime;  //use to check a new round or still the old round
	//vector<bool> complete;
	vector<unsigned long> firstTime; //use 'unsigned long' instead of 'bool', for cache read/write unit is 8bytes
	vector<unsigned long> complete;
	volatile unsigned long start_index;//used in control of bfs_forest and wcc algorithm
	int m_logged_worker; //first task generator
	vector<pthread_t> tids;
	vector<ThreadArg *> threadArgs;
	vector<pthread_cond_t> m_hasTaskConds;  //use to activate(or hang up) the thread, one for one
	vector<pthread_mutex_t> m_hasTaskMutex; //supporting the used of m_hasTaskConds
	pthread_cond_t m_sleepCond;  //use to check the finish signal of a round
	pthread_mutex_t m_sleepMutex;  //supporting the use of m_sleepCond
#ifdef SCHEDULER_ANALYSIS
	pthread_mutex_t m_printMutex;
	vector<unsigned long> steal_times_set;
	FILE *steal_info;
#endif
	//static Scheduler* instance;
	static void* parallel_push_pop_steal(void * threadData);// for pagerank and spmv
	static void* parallel_push_pop(void * threadData);// for bfs_forest and wcc
	void initThreadPoolAndWorkers(bool scheduler_choice);
	void stop();  //terminate the threads
	static unsigned victim_select(vector<TaskQueue *> threadQueue);
};

#endif /* SCHEDULER_H_ */
