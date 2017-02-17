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

#include "Scheduler.h"
#include "AppExample.h"
#include <set>
#include <iostream>
#include <sys/wait.h>

//#define PROCESS_NUM 8
//#define TRY_TIMES (PROCESS_NUM / 4 + ((PROCESS_NUM % 4) ? 1 : 0))
#define TRY_TIMES (sysconf(_SC_NPROCESSORS_CONF) / 4 + ((sysconf(_SC_NPROCESSORS_CONF) % 4) ? 1 : 0))
#define MIN(a,b) ((a <= b) ? a : b)
#define INCREMENT_ROOT_VERTICES 256

Scheduler::Scheduler(bool scheduler_choice) {
	// TODO Auto-generated constructor stub
	//m_threadNum = PROCESS_NUM;
	m_threadNum = sysconf(_SC_NPROCESSORS_CONF);
	m_sleep_threadNum = 0;
	m_activate_threadNum = 0;
	lock_free = false;
	start_index = SPECIAL_ROOT_SIZE;
	pthread_mutex_init(&m_sleepMutex, NULL);
	pthread_cond_init(&m_sleepCond, NULL);
#ifdef SCHEDULER_ANALYSIS
	pthread_mutex_init(&m_printMutex, NULL);
	for(int i = 0;i < m_threadNum;i++)steal_times_set.push_back(0);
	steal_info = fopen("steal_info", "w");
#endif
	
	initThreadPoolAndWorkers(scheduler_choice);
}

void Scheduler::initThreadPoolAndWorkers(bool scheduler_choice){
	cpu_set_t cpuset;
	workers = (worker *)malloc(sizeof(worker) * m_threadNum);
	m_logged_worker = random() % m_threadNum; /* choose the first task generator */

	for (int i = 0; i < m_threadNum;i++) {
	    TaskQueue *taskQueue = new TaskQueue();
	    threadQueue.push_back(taskQueue);

	    CPU_ZERO(&cpuset);
	    CPU_SET(i, &cpuset);
	    workers[i].cpu = cpuset;
	    workers[i].id = i;
	    workers[i].logged_worker = m_logged_worker;
	    workers[i].victim_set = (int *)malloc(sizeof(int) * (m_threadNum - 1));
	    for(int j = 0;j < i;j++)workers[i].victim_set[j] = j;
	    for(int j = i+1;j < m_threadNum;j++)workers[i].victim_set[j-1]= j;
	    workers[i].victim_num = m_threadNum - 1;
	}

	m_hasTaskConds.resize(m_threadNum);
	m_hasTaskMutex.resize(m_threadNum);
	firstTime.resize(m_threadNum);
	complete.resize(m_threadNum);
	for(int i = 0;i < m_threadNum;i++){
		pthread_cond_init(&m_hasTaskConds[i], NULL);
		pthread_mutex_init(&m_hasTaskMutex[i], NULL);
		//firstTime[i] = true;
		//complete[i] = false;
		firstTime[i] = 1;
		complete[i] = 0;
	}
	
	for (int i = 0; i < m_threadNum;i++) {
		ThreadArg *arg = new ThreadArg;
		arg->sched = this;
		arg->thread_worker = &workers[i];
		threadArgs.push_back(arg);

	    pthread_t tid = 0;
	    if(scheduler_choice)pthread_create(&tid, NULL, parallel_push_pop_steal, arg);//for pagerank and spmv
	    else pthread_create(&tid, NULL, parallel_push_pop, arg);//for bfs_forest and wcc
	    tids.push_back(tid);
	}
}

Scheduler::~Scheduler() {
	// TODO Auto-generated destructor stub
	for(int i = 0;i < m_threadNum;i++){
		delete threadQueue[i];
		free(workers[i].victim_set);
		delete threadArgs[i];
		pthread_cond_destroy(&m_hasTaskConds[i]);
		pthread_mutex_destroy(&m_hasTaskMutex[i]);
	}
	threadQueue.clear();
	threadArgs.clear();
	m_hasTaskConds.clear();
	m_hasTaskMutex.clear();
	firstTime.clear();
	complete.clear();

	pthread_mutex_destroy(&m_sleepMutex);
	pthread_cond_destroy(&m_sleepCond);
#ifdef SCHEDULER_ANALYSIS
	pthread_mutex_destroy(&m_printMutex);
	steal_times_set.clear();
	fclose(steal_info);
#endif

	free(workers);
	tids.clear();
}

void Scheduler::firstAddTask(const Task& task){
	//threadQueue[m_logged_worker]->taskQueue_push_without_sync(task);
	threadQueue[m_logged_worker]->taskQueue_push_with_lock(task);
}

void Scheduler::add_task_by_main_thread(const Task& task, int workerId){
	int id = workerId % m_threadNum;
	threadQueue[id]->taskQueue_push_with_lock(task);
	pthread_cond_signal(&m_hasTaskConds[id]); /* try to activate a thread */
	//cout<<"activate"<<endl;
}

void Scheduler::add_task_by_local_thread_with_lock(const Task& task, int workerId){
	threadQueue[workerId]->taskQueue_push_with_lock(task);
}

void Scheduler::add_task_by_local_thread_without_lock(const Task& task, int workerId){
	threadQueue[workerId]->taskQueue_push_without_lock(task);
}

unsigned Scheduler::victim_select(vector<TaskQueue *> threadQueue){
	//select victim from the top2 task queues
	pair<unsigned, unsigned> max1, max2;
	(threadQueue[0]->_task_num >= threadQueue[1]->_task_num) ? \
			(max1.first = threadQueue[0]->_task_num, max1.second = 0, \
					max2.first = threadQueue[1]->_task_num, max2.second  = 1) : \
					(max1.first = threadQueue[1]->_task_num, max1.second = 1, \
							max2.first = threadQueue[0]->_task_num, max2.second = 0);

	size_t size = threadQueue.size();
	for(int i = 2;i < size;i++){
		(threadQueue[i]->_task_num > max1.first) ? (max2.first = max1.first, max2.second = max1.second, \
				max1.first = threadQueue[i]->_task_num, max1.second = i) : \
				((threadQueue[i]->_task_num > max2.first) ? \
						(max2.first = threadQueue[i]->_task_num, max2.second = i) : (1));
	}

	unsigned random_num = random() % 2;
	if(random_num)return max2.second;
	else return max1.second;
}

void* Scheduler::parallel_push_pop_steal(void * threadData){
	ThreadArg *arg = (ThreadArg *)threadData;
	Scheduler *scheduler = arg->sched;
	worker *data = arg->thread_worker;
	pthread_setaffinity_np(pthread_self(), sizeof(data->cpu), &data->cpu);
	Task task;
	bool hasTask = false;
	size_t victim = 0;
#ifdef SCHEDULER_ANALYSIS
	struct timeval start_time, end_time;
#endif
	
	while(1){
START:
		pthread_mutex_lock(&scheduler->m_hasTaskMutex[data->id]);
		while(!hasTask && scheduler->firstTime[data->id]){
			/* 'firstTime[data->id]' is used for hang up the thread when no task has added */
			hasTask = scheduler->threadQueue[data->id]->taskQueue_pop_with_lock(task);
			/*if(!hasTask){
				victim =  random() % data->victim_num;
				hasTask = scheduler->threadQueue[data->victim_set[victim]]->taskQueue_steal_one(task);
			}*/
			if(!hasTask)pthread_cond_wait(&scheduler->m_hasTaskConds[data->id], &scheduler->m_hasTaskMutex[data->id]);
		}
		pthread_mutex_unlock(&scheduler->m_hasTaskMutex[data->id]);
		
		if(scheduler->firstTime[data->id]){
			/* when a thread has got a task, means that it has been activated */
			//scheduler->firstTime[data->id] = false;
			//scheduler->complete[data->id] = false;
			scheduler->firstTime[data->id] = 0;
			scheduler->complete[data->id] = 0;
			/* the thread has been activated */
			__sync_fetch_and_add(&scheduler->m_activate_threadNum, 1);
			assert(scheduler->m_activate_threadNum <= scheduler->m_threadNum);
		}

#ifdef SELECT_SCHEDULE_OPTION
		vector<size_t> empty_victim;
#else
		set<size_t> empty_victim;
#endif
		empty_victim.clear();
		while(!hasTask && empty_victim.size() < TRY_TIMES){
			/* first pop a task from the local taskQueue */
			if(__builtin_expect(scheduler->lock_free == true, 1)){
				hasTask = scheduler->threadQueue[data->id]->taskQueue_pop_without_lock(task);
			}else{
				hasTask = scheduler->threadQueue[data->id]->taskQueue_pop_with_lock(task);
			}
			
			if(__builtin_expect(hasTask == true, 1)){
				break;
			}
			else{
				/* local taskQueue is empty, must steal from the victim_set */
				/* we assume that TRY_TIMES empty victims indicate all victim_set are empty, empiricist */
#ifdef SELECT_SCHEDULE_OPTION
				victim = victim_select(scheduler->threadQueue);
#else
				victim = random() % data->victim_num;
#endif
#ifdef SCHEDULER_ANALYSIS
				gettimeofday(&start_time, NULL);
#endif			
#ifdef SELECT_SCHEDULE_OPTION
				//hasTask = scheduler->threadQueue[victim]->taskQueue_steal_one(task);
				hasTask = scheduler->threadQueue[victim]->taskQueue_steal_batch(task,
					scheduler->threadQueue[data->id],scheduler->lock_free);
#else
				//hasTask = scheduler->threadQueue[data->victim_set[victim]]->taskQueue_steal_one(task);
				hasTask = scheduler->threadQueue[data->victim_set[victim]]->taskQueue_steal_batch(task, 
					scheduler->threadQueue[data->id],scheduler->lock_free);
#endif
#ifdef SCHEDULER_ANALYSIS
				gettimeofday(&end_time, NULL);
				pthread_mutex_lock(&scheduler->m_printMutex);
				fprintf(scheduler->steal_info, "%lu %d %d\n", pthread_self(), (end_time.tv_sec - start_time.tv_sec) * 1000000 \
					+ (end_time.tv_usec - start_time.tv_usec), hasTask);
				pthread_mutex_unlock(&scheduler->m_printMutex);
				scheduler->steal_times_set[data->id]++;
#endif
#ifdef SELECT_SCHEDULE_OPTION
				empty_victim.push_back(victim);
#else
				empty_victim.insert(victim);
#endif
			}
		}
		
		if(!hasTask){
			/* hang up the thread */
			__sync_fetch_and_add(&scheduler->m_sleep_threadNum, 1);
			assert(scheduler->m_sleep_threadNum <= scheduler->m_activate_threadNum);
			pthread_cond_signal(&scheduler->m_sleepCond); /* ask 'waitForComplete()' to check the finish condition */
			
			pthread_mutex_lock(&scheduler->m_hasTaskMutex[data->id]);
			pthread_cond_wait(&scheduler->m_hasTaskConds[data->id], &scheduler->m_hasTaskMutex[data->id]);
			pthread_mutex_unlock(&scheduler->m_hasTaskMutex[data->id]);
			
			if(!scheduler->complete[data->id]){
				/* 'scheduler->complete[data->id] == false' means a old round, or a new round has begin */
				__sync_fetch_and_sub(&scheduler->m_sleep_threadNum, 1);
				assert(scheduler->m_sleep_threadNum >= 0);
			}else{
				//scheduler->complete[data->id] = false;
				scheduler->complete[data->id] = 0;
			}
			
			goto START;
		}
		
		/* execute the task */
		return_value *value = task();
		hasTask = false;

		/* add sub-tasks here */
		AppExample *generator = value->generator;
		unsigned startNum = value->startChunk;
		unsigned endNum = value->endChunk;

		if (startNum < endNum) {
			if(__builtin_expect(scheduler->lock_free == true, 1)){
				if (startNum + AppExample::maxRunChunk < endNum) {
					scheduler->add_task_by_local_thread_without_lock(boost::bind(&AppExample::pagerank_or_spmv_task, generator,
							startNum, startNum + AppExample::maxRunChunk, value->index, value->itera, value->result), data->id);
					scheduler->add_task_by_local_thread_without_lock(boost::bind(&AppExample::pagerank_or_spmv_task, generator,
							startNum + AppExample::maxRunChunk, endNum, value->index, value->itera, value->result), data->id);
				} else {
					scheduler->add_task_by_local_thread_without_lock(boost::bind(&AppExample::pagerank_or_spmv_task, generator,
							startNum, endNum, value->index, value->itera, value->result), data->id);
				}
			}else{
				if (startNum + AppExample::maxRunChunk < endNum) {
					scheduler->add_task_by_local_thread_with_lock(boost::bind(&AppExample::pagerank_or_spmv_task, generator,
							startNum, startNum + AppExample::maxRunChunk, value->index, value->itera, value->result), data->id);
					scheduler->add_task_by_local_thread_with_lock(boost::bind(&AppExample::pagerank_or_spmv_task, generator,
							startNum + AppExample::maxRunChunk, endNum, value->index, value->itera, value->result), data->id);
				} else {
					scheduler->add_task_by_local_thread_with_lock(boost::bind(&AppExample::pagerank_or_spmv_task, generator,
							startNum, endNum, value->index, value->itera, value->result), data->id);
				}
			}
		}

		/*if (startNum < endNum) {
			size_t task_num = (endNum - startNum) / AppExample::maxRunChunk;
			for(size_t i = 0;i < task_num;i++){
				if(__builtin_expect(scheduler->lock_free == true, 1)){
					scheduler->add_task_by_local_thread_without_lock(boost::bind(&AppExample::pagerank_or_spmv_task, generator,
							startNum, startNum + AppExample::maxRunChunk, value->index, value->itera, value->result), data->id);
				}else{
					scheduler->add_task_by_local_thread_with_lock(boost::bind(&AppExample::pagerank_or_spmv_task, generator,
							startNum, startNum + AppExample::maxRunChunk, value->index, value->itera, value->result), data->id);
				}
				startNum += AppExample::maxRunChunk;
			}

			if(startNum < endNum){
				if(__builtin_expect(scheduler->lock_free == true, 1)){
					scheduler->add_task_by_local_thread_without_lock(boost::bind(&AppExample::pagerank_or_spmv_task, generator,
							startNum, endNum, value->index, value->itera, value->result), data->id);
				}else{
					scheduler->add_task_by_local_thread_with_lock(boost::bind(&AppExample::pagerank_or_spmv_task, generator,
							startNum, endNum, value->index, value->itera, value->result), data->id);
				}
			}
		}*/

		delete value;
	}

	pthread_exit(NULL);
	return (void *)0;
}

void* Scheduler::parallel_push_pop(void * threadData){
	ThreadArg *arg = (ThreadArg *)threadData;
	Scheduler *scheduler = arg->sched;
	worker *data = arg->thread_worker;
	pthread_setaffinity_np(pthread_self(), sizeof(data->cpu), &data->cpu);
	Task task;
	bool hasTask = false;

	while(1){
START:
		pthread_mutex_lock(&scheduler->m_hasTaskMutex[data->id]);
		while(!hasTask && scheduler->firstTime[data->id]){
			/* 'firstTime[data->id]' is used for hang up the thread when no task has added */
			hasTask = scheduler->threadQueue[data->id]->taskQueue_pop_with_lock(task);
			if(!hasTask)pthread_cond_wait(&scheduler->m_hasTaskConds[data->id], &scheduler->m_hasTaskMutex[data->id]);
		}
		pthread_mutex_unlock(&scheduler->m_hasTaskMutex[data->id]);

		if(scheduler->firstTime[data->id]){
			/* when a thread has got a task, means that it has been activated */
			scheduler->firstTime[data->id] = 0;
			scheduler->complete[data->id] = 0;
			__sync_fetch_and_add(&scheduler->m_activate_threadNum, 1);
			assert(scheduler->m_activate_threadNum <= scheduler->m_threadNum);
		}

		if(!hasTask){
			/* pop a task from the local taskQueue */
			if(__builtin_expect(scheduler->lock_free == true, 1)){
				hasTask = scheduler->threadQueue[data->id]->taskQueue_pop_without_lock(task);
			}else{
				hasTask = scheduler->threadQueue[data->id]->taskQueue_pop_with_lock(task);
			}
		}

		if(!hasTask){
			/* hang up the thread */
			__sync_fetch_and_add(&scheduler->m_sleep_threadNum, 1);
			assert(scheduler->m_sleep_threadNum <= scheduler->m_activate_threadNum);
			pthread_cond_signal(&scheduler->m_sleepCond); /* ask 'waitForComplete()' to check the finish condition */

			pthread_mutex_lock(&scheduler->m_hasTaskMutex[data->id]);
			pthread_cond_wait(&scheduler->m_hasTaskConds[data->id], &scheduler->m_hasTaskMutex[data->id]);
			pthread_mutex_unlock(&scheduler->m_hasTaskMutex[data->id]);

			if(!scheduler->complete[data->id]){
				/* 'scheduler->complete[data->id] == 0' means a old round, or a new round has begin */
				__sync_fetch_and_sub(&scheduler->m_sleep_threadNum, 1);
				assert(scheduler->m_sleep_threadNum >= 0);
			}else{
				scheduler->complete[data->id] = 0;
			}

			goto START;
		}

		/* execute the task */
		return_value *value = task();
		hasTask = false;

		/* add sub-tasks here */
		if(scheduler->start_index < value->index){
			unsigned begin = __sync_fetch_and_add(&scheduler->start_index, INCREMENT_ROOT_VERTICES);
			unsigned end = std::min(begin+INCREMENT_ROOT_VERTICES, value->index);
			if(begin < value->index){
				if(__builtin_expect(scheduler->lock_free == true, 1)){
					scheduler->add_task_by_local_thread_without_lock(boost::bind(&AppExample::bfs_forest_or_wcc_task,
							value->generator, begin, end, value->index), data->id);
				}else{
					scheduler->add_task_by_local_thread_with_lock(boost::bind(&AppExample::bfs_forest_or_wcc_task,
							value->generator, begin, end, value->index), data->id);
				}
			}
		}

		delete value;
	}

	pthread_exit(NULL);
	return (void *)0;
}

void Scheduler::stop(){
	for(vector<pthread_t>::iterator iter = tids.begin(), limit = tids.end();iter != limit;iter++){
		pthread_join(*iter, NULL);
	}
}

void Scheduler::waitForComplete(){
	/*for(int i = 0;i < m_threadNum;i++){
		while(!threadQueue[i]->empty())usleep(10000);
	}*/
	
	//cout<<"sleep_threadNum = "<<m_sleep_threadNum<<", activate_threadNum = "<<m_activate_threadNum<<endl;
	/* when all activate threads go to sleep, means finish a round */
	pthread_mutex_lock(&m_sleepMutex);
	while(m_sleep_threadNum < m_activate_threadNum){
		pthread_cond_wait(&m_sleepCond,&m_sleepMutex);
		//cout<<"sleep_threadNum = "<<m_sleep_threadNum<<endl;
		__sync_synchronize();
	}
	pthread_mutex_unlock(&m_sleepMutex);
#ifdef SCHEDULER_ANALYSIS
	cout<<"m_sleep_threadNum = "<<m_sleep_threadNum<<endl;
	cout<<"m_activate_threadNum = "<<m_activate_threadNum<<endl;
	for(int i = 0;i < m_threadNum;i++){
		cout<<tids[i]<<" steal "<<steal_times_set[i]<<" times"<<endl;
		steal_times_set[i] = 0;
	}
#endif
	usleep(100000);//leave some time for the last thread to hang up
	for(int i = 0;i < m_threadNum;i++){
		//firstTime[i] = true;
		//complete[i] = true;
		firstTime[i] = 1;
		complete[i] = 1;
		threadQueue[i]->reset();
	}
	
	m_sleep_threadNum = 0;
	m_activate_threadNum = 0;
	__sync_synchronize();
	lock_free = false;
	start_index = SPECIAL_ROOT_SIZE;
}
