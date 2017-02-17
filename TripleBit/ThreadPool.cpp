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

#include "ThreadPool.h"

#include <string>
#include <iostream>

using namespace std;

CThreadPool* CThreadPool::instance = NULL;

CThreadPool::CThreadPool(int threadNum) {
	this->m_iThreadNum = threadNum;

	threadMutex.init();
	threadIdleMutex.init();
	threadBusyMutex.init();

	shutdown = false;
	Create();
}

CThreadPool::~CThreadPool()
{
	this->StopAll();
}

int CThreadPool::MoveToIdle(THREADID tid) {
	threadBusyMutex.lock();
	vector<THREADID>::iterator busyIter = m_vecBusyThread.begin();
	while (busyIter != m_vecBusyThread.end()) {
		if (tid == *busyIter) {
			break;
		}
		busyIter++;
	}
	busyIter = m_vecBusyThread.erase(busyIter);
	threadBusyMutex.unLock();

	if ( m_vecBusyThread.size() == 0) {
		threadBusyEmptyCond.notifyAll();
	}

	threadIdleMutex.lock();
	m_vecIdleThread.push_back(tid);
	threadIdleMutex.unLock();
	return 0;
}

int CThreadPool::MoveToBusy(THREADID tid) {
	threadIdleMutex.lock();
	vector<THREADID>::iterator idleIter = m_vecIdleThread.begin();
	while (idleIter != m_vecIdleThread.end()) {
		if (tid == *idleIter) {
			break;
		}
		idleIter++;
	}
	m_vecIdleThread.erase(idleIter);
	threadIdleMutex.unLock();

	threadBusyMutex.lock();
	m_vecBusyThread.push_back(tid);
	threadBusyMutex.unLock();
	return 0;
}

THREADRET THREADCALL CThreadPool::ThreadFunc(void * threadData) {
#ifdef TRIPLEBIT_WINDOWS
	THREADID tid = GetCurrentThreadId();
#else
	THREADID tid = pthread_self();
#endif
	ThreadPoolArg* arg = (ThreadPoolArg*)threadData;
	vector<Task>* taskList = arg->taskList;
	CThreadPool* pool = arg->pool;
	while (1) {
		pool->threadMutex.lock();

		while( taskList->size() == 0 && pool->shutdown == false){
			pool->threadCond.wait(pool->threadMutex);
		}

		if ( pool->shutdown == true){
			pool->threadMutex.unLock();
#ifdef TRIPLEBIT_WINDOWS
			ExitThread(0);
#else
			pthread_exit(NULL);
#endif
		}

		pool->MoveToBusy(tid);
		Task task = Task(taskList->front());
		taskList->erase(taskList->begin());

		if ( taskList->size() == 0 ) {
			pool->threadEmptyCond.notifyAll();
		}
		pool->threadMutex.unLock();
		task();
		pool->MoveToIdle(tid);
	}
	return (THREADRET)0;
}

int CThreadPool::AddTask(const Task& task) {
	threadMutex.lock();
	this->m_vecTaskList.push_back(task);
	threadMutex.unLock();
	threadCond.notifyAll();
	return 0;
}

int CThreadPool::Create() {
	m_vecTaskList.clear();
	struct ThreadPoolArg* arg = new ThreadPoolArg;
	threadIdleMutex.lock();
	for (int i = 0; i < m_iThreadNum; i++) {
		THREADID tid = 0;
		arg->pool = this;
		arg->taskList = &m_vecTaskList;
#ifdef TRIPLEBIT_WINDOWS
		CreateThread(NULL, 0, ThreadFunc, (void*)arg, 0, &tid);
#else 
		pthread_create(&tid, NULL, ThreadFunc, arg);
#endif
		m_vecIdleThread.push_back(tid);
	}
	threadIdleMutex.unLock();
	return 0;
}

int CThreadPool::StopAll() {
	shutdown = true;
	threadMutex.lock();
	threadCond.notifyAll();
	vector<THREADID>::iterator iter = m_vecIdleThread.begin();
	while (iter != m_vecIdleThread.end()) {
#ifdef TRIPLEBIT_WINDOWS
		HANDLE h_thread = OpenThread(THREAD_ALL_ACCESS, false, *iter);
		WaitForSingleObject(h_thread, INFINITE);
		CloseHandle(h_thread);
#else
		pthread_join(*iter, NULL);
#endif
		iter++;
	}

	iter = m_vecBusyThread.begin();
	while (iter != m_vecBusyThread.end()) {
#ifdef TRIPLEBIT_WINDOWS
		HANDLE h_thread = OpenThread(THREAD_ALL_ACCESS, false, *iter);
		WaitForSingleObject(h_thread, INFINITE);
		CloseHandle(h_thread);
#else
		pthread_join(*iter, NULL);
#endif
		iter++;
	}

	return 0;
}

int CThreadPool::Wait()
{
	threadMutex.lock();
	while( m_vecTaskList.size() != 0 ) {
		threadEmptyCond.wait(threadMutex);
	}
	threadMutex.unLock();
	threadBusyMutex.lock();
	while( m_vecBusyThread.size() != 0) {
		threadBusyEmptyCond.wait(threadBusyMutex);
	}
	threadBusyMutex.unLock();
	return 0;
}

CThreadPool& CThreadPool::getInstance()
{
	if(instance == NULL) {
		instance = new CThreadPool(THREAD_NUMBER);
	}
	return *instance;
}
