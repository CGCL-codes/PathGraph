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

#ifndef _MUTEXLOCK_H_
#define _MUTEXLOCK_H_

#include "TripleBit.h"

#ifdef TRIPLEBIT_WINDOWS
#include <windows.h>
#else
#include <pthread.h>
#endif

//#include <iostream>

class MutexLock
{
#ifdef TRIPLEBIT_WINDOWS
	CRITICAL_SECTION mutex;
#else
	pthread_mutex_t mutex;
#endif
public:
	MutexLock(void);

	~MutexLock() {
//		std::cout<<"delete lock"<<std::endl;		

#ifdef TRIPLEBIT_WINDOWS
		DeleteCriticalSection(&mutex);
#else
		pthread_mutex_destroy(&mutex);
#endif
	}

	void init() {
#ifdef TRIPLEBIT_WINDOWS
		InitializeCriticalSection(&mutex);
#else
		pthread_mutex_init(&mutex,NULL);
#endif
	}

	void lock();

	void unLock();

	bool tryLock();

#ifdef TRIPLEBIT_WINDOWS
	CRITICAL_SECTION& getMutex() {
		return mutex;
	}
#else
	pthread_mutex_t& getMutex() {
		return mutex;
	}

#endif
};

#endif
