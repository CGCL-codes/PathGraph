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

#include "Condition.h"

Condition::Condition(void)
{
#ifdef TRIPLEBIT_WINDOWS
	InitializeConditionVariable(&cond);
#else
	pthread_cond_init(&cond,NULL);
#endif
}


Condition::~Condition(void)
{
#ifdef TRIPLEBIT_WINDOWS
#else
	pthread_cond_destroy(&cond);
#endif
}

void Condition::wait(MutexLock& lock)
{
#ifdef TRIPLEBIT_WINDOWS
	SleepConditionVariableCS(&cond,&lock.getMutex(),INFINITE); 
#else
	pthread_cond_wait(&cond, &lock.getMutex());
#endif
}

void Condition::notifyAll()
{
#ifdef TRIPLEBIT_WINDOWS
	WakeConditionVariable(&cond);
#else
	pthread_cond_broadcast(&cond);
#endif
}
