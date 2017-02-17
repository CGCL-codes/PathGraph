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

#include "MutexLock.h"

MutexLock::MutexLock(void) 
{
}


void MutexLock::lock()
{
#ifdef TRIPLEBIT_WINDOWS
	EnterCriticalSection(&mutex);
#else
	pthread_mutex_lock(&mutex);
#endif
}

bool MutexLock::tryLock()
{
#ifdef TRIPLEBIT_WINDOWS
	return TryEnterCriticalSection(&mutex);
#else
	return pthread_mutex_trylock(&mutex) ==0 ;
#endif
}

void MutexLock::unLock()
{
#ifdef TRIPLEBIT_WINDOWS
	LeaveCriticalSection(&mutex);
#else
	pthread_mutex_unlock(&mutex);
#endif
}
