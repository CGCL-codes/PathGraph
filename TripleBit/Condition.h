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

#ifndef _CONDITION_H_
#define _CONDITION_H_

#include "TripleBit.h"
#include "MutexLock.h"

class Condition
{
#ifdef TRIPLEBIT_WINDOWS
	CONDITION_VARIABLE cond;
#else
	pthread_cond_t cond;
#endif

public:
	Condition(void);
	~Condition(void);
	void wait(MutexLock& lock);
	void notifyAll();
};
#endif
