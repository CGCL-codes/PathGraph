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

#ifndef TASKQUEUE_H_
#define TASKQUEUE_H_

#include <boost/function.hpp>

#define TASK_QUEUE_INIT_SIZE (1024 * 1024)
//#define SELECT_SCHEDULE_OPTION 1

typedef boost::function<struct _return_of_task* ()> Task;

class TaskQueue {
public:
	TaskQueue();
	virtual ~TaskQueue();
	void taskQueue_push_without_sync(Task task);
	void taskQueue_push_with_lock(Task task);
	void taskQueue_push_without_lock(Task task);
	bool taskQueue_pop_with_lock(Task &task);
	bool taskQueue_pop_without_lock(Task &task);
	bool taskQueue_steal_one(Task &task);
	bool taskQueue_steal_batch(Task &task, TaskQueue *thiefQueue, bool lock_free);
	void reset();
	bool empty();
	volatile unsigned long _task_num;

private:
	Task *task_circular_array;
	unsigned long task_circular_array_size;
	volatile unsigned long _front;
	volatile unsigned long _rear;
	//pthread_mutex_t _sync_front_and_rear;
	pthread_mutex_t _sync_push_and_pop;

	Task* resize(Task *old_task_circular_array, unsigned long newSize);
	Task* resizeAndMoveTasks(Task *old_task_circular_array, unsigned long newSize);
};

#endif /* TASKQUEUE_H_ */
