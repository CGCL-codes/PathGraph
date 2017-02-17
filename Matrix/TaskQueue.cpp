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

#include "TaskQueue.h"
#include <iostream>
#include <vector>

using namespace std;

#define ONE_THIRD(a) (a / 3 + ((a % 3) ? 1 : 0))

TaskQueue::TaskQueue() {
	// TODO Auto-generated constructor stub
	//cout<<"sizeof(Task) = "<<sizeof(Task)<<endl;
	task_circular_array = (Task *)malloc(sizeof(Task) * TASK_QUEUE_INIT_SIZE);
	assert(task_circular_array);
	task_circular_array_size = TASK_QUEUE_INIT_SIZE;
	_front = 0;
	_rear = 0;
	_task_num = 0;
	//pthread_mutex_init(&_sync_front_and_rear, NULL);
	pthread_mutex_init(&_sync_push_and_pop, NULL);
}

TaskQueue::~TaskQueue() {
	// TODO Auto-generated destructor stub
	free(task_circular_array);
	//pthread_mutex_destroy(&_sync_front_and_rear);
	pthread_mutex_destroy(&_sync_push_and_pop);
}

Task* TaskQueue::resize(Task *old, unsigned long newSize){
	cout<<"resize"<<endl;
	Task *new_arr = (Task *)malloc(sizeof(Task) * newSize);
	assert(new_arr);
	memcpy(&new_arr[0], &old[0], sizeof(Task) * task_circular_array_size);

	return new_arr;
}

Task* TaskQueue::resizeAndMoveTasks(Task *old, unsigned long newSize){
	cout<<"resizeAndMoveTasks"<<endl;
	Task *new_arr = (Task *)malloc(sizeof(Task) * newSize);
	assert(new_arr);
	//memcpy(&new_arr[front], &old[front], sizeof(Task) * (task_circular_array_size - front));  /* front ~ len-1 */
	memcpy(&new_arr[0], &old[0], sizeof(Task) * task_circular_array_size);  /* 0 ~ len-1, 0 ~ rear-1 copied for old access */
	memcpy(&new_arr[task_circular_array_size], &old[0], sizeof(Task) * _rear);  /* 0 ~ rear-1 */

	return new_arr;
}

void TaskQueue::taskQueue_push_without_sync(Task task){
	task_circular_array[_rear] = task;
	_rear = (_rear + 1) % task_circular_array_size;
#ifdef SELECT_SCHEDULE_OPTION
	_task_num++;
#endif
}

void TaskQueue::taskQueue_push_with_lock(Task task){
	unsigned long old_front;

	pthread_mutex_lock(&_sync_push_and_pop);
	old_front = _front; /* _front can be incremented by another worker */
	if(__builtin_expect(((_rear + 1) % task_circular_array_size) == old_front,0)){
		/* task_circular_array needs to be expanded */
		Task *old_task_circular_array = task_circular_array;
		if(_rear > old_front){
			task_circular_array = this->resize(old_task_circular_array, task_circular_array_size * 2);
			task_circular_array_size *= 2;
			__sync_synchronize();  /* new array_size must be visible from steal() called by another worker */
		}else{
			/* need to move Tasks that registered in place "0 ~ _rear-1" */
			task_circular_array = this->resizeAndMoveTasks(old_task_circular_array, task_circular_array_size * 2);
			//__sync_fetch_and_add(&_rear, task_circular_array_size);
			_rear += task_circular_array_size;
			task_circular_array_size *= 2;
			__sync_synchronize();  /* new array_size must be visible from steal() called by another worker */
		}
		free(old_task_circular_array);
	}

	task_circular_array[_rear] = task;
	_rear = (_rear + 1) % task_circular_array_size;
	__sync_synchronize();     /* new _rear must be visible from steal() called by another worker */
	pthread_mutex_unlock(&_sync_push_and_pop);
#ifdef SELECT_SCHEDULE_OPTION
	__sync_fetch_and_add(&_task_num, 1);
#endif
}

void TaskQueue::taskQueue_push_without_lock(Task task){
	unsigned long old_front = _front;   /* _front can be incremented by another worker */

	if(__builtin_expect(((_rear + 1) % task_circular_array_size) == old_front,0)){
		/* task_circular_array needs to be expanded */
		Task *old_task_circular_array = task_circular_array;
		if(_rear > old_front){
			task_circular_array = this->resize(old_task_circular_array, task_circular_array_size * 2);
			task_circular_array_size *= 2;
			__sync_synchronize();  /* new array_size must be visible from steal() called by another worker */
		}else{
			/* need to move Tasks that registered in place "0 ~ _rear-1" */
			task_circular_array = this->resizeAndMoveTasks(old_task_circular_array, task_circular_array_size * 2);
			//__sync_fetch_and_add(&_rear, task_circular_array_size);
			_rear += task_circular_array_size;
			task_circular_array_size *= 2;
			__sync_synchronize();  /* new array_size must be visible from steal() called by another worker */
		}
		free(old_task_circular_array);
	}

	task_circular_array[_rear] = task;
	_rear = (_rear + 1) % task_circular_array_size;
	__sync_synchronize();     /* new _rear must be visible from steal() called by another worker */
#ifdef SELECT_SCHEDULE_OPTION
	__sync_fetch_and_add(&_task_num, 1);
#endif
}

bool TaskQueue::taskQueue_pop_with_lock(Task &task){
	unsigned long old_front;
	unsigned long num_tasks;
	/*unsigned long temp_rear;
	
	temp_rear = (_rear - 1 + task_circular_array_size) % task_circular_array_size;
	__sync_synchronize();

	pthread_mutex_lock(&_sync_front_and_rear);// _rear may decreased, and _front may increased, must locked, to avoid
											  // the situation: "_rear >= _front" immediately convert to "_rear < _front" 
	if(__sync_bool_compare_and_swap(&_rear, _front, _rear)){
		pthread_mutex_unlock(&_sync_front_and_rear);
		return false;
	}else{
		_rear = temp_rear;
		__sync_synchronize(); //new _rear must be visible from steal() called by another worker.
							  //also, _front can be incremented by another worker.
	}
	pthread_mutex_unlock(&_sync_front_and_rear);*/
	
	pthread_mutex_lock(&_sync_push_and_pop);
	if(__sync_bool_compare_and_swap(&_rear, _front, _rear)){
		pthread_mutex_unlock(&_sync_push_and_pop);
		return false;
	}
	
	_rear = (_rear - 1 + task_circular_array_size) % task_circular_array_size;
	__sync_synchronize();
	
	old_front = _front;
	num_tasks = (_rear - old_front + task_circular_array_size) % task_circular_array_size;

	if (__builtin_expect(num_tasks == 0, 0)){
		/* both pop() and steal() might be trying to get an only task in taskQueue. */
		task = task_circular_array[_rear];
		__sync_synchronize();  /* _front can be incremented by another worker. */
		if (__sync_bool_compare_and_swap(&this->_front, old_front, _rear)){
			__sync_synchronize(); /* _front must be visible from steal() */
			pthread_mutex_unlock(&_sync_push_and_pop);
#ifdef SELECT_SCHEDULE_OPTION
			__sync_fetch_and_sub(&_task_num, 1);
#endif
			return true;
		}else{
			/* steal() already steal the task */
			pthread_mutex_unlock(&_sync_push_and_pop);
			return false;
		}
	}
	else{
		/* there are some number of tasks to be popped */
		if(!__sync_bool_compare_and_swap(&_rear,_front,_rear)){
		//if(_rear != _front){
			task = task_circular_array[_rear];
			pthread_mutex_unlock(&_sync_push_and_pop);
#ifdef SELECT_SCHEDULE_OPTION
			__sync_fetch_and_sub(&_task_num, 1);
#endif
			return true;
		}else{
			pthread_mutex_unlock(&_sync_push_and_pop);
			return false;
		}
	}
}

bool TaskQueue::taskQueue_pop_without_lock(Task &task){
	unsigned long old_front;
	unsigned long num_tasks;
	/*unsigned long temp_rear;
	
	temp_rear = (_rear - 1 + task_circular_array_size) % task_circular_array_size;
	__sync_synchronize();

	pthread_mutex_lock(&_sync_front_and_rear);// _rear may decreased, and _front may increased, must locked, to avoid
											  // the situation: "_rear >= _front" immediately convert to "_rear < _front" 
	if(__sync_bool_compare_and_swap(&_rear, _front, _rear)){
		pthread_mutex_unlock(&_sync_front_and_rear);
		return false;
	}else{
		_rear = temp_rear;
		__sync_synchronize(); //new _rear must be visible from steal() called by another worker.
							  //also, _front can be incremented by another worker.
	}
	pthread_mutex_unlock(&_sync_front_and_rear);*/
	
	if(__sync_bool_compare_and_swap(&_rear, _front, _rear)){
		return false;
	}
	
	_rear = (_rear - 1 + task_circular_array_size) % task_circular_array_size;
	__sync_synchronize();
	
	old_front = _front;
	num_tasks = (_rear - old_front + task_circular_array_size) % task_circular_array_size;

	if (__builtin_expect(num_tasks == 0, 0)){
		/* both pop() and steal() might be trying to get an only task in taskQueue. */
		task = task_circular_array[_rear];
		__sync_synchronize();  /* _front can be incremented by another worker. */
		if (__sync_bool_compare_and_swap(&this->_front, old_front, _rear)){
#ifdef SELECT_SCHEDULE_OPTION
			__sync_fetch_and_sub(&_task_num, 1);
#endif
			__sync_synchronize(); /* _front must be visible from steal() */
			return true;
		}else{
			/* steal() already steal the task */
			return false;
		}
	}
	else{
		/* there are some number of tasks to be popped */
		if(!__sync_bool_compare_and_swap(&_rear,_front,_rear)){
		//if(_rear != _front){
#ifdef SELECT_SCHEDULE_OPTION
			__sync_fetch_and_sub(&_task_num, 1);
#endif
			task = task_circular_array[_rear];
			return true;
		}else{
			return false;
		}
	}
}

bool TaskQueue::taskQueue_steal_one(Task &task){
	unsigned long old_front, new_front;
	unsigned long old_rear;
	unsigned long old_task_circular_array_size;
	unsigned long num_tasks;

	__sync_synchronize();  /* _front, _rear and task_circular_array_size can be changed by pop/push */
	old_front = this->_front;
	old_rear = this->_rear;
	old_task_circular_array_size = this->task_circular_array_size;
	new_front = (old_front + 1) % old_task_circular_array_size;
	num_tasks = (old_rear - old_front + old_task_circular_array_size) % old_task_circular_array_size;

	/*if (__builtin_expect(num_tasks == 0, 0)){
		return false;
	}else if(__builtin_expect(num_tasks == 1, 0)){
		task = task_circular_array[old_front];
		__sync_synchronize();  // _front can be incremented by pop
		
		pthread_mutex_lock(&_sync_front_and_rear); // avoid the situation: "_rear >= _front" immediately convert to "_rear < _front" 
		if(__sync_bool_compare_and_swap(&this->_front, this->_rear, this->_front)){
			pthread_mutex_unlock(&_sync_front_and_rear);
			return false;
		}else{
			if(__sync_bool_compare_and_swap(&this->_front, old_front, new_front)){
				//this->_front = old_rear;  // taskQueue may empty, may not (just decremented by 1)
				//__sync_synchronize();
				pthread_mutex_unlock(&_sync_front_and_rear);
				return true;
			}else{
				// pop() already took the task or other worker has token it
				pthread_mutex_unlock(&_sync_front_and_rear);
				return false;
			}
		}
	}*/
	if(__builtin_expect(num_tasks == 0, 0) || __builtin_expect(num_tasks == 1, 0)){
		return false;
	}
	else{
		__sync_synchronize();
		if(__builtin_expect(this->task_circular_array_size == old_task_circular_array_size, 1)){
			/* taskQueue did't expand, the task maybe stolen by multi-workers */
			//__sync_synchronize();
			if(__sync_bool_compare_and_swap(&this->_front, old_front, new_front)){
#ifdef SELECT_SCHEDULE_OPTION
				__sync_fetch_and_sub(&_task_num, 1);
#endif
				task = task_circular_array[old_front];
				return true;
			}else{
				return false;
			}
		}else{
			/* the task maybe stolen by multi-workers */
			unsigned long temp_front = (old_front + 1) % this->task_circular_array_size;
			if(__sync_bool_compare_and_swap(&this->_front, old_front, temp_front)){
#ifdef SELECT_SCHEDULE_OPTION
				__sync_fetch_and_sub(&_task_num, 1);
#endif
				task = task_circular_array[old_front];
				return true;
			}else{
				return false;
			}
		}
	}
}

bool TaskQueue::taskQueue_steal_batch(Task &task, TaskQueue *thiefQueue, bool lock_free){
	unsigned long old_front, new_front_by_one, new_front_by_much;
	unsigned long old_rear;
	unsigned long old_task_circular_array_size;
	unsigned long num_tasks;

	__sync_synchronize();  /* _front, _rear and task_circular_array_size can be changed by pop/push */
	old_front = this->_front;
	old_rear = this->_rear;
	old_task_circular_array_size = this->task_circular_array_size;
	//new_front_by_one = (old_front + 1) % old_task_circular_array_size;
	num_tasks = (old_rear - old_front + old_task_circular_array_size) % old_task_circular_array_size;
	new_front_by_much = (old_front + ONE_THIRD(num_tasks)) % old_task_circular_array_size;

	/*if (__builtin_expect(num_tasks == 0, 0)){
		return false;
	}else if(__builtin_expect(num_tasks == 1, 0)){
		task = task_circular_array[old_front];
		__sync_synchronize();  // _front can be incremented by pop 

		pthread_mutex_lock(&_sync_front_and_rear); // avoid the situation: "_rear >= _front" immediately convert to "_rear < _front" 
		if(__sync_bool_compare_and_swap(&this->_front, this->_rear, this->_front)){
			pthread_mutex_unlock(&_sync_front_and_rear);
			return false;
		}else{
			if(__sync_bool_compare_and_swap(&this->_front, old_front, new_front_by_one)){
				//this->_front = old_rear;  // taskQueue may empty, may not (just decremented by 1)
				//__sync_synchronize();
				pthread_mutex_unlock(&_sync_front_and_rear);
				return true;
			}else{
				// pop() already took the task or other worker has token it
				pthread_mutex_unlock(&_sync_front_and_rear);
				return false;
			}
		}
	}*/
	if(__builtin_expect(num_tasks == 0, 0) || __builtin_expect(num_tasks == 1, 0)){
		return false;
	}
	else{
		__sync_synchronize();
		if(__builtin_expect(this->task_circular_array_size == old_task_circular_array_size, 1)){
			/* taskQueue did't expand, the tasks maybe stolen by multi-workers */
			//__sync_synchronize();
			if(__sync_bool_compare_and_swap(&this->_front, old_front, new_front_by_much)){
#ifdef SELECT_SCHEDULE_OPTION
				__sync_fetch_and_sub(&_task_num, ONE_THIRD(num_tasks));
#endif
				task = task_circular_array[old_front];
				if(__builtin_expect(lock_free == true, 1)){
					old_front = (old_front + 1) % old_task_circular_array_size;
					while(old_front < new_front_by_much){
						thiefQueue->taskQueue_push_without_lock(task_circular_array[old_front]);
						old_front = (old_front + 1) % old_task_circular_array_size;
					}
				}else{
					old_front = (old_front + 1) % old_task_circular_array_size;
					while(old_front < new_front_by_much){
						thiefQueue->taskQueue_push_with_lock(task_circular_array[old_front]);
						old_front = (old_front + 1) % old_task_circular_array_size;
					}
				}
				return true;
			}else{
				return false;
			}
		}else{
			/* the tasks maybe stolen by multi-workers */
			unsigned long temp_front_by_much = (old_front + ONE_THIRD(num_tasks)) % this->task_circular_array_size;
			if(__sync_bool_compare_and_swap(&this->_front, old_front, temp_front_by_much)){
#ifdef SELECT_SCHEDULE_OPTION
				__sync_fetch_and_sub(&_task_num, ONE_THIRD(num_tasks));
#endif
				task = task_circular_array[old_front];
				if(__builtin_expect(lock_free == true, 1)){
					old_front = (old_front + 1) % task_circular_array_size;
					while(old_front < temp_front_by_much){
						thiefQueue->taskQueue_push_without_lock(task_circular_array[old_front]);
						old_front = (old_front + 1) % task_circular_array_size;
					}
				}else{
					old_front = (old_front + 1) % task_circular_array_size;
					while(old_front < temp_front_by_much){
						thiefQueue->taskQueue_push_with_lock(task_circular_array[old_front]);
						old_front = (old_front + 1) % task_circular_array_size;
					}
				}
				return true;
			}else{
				return false;
			}
		}
	}
}

void TaskQueue::reset(){
	this->_front = 0;
	this->_rear = 0;
	this->_task_num = 0;
}

bool TaskQueue::empty(){
	return this->_front == this->_rear;
}
