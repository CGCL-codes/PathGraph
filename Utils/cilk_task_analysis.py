#!/usr/bin/python

import sys
import os

def digit_line(line_list):
	for i in range(1, len(line_list)):
		if line_list[i].strip().isdigit() == False:
			return False
	return True

def dict_finder(cur_pthread_id, pthread_id_dict):
	for i in range(len(pthread_id_dict)):
		if cur_pthread_id == pthread_id_dict[i]:
			return True, i
	return False, -1

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print 'Usage: %s <task_info file>' % (sys.argv[0])
		sys.exit(-1)
	file = open(sys.argv[1], 'r')
	pthread_id_dict = []
	task_time_compute = []
	finder_flag = False
	index = -1
	total_task_num = 0
	for line in file:
		line_list = line.split(' ')
		if len(line_list) != 3:
			continue
		if (not line_list[2].startswith('ms')):
			continue
		total_task_num += 1
		finder_flag, index = dict_finder(line_list[0], pthread_id_dict)
		if finder_flag == True:
			task_time_compute[index] += float(line_list[1])
		else:
			pthread_id_dict.append(line_list[0])
			task_time_compute.append(float(line_list[1]))
	
	file.close()

	print 'len(pthread_id_dict) = %s, len(task_time_compute) = %s' % (len(pthread_id_dict), len(task_time_compute))
	if len(pthread_id_dict) != len(task_time_compute):
		print 'error, the number of pthreads should equal to the number of task sets'
		sys.exit(-1)
	
	total_seq_time = 0
	print 'pthread_id          compute_time(ms)       load_balance'
	for i in range(len(pthread_id_dict)):
		total_seq_time += task_time_compute[i]
		print '%s        %s           %s' \
			% (pthread_id_dict[i], task_time_compute[i], task_time_compute[i] * 1.0 / task_time_compute[0])
	print 'total_task_num = %s, average_one_compute_time(ms) = %s' % (total_task_num, total_seq_time * 1.0 / total_task_num)
	sys.exit(0)

