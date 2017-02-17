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
	if len(sys.argv) != 3:
		print 'Usage: %s <task_info file> <steal_info file>' % (sys.argv[0])
		sys.exit(-1)
	file = open(sys.argv[1], 'r')
	pthread_id_dict = []
	task_set_count = []
	task_steal_set = []
	task_time_compute = []
	task_time_steal = []
	task_steal_success = []
	task_steal_fail = []
	finder_flag = False
	index = -1
	for line in file:
		line_list = line.split(' ')
		if len(line_list) != 3 and len(line_list) != 4:
			continue
		if ((len(line_list) == 3) and (not line_list[2].startswith('ms'))):
			continue
		if digit_line(line_list) == True:
			finder_flag, index = dict_finder(line_list[0], pthread_id_dict)
			if finder_flag == True:
				task_set_count[index] += 1
			else:
				pthread_id_dict.append(line_list[0])
				task_set_count.append(1)
				task_steal_set.append(0)
				task_time_compute.append(0.0)
				task_time_steal.append(0)
				task_steal_success.append(0)
				task_steal_fail.append(0)
		if len(line_list) == 3:
			finder_flag, index = dict_finder(line_list[0], pthread_id_dict)
			if finder_flag == True:
				task_time_compute[index] += float(line_list[1])
			else:
				print 'error1, can not find pthread %s' % (line_list[0])
				sys.exit(-1)
	
	file.seek(0)
	for line in file:
		line_list = line.split(' ')
		if len(line_list) != 4:
			continue
		if not line_list[1].startswith('steal'):
			continue
		if not line_list[3].startswith('times'):
			continue
		finder_flag, index = dict_finder(line_list[0], pthread_id_dict)
		if finder_flag == True:
			task_steal_set[index] += int(line_list[2])
		else:
			print 'error2, can not find pthread %s' % (line_list[0])
			sys.exit(-2)
	file.close()

	file = open(sys.argv[2], 'r')
	for line in file:
		line_list = line.split(' ')
		if len(line_list) != 3:
			continue
		finder_flag, index = dict_finder(line_list[0], pthread_id_dict)
		if finder_flag == True:
			task_time_steal[index] += int(line_list[1])
			if line_list[2].strip() == '0':
				task_steal_fail[index] += 1
			else:
				task_steal_success[index] += 1
		else:
			print 'error3, can not find pthread %s' % (line_list[0])
			sys.exit(-3)
	file.close()

	print 'len(pthread_id_dict) = %s, len(task_set_count) = %s' % (len(pthread_id_dict), len(task_set_count))
	if len(pthread_id_dict) != len(task_set_count):
		print 'error, the number of pthreads should equal to the number of task sets'
		sys.exit(-1)
	total_task_num = 0
	total_steal_num = 0
	total_steal_success = 0
	total_steal_fail = 0
	total_steal_time = 0
	total_seq_time = 0
	print 'pthread_id          task_count    compute_time(ms)         load_balance        steal_num    steal_success_num      steal_fail_num    steal_time(us)'
	for i in range(len(pthread_id_dict)):
		total_task_num += int(task_set_count[i])
		total_seq_time += task_time_compute[i]
		total_steal_num += int(task_steal_set[i])
		total_steal_success += int(task_steal_success[i])
		total_steal_fail += int(task_steal_fail[i])
		total_steal_time += task_time_steal[i]
		print '%s        %s         %s            %s          %s                %s                 %s                 %s' \
			% (pthread_id_dict[i], task_set_count[i], task_time_compute[i], task_time_compute[i]*1.0/task_time_compute[0], task_steal_set[i], task_steal_success[i], task_steal_fail[i], task_time_steal[i])
	print 'total_task_num = %s, total_steal_num = %s, total_steal_time(us) = %s' % (total_task_num, total_steal_num, total_steal_time)
	print 'total_steal_success_num = %s, total_steal_fail_num = %s' % (total_steal_success, total_steal_fail)
	print 'average_one_steal_time(us) = %s, average_steal_succ_percentage = %s' % (total_steal_time * 1.0 / total_steal_num, total_steal_success * 1.0 / total_steal_num)
	print 'average_one_compute_time(ms) = %s, steal_compute_percentage = %s' % (total_seq_time*1.0/total_task_num, (total_steal_time*1.0/total_steal_num)/(total_seq_time*1000.0/total_task_num))
	sys.exit(0)

