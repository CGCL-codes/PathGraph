#!/usr/bin/python

import sys
import os

if __name__ == '__main__':
	if len(sys.argv) < 5:
		print '%s <command> <printtop> <batch times> <dataset1> <dataset2> ......' % (sys.argv[0])
		sys.exit(-1)
	os.system("echo execute %s application > result_%s.log" % (sys.argv[1], sys.argv[1]))
	dataset_num = len(sys.argv) - 4
	for i in range(dataset_num):
		command = './bin/lrelease/%s %s/ %s >> result_%s.log' % (sys.argv[1], sys.argv[i+4], sys.argv[2], sys.argv[1])
		for j in range(int(sys.argv[3])):
			os.system("echo 3 > /proc/sys/vm/drop_caches")
			os.system(command)
		os.system("echo finish dataset %s >> result_%s.log" % (sys.argv[i+4], sys.argv[1]))
	sys.exit(0)
	
