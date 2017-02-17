#!/usr/bin/python

import os
import sys

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print '%s <folder1> <folder2>' % (sys.argv[0])
		sys.exit(-1)
	filelist1 = os.listdir(sys.argv[1])
	filelist2 = os.listdir(sys.argv[2])
	for filename in filelist1:
		if filename in filelist2:
			cmd = "diff %s/%s %s/%s" % (sys.argv[1], filename, sys.argv[2], filename)
			print cmd
			os.system(cmd)
			print '-----------------------------------------------------------------'
		else:
			print '%s in folder %s is not existing in folder %s' % (filename, sys.argv[1], sys.argv[2])
	for filename in filelist2:
		if filename not in filelist1:
			print '%s in folder %s is not existing in folder %s' % (filename, sys.argv[2], sys.argv[1])
	sys.exit(0)

