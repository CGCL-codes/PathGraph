#!/usr/bin/python

import os
import sys

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print '%s <file>' % (sys.argv[0])
		sys.exit(-1)
	file = open(sys.argv[1], 'r')
	count = 0
	for line in file:
		list = line.split()
		if len(list) == 1:
			count += 1
	print 'isolate vertex number: %s' % (count)
	sys.exit(0)

