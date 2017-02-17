#!/usr/bin/python

import sys
import os

if __name__ == '__main__':
	clear_cmd = 'echo 3 > /proc/sys/vm/drop_caches'
	builder_cmd = './bin/lrelease/graph_builder dataset/twitter-2010.txt twitter/ 41652229 > builder_out'
	os.system(clear_cmd)
	os.system(builder_cmd)
	builder_cmd = './bin/lrelease/graph_builder dataset/uk2007.txt uk2007/ 105896268 >> builder_out'
	os.system(clear_cmd)
	os.system(builder_cmd)
	builder_cmd = './bin/lrelease/graph_builder dataset/ukunion.txt ukunion/ 133633040 >> builder_out'
	os.system(clear_cmd)
	os.system(builder_cmd)
	builder_cmd = './bin/lrelease/graph_builder dataset/yahulink.txt yahoo/ 1413511394 >> builder_out'
	os.system(clear_cmd)
	os.system(builder_cmd)
	sys.exit(0)

