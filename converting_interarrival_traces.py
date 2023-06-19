import csv
import argparse
import time
import logging
import math
import random
import threading
from collections import OrderedDict 
import multiprocessing
from multiprocessing import Process, Queue
from queue import PriorityQueue
import copy
import collections
import sys
import os
#import numpypy
import math
import numpy as np
from numpy import mean
from collections import defaultdict


if __name__=="__main__": 
	 
	# logging.basicConfig(filename='events_log.log',filemode='w',\
	# 	format='%(levelname)s:%(asctime)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',level=logging.INFO)
	input_path = "./traces/TW_may25_1hr_traces/"
	output_path = "./traces/TW_may25_1hr_traces_discrete/"
	tweet_batch_load = open("./traces/BATCH_tweet_workload.csv", 'w')
	j=0
	for i in range(23):
		input_file_name = "TW_may25_"+str(i)+"-"+str(i+1)
		fname = open(os.path.join(input_path,input_file_name), 'r')
		output_file =   open(os.path.join(output_path,input_file_name)+".csv", 'w')

		value = float(fname.readline())
		i = 0
		total_sum = 0
		num_task = 0
		end = False

		while (value != 0):
			while True:
				total_sum +=value
				if (total_sum < i+1):
					num_task +=1
					line = fname.readline()
					if (line != ''):
						value = float(line)
						#print ("value: {}, total_sum: {} num_task:{}".format(value,total_sum,num_task))
					else:
						end = True
						print(i,",0,1,",num_task,file=output_file,sep="")
						#print("i:{} num_task:{}".format( i, num_task))
						print(j,",0,1,",num_task,file=tweet_batch_load,sep="")
						break
				else:
					#print("i:{} num_task:{}".format( i, num_task))

					print(i,",0,1,",num_task,file=output_file,sep="")
					print(j,",0,1,",num_task,file=tweet_batch_load,sep="")
					num_task = 1
					line = fname.readline()
					if (line != ''):
						value = float(line)
						#print ("value: {}, total_sum: {} num_task:{}".format(value,total_sum,num_task))
					else:
						end = True
					break
				
			if (end == True):
				break
			i +=1
			j +=1
		output_file.close()
	tweet_batch_load.close()



