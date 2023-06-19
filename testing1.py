import csv

import time
import logging
import math
import random

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
	
	input_path = "./traces/WITS_load.csv"
	#input_path = "./traces/BATCH_tweet_workload.csv"
	
	fname = open(input_path, 'r')
	
	#fname = open(input_path, 'r')
		

	line = fname.readline()
	i = 0
	total_sum = 0
	num_task = 0
	max_num = 0
	avg_sum = 0
	total_task = 0

	while (line != ''):
		num_task = int(line.split(',')[3])
		total_sum =  total_sum + num_task
		i +=1
		if(i == 100):
			total_sum = total_sum/i
			i = 0
		if(max_num < num_task):
			max_num = num_task
		line = fname.readline()
		total_task+=1
		if(total_task >= 36000):
			break

	if (i!=0):
		print("Average:", total_sum/i)
		avg_sum = total_sum/i

	else:
		print("Average:", total_sum)
		avg_sum = total_sum
	print("Max: ",max_num)

	fname = open(input_path, 'r')
	line = fname.readline()
	num_task = 0
	i = 0
	total_sum = 0
	total_task = 0
	while (line != ''):
		total_task +=1
		num_task = int(line.split(',')[3])
		total_sum += (num_task -avg_sum)**2
		line = fname.readline()
		if(total_task >= 36000):
			break

	print("variance:", total_sum/(total_task-1))
	print("SD:", math.sqrt(total_sum/(total_task-1)))
	print("total_task:", total_task)

		