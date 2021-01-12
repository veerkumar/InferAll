import time
import logging
import math
import random
from multiprocessing import Process, Queue
import copy
import collections
import sys
import os
#import numpypy
import numpy as np
from numpy import mean
from collections import defaultdict
import pandas as pd
from sklearn.linear_model import LinearRegression

from config import *
from utils import *
from vm import *
from lambda_serverless import *

class ScheduleVMEvent(Event):

    def __init__(self, worker, job_id):
        self.worker = worker
        self.job_id = job_id
        self.worker.num_queued_tasks += 1
    def run(self, current_time):
        logging.getLogger('sim'
                ).debug('Probe for job %s arrived at worker %s at %s'
                        % (self.job_id, self.worker.id,
                            current_time))
        return self.worker.add_task(self.job_id, current_time)

class ScheduleLambdaEvent(Event):

    def __init__(self, worker, job_id):
        self.worker = worker
        self.job_id = job_id
    def run(self, current_time):
        logging.getLogger('sim'
                ).debug('Probe for job %s arrived at %s'
                        % (self.job_id,
                            current_time))
        return self.worker.execute_task(self.job_id, current_time)

class Simulation(object):

    def __init__(self, workload_file):
        self.workload_file = workload_file

        self.tasks = defaultdict()
        self.task_arrival = defaultdict(list)
        self.event_queue = Queue.PriorityQueue()
        self.VMs = defaultdict(lambda: np.ndarray(0))
        self.completed_VMs = defaultdict(list)
        self.lambdas = defaultdict()
        
        j = 0
        while j < INITIAL_WORKERS:
            i = 0
            while i < 3:
                self.VMs.setdefault(i, []).append(VM(self,0,start_up_delay,i,4,8192, 0.10,False,len(self.VMs[i])))
                i += 1
            j += 1
    def run(self):
    	self.tasks_file = open(self.workload_file, 'r')
        line = self.tasks_file.readline()

        # print line
        if 

        start_time = float(line.split(',')[0])
        num_tasks = line.split(',')[3]
        task_type = line.split(',')[2]



def get_args():
    parser =argparse.ArgumentParser()
    parser.add_argument('--batch_size', type = int, default = 5, help = "Batch size must be an integer")
    parser.add_argument('--time_out', type = float, default = 1.0, help = "Timeout value must be a float in seconds")
    parser.add_argument('--trace_name', type = str, default = 'wiki', help = "Name of the traces (1. wiki, 2.berkeley, 3. tweeter, 4. tweeter_BATCH")
    return parser.parse_args()

if __name__=="__main__": 
	for i in batch_sz:
		print(i)