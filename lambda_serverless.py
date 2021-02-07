import time
import logging
import math
import random
from multiprocessing import Queue, Process
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



class Lambda(object):
	
	def __init__ (self, simulation, config, lambda_batch_idx, lambda_model_idx, lambda_memory_size_idx):
		threading.Thread.__init__(self)
		self.simulation = simulation
		self.config =  config
		self.batch_size_idx = lambda_batch_idx
		self.model_type_idx = lambda_model_idx
		self.lambda_memory_size_idx = lambda_memory_size_idx
	
	def execute_real_request(self):
		mylambda =  boto3.client('lambda')
		mutex_lock2 = threading.Lock()
		#data= {"batch":"{}".format(self.batch_size)}
		data= {"BS":self.batch_size}
		batch_service_time = mylambda.invoke(FunctionName=self.function_name, InvocationType = 'RequestResponse', 
		            LogType = 'Tail', Payload=json.dumps(data))
		file_lambda_logs = "Lambda_logs_batch_{}_inter_arrival_{}_time_out{}_.log".format(self.actual_batch_size, 
		            self.inter_arrival,self.time_out) # "exp"
		mutex_lock2.acquire()
		with open (file_lambda_logs, "a+") as fl:
			fl.write("{}\n".format(base64.b64decode(batch_service_time['LogResult'])))
		fl.close()
		mutex_lock2.release()
		return batch_service_time['Payload'].read()

	def execute_simulated_request(self, current_time, task_id_list):

		print("execute_simulated_request: Enter")

		task_duration = self.config.lambda_latency[self.model_type_idx][self.lambda_memory_size_idx][self.batch_size_idx]
        probe_response_time = 5 + current_time
        task_end_time = task_duration + probe_response_time

        schedule_event = []

        for task_id in task_id_list:

	        task = self.simulation.tasks[task_id]
	        if task_duration > 0:
	            task_end_time = task_duration + probe_response_time
	            print >> self.simulation.tasks_file,"task_id ,", task.id, ",", "task_type," ,task.task_type,  ",",  "lambda task" , ",task_end_time ,", task_end_time, ",", "task_start_time,",task.start_time, ",", " each_task_running_time,",(task_end_time - task.start_time),",", " task_queuing_time:,", (task_end_time - task.start_time) - task.exec_time

	        if(self.simulation.add_task_completion_time(task_id,
	            task_end_time,1)):

	            print >> self.simulation.finished_file,"num_tasks ,", task.num_tasks, "," ,"VM_tasks ,", task.vm_tasks,"lambda_tasks ,", task.lambda_tasks ,"task_end_time, ", task_end_time,",", "task_start_time,",task.start_time,",", " each_task_running_time, ",(task.end_time - task.start_time)

	            new_event = TaskEndEvent(self)
	            schedule_event.append([(task_end_time, new_event)])
	    print("execute_simulated_request: End")
        return schedule_event

class ScheduleLambdaEvent(Event):

    def __init__(self, worker, task_id_list):
        self.worker = worker
        self.task_id_list = task_id_list
    def run(self, current_time):
        logging.getLogger('sim'
                ).debug('Probe for job list %s arrived at %s'
                        % (self.task_id_list,
                            current_time))
        return self.worker.execute_simulated_request(current_time, task_id_list)

