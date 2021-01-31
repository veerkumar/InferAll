import time
import logging
import math
import random
import threading
from collections import OrderedDict 
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


# This class used to schedule task to VM or lambda worker
class Scheduler_thread(threading.Thread):
	"""docstring for Scheduler_cls"""
	def __init__(self, config, simulation):
		super(Scheduler_cls, self).__init__()
		self.config = config
		self.simulation =  simulation

	def get_filtered_configuration(self, time_limit):
		'''
			This function will generate best possilbe congfiguration for given "Remaining time".
			Given "Remaining time" and for each accuracy level, get maximum batch size we can go to meet SLO
		'''
		#VM calculation
		# Assumption is that, each VM is running contatiner for all accuracy level (But only one is working at a time)
		# since rows in profiling are shorted based on model accuracy and time_requriement, we dont have to do much, 
		# Just filter the result, 3rd dimension is 1d because all batchsize latency are in sorted order
		vm_filtered_configuration = np.zeros(( len(self.config.vm_models),len(self.config.vm_available_memory), 1),dtype=float)
		lambda_filtered_configuration = np.zeros((len(self.config.lambda_available_memory), len(self.config.lambda_models), 1),dtype=float)
		for i, memory in enumerate(self.config.vm_models):
			#print("i ",i)
			for j , model in enumerate(self.config.vm_available_memory):
				#print("j",j)
				for k, batch in enumerate(self.config.batch_sz):
					#print("k",k)
					if (self.config.vm_latency[i][j][k] < time_limit):
						vm_filtered_configuration[i][j][0] = k
					else: break
		# Lambda
		for i,  memory in enumerate(self.config.lambda_available_memory):
			for j , model in enumerate(self.config.lambda_models):
				for k, batch in enumerate(self.config.batch_sz):
					if (self.config.lambda_latency[i][j][k] < time_limit):
						lambda_filtered_configuration[i][j][0] = k
					else: break
		return (vm_filtered_configuration, lambda_filtered_configuration)


		#Lambda calculation

		# Lower accuracy model help us in : Meeting the SLO 

	def get_cost_optimized_configuration_value(self, vm_filtered_configuration,  lambda_filtered_configuration):
		# This function returns ordered(by cost) dictnory
		#Input to this function is maximized on Batch size(while satisfing SLO at same time) on each memory,
		# we will calculate per unit (image) cost
		vm_cost_estimation = np.zeros((len(self.config.vm_models), len(self.config.vm_available_memory)),dtype=float)
		lambda_cost_estimation = np.zeros((len(self.config.lambda_models), len(self.config.lambda_available_memory)), dtype=float)
		for i , model in enumerate(self.config.vm_models):
			for j, memory in enumerate(self.config.vm_available_memory):
				#This is per image cost, which will help in favouring larger batch size
				if (self.config.vm_latency[i][j][int(vm_filtered_configuration[i][j][0])] == self.config.MAX):
					vm_cost_estimation[i][j] =  self.config.MAX
				else:
					vm_cost_estimation[i][j] = \
					((((self.config.vm_latency[i][j][int(vm_filtered_configuration[i][j][0])])*self.config.MILL_TO_HR)*vm_cost[j])/vm_filtered_configuration[i][j][0])

		for i , model in enumerate(self.config.lambda_models):
			for j, memory in enumerate(self.config.lambda_available_memory):
				if (self.config.lambda_latency[i][j][int(lambda_filtered_configuration[i][j][0])] == self.config.MAX):
					lambda_cost_estimation[i][j] =  self.config.MAX
				else:
					lambda_cost_estimation[i][j] = lambda_cost(1,memory,(self.config.lambda_latency[i][j][int(lambda_filtered_configuration[i][j][0])]))
		return (vm_cost_estimation, lambda_cost_estimation)
	# def get_slo_latency_optimized_configuration_value(self, vm_filtered_configuration,  lambda_filtered_configuration):

	def get_top_kth_cost_config (vm_cost_estimation, lambda_cost_estimation, top_kth):
		# This function will return top Kth config for VM and lambda 
		# (since lambda functions are available in abandannce, just return 1st always)
		# Also assumption is Every VM is able to run all type of accuracy model, which is not an limitation as based
		# on the type of optimization, we can start VM with appropriate configuration
		i =  0;
		vm_memory_size_idx = 0
		lambda_memory_size_idx = 0

		'''  Example of vm_cost_estimation, 3 model x 2 memory
			>>> vm_cost_estimation
			array([[7.21897917e-07, 1.11557778e-06],
       			[3.38356667e-06, 2.02709259e-06],
       			[3.30385556e-06, 1.66444167e-06]])
		'''
		vm_dict = {}
		#  Models are stacked in increase order of efficency, so start from last valid model(as some may not have valid value 
		#  for cost i.e MAX), Below code construct a row sorted 2D matrix and Pick the top_kth value.
		i = len(self.config.vm_models) - 1;
		temp_cost = {}
		while i >= 0:
		    j = len(self.config.vm_available_memory)-1
		    vm_dict = {}
		    while j >= 0:
		            vm_dict[vm_cost_estimation[i][j]] = j
		            j = j - 1
    		temp_cost[i] = OrderedDict(sorted(vm_dict.items()))
    		i = i - 1
    	temp_cost1 = OrderedDict(sorted(temp_cost.items(),reverse=True))
    	vm_model_idx = 0
		vm_memory_size_idx = 0
		done =  0
		i = 0
		for key in temp_cost1:
		    print("")
		    for j in temp_cost1[key]:
		        if (j != MAX):
		            i = i + 1
		            if (i >= top_kth):
		                vm_memory_size_idx = temp_cost1[key][j]
		                vm_model_idx = key
		                done = 1
		        print (temp_cost1[key][j], end=' ')
		        if(done == 1):
		            break
		    if(done ==1 ):
		        break
		# Lambda function calculation 
		i = len(self.config.lambda_models) - 1;
		temp_cost = {}
		while i >= 0:
		    j = len(self.config.lambda_available_memory)-1
		    lm_dict = {}
		    while j >= 0:
		            lm_dict[lambda_cost_estimation[i][j]] = j
		            j = j - 1
    		temp_cost[i] = OrderedDict(sorted(lm_dict.items()))
    		i = i - 1
    	temp_cost1 = OrderedDict(sorted(temp_cost.items(),reverse=True))
    	lambda_model_idx = 0
		lambda_memory_size_idx = 0
		done =  0
		i = 0
		for key in temp_cost1:
		    print("")
		    for j in temp_cost1[key]:
		        if (j != MAX):
		            i = i + 1
		            if (i >= top_kth):
		                lambda_memory_size_idx = temp_cost1[key][j]
		                lambda_model_idx = key
		                done = 1
		        print (temp_cost1[key][j], end=' ')
		        if(done == 1):
		            break
		    if(done ==1 ):
		        break

		# if (top_kth > len(self.config.vm_available_memory)-1):
		# 	return (-1, lambda_memory_size_idx) # we ran out of VM space
		return (vm_memory_size_idx, vm_model_idx, lambda_memory_size_idx, lambda_model_idx)

	def check_free_vm(self, memory):
		j = 0
		found = False
        while j < self.config.INITIAL_WORKERS:
            i = 0
            while i < 3:
                if (self.simulation.VMs[i][j].vmem == memory):
                	if (self.simulation.VMs[i][j].isIdle == True):
                		found = True
                		return (self.simulation.VMs[i][j], found)
                i += 1
            j += 1
        if (found == False):
        	return (None, found)

    def get_best_possible_vm_and_lambda_config(vm_cost_estimation, lambda_cost_estimation):
    	vm_memory_size_idx =0
		vm_model_idx = 0 
		lambda_memory_size_idx = 0 
		lambda_model_idx = 0
		vm_scheduled = False
		top_kth = 1
		while True: 
			(vm_memory_size_idx, vm_model_idx, lambda_memory_size_idx, lambda_model_idx) = \
					 get_top_kth_cost_config (vm_cost_estimation, lambda_cost_estimation, top_kth)
					 #TODO Check VM avialblity
			(vm, found) = check_free_vm(self.config.vm_available_memory[vm_memory_size_idx])
			if(found == True):
				vm_scheduled = True
				return (vm_memory_size_idx, vm_model_idx, vm_scheduled, vm, lambda_memory_size_idx, lambda_model_idx)
				break
			else:
				top_kth = top_kth + 1
				logging.debug("Scheduler_thread: Best memory is not available, finding next:",top_kth)

		return (0, 0, False, None, lambda_memory_size_idx, lambda_model_idx)



	def run(self):
		while True :
			if not task_queue.empty():
				task_id_list = []
				task =  self.simulation.task_queue.get()
				task_id_list.append(task.id)
				total_scheduled_tasks = task.num_tasks
				remaining_time =  self.simulation.current_time - task.start_time
				(vm_filtered_configuration, lambda_filtered_configuration) =  get_filtered_configuration(remaining_time)
				# optimize on VM or Lambda selection
				schedule_events = []
        		if self.config.schedule_type == 0: ########### lambda for scaleup and VM with startup latency #######
        			if self.config.optimiztion_type = 0 # Cost optimized, choose top most accuracy avaialble
        				(vm_cost_estimation, lambda_cost_estimation) = \
        					get_cost_optimized_configuration_value( vm_filtered_configuration, \
        						lambda_filtered_configuration)
        				top_kth =  1
        				resheduling_count = 0 # we have to reshedule to all available memories in VM
        				vm_memory_size_idx =0
        				vm_model_idx = 0 
        				lambda_memory_size_idx = 0 
        				lambda_model_idx = 0
        				vm_scheduled = False
        				(vm_memory_size_idx, vm_model_idx, vm_scheduled, vm, lambda_memory_size_idx, lambda_model_idx) = \
        				get_best_possible_vm_and_lambda_config(vm_cost_estimation, lambda_cost_estimation)

        				if (vm_scheduled):
	    					vm_batch_size =  self.config.batch_sz[vm_filtered_configuration[vm_model_idx][vm_memory_size_idx][0]]
	    					expected_execution_time = self.config.vm_latency[vm_model_idx][vm_memory_size_idx][vm_filtered_configuration[vm_model_idx][vm_memory_size_idx][0]]
	    					print("vm_batch_size selected:", vm_batch_size)
	    					print("expected_execution_time:", expected_execution_time)
	    					
	    					while True:
	    						resheduling_count = resheduling_count + 1
		        				if (resheduling_count >= self.config.rescheduling_limit or \
		        					self.simulation.num_queued_tasks >= vm_batch_size):
		        					#schedule VM 
		        					self.simulation.num_queued_tasks -= task.num_tasks
		        					while  total_scheduled_tasks < vm_batch_size:
		        						temp_task = self.simulation.task_queue.get()
		        						task_id_list.append(temp_task.id)
		        						self.simulation.num_queued_tasks -= temp_task.num_tasks
		        						total_scheduled_tasks += temp_task.num_tasks
		        					print("Scheduling VM with num_tasks: {}, while batch_size: {}".format(total_scheduled_tasks,vm_batch_size))
		        					self.simulation.schedule_events.append((current_time, ScheduleVMEvent(vm, expected_execution_time, task_id_list)))
		        					break
			        			else :
			        				# we will wait for (remaining_time - execution_time) / 4.
			        				wait_time =  (remaining_time - expected_execution_time)/rescheduling_limit
			        				if (wait_time<0):
			        					print("Got negative waiting time, SERIOUS ISSUE")
			        				# Add event to the queue with 
			        				sleep_start_event = SleepStartEvent(self, (current_time  + wait_time) * 1000)
			        				self.simulation.event_queue.put(((current_time + start_time) * 1000, sleep_start_event))
			        				with sleep_start_event.cond:
			        					sleep_start_event.cond.wait()
			        					#Update current time after we waited for the event
			        					current_time = current_time + wait_time 

			        	# Scheduling Lambda function
			        	else:
			        		lambda_batch_size =  self.config.batch_sz[lambda_filtered_configuration[lambda_model_idx][lambda_memory_size_idx][0]]
	    					expected_execution_time = self.config.lambda_latency[lambda_model_idx][lambda_memory_size_idx][lambda_filtered_configuration[lambda_model_idx][lambda_memory_size_idx][0]]
	    					print("vm_batch_size selected:", lambda_batch_size)
	    					print("expected_execution_time:", expected_execution_time)





	        				#time.sleep()
	        # We need to wait for expected time to get enough jobs and VM freed.



		

class Simulation(object):

    #def __init__(self, workload_file, workload_type, configuration, chedule_type, load_tracking, burst_threshold ):
    def __init__(self, workload_file, workload_type, configuration):
    	self.configuration =  configuration
        self.workload_file = workload_file
        self.tasks_file = open(self.workload_file, 'r') # file handle for workload_file

        self.tasks = defaultdict()
        self.task_arrival = defaultdict(list)
        self.event_queue = Queue.PriorityQueue()
        self.VMs = defaultdict(lambda: np.ndarray(0))
        self.completed_VMs = defaultdict(list)
        self.lambdas = defaultdict()
        self.workload_type = workload_type # SPOCK or BATACH
        self.task_queue = Queue() #Syncronized queue, no need to take lock in mutlitthread
        self.num_queued_tasks = 0 # Since multiprocessing queue doesnt support iteration, we have to keep total task count
        self.f = open(self.configuration.VM_stats_path,'w')
		# self.chedule_type = chedule_type 
		# self.load_tracking = load_tracking 
		# self.burst_threshold = burst_threshold 
		self.finished_file = open(self.configuration.finished_file_path,'w')
		self.tasks_file = open(self.configuration.all_tasks_path,'w')
		self.load_file = open(self.configuration.load_file_path,'w')
		self.current_time = 0 
        
        j = 0
        while j < self.configuration.INITIAL_WORKERS:
            i = 0
            while i < 3:
                self.VMs.setdefault(i, []).append(VM(self,0,start_up_delay,i,4,self.config.vm_available_memory[i], \
                	self.config.vm_cost[i], False, len(self.VMs[i])))
                i += 1
            j += 1

    def run(self):
    	seen_end_of_file= False
        line = self.tasks_file.readline()
        start_time = 0
        num_tasks = 0
        task_type = 0
        # print line
        if self.workload_type == "tweeter_BATCH":
        	start_time = float(line.split('\n')[0])
        	num_tasks = 1
        	task_type = 1
        else: 
	        start_time = float(line.split(',')[0])
	        num_tasks = line.split(',')[3]
	        task_type = line.split(',')[2]

	    self.event_queue.put((start_time * 1000, TaskArrival(self,
            start_time * 1000, num_tasks, task_type)))
        last_time = 0
        self.event_queue.put(((start_time*1000)+60000, PeriodicTimerEvent(self)))
        self.event_queue.put(((start_time*1000)+60000, VM_Monitor_Event(self)))
        while True : 
        	if not self.event_queue.empty():
	            (current_time, event) = self.event_queue.get()
	            print current_time, event, self.event_queue.qsize()
	            #assert current_time >= last_time
	            last_time = current_time
	            (new_events, status) = event.run(current_time)
	            if ((new_events is None) and status == False):
	            	seen_end_of_file =  True #This means everything in trace file is read completely
	            else: 
	            	for new_event in new_events:
	                self.event_queue.put(new_event)
	        if (self.event_queue.empty() and self.task_queue.empty() and seen_end_of_file):
	        	break

        self.tasks_file.close()
        # Done with queueing all the task in the task_queue
        





def get_args():
    parser =argparse.ArgumentParser()
    parser.add_argument('--trace_name', type = str, default = 'wiki', help = "Name of the traces (1. wiki, 2.berkeley, 3. tweeter, 4. tweeter_BATCH")
    parser.add_argument('--slo', type=float, default = 100 help='The SLO value in millisecond')
    parser.add_argument('--optimiztion_type', type = int, default = 0, help = "0 for SLO optimiztion, 1 for cost optimiztion")
    parser.add_argument('--scheduling_type', type=float, default = 0 help='0 for normal, 1 for LinearRegression')
    return parser.parse_args()

random.seed(1)



if __name__=="__main__": 
	 
	logging.basicConfig(filename='events_log.log',filemode='w',\
		format='%(levelname)s:%(asctime)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',level=logging.INFO)
	args = get_args()
	for i in batch_sz:
		print(i)
	#sim = Simulation(sys.argv[1], , ,int(sys.argv[2]),int(sys.argv[3] args.), float(sys.argv[4]))
	config = Configuration_cls()
	config.SLO = args.slo
	config.optimiztion_type = args.optimiztion_type
	config.scheduling_type =  args.scheduling_type
	scheduler = Scheduler_cls(config)
	sim = Simulation(sys.argv[1], args.trace_name, config)
	sim.run()
	scheduler.start() # Starting scheduler thread
	sim.f.close()
	sim.load_file.close()