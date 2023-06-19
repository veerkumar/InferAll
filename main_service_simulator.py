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
import pandas as pd
from sklearn.linear_model import LinearRegression

from config import *
from utils import *
from vm import *
from lambda_serverless import *
from periodic_events import *
from task import *


# This class used to schedule task to VM or lambda worker
class Scheduler_cls(object):
	"""docstring for Scheduler_cls"""
	def __init__(self, config):
		super(Scheduler_cls, self).__init__()
		self.config = config
		self.clear_internal_state()
		self.last_task=None


	def set_simulation_obj(self,simulation):
		self.simulation =  simulation

	def get_filtered_config_given_time(self, time_limit):
		'''
			This function will generate best possilbe congfiguration for given "Remaining time".
			Given "Remaining time" and for each accuracy level, get maximum batch size we can go to meet SLO
		'''
		#VM calculation
		# Assumption is that, each VM is running contatiner for all accuracy level (But only one is working at a time)
		# since rows in profiling are shorted based on model accuracy and time_requriement, we dont have to do much, 
		# Just filter the result, 3rd dimension is 1d because all batchsize latency are in sorted order
		vm_filtered_config = np.zeros(( len(self.config.vm_models),len(self.config.vm_available_memory), 1),dtype=int)
		lambda_filtered_config = np.zeros((len(self.config.lambda_available_memory), len(self.config.lambda_models), 1),dtype=int)
		# for i, memory in enumerate(self.config.vm_models):
		# 	#print("i ",i)
		# 	for j , model in enumerate(self.config.vm_available_memory):
		# 		#print("j",j)
		# 		for k, batch in enumerate(self.config.batch_sz):
		# 			vm_filtered_config[i][j][0] = -1

		for i, memory in enumerate(self.config.vm_models):
			#print("i ",i)
			for j , model in enumerate(self.config.vm_available_memory):
				#print("j",j)
				for k, batch in enumerate(self.config.batch_sz):
					#print("k",k)
					if (self.config.vm_latency[i][j][k] < time_limit):
						vm_filtered_config[i][j][0] = k
					else: break
		# Lambda
		# for i,  memory in enumerate(self.config.lambda_available_memory):
		# 	for j , model in enumerate(self.config.lambda_models):
		# 		for k, batch in enumerate(self.config.batch_sz):
		# 			lambda_filtered_config[i][j][0] = -1

		for i,  memory in enumerate(self.config.lambda_models):
			for j , model in enumerate(self.config.lambda_available_memory):
				for k, batch in enumerate(self.config.batch_sz):
					if (self.config.lambda_latency[i][j][k] < time_limit):
						lambda_filtered_config[i][j][0] = k
					else: break
		return (vm_filtered_config, lambda_filtered_config)


		#Lambda calculation

		# Lower accuracy model help us in : Meeting the SLO 

	def get_cost_optimized_config_value_given_SLO(self, vm_filtered_config,  lambda_filtered_config):
		# This function returns ordered(by cost) dictnory
		#Input to this function is maximized on Batch size(while satisfing SLO at same time) on each memory,
		# we will calculate per unit (image) cost
		vm_cost_estimation = np.zeros((len(self.config.vm_models), len(self.config.vm_available_memory)),dtype=float)
		lambda_cost_estimation = np.zeros((len(self.config.lambda_models), len(self.config.lambda_available_memory)), dtype=float)
		for i , model in enumerate(self.config.vm_models):
			for j, memory in enumerate(self.config.vm_available_memory):
				#This is per image cost, which will help in favouring larger batch size
				if (self.config.vm_latency[i][j][int(vm_filtered_config[i][j][0])] == self.config.MAX or (self.config.vm_latency[i][j][int(vm_filtered_config[i][j][0])] >= self.simulation.config.SLO)):
				#if (self.config.vm_latency[i][j][int(vm_filtered_config[i][j][0])] == self.config.MAX):
					vm_cost_estimation[i][j] =  self.config.MAX
				else:
					vm_cost_estimation[i][j] = \
					((((self.config.vm_latency[i][j][int(vm_filtered_config[i][j][0])])*self.config.MILL_TO_HR)*self.config.vm_cost[j])/self.config.batch_sz[vm_filtered_config[i][j][0]])

		for i , model in enumerate(self.config.lambda_models):
			for j, memory in enumerate(self.config.lambda_available_memory):
				if (self.config.lambda_latency[i][j][int(lambda_filtered_config[i][j][0])] == self.config.MAX or (self.config.lambda_latency[i][j][int(lambda_filtered_config[i][j][0])] >= self.simulation.config.SLO)):
				#if (self.config.lambda_latency[i][j][int(lambda_filtered_config[i][j][0])] == self.config.MAX):
					lambda_cost_estimation[i][j] =  self.config.MAX
				else:
					lambda_cost_estimation[i][j] = (lambda_cost(1, memory/1024, (self.config.lambda_latency[i][j][int(lambda_filtered_config[i][j][0])])/1000)/self.config.batch_sz[lambda_filtered_config[i][j][0]])
		return (vm_cost_estimation, lambda_cost_estimation)
	# def get_slo_latency_optimized_config_value(self, vm_filtered_config,  lambda_filtered_config):

	def get_top_kth_cost_config (self, vm_cost_estimation, lambda_cost_estimation, top_kth):
		# This function will return top Kth config for VM and lambda 
		# (since lambda functions are available in abandannce, just return 1st always)
		# Also assumption is Every VM is able to run all type of accuracy model, which is not an limitation as based
		# on the type of optimization, we can start VM with appropriate config
		i =  0;
		vm_memory_size_idx = -1
		lambda_memory_size_idx = -1

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
		vm_model_idx = -1
		vm_memory_size_idx = -1
		done =  0
		i = 0
		for key in temp_cost1:
			#print("")
			for j in temp_cost1[key]:
				if (j != self.simulation.config.MAX ):
					i = i + 1
					if (i >= top_kth):
						vm_memory_size_idx = temp_cost1[key][j]
						vm_model_idx = key
						done = 1
			#	print(temp_cost1[key][j], end='')
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
		lambda_model_idx = -1
		lambda_memory_size_idx = -1
		done =  0
		i = 0
		top_kth = 1 # Since we always have enough supply of lambda funcitons, so always get the best
		for key in temp_cost1:
			#print("")
			for j in temp_cost1[key]:
				if (j != self.simulation.config.MAX):
					i = i + 1
					if (i >= top_kth):
						lambda_memory_size_idx = temp_cost1[key][j]
						lambda_model_idx = key
						done = 1
			#	print (temp_cost1[key][j], end=' ')
				if(done == 1):
					break
			if(done ==1 ):
				break

		# if (top_kth > len(self.config.vm_available_memory)-1):
		# 	return (-1, lambda_memory_size_idx) # we ran out of VM space
		return (vm_memory_size_idx, vm_model_idx, lambda_memory_size_idx, lambda_model_idx)

	def check_free_vm(self, memory):
		#j = 0
		found = False

		for index in range(len(self.simulation.config.vm_available_memory)):
			width = len(self.simulation.VMs[index])
			k=0
			if (width > 0 and self.simulation.VMs[index][0].vmem == memory):
				while k < width:
					if (self.simulation.VMs[index][k].spin_up == True and (self.simulation.VMs[index][k].used_slots < self.simulation.VMs[index][k].max_slots)):
					#if (self.simulation.VMs[index][k].spin_up == True and self.simulation.VMs[index][k].isIdle == True):
						found = True
						if (self.simulation.config.debug == 1):
							print("Found Free VM")
						return (self.simulation.VMs[index][k], found)
					k+=1
				break
		if (found == False):
			return (None, found)

	def display_vm_status (self):
		#j=0
		for index in range(len(self.simulation.config.vm_available_memory)):
			width = len(self.simulation.VMs[index])
			k=0
			while k < width:
				print( "Memory: {}, worker: {}, spin_up: {}, max_slots: {}, self.used_lots:{}, lastIdleTime:{}, next_available_time:{}".format(index,k,self.simulation.VMs[index][k].spin_up, 
					self.simulation.VMs[index][k].max_slots, self.simulation.VMs[index][k].used_slots, self.simulation.VMs[index][k].lastIdleTime, self.simulation.VMs[index][k].next_available_time))
				k+=1


	def get_best_possible_vm_and_lambda_config(self, vm_cost_estimation, lambda_cost_estimation):
		vm_memory_size_idx =0
		vm_model_idx = 0 
		lambda_memory_size_idx = 0 
		lambda_model_idx = 0
		vm_scheduled = False
		top_kth = 1

		vm_infeasible = False
		lambda_infeasible =  False
		while True: 
			(vm_memory_size_idx, vm_model_idx, lambda_memory_size_idx, lambda_model_idx) = \
					 self.get_top_kth_cost_config (vm_cost_estimation, lambda_cost_estimation, top_kth)
					 #TODO Check VM avialblity
			if( vm_model_idx < 0):
				vm_memory_size_idx =0
				vm_model_idx = 0
				break
			if (vm_model_idx >= 0):
				if (self.config.scheduling_type == 0 or self.config.scheduling_type == 1):
					self.simulation.stats.update_popular_vm_stats(vm_model_idx, vm_memory_size_idx, self.vm_filtered_config[vm_model_idx][vm_memory_size_idx][0])
				if (self.config.scheduling_type == 0 or self.config.scheduling_type == 2):
					self.simulation.stats.update_popular_lambda_stats(lambda_model_idx, lambda_memory_size_idx, self.lambda_filtered_config[lambda_model_idx][lambda_memory_size_idx][0])
				(vm, found) = self.check_free_vm(self.config.vm_available_memory[vm_memory_size_idx])
				if(found == True):
					vm_scheduled = True
					return (vm_memory_size_idx, vm_model_idx, vm_scheduled, vm, lambda_memory_size_idx, lambda_model_idx)
				else:
					if (self.simulation.config.debug == 1):
						print("Scheduler_thread: Best memory is not available, finding next:",top_kth+1)
					if (top_kth >= len(self.config.vm_available_memory)):
						if (self.simulation.config.debug == 1):
							print("All VMs are busy, Lambda is only possibility")
						return (0, 0, False, None, lambda_memory_size_idx, lambda_model_idx)
					top_kth = top_kth + 1
		if (self.config.scheduling_type == 0 or self.config.scheduling_type == 2):
			self.simulation.stats.update_popular_lambda_stats(lambda_model_idx, lambda_memory_size_idx, self.lambda_filtered_config[lambda_model_idx][lambda_memory_size_idx][0])
		return (vm_memory_size_idx, vm_model_idx, False, None, lambda_memory_size_idx, lambda_model_idx)

	def assign_approximate_workload_capacity_per_slot(self):
		(vm_filtered_config, lambda_filtered_config) =  self.get_filtered_config_given_time(self.simulation.config.SLO)
		# Get highest accuracy model 
		model_idx= len(self.simulation.config.vm_models) - 1
		for index in range(len(self.simulation.config.vm_available_memory)):
			width = len(self.simulation.VMs[index])
			k=0
			while k < width:
				self.simulation.VMs[index][k].approx_workload_per_slot = self.simulation.config.batch_sz[vm_filtered_config[model_idx][index][0]]
				self.simulation.VMs[index][k].max_slots = self.simulation.config.vm_max_slots[model_idx][index][vm_filtered_config[model_idx][index][0]]
				k+=1
	
	def calculate_current_workload_capacity(self):
		workload = 0
		for index in range(len(self.simulation.config.vm_available_memory)):
			width = len(self.simulation.VMs[index])
			k=0
			while k < width:
				workload += (self.simulation.VMs[index][k].approx_workload_per_slot*self.simulation.VMs[index][k].max_slots)
				k+=1
		return workload

	def get_expected_vm_waiting_time(self):
		wait_time = 100000000000
		for index in range(len(self.simulation.config.vm_available_memory)):
			width = len(self.simulation.VMs[index])
			k=0
			while k < width:
				if (wait_time > self.simulation.VMs[index][k].next_available_time):
					wait_time = self.simulation.VMs[index][k].next_available_time
				k+=1
		#print ("Wait time : ", wait_time)
		return wait_time


	def clear_internal_state(self):
		#print( "Clearing internal state")
		self.resheduling_count = 0
		self.remaining_time = 0
		self.top_kth =  1
		self.resheduling_count = 0 # we have to reshedule to all available memories in VM
		self.vm_memory_size_idx =0
		self.vm_model_idx = 0 
		self.lambda_memory_size_idx = 0 
		self.lambda_model_idx = 0
		self.vm_scheduled = False
		self.vm_filtered_config = None
		self.lambda_filtered_config = None
		self.vm_cost_estimation = None
		self.lambda_cost_estimation = None
		self.vm = None # VM to be used if available 
		self.initial_submitted_jobs =  0
		self.temp_task = None
		self.task_id_list = []
		self.last_task = None

		#self.last_task = None


	def run(self, task, current_time):
		#print("Running Scheduler thread")
		
		new_events = []

		if(self.last_task is None):
			#print("Fresh in scheduler")
			self.last_task = task
			
			self.remaining_time =  self.config.SLO - (current_time - task.start_time)
			if(self.remaining_time < 0):
				print("Remaing time is negative")
				return (False,None,-1)


			# print("In Scheduler: current_time: {}, task.start_time {}, remaining_time {}".format(current_time,task.start_time, self.remaining_time))
			# optimize on VM or Lambda selection
			(self.vm_filtered_config, self.lambda_filtered_config) =  self.get_filtered_config_given_time(self.remaining_time)
				
		if self.config.scheduling_type == 0: ########### lambda for scaleup and VM with startup latency #######
			if self.config.optimiztion_type == 0: # Cost optimized, choose top most accuracy avaialble

				# because cost is not going to change as suggested batch_size is same
				(self.vm_cost_estimation, self.lambda_cost_estimation) = self.get_cost_optimized_config_value_given_SLO( self.vm_filtered_config, \
							self.lambda_filtered_config)
				
				self.initial_submitted_jobs = self.simulation.num_queued_tasks
				self.temp_task = task
				while True:
					if (self.simulation.config.debug == 1):
						print("current_time ----------------------------------:",current_time)
					if (self.temp_task is None):
						break
					#These values may change if some EndTaskevent get processed, hence we ned to calculate everytime
					(self.vm_memory_size_idx, self.vm_model_idx, self.vm_scheduled, self.vm, self.lambda_memory_size_idx, self.lambda_model_idx) = \
					self.get_best_possible_vm_and_lambda_config(self.vm_cost_estimation, self.lambda_cost_estimation)
					vm_batch_size_idx = self.vm_filtered_config[self.vm_model_idx][self.vm_memory_size_idx][0]
					vm_batch_size =  self.config.batch_sz[vm_batch_size_idx]
					lambda_batch_size_idx =  self.lambda_filtered_config[self.lambda_model_idx][self.lambda_memory_size_idx][0]
					lambda_batch_size =  self.config.batch_sz[lambda_batch_size_idx]
						
					expected_vm_execution_time = self.config.vm_latency[self.vm_model_idx][self.vm_memory_size_idx][vm_batch_size_idx]
					expected_lambda_execution_time = self.config.lambda_latency[self.lambda_model_idx][self.lambda_memory_size_idx][lambda_batch_size_idx]
					
					if (self.simulation.config.debug == 1):
						print ("All values:",(self.vm_model_idx, self.vm_memory_size_idx, self.vm_scheduled, self.vm,  self.lambda_model_idx, self.lambda_memory_size_idx))
						print("vm_batch_size selected:{} ".format(vm_batch_size))
						print("Lambda_batch_size selected:{}".format(lambda_batch_size))
						print("expected_VM_execution_time:", expected_vm_execution_time)
						print("expected_LAMBDA_execution_time:", expected_lambda_execution_time)
						print("number of task: ",self.simulation.num_queued_tasks)

					
					
					if( not self.vm is None):
						if(self.vm.approx_workload_per_slot == 0):
							self.vm.approx_workload_per_slot = max(vm_batch_size, self.vm.approx_workload_per_slot)
						else:
							self.vm.approx_workload_per_slot = (self.vm.approx_workload_per_slot + vm_batch_size)/2

					if (self.simulation.config.debug == 1):
						if (current_time >=6000 and current_time <= 7000):
							print ("Reached the point")

					if (self.vm_scheduled):
						if (self.simulation.num_queued_tasks >= vm_batch_size) :
						#Means there are enough jobs but we need to fetch more from the queue
							total_scheduled_tasks = 0
							while (total_scheduled_tasks < vm_batch_size):
								if (self.temp_task.num_tasks >= (vm_batch_size - total_scheduled_tasks)):
									#it means this task need to split
									num_of_tasks_scheduled = (vm_batch_size - total_scheduled_tasks)
									total_scheduled_tasks += num_of_tasks_scheduled
									self.simulation.num_queued_tasks -= num_of_tasks_scheduled
									self.task_id_list.append((self.temp_task.id, num_of_tasks_scheduled))
									self.temp_task.num_tasks -= num_of_tasks_scheduled
									# Marking VM as busy so that it wont be selected twice at same current_time,even thought event will be executed later
									self.vm.isIdle = False
									self.vm.used_slots +=1
									self.vm.next_available_time = current_time + expected_vm_execution_time
									#print("Scheduling VM with num_tasks: {}, while batch_size: {}".format(total_scheduled_tasks,vm_batch_size))
									new_events.append((current_time, ScheduleVMEvent(self.vm, vm_batch_size_idx, self.vm_model_idx, self.vm_memory_size_idx, self.task_id_list)))
									self.simulation.stats.update_used_vm_stats(self.vm_model_idx, self.vm_memory_size_idx, vm_batch_size_idx)
									self.simulation.task_arrival.setdefault(self.vm_memory_size_idx, []).append([int(current_time),num_of_tasks_scheduled])
									self.task_id_list = []
									if (self.temp_task.num_tasks == 0 and (self.simulation.task_queue.empty() == False)):
										self.temp_task = self.simulation.task_queue.get()
									break
								else:
									num_of_tasks_scheduled =  self.temp_task.num_tasks
									total_scheduled_tasks += self.temp_task.num_tasks
									self.simulation.num_queued_tasks -= num_of_tasks_scheduled
									self.task_id_list.append((self.temp_task.id, num_of_tasks_scheduled))
									self.temp_task.num_tasks -= num_of_tasks_scheduled

								if (self.temp_task.num_tasks == 0 and (self.simulation.task_queue.empty() == False)):
									self.temp_task = self.simulation.task_queue.get()

							if(self.simulation.num_queued_tasks > 0):
								continue #There are more jobs to schedule
							else:
								return (True, new_events, self.simulation.next_task_time + 1, None)

						else: # Number of available tasks are lesser than batch size
							#Find approximate waiting time 
							wait_time =  (self.remaining_time - expected_vm_execution_time)
							next_task_arrival_time = self.simulation.next_task_time
							total_scheduled_tasks = 0

							#This condition is introduced to make simulation faster otherwise we can sleep for wait_time and wakeup
							# again to see the task queue as in real world next_task_arrival_time wont be availble.
							if (next_task_arrival_time >= wait_time):
								#it means there is not point in waiting for next task
								while (total_scheduled_tasks <= vm_batch_size):
									num_of_tasks_scheduled =  self.temp_task.num_tasks
									total_scheduled_tasks += self.temp_task.num_tasks
									self.simulation.num_queued_tasks -= num_of_tasks_scheduled
									self.task_id_list.append((self.temp_task.id, num_of_tasks_scheduled))
									if (self.simulation.task_queue.empty() == False):
										self.temp_task = self.simulation.task_queue.get()
									else:
										break

								self.vm.isIdle = False
								self.vm.used_slots +=1
								self.vm.next_available_time = current_time + expected_vm_execution_time
								new_events.append((current_time, ScheduleVMEvent(self.vm, vm_batch_size_idx, self.vm_model_idx, self.vm_memory_size_idx, self.task_id_list)))
								self.simulation.stats.update_used_vm_stats(self.vm_model_idx, self.vm_memory_size_idx, vm_batch_size_idx)
								self.simulation.task_arrival.setdefault(self.vm_memory_size_idx, []).append([int(current_time), total_scheduled_tasks])
								self.temp_task = None
								self.clear_internal_state()
								return (True, new_events, next_task_arrival_time + 1, self.temp_task)
							else :
								self.last_task = self.temp_task
								# we need to wait for
								return (False, new_events, next_task_arrival_time + 1, self.temp_task)
					
					else:
						if (self.simulation.config.debug == 1):
							print("lambda_batch_size selected:", lambda_batch_size)
							print("lambda expected_execution_time:", expected_lambda_execution_time)

							print("Total vm Handling capacity", self.calculate_current_workload_capacity())

						if( self.initial_submitted_jobs > self.calculate_current_workload_capacity()):
							if (not self.simulation.config.VM_PREDICTION):
								idx = self.simulation.stats.get_most_popular_vm_mem_idx()
								if (self.simulation.config.debug == 1):
									print("Adding new VM",idx)
								# Need to scale up the VM's, currenlty scalling only 4GB vms
								vm = VM(self.simulation, self.simulation.config.start_up_delay + current_time, self.simulation.config.start_up_delay, 0, 4,self.simulation.config.vm_available_memory[idx], self.simulation.config.vm_cost[idx], False, len(self.simulation.VMs[idx]))
								vm.approx_workload_per_slot = self.simulation.VMs[idx][0].approx_workload_per_slot
								vm.max_slots = self.simulation.VMs[idx][0].max_slots
								self.simulation.VMs[idx].append(vm)
								new_events.append((current_time+self.simulation.config.start_up_delay,VMCreateEvent(self.simulation, self.simulation.VMs[idx][-1],0)))

						if (self.simulation.num_queued_tasks >= lambda_batch_size):
							total_scheduled_tasks = 0
							while  (total_scheduled_tasks < lambda_batch_size):
								if (self.temp_task.num_tasks >= (lambda_batch_size - total_scheduled_tasks)):
									#it means this task need to split
									num_of_tasks_scheduled = (lambda_batch_size - total_scheduled_tasks)
									total_scheduled_tasks += num_of_tasks_scheduled
									self.simulation.num_queued_tasks -= num_of_tasks_scheduled
									self.task_id_list.append((self.temp_task.id, num_of_tasks_scheduled))
									self.temp_task.num_tasks -= num_of_tasks_scheduled
									

									lambda_worker =  Lambda(current_time, self.simulation, self.config, lambda_batch_size_idx, self.lambda_model_idx, self.lambda_memory_size_idx)
									self.simulation.lambdas.setdefault(0,[]).append(lambda_worker)
									new_events.append((current_time, ScheduleLambdaEvent(lambda_worker, self.task_id_list)))
									self.simulation.stats.update_used_lambda_stats(self.lambda_model_idx, self.lambda_memory_size_idx, lambda_batch_size_idx)
									self.simulation.task_arrival.setdefault(0, []).append([int(current_time), total_scheduled_tasks])
									
									self.task_id_list = []
									if (self.temp_task.num_tasks == 0 and (self.simulation.task_queue.empty() == False)):
										self.temp_task = self.simulation.task_queue.get()
									break
								else:
									num_of_tasks_scheduled =  self.temp_task.num_tasks
									total_scheduled_tasks += self.temp_task.num_tasks
									self.simulation.num_queued_tasks -= num_of_tasks_scheduled
									self.task_id_list.append((self.temp_task.id, num_of_tasks_scheduled))
									self.temp_task.num_tasks -= num_of_tasks_scheduled

								if (self.temp_task.num_tasks == 0 and (self.simulation.task_queue.empty() == False)):
										self.temp_task = self.simulation.task_queue.get()
								
							if(self.simulation.num_queued_tasks > 0):
								continue #There are more jobs to schedule
							else:
								return (True, new_events, self.simulation.next_task_time + 1, None)

						else: # Number of available tasks are lesser than batch size
							#Find approximate waiting time 
							wait_time =  (self.remaining_time - expected_lambda_execution_time)
							next_task_arrival_time = self.simulation.next_task_time
							total_scheduled_tasks = 0

							#This condition is introduced to make simulation faster otherwise we can sleep for wait_time and wakeup
							# again to see the task queue as in real world next_task_arrival_time wont be availble.
							if (next_task_arrival_time >= wait_time):
								#it means there is not point in waiting for next task
								while (total_scheduled_tasks <= lambda_batch_size):
									num_of_tasks_scheduled =  self.temp_task.num_tasks
									total_scheduled_tasks += self.temp_task.num_tasks
									self.simulation.num_queued_tasks -= num_of_tasks_scheduled
									self.task_id_list.append((self.temp_task.id, num_of_tasks_scheduled))
									if (self.simulation.task_queue.empty() == False):
										self.temp_task = self.simulation.task_queue.get()
									else:
										break

								lambda_worker =  Lambda(current_time, self.simulation, self.config, lambda_batch_size_idx, self.lambda_model_idx, self.lambda_memory_size_idx)
								self.simulation.lambdas.setdefault(0,[]).append(lambda_worker)
								new_events.append((current_time, ScheduleLambdaEvent(lambda_worker, self.task_id_list)))
								self.simulation.stats.update_used_lambda_stats(self.lambda_model_idx, self.lambda_memory_size_idx, lambda_batch_size_idx)
								self.simulation.task_arrival.setdefault(0, []).append([int(current_time), total_scheduled_tasks])
								self.temp_task = None
								self.clear_internal_state()
								return (True, new_events, next_task_arrival_time+1, self.temp_task)
							else :
								# we need to wait for
								self.last_task = self.temp_task
								return (False, new_events, next_task_arrival_time+1, self.temp_task)

		if (self.config.scheduling_type == 1): ######## schedule all in VMs ##################
			# because cost is not going to change as suggested batch_size is same
			(self.vm_cost_estimation, self.lambda_cost_estimation) = self.get_cost_optimized_config_value_given_SLO( self.vm_filtered_config, \
						self.lambda_filtered_config)
			
			self.initial_submitted_jobs = self.simulation.num_queued_tasks
			self.temp_task = task
			while True:
				if (self.simulation.config.debug == 1):
					print("current_time ----------------------------------:",current_time)
				if (self.temp_task is None):
					break
				#These values may change if some EndTaskevent get processed, hence we ned to calculate everytime
				(self.vm_memory_size_idx, self.vm_model_idx, self.vm_scheduled, self.vm, self.lambda_memory_size_idx, self.lambda_model_idx) = \
				self.get_best_possible_vm_and_lambda_config(self.vm_cost_estimation, self.lambda_cost_estimation)
				vm_batch_size_idx = self.vm_filtered_config[self.vm_model_idx][self.vm_memory_size_idx][0]
				vm_batch_size =  self.config.batch_sz[vm_batch_size_idx]
				
					
				expected_vm_execution_time = self.config.vm_latency[self.vm_model_idx][self.vm_memory_size_idx][vm_batch_size_idx]
				
				if (self.simulation.config.debug == 1):
					print ("All values:",(self.vm_model_idx, self.vm_memory_size_idx, self.vm_scheduled, self.vm))
					print("vm_batch_size selected:{} ".format(vm_batch_size))
					print("expected_VM_execution_time:", expected_vm_execution_time)
					print("number of task: ",self.simulation.num_queued_tasks)
				
				if( not self.vm is None):
					if(self.vm.approx_workload_per_slot == 0):
						self.vm.approx_workload_per_slot = max(vm_batch_size, self.vm.approx_workload_per_slot)
					else:
						self.vm.approx_workload_per_slot = (self.vm.approx_workload_per_slot + vm_batch_size)/2

				if (self.simulation.config.debug == 1):
					if (current_time >=100000 ):
						print ("Reached the point")

				if (self.vm_scheduled):
					if (self.simulation.num_queued_tasks >= vm_batch_size) :
					#Means there are enough jobs but we need to fetch more from the queue
						total_scheduled_tasks = 0
						while (total_scheduled_tasks < vm_batch_size):
							if (self.temp_task.num_tasks >= (vm_batch_size - total_scheduled_tasks)):
								#it means this task need to split
								num_of_tasks_scheduled = (vm_batch_size - total_scheduled_tasks)
								total_scheduled_tasks += num_of_tasks_scheduled
								self.simulation.num_queued_tasks -= num_of_tasks_scheduled
								self.task_id_list.append((self.temp_task.id, num_of_tasks_scheduled))
								self.temp_task.num_tasks -= num_of_tasks_scheduled
								# Marking VM as busy so that it wont be selected twice at same current_time,even thought event will be executed later
								self.vm.isIdle = False
								self.vm.used_slots +=1
								self.vm.next_available_time = current_time + expected_vm_execution_time
								#print("Scheduling VM with num_tasks: {}, while batch_size: {}".format(total_scheduled_tasks,vm_batch_size))
								new_events.append((current_time, ScheduleVMEvent(self.vm, vm_batch_size_idx, self.vm_model_idx, self.vm_memory_size_idx, self.task_id_list)))
								
								self.simulation.stats.update_used_vm_stats(self.vm_model_idx, self.vm_memory_size_idx, vm_batch_size_idx)
								self.simulation.task_arrival.setdefault(self.vm_memory_size_idx, []).append([int(current_time),num_of_tasks_scheduled])
								self.task_id_list = []
								if (self.temp_task.num_tasks == 0 and (self.simulation.task_queue.empty() == False)):
									self.temp_task = self.simulation.task_queue.get()
								break
							else:
								num_of_tasks_scheduled =  self.temp_task.num_tasks
								total_scheduled_tasks += self.temp_task.num_tasks
								self.simulation.num_queued_tasks -= num_of_tasks_scheduled
								self.task_id_list.append((self.temp_task.id, num_of_tasks_scheduled))
								self.temp_task.num_tasks -= num_of_tasks_scheduled

							if (self.temp_task.num_tasks == 0 and (self.simulation.task_queue.empty() == False)):
								self.temp_task = self.simulation.task_queue.get()

						if(self.simulation.num_queued_tasks > 0):
							continue #There are more jobs to schedule
						else:
							return (True, new_events, self.simulation.next_task_time + 1, None)

					else: # Number of available tasks are lesser than batch size
						#Find approximate waiting time 
						wait_time =  (self.remaining_time - expected_vm_execution_time)
						next_task_arrival_time = self.simulation.next_task_time
						total_scheduled_tasks = 0

						#This condition is introduced to make simulation faster otherwise we can sleep for wait_time and wakeup
						# again to see the task queue as in real world next_task_arrival_time wont be availble.
						if (next_task_arrival_time >= wait_time):
							#it means there is not point in waiting for next task
							while (total_scheduled_tasks <= vm_batch_size):
								num_of_tasks_scheduled =  self.temp_task.num_tasks
								total_scheduled_tasks += self.temp_task.num_tasks
								self.simulation.num_queued_tasks -= num_of_tasks_scheduled
								self.task_id_list.append((self.temp_task.id, num_of_tasks_scheduled))
								if (self.simulation.task_queue.empty() == False):
									self.temp_task = self.simulation.task_queue.get()
								else:
									break

							self.vm.isIdle = False
							self.vm.used_slots +=1
							self.vm.next_available_time = current_time + expected_vm_execution_time
							new_events.append((current_time, ScheduleVMEvent(self.vm, vm_batch_size_idx, self.vm_model_idx, self.vm_memory_size_idx, self.task_id_list)))
							self.simulation.stats.update_used_vm_stats(self.vm_model_idx, self.vm_memory_size_idx, vm_batch_size_idx)
							self.simulation.task_arrival.setdefault(self.vm_memory_size_idx, []).append([int(current_time), total_scheduled_tasks])
							self.temp_task = None
							self.clear_internal_state()
							return (True, new_events, next_task_arrival_time + 1, self.temp_task)
						else :
							self.last_task = self.temp_task
							# we need to wait for
							return (False, new_events, next_task_arrival_time + 1, self.temp_task)
				else :
					# we have to put this request to waiting
					#print("in waiting area")
					if( self.initial_submitted_jobs > self.calculate_current_workload_capacity()):
						if (not self.simulation.config.VM_PREDICTION):
							idx = self.simulation.stats.get_most_popular_vm_mem_idx()
							if (self.simulation.config.debug == 1):
								print("Adding new VM",idx)
							# Need to scale up the VM's, currenlty scalling only 4GB vms
							vm = VM(self.simulation, self.simulation.config.start_up_delay + current_time, self.simulation.config.start_up_delay, 0, 4,self.simulation.config.vm_available_memory[idx], self.simulation.config.vm_cost[idx], False, len(self.simulation.VMs[idx]))
							vm.approx_workload_per_slot = self.simulation.VMs[idx][0].approx_workload_per_slot
							vm.max_slots = self.simulation.VMs[idx][0].max_slots
							self.simulation.VMs[idx].append(vm)
							new_events.append((current_time+self.simulation.config.start_up_delay,VMCreateEvent(self.simulation, self.simulation.VMs[idx][-1],0)))

					wait_time = self.get_expected_vm_waiting_time() # this wait time includes current time too
					self.last_task = self.temp_task
					# added 6 as 5 is prob time in schedule_vm event
					return (False, new_events, wait_time+6, self.temp_task)
					

		if (self.config.scheduling_type == 2): ######## schedule all in lambdas ##################
			# because cost is not going to change as suggested batch_size is same
				(self.vm_cost_estimation, self.lambda_cost_estimation) = self.get_cost_optimized_config_value_given_SLO( self.vm_filtered_config, \
							self.lambda_filtered_config)
				
				self.initial_submitted_jobs = self.simulation.num_queued_tasks
				self.temp_task = task
				while True:
					if (self.simulation.config.debug == 1):
						print("current_time ----------------------------------:",current_time)
					if (self.temp_task is None):
						break
					#These values may change if some EndTaskevent get processed, hence we ned to calculate everytime
					(self.vm_memory_size_idx, self.vm_model_idx, self.vm_scheduled, self.vm, self.lambda_memory_size_idx, self.lambda_model_idx) = \
					self.get_best_possible_vm_and_lambda_config(self.vm_cost_estimation, self.lambda_cost_estimation)
					
					lambda_batch_size_idx =  self.lambda_filtered_config[self.lambda_model_idx][self.lambda_memory_size_idx][0]
					lambda_batch_size =  self.config.batch_sz[lambda_batch_size_idx]
						
					expected_lambda_execution_time = self.config.lambda_latency[self.lambda_model_idx][self.lambda_memory_size_idx][lambda_batch_size_idx]
					
					if (self.simulation.config.debug == 1):
						print ("All values:",(self.lambda_model_idx, self.lambda_memory_size_idx))
						print("Lambda_batch_size selected:{}".format(lambda_batch_size))
						print("expected_LAMBDA_execution_time:", expected_lambda_execution_time)
						print("number of task: ",self.simulation.num_queued_tasks)
					
					
					if (self.simulation.config.debug == 1):
						if (current_time >=6000 and current_time <= 7000):
							print ("Reached the point")

					
					if (self.simulation.config.debug == 1):
						print("lambda_batch_size selected:", lambda_batch_size)
						print("lambda expected_execution_time:", expected_lambda_execution_time)

						print("Total vm Handling capacity", self.calculate_current_workload_capacity())

					if (self.simulation.num_queued_tasks >= lambda_batch_size):
						total_scheduled_tasks = 0
						while  (total_scheduled_tasks < lambda_batch_size):
							if (self.temp_task.num_tasks >= (lambda_batch_size - total_scheduled_tasks)):
								#it means this task need to split
								num_of_tasks_scheduled = (lambda_batch_size - total_scheduled_tasks)
								total_scheduled_tasks += num_of_tasks_scheduled
								self.simulation.num_queued_tasks -= num_of_tasks_scheduled
								self.task_id_list.append((self.temp_task.id, num_of_tasks_scheduled))
								self.temp_task.num_tasks -= num_of_tasks_scheduled
								

								lambda_worker =  Lambda(current_time, self.simulation, self.config, lambda_batch_size_idx, self.lambda_model_idx, self.lambda_memory_size_idx)
								self.simulation.lambdas.setdefault(0,[]).append(lambda_worker)
								new_events.append((current_time, ScheduleLambdaEvent(lambda_worker, self.task_id_list)))
								self.simulation.stats.update_used_lambda_stats(self.lambda_model_idx, self.lambda_memory_size_idx, lambda_batch_size_idx)
								self.simulation.task_arrival.setdefault(0, []).append([int(current_time), total_scheduled_tasks])
								
								self.task_id_list = []
								if (self.temp_task.num_tasks == 0 and (self.simulation.task_queue.empty() == False)):
									self.temp_task = self.simulation.task_queue.get()
								break
							else:
								num_of_tasks_scheduled =  self.temp_task.num_tasks
								total_scheduled_tasks += self.temp_task.num_tasks
								self.simulation.num_queued_tasks -= num_of_tasks_scheduled
								self.task_id_list.append((self.temp_task.id, num_of_tasks_scheduled))
								self.temp_task.num_tasks -= num_of_tasks_scheduled

							if (self.temp_task.num_tasks == 0 and (self.simulation.task_queue.empty() == False)):
									self.temp_task = self.simulation.task_queue.get()
							
						if(self.simulation.num_queued_tasks > 0):
							continue #There are more jobs to schedule
						else:
							return (True, new_events, self.simulation.next_task_time + 1, None)

					else: # Number of available tasks are lesser than batch size
						#Find approximate waiting time 
						wait_time =  (self.remaining_time - expected_lambda_execution_time)
						next_task_arrival_time = self.simulation.next_task_time
						total_scheduled_tasks = 0

						#This condition is introduced to make simulation faster otherwise we can sleep for wait_time and wakeup
						# again to see the task queue as in real world next_task_arrival_time wont be availble.
						if (next_task_arrival_time >= wait_time):
							#it means there is not point in waiting for next task
							while (total_scheduled_tasks <= lambda_batch_size):
								num_of_tasks_scheduled =  self.temp_task.num_tasks
								total_scheduled_tasks += self.temp_task.num_tasks
								self.simulation.num_queued_tasks -= num_of_tasks_scheduled
								self.task_id_list.append((self.temp_task.id, num_of_tasks_scheduled))
								if (self.simulation.task_queue.empty() == False):
									self.temp_task = self.simulation.task_queue.get()
								else:
									break

							lambda_worker =  Lambda(current_time, self.simulation, self.config, lambda_batch_size_idx, self.lambda_model_idx, self.lambda_memory_size_idx)
							self.simulation.lambdas.setdefault(0,[]).append(lambda_worker)
							new_events.append((current_time, ScheduleLambdaEvent(lambda_worker, self.task_id_list)))
							self.simulation.stats.update_used_lambda_stats(self.lambda_model_idx, self.lambda_memory_size_idx, lambda_batch_size_idx)
							self.simulation.task_arrival.setdefault(0, []).append([int(current_time), total_scheduled_tasks])
							self.temp_task = None
							self.clear_internal_state()
							return (True, new_events, next_task_arrival_time+1, self.temp_task)
						else :
							# we need to wait for
							self.last_task = self.temp_task
							return (False, new_events, next_task_arrival_time+1, self.temp_task)
		if(self.config.scheduling_type == 3):########  VM reactive with buffer. ###############
			return (False, [], 0, self.temp_task)

class Stats(object):
	def __init__(self, simulation):
		#popular is different from "used", popular may not be available so different config might be used
		self.simulation = simulation
		self.config = self.simulation.config
		self.popular_vm_mem_sz_idx = defaultdict()
		self.popular_vm_batch_sz_idx = defaultdict()
		self.popular_vm_model_idx = defaultdict()
		self.popular_lambda_mem_sz_idx = defaultdict()
		self.popular_lambda_batch_sz_idx = defaultdict()
		self.popular_lambda_model_idx = defaultdict()

		self.used_vm_mem_sz_idx = defaultdict()
		self.used_vm_model_idx = defaultdict()
		self.used_vm_batch_sz_idx = defaultdict()
		self.used_lambda_mem_sz_idx = defaultdict()
		self.used_lambda_model_idx = defaultdict()
		self.used_lambda_batch_sz_idx = defaultdict()

		self.num_vm_created = 0
		self.num_vm_deleted = 0

		for idx in range(len(self.config.vm_models)):
			self.popular_vm_model_idx[idx] = 0
			self.used_vm_model_idx[idx] = 0

		for idx in range(len(self.config.lambda_models)):
			self.popular_lambda_model_idx[idx] = 0
			self.used_lambda_model_idx[idx] = 0

		for idx in range(len(self.config.vm_available_memory)):
			self.popular_vm_mem_sz_idx[idx] = 0
			self.used_vm_mem_sz_idx[idx] = 0

		for idx in range(len(self.config.lambda_available_memory)):
			self.popular_lambda_mem_sz_idx[idx] = 0
			self.used_lambda_mem_sz_idx[idx] = 0

		for idx in range(len(self.config.batch_sz)):
			self.popular_vm_batch_sz_idx[idx] = 0
			self.popular_lambda_batch_sz_idx[idx] = 0
			self.used_vm_batch_sz_idx[idx] = 0
			self.used_lambda_batch_sz_idx[idx] = 0

	# def update_popular_model_stats(vm_model_idx, lambda_model_idx):
	# 	self.popular_vm_model_idx[vm_model_idx] += 1
	# 	self.popular_lambda_model_idx[lambda_model_idx] += 1

	# def update_popular_memory_stats(vm_memory_size_idx, lambda_memory_size_idx):
	# 	self.popular_vm_mem_sz_idx[vm_memory_size_idx] 	+= 1
	# 	self.popular_lambda_mem_sz_idx[lambda_memory_size_idx] += 1

	# def update_popular_batch_stats(self, vm_batch_idx, lambda_batch_idx):
	# 	self.popular_vm_batch_sz_idx[vm_batch_idx] += 1
	# 	self.popular_lambda_batch_sz_idx[lambda_batch_idx] += 1

	def update_popular_lambda_stats(self, lambda_model_idx, lambda_memory_size_idx, lambda_batch_idx):
		self.popular_lambda_model_idx[lambda_model_idx] += 1
		self.popular_lambda_mem_sz_idx[lambda_memory_size_idx] += 1
		self.popular_lambda_batch_sz_idx[lambda_batch_idx] += 1

	def  update_popular_vm_stats(self, vm_model_idx, vm_memory_size_idx, vm_batch_idx):
		self.popular_vm_model_idx[vm_model_idx] += 1
		self.popular_vm_mem_sz_idx[vm_memory_size_idx] 	+= 1
		self.popular_vm_batch_sz_idx[vm_batch_idx] += 1


	def update_used_vm_stats(self, vm_model_idx, vm_mem_idx, vm_batch_idx):
		self.used_vm_model_idx[vm_model_idx] += 1
		self.used_vm_mem_sz_idx[vm_mem_idx] 	+= 1
		self.used_vm_batch_sz_idx[vm_batch_idx] += 1

	def update_used_lambda_stats(self, lambda_model_idx, lambda_mem_idx, lambda_batch_idx):
		self.used_lambda_model_idx[lambda_model_idx] += 1
		self.used_lambda_mem_sz_idx[lambda_mem_idx] += 1
		self.used_lambda_batch_sz_idx[lambda_batch_idx] += 1

	def get_most_popular_vm_mem_idx(self):
		max_value = 0
		max_value_indx = 0
		for key in self.popular_vm_mem_sz_idx:
			if(max_value < self.popular_vm_mem_sz_idx[key]):
				max_value_indx = key
				max_value = self.popular_vm_mem_sz_idx[key]

		return max_value_indx

	def display_all_stats(self, cost_file):
		print("-----------------------------------Popular choices:----------------------------------------------------")
		print("-----------------------------------Popular choices:----------------------------------------------------",file=cost_file)
		print ("	Popular VM models")
		print ("	Popular VM models",file=cost_file)
		temp_dict = OrderedDict(sorted(self.popular_vm_model_idx.items(), key=lambda item: item[1],reverse=True))
		for key in temp_dict:
			print("		"+self.config.vm_models[key],temp_dict[key])
			print("		"+self.config.vm_models[key],temp_dict[key],file=cost_file)

		print("	Popular VM memory")
		print("	Popular VM memory",file=cost_file)
		temp_dict = OrderedDict(sorted(self.popular_vm_mem_sz_idx.items(), key=lambda item: item[1],reverse=True))
		for key in temp_dict:
			print("		"+ str(self.config.vm_available_memory[key]),temp_dict[key])
			print("		"+ str(self.config.vm_available_memory[key]),temp_dict[key],file=cost_file)

		print("	Popular Top 5 VM Batch")
		print("	Popular Top 5 VM Batch",file=cost_file)
		temp_dict = OrderedDict(sorted(self.popular_vm_batch_sz_idx.items(), key=lambda item: item[1],reverse=True))
		i = 0
		for key in temp_dict:
			print("		"+ str(self.config.batch_sz[key]),temp_dict[key])
			print("		"+ str(self.config.batch_sz[key]),temp_dict[key],file=cost_file)
			if (i == 5):
				break
			i+=1

		print ("	Popular Lambda models")
		print ("	Popular Lambda models",file=cost_file)
		temp_dict = OrderedDict(sorted(self.popular_lambda_model_idx.items(), key=lambda item: item[1],reverse=True))
		for key in temp_dict:
			print("		"+self.config.vm_models[key],temp_dict[key])
			print("		"+self.config.vm_models[key],temp_dict[key],file=cost_file)

		print("	Popular lambda memory")
		print("	Popular lambda memory",file=cost_file)
		temp_dict = OrderedDict(sorted(self.popular_lambda_mem_sz_idx.items(), key=lambda item: item[1],reverse=True))
		for key in temp_dict:
			print("		"+ str(self.config.lambda_available_memory[key]),temp_dict[key])
			print("		"+ str(self.config.lambda_available_memory[key]),temp_dict[key],file=cost_file)

		print("	Popular Top 5 Lambda Batch")
		print("	Popular Top 5 Lambda Batch",file=cost_file)
		temp_dict = OrderedDict(sorted(self.popular_lambda_batch_sz_idx.items(), key=lambda item: item[1],reverse=True))
		i = 0
		for key in temp_dict:
			print("		"+ str(self.config.batch_sz[key]),temp_dict[key])
			print("		"+ str(self.config.batch_sz[key]),temp_dict[key],file=cost_file)
			if (i == 5):
				break
			i+=1

		print("-----------------------------------Most Used Config:----------------------------------------------------")
		print("-----------------------------------Most Used Config:----------------------------------------------------",file=cost_file)
		print ("	Most Used VM models")
		print ("	Most Used VM models",file=cost_file)
		temp_dict = OrderedDict(sorted(self.used_vm_model_idx.items(), key=lambda item: item[1],reverse=True))
		for key in temp_dict:
			print("		"+self.config.vm_models[key],temp_dict[key])
			print("		"+self.config.vm_models[key],temp_dict[key],file=cost_file)

		print("	Most Used VM memory")
		print("	Most Used VM memory",file=cost_file)
		temp_dict = OrderedDict(sorted(self.used_vm_mem_sz_idx.items(), key=lambda item: item[1],reverse=True))
		for key in temp_dict:
			print("		"+ str(self.config.vm_available_memory[key]),temp_dict[key])
			print("		"+ str(self.config.vm_available_memory[key]),temp_dict[key],file=cost_file)

		print("	Most Used Top 5 VM Batch")
		print("	Most Used Top 5 VM Batch",file=cost_file)
		temp_dict = OrderedDict(sorted(self.used_vm_batch_sz_idx.items(), key=lambda item: item[1],reverse=True))
		i = 0
		for key in temp_dict:
			print("		"+ str(self.config.batch_sz[key]),temp_dict[key])
			print("		"+ str(self.config.batch_sz[key]),temp_dict[key],file=cost_file)
			if (i == 5):
				break
			i+=1

		print ("	Most Used Lambda models")
		print ("	Most Used Lambda models",file=cost_file)
		temp_dict = OrderedDict(sorted(self.used_lambda_model_idx.items(), key=lambda item: item[1],reverse=True))
		for key in temp_dict:
			print("		"+self.config.vm_models[key],temp_dict[key])
			print("		"+self.config.vm_models[key],temp_dict[key],file=cost_file)

		print("	Most Used lambda memory")
		print("	Most Used lambda memory",file=cost_file)
		temp_dict = OrderedDict(sorted(self.used_lambda_mem_sz_idx.items(), key=lambda item: item[1],reverse=True))
		for key in temp_dict:
			print("		"+ str(self.config.lambda_available_memory[key]),temp_dict[key])
			print("		"+ str(self.config.lambda_available_memory[key]),temp_dict[key],file=cost_file)

		print("	Most Used Top 5 Lambda Batch")
		print("	Most Used Top 5 Lambda Batch",file=cost_file)
		temp_dict = OrderedDict(sorted(self.used_lambda_batch_sz_idx.items(), key=lambda item: item[1],reverse=True))
		i = 0
		for key in temp_dict:
			print("		"+ str(self.config.batch_sz[key]),temp_dict[key])
			print("		"+ str(self.config.batch_sz[key]),temp_dict[key],file=cost_file)
			if (i == 5):
				break
			i+=1
		


class Simulation(object):

	#def __init__(self, workload_file, workload_type, config, chedule_type, load_tracking, burst_threshold ):
	def __init__(self, workload_file, workload_type, config, scheduler):
		self.config =  config
		self.scheduler_obj = scheduler
		self.manager = multiprocessing.Manager()
		self.workload_file = "./traces/"+workload_file+".csv"
		self.workload_file_hdl = open(self.workload_file, 'r') # file handle for workload_file

		self.tasks = defaultdict()
		self.task_arrival = defaultdict(list)
		self.event_queue = PriorityQueue()
		self.VMs = defaultdict(lambda: np.ndarray(0))
		self.completed_VMs = defaultdict(list)
		self.lambdas = defaultdict()
		self.workload_type = workload_type # spock or inferall or batch 
		self.task_queue = self.manager.Queue() #Syncronized queue, no need to take lock in mutlitthread
		self.num_queued_tasks = 0 # Since multiprocessing queue doesnt support iteration, we have to keep total task count
		self.f = open(self.config.VM_stats_path,'w')
		# self.chedule_type = chedule_type 
		self.load_tracking = 0 
		# self.burst_threshold = burst_threshold 
		self.finished_file = open(self.config.finished_file_path,'w')
		self.tasks_file = open(self.config.all_tasks_path,'w')
		self.load_file = open(self.config.load_file_path,'w')
		self.current_time = 0 
		self.simulation_cond = multiprocessing.Condition()
		self.VM_PREDICTION = 0
		self.last_task = 0
		self.next_task_time = 0 #This will store next task: to optimize simulator 
		self.end_of_file = 0
		self.stats = None

		
		if (not (self.config.scheduling_type == 2)):
			j = 0
			while j < self.config.INITIAL_WORKERS:
				i = 0
				while i < len(self.config.vm_available_memory):
					self.VMs.setdefault(i, []).append(VM(self,0,self.config.start_up_delay,i,4,self.config.vm_available_memory[i], \
						self.config.vm_cost[i], True, len(self.VMs[i])))
					i += 1
				j += 1

	def set_stats_obj(self, stats_obj):
		self.stats = stats_obj


	def calculate_cost(self, end_time):
		print("Calculating cost")
		total_cost = 0
		#/ f = open(VM_stats_path,'w')
		for i in range(3):
			for j in range(len(self.VMs[i])):
				print (i,",", end_time ,",",self.VMs[i][j].start_time,",", self.VMs[i][j].lastIdleTime,file=self.f)
				total_cost+=self.VMs[i][j].price * ((end_time - self.VMs[i][j].start_time)/3600000)
		for i in range(3):
			for j in range(len(self.completed_VMs[i])):
				total_cost+=self.completed_VMs[i][j].price * ((self.completed_VMs[i][j].end_time - self.completed_VMs[i][j].start_time)/3600000)

		return total_cost
	
	def add_task_completion_time(self, task_id, num_of_tasks_completed, completion_time, isLambda):
		task_complete = \
				self.tasks[task_id].task_completed(completion_time, num_of_tasks_completed)
		if(isLambda == 1):
			self.tasks[task_id].lambda_tasks+=num_of_tasks_completed
		else:
			self.tasks[task_id].vm_tasks+=num_of_tasks_completed
		return task_complete

	def run(self):
		#print("Running simulation run")
		line = self.workload_file_hdl.readline()
		start_time = 0
		num_tasks = 0
		task_type = 0
		# print line
		if self.workload_type == "BATCH":
			start_time = float(line.split('\n')[0])
			num_tasks = 1
			task_type = 1
		else: 
			start_time = float(line.split(',')[0])
			num_tasks = line.split(',')[3]
			task_type = line.split(',')[2]

		self.event_queue.put(PrioritizedItem(start_time * 1000, TaskArrival(self,
			start_time * 1000, num_tasks, task_type)))
		last_time = 0
		#print("PeriodicTimerEvent event added")
		self.event_queue.put(PrioritizedItem((start_time*1000) + self.config.PERIODIC_TIMER_EVENT_TIMER, PeriodicTimerEvent(self)))
		#print("VM_Monitor_Event event added")
		self.event_queue.put(PrioritizedItem((start_time*1000) + self.config.VM_MONITOR_EVENT_TIMER, VM_Monitor_Event(self)))
		#print(" PeriodicSchedulerEvent event added")
		self.event_queue.put(PrioritizedItem((start_time*1000)+1, PeriodicSchedulerEvent(self,None)))
		while not self.event_queue.empty():
			temp_PrioritizedItem = self.event_queue.get()
			(current_time, event) = (temp_PrioritizedItem.current_time, temp_PrioritizedItem.data)
			#print current_time, event, self.event_queue.qsize()
			#assert current_time >= last_time
			last_time = current_time
			#if (not isinstance(event, PeriodicSchedulerEvent)):
			#print("************************************************************")
			#print("Running this event:{} at {}".format(event, current_time))
			new_events = event.run(current_time)
			#print("Returned events:", new_events)
			for new_event in new_events:
				(current_time, event) = new_event
				# if (not isinstance(new_event, PeriodicSchedulerEvent)):
				# 	print("Adding new event:",new_event)
				
				self.event_queue.put(PrioritizedItem(current_time, event))
			

		self.workload_file_hdl.close()
		# Done with queueing all the task in the task_queue
		total_VM_cost = self.calculate_cost(current_time)
		cost_file = open(self.config.cost_path,'w')
		print ("total VM cost is",total_VM_cost)
		print ("total VM cost is",total_VM_cost,file=cost_file)
		self.file_prefix = "pdf"
		complete_jobs = [j for j in self.tasks.values()
				if j.completed_tasks == j.num_tasks]
		print ('%s complete jobs' % len(complete_jobs))
		print ('%s complete jobs' % len(complete_jobs),file=cost_file)
		response_times = [job.end_time - job.start_time for job in
				complete_jobs if job.start_time > 500]

		print ("Included %s jobs" % len(response_times))
		plot_cdf(response_times, self.config.pdf_file_path)

		print ('Average response time: ', np.mean(response_times))
		print ('Average response time: ', np.mean(response_times),file=cost_file)

		total_lambda_cost = 0
		for i in range(len(self.lambdas)):
			print ("type ",i,"lambda tasks", len(self.lambdas[i]))
			print ("type ",i,"lambda tasks", len(self.lambdas[i]), file=cost_file)
			print ("type ",i,"lamda cost: ",lambda_cost(len(self.lambdas[i]), self.lambdas[i][0].mem/1024, self.lambdas[i][0].exec_time/1000))
			print ("type ",i,"lamda cost: ",lambda_cost(len(self.lambdas[i]), self.lambdas[i][0].mem/1024, self.lambdas[i][0].exec_time/1000),file=cost_file)
			total_lambda_cost+=lambda_cost(len(self.lambdas[i]), self.lambdas[i][0].mem/1024, self.lambdas[i][0].exec_time/1000)
		# longest_tasks = [job.longest_task for job in complete_jobs]
		print ("total lambda cost ", total_lambda_cost)
		print ("total lambda cost ", total_lambda_cost,file=cost_file)
		print ("total cost of deployment ", total_lambda_cost + total_VM_cost)
		print ("total cost of deployment ", total_lambda_cost + total_VM_cost,file=cost_file)

		self.stats.display_all_stats(cost_file)
		#print("Most popular memory index: ", self.stats.get_most_popular_vm_mem_idx())





def get_args():
	parser =argparse.ArgumentParser()
	parser.add_argument('--trace_type', type = str, default = 'wiki', help = "SPOCK or BATCH")
	parser.add_argument('--execution_mode', type = str, default = 'inferall', help = "spock or inferall or batch")
	parser.add_argument('--trace_name', type = str, default = 'wiki', help = "Name of the traces (1. WITS_load, 2.berkeley, 3. tweeter, 4. tweeter_BATCH")
	parser.add_argument('--slo', type=float, default = 1000, help='The SLO value in millisecond')
	parser.add_argument('--optimiztion_type', type = int, default = 0, help = "0 for SLO optimiztion, 1 for cost optimiztion")
	parser.add_argument('--scheduling_type', type=float, default = 0, help='0 for VM and Lambda, 1 for VM only, 2 for Lambda Only')
	return parser.parse_args()

random.seed(1)



if __name__=="__main__": 
	 
	# logging.basicConfig(filename='events_log.log',filemode='w',\
	# 	format='%(levelname)s:%(asctime)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',level=logging.INFO)
	logging.basicConfig(format='%(message)s',level=logging.INFO)
	args = get_args()
	
	#sim = Simulation(sys.argv[1], , ,int(sys.argv[2]),int(sys.argv[3] args.), float(sys.argv[4]))
	config = Configuration_cls(args.trace_name, args.execution_mode, args.slo,args.scheduling_type, args.optimiztion_type)
	# for i in config.batch_sz:
	# 	print(i)
	
	scheduler = Scheduler_cls(config)
	#print("Running simulator")
	#print("./traces/"+args.trace_name+".csv")
	sim = Simulation(args.trace_name, args.trace_type, config, scheduler)
	scheduler.set_simulation_obj(sim)
	scheduler.assign_approximate_workload_capacity_per_slot()
	stats_cls = Stats(sim)
	sim.set_stats_obj(stats_cls)

	sim.run()
	#scheduler.start() # Starting scheduler thread
	sim.f.close()
	sim.load_file.close()