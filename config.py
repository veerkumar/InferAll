
'''
	This file contains initial configuration needed for the simulator
'''
import time
import logging
import math
import random
import sys
import os
import multiprocessing
from multiprocessing import Process, Queue
from queue import PriorityQueue

class Event(object):
    """ Abstract class representing events. """

    def __init__(self):
        raise NotImplementedError('Event is an abstract class and cannot be instantiated directly'
                )

    def run(self, current_time):
        """ Returns any events that should be added to the queue. """

        raise NotImplementedError('The run() method must be implemented by each class subclassing Event'
                )

class Configuration_cls(object):
	"""docstring for Configuration_cls"""
	
	def __init__(self):
		super(Configuration_cls, self).__init__()
		self.MAX = 100000000
		self.MILL_TO_HR = 1/3600000
		self.batch_sz = [1,2,4,8,16,32,64,128,256,512,1024,2048,4096,8192,16384]
		# Accuracy picked from https://github.com/dmlc/mxnet-model-gallery and 
		# https://github.com/knjcode/mxnet-finetuner/blob/master/docs/pretrained_models.md
		self.top1_accuracy = [54.5, 55.4, 71.0, 72.5, 77.9]
		self.top5_accuracy = [78.3,78.8,89.8, 90.8,93.8]
		self.models_accuracy = [60,70,80,90,95]
		self.lambda_models = ["caffenet","squeeznet", "vggnet16","inception", "resnet200"]
		#self.lambda_models = ["caffenet", "inception", "resnet200", "squeeznet","vggnet16"]
		self.vm_models = ["caffenet","squeeznet", "vggnet16","inception", "resnet200"]
		self.vm_available_memory = [4096,8192,16384]
		#self.vm_available_memory = [7610,15438]
		self.vm_cost = [0.085, 0.17, 0.34] # $/hour
		self.lambda_available_memory = [256,512,1024,2048,3008]
		# Model x Memory size x BATCH = 5x5x15 =>375
		MAX= self.MAX
		self.lambda_latency = [[[MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX],
								[MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX],
								[250,255,255,265,265,285,325,407,552,MAX,MAX,MAX,MAX,MAX,MAX],
								[140,140,160,160,150,162,195,230,310,530,1100,1500,MAX,MAX,MAX],
								[135,135,137,140,140,150,170,235,310,500,1000,1500,MAX,MAX,MAX]
								],[[760,760,760,800,850,970,1200,1650,2500,MAX,MAX,MAX,MAX,MAX,MAX],
								[335,335,335,345,360,420,545,750,1150,2150,MAX,MAX,MAX,MAX,MAX],
								[160,165,168,170,175,205,255,330,540,850,1600,MAX,MAX,MAX,MAX],
								[85,85,85,85,100,120,135,190,300,500,980,1200,MAX,MAX,MAX],
								[65,70,75,70,80,100,125,170,325,520,900,1800,2600,MAX,MAX]
								],[[MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX],
								[MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX],
								[MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX],
								[MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX],
								[890, 890,900, 920,920,930,940,960,1050,1450,1700,2400,MAX,MAX,MAX]
								],[[2100,2100,2150,2150,2200,2250,2500,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX],
								[830,830,830,830,850,900,1050,1200,1600,2800,MAX,MAX,MAX,MAX,MAX],
								[380,395,400,400,410,410,450,570,750,1200,1900,MAX,MAX,MAX,MAX],
								[205, 210,210,210,210,210,270,310,400,620,1100,1800,MAX,MAX,MAX],
								[144,144,144,150,150,152,180,230,350,520,900,1800,3300,MAX,MAX]
								],[[MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX],
								[MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX],
								[MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX],
								[960,960,965,970,980,1020,1020,1020,1100,1400,1800,2500,MAX,MAX,MAX],
								[710,710,740,740,740,760,760,790,860,1100,1550,2200,MAX,MAX,MAX]
								]]
		
		# Convert to Model X memory x batch
		self.vm_latency = [[[32,33,34,36,42,50,70,125,224,415,800,1400,2400,MAX,MAX],
							[20,30,22,24,29,39,60,101,210,406,800,1590,2500,4700,MAX],
							[13,14,15,17,22,32,54,106,199,415,780,1400,2400,4700,9500]
						   ],[[20,21,22,25,32,44,68,115,212,400,800,1400,2500,MAX,MAX],
							[14,15,17,20,25,39,62,100,199,405,700,1150,2400,5000,MAX],
							[10,10,12,14,19,30,52,98,190,350,750,1500,2400,5000,10000]
						   ],[[242,242,243,250,250,260,280,335,420,620,1000,2000,MAX,MAX,MAX],
							[132,133,134,135,140,150,175,230,290,500,920,1700,3000,5000,MAX],
							[70,70,70,73,80,85,110,165,260,450,885,1600,3000,5000,10000]
						   ],[[35,36,37,39,44,56,82,129,225,420,780,1550,2700,MAX,MAX],
							[22,23,24,26,32,50,66,116,217,410,810,1550,3230,5200,MAX],
							[14,15,16,20,23,34,60,108,210,400,770,1300,2400,4500,9000]
						   ],[[232,233,235,236,239,250,270,323,420,610,990,1770,MAX,MAX,MAX],
							[133,133,133,136,140,155,172,225,320,505,900,1700,3400,6000,MAX],
							[72,74,74,77,80,100,110,165,260,440,820,1600,3050,6000,12000]
						   ]]
		
		self.vm_memory = [[1401.898438,1402.128906,1407.542969,1408.980469,1463.40625,1536.988281,1684.644531,1979.703125,2568.851563,3747.894531,6106.210938,10822.10547],
					 [2476.695313,2486.550781,2495.710938,2514.160156,2550.828125,2624.867188,2772.277344,3068.171875,3657.363281,4837.628906, 7199.375, 11922.63672],
					]
		self.trace_dir = "traces/"

		self.INITIAL_WORKERS = 10
		self.DONOT_RESHEDULE = -1
		self.VM_PREDICTION = 0
		self.load_tracking = 0
		self.MONITOR_INTERVAL = int(100000)
		self.start_up_delay = int(100000)
		self.last_task = 0
		self.SLO =  300  # milisecond

		os.system("rm -rf outputs")
		os.mkdir('outputs')
		self.finished_file_path = os.path.join('outputs', 'finished_file.csv')
		self.all_tasks_path = os.path.join('outputs', 'all_tasks.csv')
		self.VM_stats_path = os.path.join('outputs', 'VM_stats.csv')
		self.load_file_path =  os.path.join('outputs', 'load')
		self.cost_path = os.path.join('outputs', 'cost')
		self.optimiztion_type = 0
		self.scheduling_type = 0
		self.rescheduling_limit = 4
		self.scheduler_wakeup_timer = 30 # in millisecond



# print ("Lambda latency with batch size [1,16,32,64,128,256,512,1024,2048,4096,8192]")
# for i, element in enumerate(vm_latency):
# 	print(models[i] + ": ", end='')
# 	print (element)