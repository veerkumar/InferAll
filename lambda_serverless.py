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
class MyServerless(threading.Thread):
	
	def __init__ (self, batch_size, queue_time, request_list, actual_batch_size, inter_arrival, time_out, function_name):
		threading.Thread.__init__(self)
		self.batch_size = batch_size
		self.queue_time = queue_time
		self.request_list = request_list
		self.actual_batch_size = actual_batch_size
		self.time_out = time_out
		self.function_name = function_name
		
	
	def send_real_request(self):
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

	def send_simulated_request(self):

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

