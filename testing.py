import logging
import threading
import multiprocessing
from multiprocessing import Process, Queue
import copy
import collections

import time


class config(object):
	def __init__(self, arg):
		self.task_queue = Queue()
		self.cond = multiprocessing.Condition()



class Task(object):
	def __init__(self, arg):
		print("Init function in task")
		self.config = arg

	def task_scheduler(self):
		print("In tas function function in task")
		global c, num
		
		logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
		time.sleep(2)
		i = 0
		
		while not c.task_queue.empty():
			with self.config.cond:
				i =  i+1
				if i == 9:
					self.config.cond.wait()
					print("recevid")
				print(c.task_queue.get())

if __name__=="__main__": 
	
	num = 10
	c =  config(1)
	logging.basicConfig(filename='events_log.log',filemode='w',format='%(levelname)s:%(asctime)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',level=logging.INFO)
	logging.debug('This message should appear on the console')
	logging.info('So should this')
	logging.warning('And this, too')
	t =  Task(c)
	
	x = threading.Thread(target=t.task_scheduler, args=())
	
	logging.warning('NEW WARNING And this, too')
	x.start()
	for i in range(10):
		if i == 9:
			time.sleep(7)
			print("releasing")
			with c.cond:
				c.cond.notify_all()
		c.task_queue.put(i)
	
	x.join()