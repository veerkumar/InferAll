import logging
import threading
import multiprocessing
from multiprocessing import Process, Queue
import copy
import collections
from pdb import set_trace as bp

import time


class config(object):
	def __init__(self, arg):
		self.task_queue = Queue()
		self.cond = multiprocessing.Condition()



class Task(object):
	def __init__(self, arg):
		print("Init function in task")
		self.config = arg

	def task_scheduler(self, name):
		print("In tas function function in task")
		global c, num
		bp()
		print(name)
		logging.info ("Thread: Frist name:" + name)
		logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
		logging.info("Thread logg")
		time.sleep(2)
		i = 0
		
		while not c.task_queue.empty():
			with self.config.cond:
				i =  i+1
				if i == 9:
					self.config.cond.wait()
					print("recevid")
					name = "Kumar"
				print(c.task_queue.get())
		print(name)
		logging.info ("Thread: Last name:"+ name)

if __name__=="__main__": 
	
	num = 10
	c =  config(1)
	logging.basicConfig(filename='events_log.log',filemode='w',format='%(levelname)s:%(asctime)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',level=logging.INFO)
	logging.debug('This message should appear on the console')
	logging.info('So should this')
	logging.warning('And this, too')
	name = "Veer"
	print(name)
	t =  Task(c)
	
	x = threading.Thread(target=t.task_scheduler, args=(name,))
	
	logging.warning('NEW WARNING And this, too')
	x.start()
	for i in range(10):
		if i == 9:
			time.sleep(7)
			print("releasing")
			with c.cond:
				c.cond.notify_all()
		c.task_queue.put(i)
	logging.info("Last message")
	
	x.join()
	logging.info ("Final name:" + name)