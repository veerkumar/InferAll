import time
import logging
import math
import random
from config import *
from utils import *


class TaskArrival(Event):
    """ Event to signify a job arriving at a scheduler. """
    def __init__(
            self,
            simulation,
            interarrival_delay,
            num_tasks,
            task_type,
            ):
        self.simulation = simulation
        self.interarrival_delay = float(interarrival_delay)
        self.num_tasks = int(num_tasks)
        self.task_type = int(task_type)

        # self.task_distribution= task_distribution

    def run(self, current_time):
        #print("Running task arrival event")
        s_time = 0
        num_tasks = 0
        task_type = 0
        last_task = 0
        last_start_time = 0
        task = Task(current_time, self.num_tasks, self.task_type)
        self.simulation.tasks[task.id] = task
        logging.getLogger('sim').debug('Job %s arrived at %s'
                % (task.id, current_time))

        # Queue the task to task schedular.
        new_events = []
        self.simulation.task_queue.put(task)
        #print("Task Queue size: {}, is empty?:{}".format(self.simulation.task_queue.qsize(), self.simulation.task_queue.empty()))
        self.simulation.num_queued_tasks = self.simulation.num_queued_tasks + self.num_tasks
        line = self.simulation.workload_file_hdl.readline()
        if line == '':
                print('workload finished')
                self.simulation.end_of_file = 1
                return new_events
        else:
            if self.simulation.workload_type == "tweeter_BATCH":
                s_time = float(line.split('\n')[0])
                num_tasks = 1
                task_type = 1
            else: 
                s_time = float(line.split(',')[0])
                num_tasks = line.split(',')[3]
                task_type = line.split(',')[2]
            autolog("TaskArrival event")
            if(s_time <= 36000):
                new_events.append(((s_time * 1000), TaskArrival(self.simulation,
                    s_time * 1000, num_tasks, task_type)))
            else:
                self.simulation.end_of_file = 1
            self.simulation.next_task_time = s_time*1000

        logging.getLogger('sim').debug('Retuning %s events'
                % len(new_events))
        
        #print ("task arrival new events", new_events)
        # return (new_events, True)
        return new_events

class TaskEndEvent:

    def __init__(self, worker):
        self.worker = worker

    def run(self, current_time):
        #print ("task end event", current_time)
        self.worker.isIdle = True
        self.worker.lastIdleTime = current_time
        self.worker.next_available_time =  current_time
        if (self.worker.used_slots > 0):
            self.worker.used_slots -= 1
        return []

class EndOfFileEvent(Event):
    """docstring for EndOfFileEvent"""
    def __init__(self, simulation, start_time):
        #super(EndOfFileEvent, self).__init__()
        self.simulation = simulation
        self.start_time = start_time

    def run(self):
        # return (None, False)
        return None


class SleepEndEvent(Event):
    def __init__(self, simulation, start_time, condition):
        self.cond = condition
        self.simulation = simulation
        self.start_time = start_time
        
    def run(self):
        with self.cond:
                self.cond.notify_all()
        new_events = []
        # Now we need to wait for scheduler to enueue its events
        with self.simulation.simulation_cond:
            self.simulation.simulation_cond.wait()
        logging.debug("simulation.event_queue size:",self.simulation.event_queue.qsize())

        # return (new_events, True)
        return new_events

class SleepStartEvent(Event):
    """docstring for SleepStartEvent"""
    def __init__(self, simulation, start_time):
        super(SleepStartEvent, self).__init__()
        self.cond = multiprocessing.Condition()
        self.simulation = simulation
        self.start_time = start_time

    def run(self):
        line = self.simulation.workload_file_hdl.readline()
        s_time = 0
        num_tasks = 0
        task_type = 0
        last_task = 0
        last_start_time = 0
        while True:
            #print line
            if line == '':
                print('task empty')
                last_task = 1
                break
            else:
                if self.simulation.workload_type == "tweeter_BATCH":
                    s_time = float(line.split('\n')[0])
                    num_tasks = 1
                    task_type = 1
                else: 
                    s_time = float(line.split(',')[0])
                    num_tasks = line.split(',')[3]
                    task_type = line.split(',')[2]

                if (s_time <= (self.start_time/1000)):
                    new_events.append((s_time * 1000, TaskArrival(self.simulation, s_time*1000, num_tasks, task_type)))
                else:
                    new_events.append((self.start_time*1000,SleepEndEvent(self.simulation, start_time*1000, self.cond)))
                    #TODO Check this logic again
                    new_events.append((s_time * 1000, TaskArrival(self.simulation, s_time*1000, num_tasks, task_type)))
                    break
            last_start_time =  s_time
            line = self.simulation.workload_file_hdl.readline()

        if (last_task):
            # Randomly add time
            new_events.append(((last_start_time + 100) * 1000, EndOfFileEvent(self.simulation, (last_start_time + 100)*1000)))
            
            # 
        #print "adding new task",int(start_time*1000), num_tasks, task_type
        # new_task = Task(self, line, 1000, start_time, num_tasks, task_type)
        for new_event in new_events:
                (current_time, event) = new_event
                self.simulation.event_queue.put(PrioritizedItem(current_time, event))

        #return ([],True) #Since we already added the events so returning empty list
        return []


class Task(object):

    task_count = 0

    def __init__(
            self,
            start_time,
            num_tasks,
            task_type,
            ):
        self.id = int(Task.task_count)
        Task.task_count += 1
        self.start_time = start_time
        self.num_tasks = num_tasks
        self.task_type = task_type
        self.end_time = start_time
        self.completed_tasks = 0
        self.lambda_tasks = 0
        self.vm_tasks = 0
        self.exec_time = 0
        # if task_type == 0:
        #     self.exec_time = 400
        #     self.mem = 1024
        # if task_type == 1:
        #     self.exec_time = 400
        #     self.mem = 2024
        # if task_type == 2:
        #     self.exec_time = 950
        #     self.mem = 3048

    def task_completed(self, completion_time, num_of_tasks_completed):
        self.completed_tasks += num_of_tasks_completed
        self.end_time = max(completion_time, self.end_time)
        assert self.completed_tasks <= self.num_tasks
        return self.num_tasks == self.completed_tasks
