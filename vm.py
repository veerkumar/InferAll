import time
import logging
import math
import random
import multiprocessing
from multiprocessing import Process, Queue
from queue import PriorityQueue
from config import *
from utils import *
from task import * 

class VM(object):

    vm_count = 1

    def __init__(
            self,
            simulation,
            current_time,
            up_time,
            task_type,
            vcpu,
            vmem,
            price,
            spin_up,
            id):
        self.simulation = simulation
        self.config = simulation.config
        self.start_time = current_time
        self.up_time = up_time
        self.end_time = current_time
        self.vcpu = vcpu
        self.vmem = vmem
        self.queued_tasks = PriorityQueue()
        self.id = id
        self.isIdle = True
        self.lastIdleTime = current_time
        self.price = price
        self.task_type = task_type
        self.spin_up = spin_up
        #print("adding worker id and type",self.id,self.task_type)
        #A VM can execute any type of task, but during execution, it will make VM exclusive for that task
            # if task_type == 0:
            #     self.free_slots = 6
            #     self.max_slots = 6
            # if task_type == 1:
            #     self.free_slots = 5
            #     self.max_slots = 5
            # if task_type == 2:
            #     self.free_slots = 2
            #     self.max_slots = 2

    def VM_status(self, current_time):
        if (not self.isIdle):
            self.isIdle = True
            self.lastIdleTime = current_time
            return True
        return False

    def execute_simulated_task(self, vm_event_context, current_time):

        #print("In the VM id: {} add_task_function".format(self.id))
        
        
        self.isIdle = False
        self.spin_up = False

        schedule_event = []
        #print("running task on worker",self.id,self.task_type)
        task_duration = vm_event_context.expected_execution_time
        probe_response_time = 5 + current_time
        for task_id in vm_event_context.task_id_list:
            #print("Executing new tasks {} In the VM", task_id,self.id)
            task_end_time = task_duration + probe_response_time
            #print("worker not empty at time",self.id,self.task_type,task_end_time)
            new_event = TaskEndEvent(self)
            task = self.simulation.tasks[task_id]
            #if task.id >=15548:
             #   print ("task id ", task.id, "task type" , "VM id" , self.id, task.task_type, "task_end_time ", task_end_time, "task_start_time:",task.start_time, " each_task_running_time: ",(task_end_time - task.start_time))
            print ("task_id ,", task.id,",",  "task_type," ,task.task_type, ",", "VM_id," , self.id ,",", "task_end_time ,", task_end_time, ",", "task_start_time,",task.start_time, ",", " each_task_running_time,",(task_end_time - task.start_time), ",", " task_queuing_time:,", (task_end_time - task.start_time) - task.exec_time,file=self.simulation.tasks_file)
            if(self.simulation.add_task_completion_time(task_id,
                task_end_time,0)):
                #print "writing to file"
                print ("num tasks ", task.num_tasks, "," ,"VM_tasks ,", task.vm_tasks,"lambda_tasks ,", task.lambda_tasks , "task_end_time, ", task_end_time, "task_start_time,",task.start_time, " each_task_running_time ,",(task.end_time - task.start_time),file=self.simulation.finished_file)
                schedule_event.append((task_end_time, new_event))
        return schedule_event



class ScheduleVMEvent(Event):

    def __init__(self, worker, vm_batch_size_idx, vm_model_idx, expected_execution_time, task_id_list):
        self.worker = worker
        self.vm_batch_size_idx = vm_batch_size_idx
        self.vm_model_idx = vm_model_idx
        self.expected_execution_time =  expected_execution_time
        self.task_id_list = task_id_list

    def run(self, current_time):
        #print("Running ScheduleVMEvent run")
        for task_id in self.task_id_list:
            logging.getLogger('sim'
                ).debug('Probe for job %s arrived at worker %s at %s'
                        % (task_id, self.worker.id,
                            current_time))
        return self.worker.execute_simulated_task(self, current_time)

class VMCreateEvent(Event):
    def __init__(self,simulation, VM, task_type):
        self.simulation = simulation
        self.VM = VM
        self.task_type = task_type
    def run(self, current_time):
        #self.VMs[self.task_type].append(VM(self.simulation,current_time,60000,self.task_type,4,8192,0.10,True,len(self.VMs[self.task_type])))
        print (" spin up compeleted for VM", self.VM.id, self.task_type)
        self.VM.spin_up = False
        new_events = []
        return new_events


class VM_Monitor_Event(Event):
    def __init__(self, simulation):
        self.simulation = simulation

    def run(self, current_time):
        new_events = []
        #print("M_monito_EVENT")
        for index in range(3):
            width = len(self.simulation.VMs[index])
            k=0
            #print( len(self.simulation.VMs[index]), len(self.simulation.completed_VMs[index]))
            while k < width:
                if (not self.simulation.VMs[index][k].spin_up):
                    if(not self.simulation.VMs[index][k].VM_status(current_time) and self.simulation.VMs[index][k].isIdle):
                        if ((current_time - self.simulation.VMs[index][k].lastIdleTime) > 180000):
                        #if(current_time - self.simulation.VMs[index][k].start_time >=3600000):
                            self.simulation.completed_VMs.setdefault(index,[]).append(self.simulation.VMs[index][k])
                            self.simulation.VMs[index][k].end_time = current_time
                            print ( self.simulation.VMs[index][k].id, ",",self.simulation.VMs[index][k].end_time,",", self.simulation.VMs[index][k].start_time,",", self.simulation.VMs[index][k].lastIdleTime,file=self.simulation.f)
                            del self.simulation.VMs[index][k]
                            #print (index, len(self.simulation.completed_VMs[index]),"width changing")
                        width-=1
                k+=1
        if(self.simulation.event_queue.qsize() > 1 and self.simulation.last_task == 0):
            new_events.append((current_time+180000,VM_Monitor_Event(self.simulation)))

        return new_events