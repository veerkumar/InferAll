import time
import logging
import math
import random
import multiprocessing
from multiprocessing import Process, Queue
from queue import PriorityQueue
from vm import *

MONITOR_INTERVAL = int(100000)

class PeriodicSchedulerEvent(Event):
    """docstring for PeriodicSchedulerEvent"""

    def __init__(self, simulation, last_read_element):
        #super(PeriodicSchedulerEvent, self).__init__()
        self.simulation= simulation
        self.last_read_element = last_read_element

    def run(self, current_time):
        new_events = []
        
        if (self.last_read_element is None):
            #print("Qsize: {}, queue is empty: {} ".format(self.simulation.task_queue.qsize(), self.simulation.task_queue.empty()))
            #print("Running PeriodicSchedulerEvent : New last_read_element:",self.last_read_element)
            if not self.simulation.task_queue.empty():
                self.last_read_element =  self.simulation.task_queue.get()
            else:
                new_events.append((current_time + self.simulation.config.scheduler_wakeup_timer, PeriodicSchedulerEvent(self.simulation, None)))
                #print("PeriodicSchedulerEvent: Task_queue is empty and current_time:",current_time)
                return new_events
        
        (scheduling_return_code, new_events, requested_sleep_timer) = self.simulation.scheduler_obj.run(self.last_read_element, current_time)
        
        if (requested_sleep_timer < 0): # one scheduling completed, Moving to next
            next_wakeup_time = self.simulation.config.scheduler_wakeup_timer
            new_events.append(((current_time + next_wakeup_time), PeriodicSchedulerEvent(self.simulation, None)))
        else:
            next_wakeup_time = requested_sleep_timer # SHould be in millisecond
            new_events.append(((current_time + next_wakeup_time), PeriodicSchedulerEvent(self.simulation, self.last_read_element)))
        return new_events

class PeriodicTimerEvent(Event):

    def __init__(self,simulation):
        self.simulation= simulation

    def run(self, current_time):
        #print("Running PeriodicTimerEvent run")
        new_events = []
        #print("periodic timer event",current_time,"VM1 VM2 VM3",len(self.simulation.VMs[0]),len(self.simulation.VMs[1]),len(self.simulation.VMs[2]))
      #  total_load       = str(int(10000*(1-self.simulation.total_free_slots*1.0/(TOTAL_WORKERS*SLOTS_PER_WORKER)))/100.0)
      #  small_load       = str(int(10000*(1-self.simulation.free_slots_small_partition*1.0/len(self.simulation.small_partition_workers)))/100.0)
      #  big_load         = str(int(10000*(1-self.simulation.free_slots_big_partition*1.0/len(self.simulation.big_partition_workers)))/100.0)
       # small_not_big_load ="N/A"
       # if(len(self.simulation.small_not_big_partition_workers)!=0):
            #Load        = str(int(10000*(1-self.simulation.free_slots_small_not_big_partition*1.0/len(self.simulation.small_not_big_partition_workers)))/100.0)
        if (self.simulation.config.load_tracking == 1):
            for i in range(3):
                low_load = 0
                for j in range (len(self.simulation.VMs[i])):
                    if((float(self.simulation.VMs[i][j].num_queued_tasks)/float(self.simulation.VMs[i][j].max_slots)) <=0.4):
                        low_load+=1
                print ("VM type," + str(i) + "low_load: "+ str(low_load) + ",num_vms," + str(len(self.simulation.VMs[i])) + ",current_time: " + str(current_time),file=self.simulation.load_file)
                print ("load written", i, low_load, len(self.simulation.VMs[i]), float(self.simulation.VMs[i][0].free_slots),self.simulation.VMs[i][0].num_queued_tasks)
        if(self.simulation.VM_PREDICTION == 1):
            for i in range(3):
                #print self.simulation.task_arrival[i]
                df = pd.DataFrame((self.simulation.task_arrival[i]),columns=['time','req'])
                train = df[-500:]
                X = train.time.tolist()
                y = train.req.tolist()
                X = np.array(X).reshape(-1,1)
                #print X
                y = np.array(y).reshape(-1,1)
                #print y
                model = LinearRegression()
                model.fit(X, y)
                #print model
                X_predict = np.array([current_time+start_up_delay]).reshape(-1,1)
                y_predict = burst_threshold * model.predict([X_predict[0]])
                print ("predicted requests",int(y_predict),"VMs",int(y_predict)/self.simulation.VMs[i][0].max_slots)
                if(int(y_predict) > len(self.simulation.VMs[i])*self.simulation.VMs[i][0].max_slots):
                    print ("rolling mean more",y_predict, "existing VM size", len(self.simulation.VMs[i])*self.simulation.VMs[i][0].max_slots)
                    num_vms = int(math.ceil(int(int(y_predict)- len(self.simulation.VMs[i])*self.simulation.VMs[i][0].max_slots)/int(self.simulation.VMs[i][0].max_slots)))
                    if(num_vms !=0):
                        print ("num_vms spawning",burst_threshold*num_vms)
                        for j in range(num_vms):
                        #print "adding new VMs", num_vms
                            self.simulation.VMs[i].append(VM(self.simulation,current_time,start_up_delay,i,4,8192,0.10,True,len(self.simulation.VMs[i])))
                            new_events.append((current_time+start_up_delay,VMCreateEvent(self.simulation,self.simulation.VMs[i][-1],i)))

        if(self.simulation.event_queue.qsize() >1 and self.simulation.last_task==0):
            #print "events left ",self.simulation.event_queue.qsize()," tasks arrived", len(self.simulation.task_arrival)
            new_events.append((float(current_time) + MONITOR_INTERVAL,PeriodicTimerEvent(self.simulation)))
        return new_events
