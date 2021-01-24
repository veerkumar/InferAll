
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
        global last_task
        task = Task(current_time, self.num_tasks, self.task_type)
        logging.getLogger('sim').debug('Job %s arrived at %s'
                % (task.id, current_time))

        # Queue the task to task schedular.
        new_events = self.simulation.task_queue.put((task, current_time))


        #new_events = self.simulation.send_tasks(task, current_time)

        logging.getLogger('sim').debug('Retuning %s events'
                % len(new_events))
        line = self.simulation.tasks_file.readline()
        start_time = 0
        num_tasks = 0
        task_type = 0
        #print line
        if line == '':
            print('task empty')
            last_task = 1
            return new_events
        if self.simulation.workload_type == "tweeter_BATCH":
            start_time = float(line.split('\n')[0])
            num_tasks = 1
            task_type = 1
        else: 
            start_time = float(line.split(',')[0])
            num_tasks = line.split(',')[3]
            task_type = line.split(',')[2]

        #print "adding new task",int(start_time*1000), num_tasks, task_type
        # new_task = Task(self, line, 1000, start_time, num_tasks, task_type)

        new_events.append((start_time * 1000,
            TaskArrival(self.simulation, start_time
                * 1000, num_tasks, task_type)))
        #print ("task arrival new events", new_events)
        return new_events

class SleepEvent(Event):
    def __init__(self, simulation, current_time):
        self.cond = multiprocessing.Condition()
        self.simulation = simulation
        self.start_time = current_time
        
    def run(self, current_time):
        with self.cond:
                self.cond.notify_all()
        new_events = []
        return new_events



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

    def task_completed(self, completion_time):
        self.completed_tasks += 1
        self.end_time = max(completion_time, self.end_time)
        assert self.completed_tasks <= self.num_tasks
        return self.num_tasks == self.completed_tasks
