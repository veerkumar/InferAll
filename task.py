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
