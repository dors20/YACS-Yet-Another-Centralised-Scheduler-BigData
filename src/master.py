import threading
import socket
import json
import random
import sys
from queue import Queue
import logging
import copy

mutex = threading.Lock() #for slots
task_mutex = threading.Lock() #for tasks_completed 
exec_mutex = threading.Lock() # for deleting / adding to execution pool

class Worker_details:
    """
        Class to store worker details
        Methods: increment_slot
                 decrement_slot

    """
    def __init__(self, worker_id, slots, ip_addr, port):
        self.worker_id = worker_id
        self.no_of_slots = slots
        self.ip_addr = ip_addr
        self.port = port

    def increment_slot(self):
        """
            Given the worker id, update its number of available slots
        """
        mutex.acquire()
        self.no_of_slots += 1
        mutex.release()

    def decrement_slot(self):
        """
            Given the worker id, update its number of available slots
        """
        mutex.acquire()
        self.no_of_slots -= 1
        mutex.release()



class job_details:
    def __init__(self, info):
        self.job_id = info['job_id']
        self.map_tasks = []	#list of tuples
        self.reduce_tasks = []
        self.is_running_reduce = False

        for i in info['map_tasks']:	#i is each task
            self.map_tasks.append((i['task_id'], i['duration']))

        for i in info['reduce_tasks']:	#i is each task
            self.reduce_tasks.append((i['task_id'], i['duration']))

        self.map_count = len(self.map_tasks)
        self.reduce_count = len(self.reduce_tasks)

    def get_ids(self, ch):
        l = []
        if ch == 'm':
            for i in self.map_tasks:
                l.append(i[0])
        elif ch == 'r':
            for i in self.reduce_tasks:
                l.append(i[0])
        return l

    def decrement_task_count(self, task_id):
        if task_id in self.get_ids('m'):
            self.map_count-= 1

        elif task_id in self.get_ids('r'):	
            self.reduce_count -= 1

    def is_completed(self):
        if self.map_count==0 and self.reduce_count==0:
            return True

        return False

    def is_map_completed(self):
        return self.map_count==0


class Master:
    def __init__(self, algo):
        """
            Initialises Master attributes
            self.sem will tell us the number of slots available.
            
        """
        self.wait_queue = Queue()
        self.workers = dict()  # changed to dictionary
        self.algo = algo

        # Tasks completed is a set of task ids that have been completed
        self.tasks_completed = set()

        # Execution pool is a dictionary containing information about the running jobs
        # Key: job_id
        # Value: The job object
        self.execution_pool = dict()

        # For round robin
        self.worker_ids = list()
        self.last_used_worker = None
        self.no_of_workers = 0

    def read_config(self, config):
        """
            Argument(s): config text
            This function reads the json text, creates a worker_details object for each worker 
            and appends the object to the workers list
        """
        content = json.loads(config)
        summ = 0
        for worker in content['workers']:
            obj = Worker_details(worker['worker_id'], worker['slots'], '127.0.0.1', worker['port'])
            summ += int(worker['slots'])
            self.workers[worker['worker_id']] = obj
            self.worker_ids.append(worker['worker_id'])
            self.no_of_workers += 1

        # self.config_workers will store the original configuration details. 
        self.config_workers = copy.deepcopy(self.workers)  

        # Initialising the counter with the number of slots
        self.sem = threading.BoundedSemaphore(summ)

    def get_available_workers(self):
        """
            Returns a list of worker ids which have at least one slot available
        """
        available_workers = []
        mutex.acquire()
        temp = copy.deepcopy(self.workers) # Why deep copy? Because we need the slots' details at that instant. 
        mutex.release()
        for i in temp:
            if temp[i].no_of_slots > 0:
                available_workers.append(i)
        
        return available_workers

    def find_worker(self):
        """
            Finds a worker based on the algorithm provided as an argument
        """
        available_workers = self.get_available_workers()
        if self.algo == 'random':
            choice = random.choice(available_workers)
            self.last_used_worker = choice
            return choice
        elif self.algo == 'round-robin':
            if self.last_used_worker == None: # This is the first time this function has been called
                self.last_used_worker = available_workers[0]
                return available_workers[0]
            
            # So this part is to find the index of the last used worker in the list 'self.worker_ids'
            # While loop terminates when we've found the last_used_worker's index in the list
            i = 0
            while (self.worker_ids[i] != self.last_used_worker):
                i = (i + 1)%(self.no_of_workers)
            # i will now equal the index of the worker to the right of last_used_worker (is last_used_worker is the rightmost worker, then i = first worker)
            i = (i + 1)%(len(self.worker_ids))
            
            # Here, the first worker we find that has slots available will be the worker returned
            # available_workers has the list of workers with available slots.
            # Alternative implementation: while (self.workers[self.worker_ids[i]].no_of_slots == 0) i++
            while (self.worker_ids[i] not in available_workers):
                i = (i + 1)%(self.no_of_workers)
            self.last_used_worker = self.worker_ids[i]
            return self.worker_ids[i]

        else:
            # Worst fit algorithm
            # among the workers with available slots, we choose the worker with the most number of slots available
            max1 = available_workers[0]
            for worker in available_workers:
                if (self.workers[worker].no_of_slots > self.workers[max1].no_of_slots):
                    max1 = worker
            self.last_used_worker = max1
            return max1


    def schedule_task(self, task):
        """
            Schedules the given task to one of the workers
        """
        # Waiting until there's at least one slot available
        self.sem.acquire(blocking=True)

        # Getting the worker id with available slots based on the scheduling algorithm
        worker_id = self.find_worker()
        port_number = self.workers[worker_id].port
        task=(task,self.algo)
        task = json.dumps(task)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.workers[worker_id].ip_addr, port_number))
            s.send(task.encode())

        self.workers[worker_id].decrement_slot()

    def schedule_reduce_tasks(self, job_id):
        """
            Given job id of a job currently running,
            this function schedules all the reduce tasks of the job
        """
        reduce_tasks = self.execution_pool[job_id].reduce_tasks
        for task in reduce_tasks:
            self.schedule_task(task)



    def listen_for_worker_updates(self):
        """
            Listens for updates from the workers
            Adds the task received to the tasks_completed set
            Increments the sempahore to indicate that a slot is available
        """
        rec_port = 5001
        rec_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        rec_socket.bind(('', rec_port))
        rec_socket.listen(10) # Why 10? It could be anything really. 10 is a safe size of queue, but we could use something lesser.
        while True:
            connectionSocket, addr = rec_socket.accept()
            message = connectionSocket.recv(2048).decode()
            message1 = message.split("?") #Incoming message will look like this: (worker_id, task_id)?timestamp?duration
            message = json.loads(message1[0])
            self.workers[message[0]].increment_slot() # Updating the no_of_slots 
            self.sem.release()
            connectionSocket.close()
            logging.debug('{};Completed task {} at {} duration {}'.format(self.algo,message[1], message1[1],message1[2].split(":")[-1]) )# Logging the task_id of task completed and the timestamp of its completion
            task_mutex.acquire()
            self.tasks_completed.add(message[1])  # message[1] has task_id
            task_mutex.release()

    def update_dependencies(self):
        while True:
            tasks_completed_copy = self.tasks_completed.copy()

            pool_keys = list(self.execution_pool.keys())
            for task in tasks_completed_copy:

                for j_id in pool_keys:
                        self.execution_pool[j_id].decrement_task_count(task)
                task_mutex.acquire();self.tasks_completed.remove(task);task_mutex.release()
                

            for j_id in pool_keys:
                if self.execution_pool[j_id].is_map_completed():
                    if self.execution_pool[j_id].is_completed():
                        exec_mutex.acquire()
                        logging.debug("{};Completed job {}".format(self.algo,j_id))
                        del self.execution_pool[j_id]
                        exec_mutex.release()
                        
                    elif not self.execution_pool[j_id].is_running_reduce:
                        self.execution_pool[j_id].is_running_reduce = True
                        self.schedule_reduce_tasks(j_id)




    def listen_for_job_requests(self):
        """
            This function listens for job requests and
            adds the jobs received to the wait queue
        """
        rec_port = 5000
        rec_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        rec_socket.bind(('', rec_port))
        rec_socket.listen(1)
        while True:
            connectionSocket, addr = rec_socket.accept()  
            message = connectionSocket.recv(2048).decode()
            message = json.loads(message)
            job_obj = job_details(message)  
            self.wait_queue.put(job_obj)
            connectionSocket.close()

    def schedule_map_tasks(self, job_id):
        """
            Basically schedules all map tasks of the job
        """
        for task in self.execution_pool[job_id].map_tasks:
            self.schedule_task(task)

    def schedule_job(self):
        """
            This function waits until a slot is available, after which
            it dequeus from the wait queue and adds the job to the execution pool
        """
        self.sem.release() # Because we used semaphore.acquire(), it would've decremented the sempahore.
                           # We have to increment it because we haven't used a slot yet.
                           # We used a semaphore to keep track of the slots available
                           # Whenever a task is scheduled, the semaphore is decremented automatically (line 160)
        job_obj = self.wait_queue.get()
        logging.debug('{};Started job with job id {}'.format(self.algo,job_obj.job_id))
        
        
        exec_mutex.acquire()
        self.execution_pool[job_obj.job_id] = job_obj #deb_mutex for dependency pool, exec_mutex for execution pool
        exec_mutex.release()
        self.schedule_map_tasks(job_obj.job_id) # Schedule all map tasks of this job


    def schedule_jobs(self):
        """
            Whenever there's a job waiting in the wait_queue, 
            this function creates a new thread for each job and runs it
        """
        while True:
            if (self.wait_queue.qsize() != 0): # Wait till there's at least one job in the queue
                self.sem.acquire(blocking=True) # Wait till there's at least one slot available
                # threading.Thread(target=Master.schedule_job,args=[self]).start()
                self.schedule_job()

def main():

    # Logging configuration
    
    
    config_file = open(sys.argv[1], "r")
    algo = sys.argv[2]
    masterProcess = Master(algo)
    masterProcess.read_config(config_file.read())
    config_file.close()
    
    logging.basicConfig(filename="master.log", level=logging.DEBUG,format='%(filename)s;%(message)s;%(asctime)s')


    t1 = threading.Thread(target=Master.listen_for_job_requests, args=[masterProcess])
    t2 = threading.Thread(target=Master.listen_for_worker_updates, args=[masterProcess])
    t3 = threading.Thread(target=Master.schedule_jobs, args=[masterProcess])
    t4 = threading.Thread(target=Master.update_dependencies, args=[masterProcess])

    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t1.join()
    t2.join()
    t3.join()
    t4.join()


if __name__ == "__main__":
    main()
