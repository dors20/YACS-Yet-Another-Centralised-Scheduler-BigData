#!/usr/bin/python

import threading
from time import sleep
import socket
from datetime import datetime
import sys
import json
import logging

someLock = threading.Lock()

class Worker:
    def __init__(self,port,worker_id):
        """
            Attributes:
                self.port: port number of the worker
                self.worker_id: worker_id of the worker
                self.pool: dictionary containing all the currently runnning tasks
                           key: task_id
                           value: remaining duration 
                self.server_port: port number to use when sending messages to the master
        """
        self.port=port
        self.worker_id=worker_id
        self.pool=dict()
        self.server_port=5001
        self.wait_list = []
        self.algo=None
    
   
    def listen_master(self):
        """
            This function listens for tasks to do.
            The worker acts like the server during the communication

        """
        recv_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        recv_socket.bind(('',self.port)) # listens on the specified port
        recv_socket.listen(5) 
        while True:
            conn_socket, addr = recv_socket.accept()
            task = conn_socket.recv(2048).decode() #recv the msg for task launch by master
            self.execution_pool(task) #add task to the execution pool
            conn_socket.close()
        
        
    def update_master(self,task=None):
        """
            This is called when a task is completed.
            In this case, the worker file acts as a client and sends the message to the master 
            indicating the completion of the given task
            message = [worker_id, task_id]
        """
        if (task):
            message1 = list((self.worker_id, task[0]))
            message = json.dumps(message1) # Converting the list to string 
            message = message + "?" + str(datetime.now()) + "?"+str(task[1])
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(('localhost', self.server_port))
                s.send(message.encode())

    
    def execution_pool(self,task): 
        """
            This function adds the given task to the execution pool.
            key = task_id
            value = remaining time of the task (initialised to its duration)
        """
        task=json.loads(task)
        task_id = task[0][0]
        remain_time = task[0][1] + 1
        self.algo=task[1]
        # self.pool[task_id]=remain_time
        self.wait_list.append(tuple((task_id, remain_time)))
        


    def task_monitor(self):
        """
            This function updates the remaining times of the tasks in the execution pool.
            If any of the tasks has been completed, it updates the master about it.
            tempSet is used to get the set of keys in the execution pool at that instant.
        """
        exec_time=0.0
        someLock.acquire()

        while (self.wait_list):
            ta = self.wait_list.pop()
            self.pool[ta[0]] = ta[1]
            logging.debug("Started task {};{}".format(ta[0],self.algo))

        tempKeys = list(self.pool.keys())
        tempList = []
        for task_id in tempKeys:
            self.pool[task_id]-=1    #reduce time by 1 unit every clock cycle --sleep the threa,d for 1s

            if self.pool[task_id]==0: #the task execution is completed
                logging.debug('Completed task {};{}'.format(task_id,self.algo))
                exec_time=self.duration(task_id)
                self.update_master((task_id,exec_time)) #t2 must update the master about the status of the task completion
                tempList.append(task_id)
        for i in tempList:
            self.pool.pop(i)
        someLock.release()


    def clock(self):
        """
            Simulates a clock
        """
        while True:
            threading.Thread(target=self.task_monitor()).start()
            # self.task_monitor()
            sleep(1)

    def duration(self,task_id):
        start_time=0
        file = open("worker" + str(self.worker_id) + ".log", "r")
        time_elapsed=0
        for line in file.readlines():
            temp=line.split(";")
            if task_id==temp[1].split()[2]:
              if (temp[1].split()[0] == "Started"):
                  start_time = datetime.strptime(temp[3][:-2],'%Y-%m-%d %H:%M:%S,%f')
              else:
                  time_elapsed = datetime.strptime(temp[3][:-2], '%Y-%m-%d %H:%M:%S,%f') - start_time
			
        return time_elapsed      

if __name__ == "__main__": 

    
    port=int(sys.argv[1])
    worker_id=int(sys.argv[2])
    worker=Worker(port,worker_id)
    
    logging.basicConfig(filename="worker"+str(worker_id)+".log", level=logging.DEBUG,format='%(filename)s;%(message)s;%(asctime)s')


    t1 = threading.Thread(target=worker.listen_master) 
    t2 = threading.Thread(target=worker.clock)

  
    t1.start() 
    t2.start() 
    t1.join() 
    t2.join() 
  




