# YACS: Yet Another Centralized Scheduler

## About
This project aims to simulate a classic job scheduler in a distributed system. The architecture consists of one master machine and several worker machines.


The Master machine listens for job requests, parses the various the various tasks that make up the job and schedules these tasks to the workers based on the given algorithm. The master is responsible for resolving dependencies among the tasks in a job and keeping track of jobs running in parallel in the worker machines.


Three sheduling algorithms were implemented namely random, round-robin and least-loaded (worst fit)


The worker machines listen for tasks from the master, run the given tasks and update the master whenever a task completes. 


## Steps to run the project:
1. cd into the 'local_files' directory and run `start-all.sh` to set up the master and the worker files.
2. Run the following command: `python3 requests.py 5`  
This code sends five job requests to the master. To change the number of jobs, simply change the number passed as an argument to the requests.py
3. To stop all the processes, run `end-all.sh`.

## Submission Files:
The files required for submission are present in the `src` folder.
1) master.py
2) worker.py
3) analysis.py
4) BD_0210_0416_1879_2057_report.pdf

### Note: 
1) To change the number of workers in the cluster, modify the `start-all.sh` script.
2) To change the port numbers on which the workers listen to, modify the `config.json` and `start-all.sh` scripts.
3) Each worker has its own log, named `worker<worker_id>.log` 
4) The scheduling algorithm can be changed by modifying the `start-all.sh` script 
5) To get more insight from the log files, run `python3 analysis.py`.To view the plots,navigate to the `Visualisation` folder.
