#!/usr/bin/env python


#TO VIEW THE GRAPHS---navigate to the local_files/Visualisation directory

from datetime import datetime
import statistics
import sys
import matplotlib.pyplot as plt
from matplotlib import rcParams
import seaborn as sns
from datetime import datetime
import pandas as pd
import json
import time


from pathlib import Path
Path("./Visualisation/").mkdir(parents=True, exist_ok=True)
Path("./Visualisation/Task-2/").mkdir(parents=True, exist_ok=True)
Path("./Visualisation/Task-1/").mkdir(parents=True, exist_ok=True)

#TASK1

#BAR GRAPH 

#Calculation of mean and median time for tasks
final_dict=dict()
final_dict['random']=list((0.0,0.0,0.0,0.0))
final_dict['round-robin']=list((0.0,0.0,0.0,0.0))
final_dict['least-loaded']=list((0.0,0.0,0.0,0.0))

tempList1=[]
tempList2=[]
tempList3=[]

file=open("master.log","r")
for line in file.readlines():
    temp=line.split(";")
    algo=temp[1]
    temp1=temp[2].split()
    if(temp1[1]=="task"):
        if(temp[1]=="random"):
            tempList1.append(float(temp1[7]))
        elif(temp[1]=="round-robin"):
            tempList2.append(float(temp1[7]))
        elif(temp[1]=='least-loaded'):
            tempList3.append(float(temp1[7]))
file.close()
if (tempList1):
    final_dict['random'][0] = (float(sum(tempList1))/len(tempList1))
    final_dict['random'][1] = (statistics.median(tempList1))
if (tempList2):
    final_dict['round-robin'][0] = (float(sum(tempList2))/len(tempList2))
    final_dict['round-robin'][1] = (statistics.median(tempList2))
if (tempList3):
    final_dict['least-loaded'][0] = (float(sum(tempList3))/len(tempList3))
    final_dict['least-loaded'][1] = (statistics.median(tempList3))
    
    

'''
#Analysis can be done on individual workers

for i in range(1, n + 1):
    file = open("worker" + str(i) + ".log", "r")
    for line in file.readlines():
        temp = line.split(";")
        algo = temp[2]
        if (temp[1].split()[0] == "Started"):
            d[temp[1].split()[2]] = datetime.strptime(temp[3][:-2],'%Y-%m-%d %H:%M:%S,%f')
        else:
            time_elapsed = datetime.strptime(temp[3][:-2], '%Y-%m-%d %H:%M:%S,%f') - d[temp[1].split()[2]]
            if (algo == 'random'):
                tempList1.append(float(str(time_elapsed).split(":")[-1]))
            elif (algo == 'round-robin'):
                tempList2.append(float(str(time_elapsed).split(":")[-1]))
            else:
                tempList3.append(float(str(time_elapsed).split(":")[-1]))
    file.close()




if (tempList1):
    final_dict['random'][0] = (float(sum(tempList1))/len(tempList1))
    final_dict['random'][1] = (statistics.median(tempList1))
if (tempList2):
    final_dict['round-robin'][0] = (float(sum(tempList2))/len(tempList2))
    final_dict['round-robin'][1] = (statistics.median(tempList2))
if (tempList3):
    final_dict['least'][0] = (float(sum(tempList3))/len(tempList3))
    final_dict['least'][1] = (statistics.median(tempList3))


'''


#Calculation of mean and median time for job

start_times = dict()
end_times = dict()
algos = dict()
per_algos = dict()

file = open("master.log", "r")

lines = file.readlines()
file.close()


tempList1=[]
tempList2=[]
tempList3=[]


for line in lines:
    temp = line.split(";")
    a = temp[2].split()
    if (a[0] == 'Completed'):
        if (a[1] == 'task'):
            job_id = a[2][0:a[2].find('_')]
            if (a[2][a[2].find('_')+1] == 'R'):
                b = a[4] + " " + a[5]
                end_times[tuple((job_id, temp[1]))] = datetime.strptime(b,'%Y-%m-%d %H:%M:%S.%f')
    elif (a[0] == 'Started'):
        job_id = a[-1]
        b = a[4] + " " + a[5]
        start_times[tuple((job_id, temp[1]))] = datetime.strptime(temp[3][:-1],'%Y-%m-%d %H:%M:%S,%f')
        # algos[tuple((job_id, temp[1]))] = temp[1]




for job in start_times:
    if (job[1] == 'random'):
        tempList1.append((end_times[job] - start_times[job]).total_seconds())
    elif (job[1] == 'round-robin'):
        tempList2.append((end_times[job] - start_times[job]).total_seconds())
    elif (job[1] == 'least-loaded'):
        tempList3.append((end_times[job] - start_times[job]).total_seconds())

if (tempList1):
    final_dict['random'][2] = (float(sum(tempList1))/len(tempList1))
    final_dict['random'][3] = (statistics.median(tempList1))
if (tempList2):
    final_dict['round-robin'][2] = (float(sum(tempList2))/len(tempList2))
    final_dict['round-robin'][3] = (statistics.median(tempList2))
if (tempList3):
    final_dict['least-loaded'][2] = (float(sum(tempList3))/len(tempList3))
    final_dict['least-loaded'][3] = (statistics.median(tempList3))

# for i in final_dict:
    # print(final_dict[i])


for i in final_dict:
    plt.figure(figsize=(10,6))

    plt.title("Algorithm Used: {}".format(i))

    
    xx = ['mean_task','median_task','mean_job','median_job']
    splot = sns.barplot(x=xx, y=final_dict[i])
    for p in splot.patches:
        splot.annotate(format(p.get_height(), '.2f'), (p.get_x() + p.get_width() / 2., p.get_height()), ha = 'center', va = 'center', xytext = (0, 10), textcoords = 'offset points')

    plt.ylabel("Time")
    plt.savefig("Visualisation/Task-1/"+i+".png",bbox_inches='tight')

    plt.show()
    
#--------------------------------------------------------------------------------------------------------------------------

#TASK2



#heatmap
#To visualise the number of tasks scheduled on each worker per algorithm during each run

file_path=['worker1.log','worker2.log','worker3.log']
counts = dict()
counts['random'] = [0, 0, 0]
counts['round-robin'] = [0, 0, 0]
counts['least-loaded'] = [0, 0, 0]

for path in file_path:
    file = open(path, "r")
    for line in file.readlines():
        temp = line.split(";")
        if (temp[1].split()[0] == 'Started'):
            counts[temp[2]][int(path[-5]) - 1] += 1

counts['workers'] = ['worker1', 'worker2', 'worker3']

new = pd.DataFrame.from_dict(counts)
new.set_index("workers", inplace = True)

plt.figure(figsize=(14,7))
#ax=sns.heatmap(new,annot=True)
#bottom, top = ax.get_ylim()
#ax.set_ylim(bottom-1,top-1)

plt.title("Number of tasks scheduled per worker")
sns.heatmap(data=new, annot=True,linewidths=.5)
#plt.figure(figsize = (5,5))
plt.xlabel("Algorithm")
plt.savefig("Visualisation/Task-2/heatmap.png",bbox_inches='tight')
plt.show()



#step-graph

#To visualise the number of running tasks on every worker for every 3 algorithms for each run

file_path=['worker1.log','worker2.log','worker3.log']

color=['b','g','r','c','m','y','k','w']

algos = dict()
algos['random'] = [dict(),dict(),dict()]
algos['round-robin'] = [dict(),dict(),dict()]
algos['least-loaded'] = [dict(),dict(),dict()]
countt = dict()
countt['random'] = [0,0,0]
countt['round-robin'] = [0,0,0]
countt['least-loaded'] = [0,0,0]


worker=[]
for path in file_path: 

    file = open(path, "r")
    for line in file.readlines():
        temp = line.split(";")
        alg = temp[-2]
        # print(countt)
        if (temp[1].split()[0] == 'Started'):
            countt[alg][int(path[-5]) - 1] += 1
            algos[alg][int(path[-5]) - 1][temp[-1].split()[1]] = countt[alg][int(path[-5]) - 1]
        else:
            countt[alg][int(path[-5]) - 1] -= 1
            algos[alg][int(path[-5]) - 1][temp[-1].split()[1]] = countt[alg][int(path[-5]) - 1]
    file.close()



# figure size in inches
#rcParams['figure.figsize'] = 10,15


for algo in algos:
    # plt.figure(figsize=(8,8))
    # plt.xticks(range(min(valueX), max(valueX)+1))
    plt.title("Algorithm: {}".format(algo))
    for i in range(0,len(file_path)):
        x='worker' +str(i+1)
        try:
            plt.step(*zip(*sorted(algos[algo][i].items())),marker='o',color= color[i],label=x, where="post")
        except:
            pass
    plt.xlabel('Time')
    plt.ylabel('Running Tasks')
    sns.set(rc={'figure.figsize':(15,5)})
    sns.set_style("whitegrid")
    plt.legend()
    plt.savefig("Visualisation/Task-2/"+algo+".png",bbox_inches='tight')
    plt.show() 








