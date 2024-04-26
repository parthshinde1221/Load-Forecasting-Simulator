# %%
import pandas as pd
import numpy as np
# import freeport

# %%
# !pip install timesynth

# %%
import zmq
import random
import sys
import time
import pickle
# import threading
from threading import Thread

class client:
    def __init__(self,clientId,datasetHourly,datasetDaily=None):
        self.clientId = clientId
        self.datasetHourly = datasetHourly
        self.datasetDaily = datasetDaily
        self.context = zmq.Context()


    def recalibrator(self,df):
        df["client_id"] = self.clientId
        df = df.iloc[:,1:]
        start_datetime = pd.Timestamp.now().floor('H')
        df['datetime'] = [start_datetime + pd.Timedelta(hours=i) for i in range(len(df))]
        return df

    def publishDataHourly(self):
        socket = self.context.socket(zmq.PUSH)
        # socket.connect("tcp://localhost:5556")
        socket.connect("tcp://192.168.1.244:5556")
        self.datasetHourly = self.recalibrator(self.datasetHourly)
        self.datasetHourly.to_csv(f"client_{self.clientId}.csv")
        df = pd.read_csv(f"client_{self.clientId}.csv")
        df = df[0:SIMULATION_RUN_TIME]

        for _, row in df.iterrows():
            # print(f"{topic}, {row}")
            print(row.values,f"data sent{self.clientId}")
            serialized_row = pickle.dumps(row,protocol=4)
            socket.send(serialized_row)
            time.sleep(1)


    # def publishDataDaily(self):
    #     topic = '#daily'
    #     socket = self.createSocket()
    #     context = zmq.Context()
    #     socket = context.socket(zmq.PUSH)
    #     socket.connect("tcp://localhost:5555")

    #     for _, row in self.datasetHourly.iterrows():
    #         print(f"{topic}, {row}")
    #         serialized_row = pickle.dumps(row)
    #         socket.send(serialized_row)

# %%
# def createDummyDataset():
dataset = pd.read_csv('household_power_consumption_hourly.csv')
dataset = dataset[['datetime','Global_active_power']]
print(len(dataset))

# %%
def introNoise(dataset):
    return dataset

# samples, signals, errors = timeseries.sample(irregular_time_samples)

# %%
NO_OF_CLIENTS = 5
# This is the number of hours
SIMULATION_RUN_TIME = 360
# 600 hours 24 days - gives hourly predictions of every 10 hours after training/retraining


# class MyThread(threading.Thread):
#     def __init__(self, target):
#         super().__init__(target=target)
#         self._stop_event = threading.Event()

#     def run(self):
        
#         while not self._stop_event.is_set():
#             try:

#                 if self._target:
#                     try:
#                         self._target(*self._args, **self._kwargs)
#                     except Exception as e:
#                         print(e)

#             finally:
#                 # Avoid a refcycle if the thread is running a function with
#                 # an argument that has a member that points to the thread.
#                 del self._target, self._args, self._kwargs

#     def stop(self):
#         self._stop_event.set()

class clientSimulator:
    def __init__(self,noOfClients=NO_OF_CLIENTS,dataset=dataset,dataType='hourly'):
        print('---------Starting simulator----------')
        self.noOfClients = noOfClients
        self.dataType = dataType
        self.datasetSim = dataset
        self.clientObjList = []

    
    # def splitDatasetCLients(self):   
    #     # newDatasetArray = np.array_split(self.datasetSim, self.noOfClients)
    #     newDatasetArray = [self.datasetSim for i in range(self.noOfClients)]
    #     datasetList = []
    #     for clientIndex,newData in enumerate(newDatasetArray):
    #         newDataset = pd.DataFrame(data=newData,columns=['datetime','Global_active_power'])
    #         newDataset = introNoise(newDataset)
    #         datasetList.append(newDataset)
    #     return datasetList
    
    def createClients(self):
        print('Creating Clients .....................')
        for clientIdx in range(self.noOfClients):
            newDataset = pd.DataFrame(data=self.datasetSim,columns=['datetime','Global_active_power'])
            newDataset = introNoise(newDataset)
            clientObj = client(clientIdx,datasetHourly=newDataset)
            self.clientObjList.append(clientObj)
            print(f'Client {clientIdx} created..')
            time.sleep(2)
        print('Finished creating clients')

    def simulate(self):
        self.createClients()
        threadPool = []
        print('-----Starting client side simulation--')
        # create threadPool
        for client in self.clientObjList:
            threadPool.append(Thread(target=client.publishDataHourly))
        
            
        runTime = SIMULATION_RUN_TIME
        

        for thread in threadPool:
            thread.start()
            time.sleep(1)
        
        # timer = threading.Timer(runTime,thread.join)
        # timer.start

        # while runTime > 0:
        #     # start threadPool
            
        #     runTime = runTime - 1
        #     print(f'Time.....{runTime}')
        #     time.sleep(1)
        
        # end threadPool
        for thread in threadPool:
            thread.join()

        for index,client in enumerate(self.clientObjList):
            client.context.term()
            print(f'Client {index} SuccessFully closed')
        



        

        print('--------------END OF SIMULATION-----------')

# %%
clientSimulator().simulate()

# %%



