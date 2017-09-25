from __future__ import print_function
from multiprocessing import Process
import multiprocessing
import collections
import numpy as np
import os, sys, time
import pandas as pd
import psutil
import os
from multiprocessing import Manager
import collections
import warnings
warnings.filterwarnings("ignore")
import time
import inspect
import random
import requests
import json

class time_est():
    def __init__(self, total_len):
        self.t_start = time.time()
        self.total_len = total_len
        self.count = 0
        self.t_ref = time.time()
    
    def check(self,no_of_check=1,info=""):
        self.count += no_of_check
        if time.time() - self.t_ref > 1 and self.count > 0:
            t_used = time.time() - self.t_start
            t_total = t_used * self.total_len / self.count
            t_remain = t_total - t_used
            process_bar = "|"
            for i in range(40):
                if (i/40) < (self.count/self.total_len):
                    process_bar += "█"
                else:
                    process_bar += " "
            process_bar += "|"
            if info != "":
                info = str(info) + "  "
            print("\r" + (str(info) + "{:.2f}% ({}/{})  ".format(self.count * 100/self.total_len, self.count,self.total_len)) 
                  + str(process_bar).ljust(45) 
                  + "Used: {:02.0f}:{:02.0f}:{:02.0f}".format(int(t_used/3600), int(t_used/60)%60, t_used % 60).ljust(16) 
                  + "ETA: {:02.0f}:{:02.0f}:{:02.0f}".format(int(t_remain/3600), int(t_remain/60)%60, t_remain % 60),end="")
            self.t_ref = time.time()
        if self.count == self.total_len:
            t_used = time.time() - self.t_start
            if info != "":
                info = str(info) + "  "
            print("\r" + str(info) + "Finished in " 
                  + "{:02.0f}:{:02.0f}:{:02.0f}".format(int(t_used/3600), int(t_used/60)%60, t_used % 60).ljust(100))
    def get(self,no_of_check=1):
        process_bar = "|"
        for i in range(40):
            if (i/40) < (self.count/self.total_len):
                process_bar += "█"
            else:
                process_bar += " "
        process_bar += "|"
        self.count += no_of_check
        t_used = time.time() - self.t_start
        t_total = t_used * self.total_len / self.count
        t_remain = t_total - t_used
        return "{} ETA: {:02.0f}:{:02.0f}:{:02.0f}".format(process_bar, int(t_remain/3600), int(t_remain/60)%60, t_remain % 60)
        
        
def get_cpu_usage():
    cpu_percent = int(psutil.cpu_percent())
    while cpu_percent == 0:
        cpu_percent = int(psutil.cpu_percent())
    if cpu_percent > 99:
        cpu_percent = 99
    return cpu_percent
            
class MP():

    def __init__(self, max_thread=64, process_start_duration=0, 
                 cpu_max=95, servers=[]):
        self.servers = servers
        self.parameter_list = []
        self.max_thread = max_thread
        self.process_start_duration = process_start_duration
        self.cpu_max = cpu_max
        self.run_func = self.local_func
    
    def give(self,i):
        self.parameter_list.append(i)
        
    def store(self, key, value):
        self.return_dict[key] = value
    
    def get(self):
        return self.return_dict.copy()
    
    def local_func(self, process_num):
        value = list(self.object_func(*self.parameter_dict[process_num]))
        self.store(key=process_num, value=value)
        
    def map_func(self, process_num):
        code = inspect.getsource(self.object_func)
        func_name = str(self.object_func.__name__)
        data_dict = self.list_to_dict(self.parameter_dict[process_num])
        while 1:
            try:
                url = self.servers[process_num % len(self.servers)]
                dict_data = self.auto_send(url, code, data_dict, func_name)
                break
            except Exception as e:
                print(e)
                pass
            time.sleep(0.1)

        value = list(self.dict_to_list(dict_data))
        self.store(key=process_num, value=value)
    
    def run_for_loop(self):
        self.return_dict = {}
        self.est = time_est(len(self.parameter_dict.keys()))
        for process_num in self.parameter_dict:
            self.local_func(process_num)
            self.est.check()
        self.parameter_dict = {}
        
    def run(self, object_func, mode=1, print_flag=True):
        self.print_flag = print_flag
        self.object_func = object_func
        self.parameter_dict = {}
        for i in range(len(self.parameter_list)):
            self.parameter_dict[i] = tuple(self.parameter_list[i])
        

        if mode==0:
            self.run_for_loop()
        elif mode==1:
            self.run_func = self.local_func
            self.run_multiprocess()
        else:
            self.run_func = self.map_func
            self.run_multiprocess()

            
    def clean_process_and_get_cpu(self):
        p1_key = list(self.p1.keys()).copy()
        if len(p1_key) > 64 or time.time() - self.t_ref > 1:
            self.t_ref = time.time()
            self.cpu_percent = get_cpu_usage()
            for process in p1_key:
                if not self.p1[process].is_alive():
                    self.ETA = self.est.get()
                    del self.p1[process]
                    
    def start_process(self, process_num):
        if self.process_start_duration > 0:
            time.sleep(self.process_start_duration)
        self.p1[process_num] = Process(target=self.run_func, args=(process_num,))
        self.p1[process_num].start()
    
    def get_rem_thread_and_print(self,process_num):
        self.rem_thread = len(multiprocessing.active_children()) - self.original_no_threads
        if self.print_flag:
            print('\r{} process, {:<2} cpu, {}/{} {}        '
            .format(self.rem_thread, self.cpu_percent, process_num + 1 - self.rem_thread, self.total_len, self.ETA), end="")

    def wait_for_threads(self, process_num):
        while self.rem_thread >= self.max_thread or self.cpu_percent >= self.cpu_max:
            if self.rem_thread < self.max_thread and self.cpu_percent < self.cpu_max:
                break
            time.sleep(0.1)
            self.clean_process_and_get_cpu()
            self.get_rem_thread_and_print(process_num)        
    
    def run_multiprocess(self):
        self.return_dict = Manager().dict()
                
        self.ETA = ""
        self.p1 = collections.defaultdict(lambda: 1)

        self.total_len = len(self.parameter_dict.keys())
        if self.print_flag:
            print("")
            
        self.original_no_threads = len(multiprocessing.active_children())
        self.est = time_est(len(self.parameter_list))
        
        self.cpu_percent = -1
        self.t_ref = time.time()
        
        for process_num in self.parameter_dict:
            self.get_rem_thread_and_print(process_num)
            self.wait_for_threads(process_num)
            self.clean_process_and_get_cpu()
            self.start_process(process_num)

        while self.rem_thread > 0: 
            self.clean_process_and_get_cpu()
            time.sleep(1)
            self.get_rem_thread_and_print(process_num)

        self.clean_process_and_get_cpu()
        if self.print_flag:            
            self.est.check(no_of_check=0, info=self.object_func.__name__)
                
        self.parameter_dict = {}
        self.parameter_list = []
        
    def data_to_json(self, data):
        json_data = {}
        data_type = {}

        for i in data:
            data_type[i] = str(type(data[i]))
            if type(data[i]) == np.ndarray:
                json_data[i] = data[i].tolist()
            elif type(data[i]) == pd.core.frame.DataFrame:
                json_data[i] = data[i].to_json()
            else:
                json_data[i] = data[i]

        return json_data, data_type

    def json_to_data(self, json_data, data_type):
        data = {}

        for i in json_data:
            dtype = data_type[i]
            if dtype == str(np.ndarray):
                data[i] = np.array(json_data[i])
            elif dtype == str(pd.core.frame.DataFrame):
                data[i] = pd.read_json(json_data[i])
            else:
                data[i] = json_data[i]
        return data


    def auto_send(self, url, code, data, func_name):
        json_data, data_type = self.data_to_json(data)
        post_dict = {"code": code,
                    "data":json_data,
                    "data_type":data_type, 
                    "func_name": func_name}
        r = requests.post(url, json=post_dict)
        receive = json.loads(r.text)
        receive = self.json_to_data(receive["data"], receive["data_type"])
        return receive

    def list_to_dict(self, data):
        data_dict = {v: k for v, k in enumerate(data)}
        return data_dict

    def dict_to_list(self, dict_data):
        data = [dict_data[i] for i in list(dict_data)]
        return data
