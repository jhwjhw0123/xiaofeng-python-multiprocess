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

    def __init__(self, max_restart=10, max_thread=64, penalty=100000000, process_start_duration=0, 
                 cpu_max=95):
        self.parameter_list = []
        self.max_restart = max_restart
        self.max_thread = max_thread
        self.penalty = penalty
        self.process_start_duration = process_start_duration
        self.cpu_max = cpu_max

    def give(self,i):
        self.parameter_list.append(i)
        
    def store(self,value,key):
        self.return_dict[key] = value
    
    def get(self):
        return self.return_dict.copy()
        
    def run(self, object_func, print_flag=True, for_loop=False):
            
        parameter_list = self.parameter_list
        
        name_list = []
        for i in parameter_list:
            name_list.append(tuple(i))
        name_set = name_list[:]
        
        if for_loop==False:
            self.return_dict = Manager().dict()
        else:
            self.return_dict = collections.defaultdict()
            est = time_est(len(name_set))
            for i in name_set:
                object_func(*i)
                est.check()
            return
        
        max_restart = self.max_restart
        max_thread = self.max_thread
        penalty = self.penalty
        process_start_duration = self.process_start_duration
        cpu_max = self.cpu_max
        return_dict = self.return_dict
        ETA = ""
        alive1 = collections.defaultdict(lambda: 1)
        p1 = collections.defaultdict(lambda: 1)

        total_len = len(name_list)
        t_save_ref = time.time()
        if print_flag:
            print("")
        original_thread_number = len(multiprocessing.active_children())
        est = time_est(len(parameter_list))
        cpu_percent = -1
        while len(name_set) > 0:
            t_ref = time.time()
            for i in name_set:
                i_index = name_list.index(i)

                if cpu_max < 100:
                    cpu_percent = get_cpu_usage()
                rem_thread = len(multiprocessing.active_children()) - original_thread_number
                if print_flag:
                    print('\r{} process, {:<2} cpu, {}/{} {}        '
                       .format(rem_thread, cpu_percent, name_list.index(i) + 1 - rem_thread, total_len, ETA), end="")
                
                while rem_thread >= max_thread or cpu_percent >= cpu_max:
                    if rem_thread < max_thread and cpu_percent < cpu_max:
                        break
                    time.sleep(0.1)
                    if cpu_max < 100:
                        cpu_percent = get_cpu_usage()
                    rem_thread = len(multiprocessing.active_children()) - original_thread_number
                    if print_flag:
                        print('\r{} process, {:<2} cpu, {}/{} {}        '
                           .format(rem_thread, cpu_percent, (name_list.index(i) + 1 - rem_thread), total_len, ETA), end="")
                
                p1_key = list(p1.keys()).copy()
                if len(p1_key) > 64 or time.time() - t_ref > 1:
                    t_ref = time.time()
                    for process in p1_key:
                        if not p1[process].is_alive():
                            ETA = est.get()
                            del alive1[process]
                            del p1[process]
                            
                if alive1[i_index] == 1:
                    if process_start_duration > 0:
                        time.sleep(process_start_duration)
                    p1[i_index] = Process(target=object_func, args=i)
                    p1[i_index].start()

            rem_thread = len(multiprocessing.active_children()) - original_thread_number
            while rem_thread > 0:
                if cpu_max < 100:
                    cpu_percent = get_cpu_usage()
                rem_thread = len(multiprocessing.active_children()) - original_thread_number
                
                p1_key = list(p1.keys()).copy()
                for process in p1_key:
                    if not p1[process].is_alive():
                        ETA = est.get()
                        del alive1[process]
                        del p1[process]
                time.sleep(1)
                if print_flag:
                    print('\r{} process, {:<2} cpu, {}/{} {}        '
                       .format(rem_thread, cpu_percent, name_list.index(i) + 1 - rem_thread, total_len, ETA), end="")
            
            p1_key = list(p1.keys()).copy()
            while len(p1_key) > 0:
                p1_key = list(p1.keys()).copy()
                for process in p1_key:
                    if not p1[process].is_alive():
                        ETA = est.get()
                        del alive1[process]
                        del p1[process]
            name_set = []
            if print_flag:            
                est.check(no_of_check=0, info=object_func.__name__)
        self.parameter_list = []