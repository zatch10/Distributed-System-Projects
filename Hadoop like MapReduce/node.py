# reference : https://www.simplifiedpython.net/python-threading-example/#:~:text=%20Python%20Threading%20Example%20%201%20Step%20,call%20the%20join%20%28%29%20function%20to...%20More%20
# reference : https://realpython.com/python-logging/#:~:text=%20Logging%20in%20Python%20%201%20The%20Logging,a%20string%20from%20your%20program%20as...%20More%20
# reference : https://wiki.python.org/moin/UdpCommunication

import threading
import logging
import os 
import socket
import argparse
import json
import random
from messages import Message
from datetime import datetime
from threading import Lock
import numpy as np
import sys
import time
import math
import os
from copy import deepcopy
import struct
import shutil
import subprocess
import glob
#Initializing Logger
logging.basicConfig(level=logging.DEBUG)
#Initializing Messaging object
messenger = Message()

#Initializing Mutex Lock Object for membership list
mutex = Lock()

#Initializing Mutex Lock for Gossipping
mutex_gossiping = Lock()
gossiping_massages = []#elements of (MESSAGE:json, time_when_receive:datetime.now())
known_gossiping_messages = []#elements of MESSAGE:json that already finish gossiping
process_end = False #whether all threads should end, if quit or aritificial fail
mutex_file_inst_lock = Lock()
mutex_file_inst = []#list acting as a queue for all file instructions:PUT, DELET, GET, as message_dict
file_inst_dict_lock = Lock()
file_inst_dict = {}#dicionary of <file_name, list of file_instrs>
BUFFERSIZE = 1024 #buffer size per round of transfer of data
loss_rate = 5 #5% loss rate
#list of 1:100 numbers
list_to_rand = list(range(1,101))





def parse():
    """
    This function parses the command-line flags

    Parameters: 
      None
    Returns:
      parser.parse_args object
    """
    parser = argparse.ArgumentParser(description='ECE428/CS425 MP1')
    parser.add_argument('--ip', dest="ip", type=str,
                        help='ip address of node')
    parser.add_argument('--algo', dest="algo", type=str,
                        help='enter gos for gossip or all for all-to-all')

    return parser.parse_args()

class Node:
    def __init__(self, new_ip_address : str, port : int, algo):
      """
      Constructor for the Node class.
      """
      self.IP_ADDRESS = new_ip_address
      self.PORT = int(port)
      #membership_dict : {key:<ip_address>, value:[time_stamp_of_last_join, last_heartbeat_time:datetime, heartbeatcounter, port:int, 4: file_dict]}
      self.membership_dict = {self.IP_ADDRESS:[time.time(), datetime.now(), 0, self.PORT, []]} # critical section variable
      self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
      self.socket.bind((self.IP_ADDRESS, self.PORT))
      self.has_joined = False # critical section variable
      self.all_to_all_OR_gossip_detection = algo
      self.file_port = 2002
      self.file_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
      self.file_socket.bind((self.IP_ADDRESS, self.file_port))
      self.file_socket.listen(10)
      self.total_num_maples = 0 #total number of maples
      self.sdfs_intermediate_filename_prefix = '' # sdfs_intermediate_filename_prefix for all Key K
      self.maples_finished_ips = [] # list of maples's ip finished
      self.maplejuice_task_assgined = {} # dictionary of key: ip, value:[files] of files assigned to ip 

      #clean node_files dir if exits
      if (os.path.lexists('node_files')):
        shutil.rmtree('node_files')

      #creat node_files dir
      os.mkdir('node_files')

    def send_join_request(self, intro_port:int):
        """
        This is the first function which sends the join request to the introducer. It keeps sending the join UDP packet until the Node is marked as joined. 

        Paramters:
            introducer_port : The introducer's port number to send the join request to. 
        """
        while ((self.has_joined is False) and (process_end is False)):
            logging.info(f'TIME : {datetime.now()} MESSAGE : Executing Node\'s join request. Introducer\'s target port is {intro_port}')
            messenger.send_join_request_to_socket("172.22.156.32", intro_port, self.socket, self.IP_ADDRESS, self.PORT)
            time.sleep(0.5)
    def listen(self):
        """
        This function makes the node object listen to other node objects about their membership list and subsequently updates its membership list.
        """
        #logging.info(f'TIME : {datetime.now()} MESSAGE : Executing Introducer\'s listen')
        while (not process_end):
            data, addr = self.socket.recvfrom(67108864) 
            if (not (data in known_gossiping_messages)):#only process new message, if haven't received same message
              known_gossiping_messages.append(data)#add new message to known messages
              message_dict = json.loads(data.decode('utf-8'))
              self.process_info(message_dict, data)
              
    def process_info(self, message_dict:dict, data):
        """
        Function to process messages sent by other nodes and handle them appropriately. 

        Parameters:
            message_dict: The message in a dictionary format, to make it easier for processing.
        """
        if (message_dict["Type"] == 'Ack') :
            logging.info(f'TIME : {datetime.now()} MESSAGE :Introducer has acknowledged the current node')
            mutex.acquire() # need to update membership dict and has_joined status.
            self.membership_dict = json.loads(message_dict["Membership_dict"])
            for ip in self.membership_dict.keys():
              self.membership_dict[ip][1] = datetime.now()
            #logging.info(f'TIME : {datetime.now()} MESSAGE : current membership dict \n {self.membership_dict}')
            self.has_joined = True
            mutex.release()
            #no need to gossip

        elif (message_dict["Type"] == 'Join_req'):
            #1.update memebership list if not already exist 
            #2.gossip the Join_req
            logging.info(f'Received a gossiping join request')
            mutex.acquire() # critical section
            if(not (message_dict['IP_address'] in self.membership_dict.keys())):
              self.membership_dict[message_dict['IP_address']] = [time.time(), datetime.now(), 0, int(message_dict['Port']), []]
              logging.info(f'update membership list : \n')
              self.print_memTable()
            mutex.release()
            #gossip the data
            mutex_gossiping.acquire()
            gossiping_massages.append((data,datetime.now()))#add on list of messages to gossip
            #logging.info(f'Gossip {data} started')
            mutex_gossiping.release()


        elif (message_dict["Type"] == 'Quit'):
            #This indicates that another other machine has Quit 
            #1.update membershipTable 
            #2.gossip the Quit
            logging.info(f'Received a gossiping Quit request')
            mutex.acquire() # critical section
            self.membership_dict.pop(message_dict['IP_address'])
            logging.info(f'update membership list : \n')
            self.print_memTable()
            mutex.release()
            #gossip the data
            mutex_gossiping.acquire()
            gossiping_massages.append((data,datetime.now()))#add on list of messages to gossip
            #logging.info(f'Gossip {data} started')
            mutex_gossiping.release()

        elif (message_dict["Type"] == 'Failure'):
            #This indicates that another machine has failed 
            #1.update membershipTable 
            #2.gossip the Failure
            fail_ip = message_dict['Failed_machine_ip']
            logging.info(f'Received a gossiping Failure request of ip:{fail_ip}')
            mutex.acquire() # critical section
            if (fail_ip in self.membership_dict.keys()):
              repair = threading.Thread(target = self.files_repair, args=(self.membership_dict[fail_ip][4].copy(), fail_ip))#do files_repair
              repair.start()
              self.membership_dict.pop(message_dict['Failed_machine_ip'])
              logging.info(f'update membership list : \n')
              self.print_memTable()
            mutex.release()
            #gossip the data
            mutex_gossiping.acquire()
            gossiping_massages.append((data,datetime.now()))#add on list of messages to gossip
            #logging.info(f'Gossip {data} started')
            mutex_gossiping.release()

        elif (message_dict["Type"] == 'all_to_all_heart_beat'):
          other_ip = message_dict['IP_address']
          mutex.acquire()
          if (other_ip in self.membership_dict.keys()):#receive normal heartbeat
            self.membership_dict[other_ip][1] = datetime.now()#this is last_recieve_time, update
          else:#if heartbeat arrive sooner than join request
            self.membership_dict[message_dict['IP_address']] = [time.time(), datetime.now(), 0, int(message_dict['Port']), []]
          mutex.release()

        elif (message_dict["Type"] == 'gossip__heartbeat'):
          other_ip = message_dict['IP_address']
          mutex.acquire()
          if (other_ip in self.membership_dict.keys()):#receive normal heartbeat
            #get the other_ip's membershipTable
            other_table = json.loads(message_dict["Membership_dict"])
            #merg Table
            for ip in self.membership_dict.keys():
              if (ip in other_table.keys()):
                if (other_table[ip][2] > self.membership_dict[ip][2]):
                  self.membership_dict[ip][2] = other_table[ip][2]
                  self.membership_dict[ip][1] = datetime.now()
          else:#if heartbeat arrive sooner than join request
            self.membership_dict[message_dict['IP_address']] = [time.time(), datetime.now(), 0, int(message_dict['Port']), []]
          mutex.release()

        elif (message_dict["Type"] == 'change_heartBeat_request'):
          #change scheme
          self.all_to_all_OR_gossip_detection = message_dict['scheme']
          #gossip the data
          mutex_gossiping.acquire()
          gossiping_massages.append((data,datetime.now()))#add on list of messages to gossip
          #logging.info(f'Gossip {data} started')
          mutex_gossiping.release()

        
        elif (message_dict["Type"] == 're_Join_req'):
          logging.info(f'Received a rejoin request')
          mutex.acquire() # critical section
          self.membership_dict[message_dict['IP_address']] = [time.time(), datetime.now(), 0, int(message_dict['Port']), []]
          logging.info(f'update membership list : \n')
          self.print_memTable()
          mutex.release()
          messenger.send_ack_msg_to_socket(message_dict['IP_address'], int(message_dict['Port']), self.socket, self.IP_ADDRESS, self.PORT, self.membership_dict)
          IP = message_dict['IP_address']
          Port = int(message_dict['Port'])
          logging.info(f'ack send to ip: {IP} port: {Port}')

          #gossip the join request
          mutex_gossiping.acquire()
          gossiping_massages.append((data,datetime.now()))#add on list of messages to gossip
          #logging.info(f'Gossip {data} started')
          mutex_gossiping.release()

        elif (message_dict["Type"] == 'put_request_to_master'):
          #send to node acrodinglly
          #to all ips who should and who has the sdfs_file_name
          ips_hashed = self.get_ips_from_filename(message_dict["sdfs_file_name"])
          ips_have = self.ips_have_filename(message_dict["sdfs_file_name"])
          allips = []
          if (len(ips_have) > 0):
            allips = ips_have
          else:
            allips = ips_hashed
          message_dict["Type"] = message_dict["Type"].replace('master', 'node')
          request_json = json.dumps(message_dict)
          for ip in allips:
            self.socket.sendto((request_json).encode('utf-8'), (ip, 2001))

        elif (message_dict["Type"] == 'get_request_to_master'):
          message_dict["Type"] = message_dict["Type"].replace('master', 'node')
          request_json = json.dumps(message_dict)
          allips = self.ips_have_filename(message_dict["sdfs_file_name"])
          if (len(allips) != 0):
            ip_to_send, = random.sample(allips, 1)
            self.socket.sendto((request_json).encode('utf-8'), (ip_to_send, 2001))
        
          
        elif (message_dict["Type"] == 'delete_request_to_master'):
          message_dict["Type"] = message_dict["Type"].replace('master', 'node')
          request_json = json.dumps(message_dict)
          allips = self.ips_have_filename(message_dict["sdfs_file_name"])
          if (len(allips) != 0):
            for ip in allips:
              self.socket.sendto((request_json).encode('utf-8'), (ip, 2001))


        elif (message_dict["Type"] == 'put_request_to_node' or message_dict["Type"] == 'get_request_to_node' or message_dict["Type"] == 'delete_request_to_node'):
          mutex_file_inst_lock.acquire()
          mutex_file_inst.append(message_dict)
          mutex_file_inst_lock.release()
          if (message_dict["Type"] == 'put_request_to_node'):
            messenger.send_put_ack(self.socket, 2001, self.get_master_ip(), message_dict["sdfs_file_name"], self.IP_ADDRESS)
          if (message_dict["Type"] == 'delete_request_to_node'):
            messenger.send_delete_ack(self.socket, 2001, self.get_master_ip(), message_dict["sdfs_file_name"], self.IP_ADDRESS)



        elif (message_dict["Type"] == 'put_ack'):
          mutex.acquire()
          if (not (message_dict["sdfs_file_name"] in self.membership_dict[message_dict['IP_putted']][4])):
            self.membership_dict[message_dict['IP_putted']][4].append(message_dict["sdfs_file_name"])
          mutex.release()
          #gossip the data
          mutex_gossiping.acquire()
          gossiping_massages.append((data,datetime.now()))#add on list of messages to gossip
          #logging.info(f'Gossip {data} started')
          mutex_gossiping.release()


        elif (message_dict["Type"] == 'delete_ack'):
          mutex.acquire()
          if (message_dict["sdfs_file_name"] in self.membership_dict[message_dict['IP_who_delete_file']][4]):
            self.membership_dict[message_dict['IP_who_delete_file']][4].remove(message_dict["sdfs_file_name"])
          mutex.release()
          #gossip the data
          mutex_gossiping.acquire()
          gossiping_massages.append((data,datetime.now()))#add on list of messages to gossip
          #logging.info(f'Gossip {data} started')
          mutex_gossiping.release()

        elif (message_dict["Type"] == 'maple_start_request_to_master'):
          #master assgin maple tasks
          self.total_num_maples = int(message_dict['num_maples'])
          self.sdfs_intermediate_filename_prefix = message_dict['sdfs_intermediate_filename_prefix']
          self.maples_finished_ips = []
          self.maplejuice_task_assgined = {}
          file_list = message_dict["file_list"]
          mutex.acquire()
          all_ips = sorted(list(self.membership_dict.keys()))
          mutex.release()
          if (len(file_list) < self.total_num_maples):
            for i in range(len(file_list)):
              self.maplejuice_task_assgined[all_ips[i]] = [file_list[i]]
          else:
            interval = int(len(file_list)/self.total_num_maples)
            file_list = np.array(file_list)
            for i in range(self.total_num_maples - 1):
              self.maplejuice_task_assgined[all_ips[i]] = list(file_list[i*interval:(i+1)*interval])
            self.maplejuice_task_assgined[all_ips[self.total_num_maples - 1]] = list(file_list[(self.total_num_maples - 1)*interval:])
          
          #send maple task to keys of task self.maplejuice_task_assgined[key]
          for each_ip in self.maplejuice_task_assgined.keys():
            messenger.send_maple_task_to_node(self.socket, each_ip , 2001, message_dict["maple_exe"], self.maplejuice_task_assgined[each_ip])


        elif (message_dict["Type"] == 'send_maple_task_to_node'):
          th = threading.Thread(target = self.maple_task, args=(message_dict,))
          th.start()

        elif (message_dict["Type"] == 'one_maple_task_complete'):
          self.maples_finished_ips.append(message_dict["finished_maple_ip"])
          if (len(self.maples_finished_ips) == self.total_num_maples):
            th = threading.Thread(target = self.origanize_maple_result)
            th.start()

        elif (message_dict["Type"] == 'juice_start_request_to_master'):
          #master assign juice task to juices
          self.total_num_juices = int(message_dict['num_juices'])
          self.sdfs_intermediate_filename_prefix = message_dict['sdfs_intermediate_filename_prefix']
          self.juice_finished_ips = []
          self.sdfs_dest_filename = message_dict['sdfs_dest_filename']
          self.delete_input = message_dict['delete_input']
          self.maplejuice_task_assgined = {}
          file_list_in_cache = glob.glob('./cache_files/' + self.sdfs_intermediate_filename_prefix + '*')
          mutex.acquire()
          all_ips = sorted(list(self.membership_dict.keys()))
          mutex.release()
          if (len(file_list_in_cache) < self.total_num_juices):#condition of hash or range partitioning is only placeholder
            for i in range(len(file_list_in_cache)):
              #hash partitioning
              tmp_file_name = file_list_in_cache[i].replace('./cache_files/','')
              tmp_ip = all_ips[hash(tmp_file_name)%self.total_num_juices]
              if (tmp_ip in self.maplejuice_task_assgined.keys()):
               self.maplejuice_task_assgined[tmp_ip].append(tmp_file_name)
              else: 
               self.maplejuice_task_assgined[tmp_ip] = [tmp_file_name]
          else:
            #range partitioning
            file_list_in_cache = sorted(file_list_in_cache)
            interval = int(len(file_list_in_cache)/self.total_num_juices)
            file_list_in_cache = np.char.replace(np.array(file_list_in_cache),'./cache_files/','')
            for i in range(self.total_num_juices - 1):
              self.maplejuice_task_assgined[all_ips[i]] = list(file_list_in_cache[i*interval:(i+1)*interval])
            self.maplejuice_task_assgined[all_ips[self.total_num_juices - 1]] = list(file_list_in_cache[(self.total_num_juices - 1)*interval:])
          
          #send juice tasks by key:ip self.maplejuice_task_assgined[key]
          for each_ip in self.maplejuice_task_assgined.keys():
            messenger.send_juice_task_to_node(self.socket, each_ip , 2001, message_dict["juice_exe"], self.maplejuice_task_assgined[each_ip])


        elif (message_dict["Type"] == 'send_juice_task_to_node'):
          th = threading.Thread(target = self.juice_task, args=(message_dict,))
          th.start()


        elif (message_dict["Type"] == 'one_juice_task_complete'):
          self.juice_finished_ips.append(message_dict["finished_juice_ip"])
          if (len(self.juice_finished_ips) == self.total_num_juices):
            th = threading.Thread(target = self.origanize_juice_result)
            th.start()


        #whenever adding new message at here, gossip it on condition


    def delete_intermidiate_files(self):
      #delete only intermidiate files
      mutex.acquire()
      all_ips = sorted(list(self.membership_dict.keys()))
      mutex.release()
      middle_key_files = []
      for this_ip in all_ips:
        mutex.acquire()
        middle_key_files = middle_key_files + self.membership_dict[this_ip][4]
        mutex.release()
        time.sleep(0.1)
      middle_key_files = np.unique(middle_key_files)
      speed_counter = 0
      for each_file_name in middle_key_files:
        if (self.sdfs_intermediate_filename_prefix in each_file_name or '_maple_result' in each_file_name or '_juice_result' in each_file_name):
          messenger.send_delete_request_to_master(self.socket, 2001, self.get_master_ip(), each_file_name)
          speed_counter = speed_counter + 1
          if (speed_counter%20 == 0):
            time.sleep(0.3)


    def remove_none_file_char_from_string(self, this_str:str):
      #helper function to replace punctuations in the string.
      tmp = this_str.replace('/','')
      tmp = tmp.replace(',','')
      tmp = tmp.replace(':','')
      tmp = tmp.replace('"','')
      tmp = tmp.replace('/','')
      tmp = tmp.replace(' ','')
      return tmp




    def origanize_juice_result(self):
      #download all result_files
      result_file_names = []
      speed_counter = 0
      for this_finished_ip in self.juice_finished_ips:
        this_sdfs_name = this_finished_ip + '_juice_result'
        result_file_names.append('./cache_files/' + this_sdfs_name)
        messenger.send_get_request_to_master(self.socket, self.IP_ADDRESS, 2001, self.get_master_ip(), this_sdfs_name, result_file_names[-1])
        speed_counter = speed_counter + 1
        if (speed_counter%20 == 0):
          time.sleep(0.3)
      self.wait_listof_download_complete(result_file_names)

      #merge files
      final_file_des = open('./cache_files/' + self.sdfs_dest_filename,'a')
      for each_file_name in result_file_names:
        this_file_des = open(each_file_name, 'r')
        while (True):
          this_line = this_file_des.readline()
          if (this_line == '' or this_line == '/n'):
            break
          final_file_des.write(this_line)

        this_file_des.close()
        #delete the file
        os.remove(each_file_name)
      final_file_des.close()
      #upload result
      messenger.send_put_request_to_master(self.socket, 2001, self.get_master_ip(), self.IP_ADDRESS, self.sdfs_dest_filename, './cache_files/' + self.sdfs_dest_filename)
      print('')
      print("Juice uploading results.")
      if (int(self.delete_input) == 1):
        print('')
        print("Juice cleaning intermediate files.")
        th = threading.Thread(target = self.delete_intermidiate_files)
        th.start()



    def origanize_maple_result(self):
      #download all result_files
      result_file_names = []
      for this_finished_ip in self.maples_finished_ips:
        this_sdfs_name = this_finished_ip + '_maple_result'
        result_file_names.append('./cache_files/' + this_sdfs_name)
        messenger.send_get_request_to_master(self.socket, self.IP_ADDRESS, 2001, self.get_master_ip(), this_sdfs_name, result_file_names[-1])
      self.wait_listof_download_complete(result_file_names)

      #merge files
      for each_file_name in result_file_names:
        this_file_des = open(each_file_name, 'r')
        while (True):
          this_pair = this_file_des.readline()
          if (this_pair == ''):
            break
          tmp_key = (this_pair.split(',')[0][1:])
          tmp_key = self.remove_none_file_char_from_string(tmp_key)
          key_file_name = self.sdfs_intermediate_filename_prefix + '_' + tmp_key
          key_file_des = open('./cache_files/' + key_file_name, 'a')
          key_file_des.write(this_pair)
          key_file_des.close()

        this_file_des.close()
        #delete the file
        os.remove(each_file_name)
      #upload all sdfs_intermediate_filename_prefix_Keys
      sdfs_intermediate_filename_prefix_Keys_file_names = glob.glob('./cache_files/' + self.sdfs_intermediate_filename_prefix + '*')
      speed_counter = 0
      for each_file_name in sdfs_intermediate_filename_prefix_Keys_file_names:
        speed_counter = speed_counter + 1
        if (speed_counter%100 == 0):
          time.sleep(0.3)
        messenger.send_put_request_to_master(self.socket, 2001, self.get_master_ip(), self.IP_ADDRESS, each_file_name.replace('./cache_files/',''), each_file_name)
      print('')
      print("Maple uploading results.")



    def wait_listof_download_complete(self, task_files:list):
      for last_file_name in task_files:
        prev_size = 0
        current_size = 0
        print(last_file_name)
        while (True):
          if (os.path.lexists(last_file_name)):
            time.sleep(0.1)
            current_size = os.path.getsize(last_file_name)
            if (current_size == prev_size):
              break
            else:
              prev_size = current_size


    def maple_task(self, message_dict):
      #clean cache_files
      if (os.path.lexists('cache_files')):
        shutil.rmtree('cache_files')
      os.mkdir('cache_files')

      #download all message_dict['task_files']
      last_file_name = []
      speed_counter = 0
      for this_task in message_dict['task_files']:
        last_file_name.append("./cache_files/" + this_task)
        messenger.send_get_request_to_master(self.socket, self.IP_ADDRESS, 2001, self.get_master_ip(), this_task, last_file_name[-1])
        speed_counter = speed_counter + 1
        if (speed_counter%20 == 0):
          time.sleep(0.3)
      #wait for download complete
      self.wait_listof_download_complete(last_file_name)
      
      #record results:
      result_file_name = "./cache_files/" + self.IP_ADDRESS + "_maple_result"
      result_file = open(result_file_name, 'a')
      maple_exe = message_dict["maple_exe"]
      for each_file in message_dict['task_files']:
        task_file_name = "./cache_files/" + each_file
        one_maple = subprocess.call(['python3', maple_exe, task_file_name], stdout = result_file)
      result_file.close()
      #1.uploads result to DFS and 2.send ack to master 
      messenger.send_put_request_to_master(self.socket, 2001, self.get_master_ip(), self.IP_ADDRESS, self.IP_ADDRESS + "_maple_result", result_file_name)
      messenger.send_one_maple_task_complete(self.socket, self.get_master_ip(), 2001, self.IP_ADDRESS)



    def juice_task(self, message_dict):
      #clean cache_files
      if (os.path.lexists('cache_files')):
        shutil.rmtree('cache_files')
      os.mkdir('cache_files')

      #download all message_dict['task_files']
      last_file_name = []
      speed_counter = 0
      for this_task in message_dict['task_files']:
        last_file_name.append("./cache_files/" + this_task)
        messenger.send_get_request_to_master(self.socket, self.IP_ADDRESS, 2001, self.get_master_ip(), this_task, last_file_name[-1])
        speed_counter = speed_counter + 1
        if (speed_counter%20 == 0):
          time.sleep(0.3)
      #wait for download complete
      print("Juice downloading keys")
      self.wait_listof_download_complete(last_file_name)
      print("Juice download finishes")
      #record results:
      result_file_name = './cache_files/' + self.IP_ADDRESS + '_juice_result'
      result_file = open(result_file_name, 'a')
      juice_exe = message_dict["juice_exe"]
      for each_file in message_dict['task_files']:
        task_file_name = "./cache_files/" + each_file
        one_juice = subprocess.call(['python3', juice_exe, task_file_name], stdout = result_file)
      result_file.close()
      #1.uploads result to DFS and 2.send ack to master 
      messenger.send_put_request_to_master(self.socket, 2001, self.get_master_ip(), self.IP_ADDRESS, self.IP_ADDRESS + "_juice_result", result_file_name)
      messenger.send_one_juice_task_complete(self.socket, self.get_master_ip(), 2001, self.IP_ADDRESS)




    def proces_file_instr(self):
      """
      Processes the queue of file instructions for the sdfs. For each instruction, perform the necessary operationby calling process_this_file_instr.

      Parameters: None, works with the mutex_file_inst

      returns: None
      """
      limit_wait_list = []
      while (not process_end):
        mutex_file_inst_lock.acquire()
        if (len(mutex_file_inst) != 0):
          while (len(mutex_file_inst) != 0):
            message_dict = mutex_file_inst.pop(0)
            file_inst_dict_lock.acquire()
            th = threading.Thread(target = self.process_this_file_instr, args=(message_dict["Type"][0], message_dict,))
            limit_wait_list.append(th)
            if (message_dict["sdfs_file_name"] in list(file_inst_dict.keys())):
             file_inst_dict[message_dict["sdfs_file_name"]].append([message_dict["Type"][0], message_dict["Timestamp"]])
            else:
             file_inst_dict[message_dict["sdfs_file_name"]] = []
             file_inst_dict[message_dict["sdfs_file_name"]].append([message_dict["Type"][0], message_dict["Timestamp"]]) 
            th.start()
            file_inst_dict_lock.release()
            if (len(limit_wait_list) >= 50):
              break
          mutex_file_inst_lock.release()
          for th in limit_wait_list:
            th.join()
          limit_wait_list = []
        else:
          mutex_file_inst_lock.release()
          time.sleep(0.3)



    def process_this_file_instr(self, my_type, message_dict):
      """
      Processes a particular file instruction.

      Parameters : 
        my_type : the type of the instruction
        message_dict : The message

      Returns : None
      """
      my_instr = [message_dict["Type"][0], message_dict["Timestamp"]]
      while (True):
        file_inst_dict_lock.acquire()
        li = (file_inst_dict[message_dict["sdfs_file_name"]]).copy()
        file_inst_dict_lock.release()
        index = li.index(my_instr)
        if (my_type == 'p' and index == 0):
          break
        elif (my_type == 'g' and np.char.count((np.array(li)[:index, 0]),'p').sum() == 0 and np.char.count((np.array(li)[:index, 0]),'d').sum() == 0):
          break
        elif (my_type == 'd' and np.char.count((np.array(li)[:index, 0]),'p').sum() == 0 and np.char.count((np.array(li)[:index, 0]),'g').sum() == 0):
          break            
        time.sleep(0.1)
      #1.process this file instruction 2.send ack to master 3.remove my_instr from file_inst_dict[message_dict["file_name"]]
      #PUT:
      if (my_type == 'p'):
        to_node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        to_node_socket.connect((message_dict["requestor_ip"], self.file_port))
        to_node_socket.send(('request ' + message_dict['local_file_name_to_store'].replace(' ','')).encode())
        file_size = int((to_node_socket.recv(1024)).decode())
        file_name = './node_files/' + message_dict["sdfs_file_name"]
        file_des = open(file_name, 'wb')
        to_node_socket.send(("ACK").encode())
        while True:
          if file_size >= BUFFERSIZE:   
            content = self.recv_msg(to_node_socket) 
            file_des.write(content) 
            file_size -= BUFFERSIZE 
          else:
            content = self.recv_msg(to_node_socket) 
            file_des.write(content)
            break
        
        file_des.close()
        to_node_socket.send('ACK'.encode())
        to_node_socket.close()  
        



      #GET:
      if (my_type == 'g'):
        to_node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        to_node_socket.connect((message_dict["requestor_ip"], self.file_port))
        to_node_socket.send(('transfer ' + message_dict['local_file_name_to_store'].replace(' ','')).encode())
        file_name = './node_files/' + message_dict["sdfs_file_name"]
        file_size = os.path.getsize(file_name)
        file_des = open(file_name, 'rb')
        to_node_socket.recv(1024)
        to_node_socket.send(str(file_size).encode())
        to_node_socket.recv(1024)
        while True:
            if file_size >= BUFFERSIZE: 
              content = file_des.read(BUFFERSIZE) 
              self.send_msg(to_node_socket, content)
              file_size -= BUFFERSIZE
            else:
              content = file_des.read(file_size)
              self.send_msg(to_node_socket, content)
              break

        file_des.close()
        to_node_socket.recv(3)
        to_node_socket.close()
        

      #DELETE:
      if (my_type == 'd'):
        file_name = './node_files/' + message_dict["sdfs_file_name"]
        if os.path.exists(file_name):
          os.remove(file_name)

      #remove my_instr from file_inst_dict[message_dict["file_name"]]:
      file_inst_dict_lock.acquire()
      file_inst_dict[message_dict["sdfs_file_name"]].remove(my_instr)
      file_inst_dict_lock.release()



    def file_transfer(self):
      """
      Driver function that transfers files between two machines.

      Parameters: None
      
      Returns: None 
      """
      while (not process_end):
        other_socket, addr = self.file_socket.accept()
        intr_type, file_name = ((other_socket.recv(1024)).decode()).split()
        if (intr_type == 'request'):
          file_size = os.path.getsize(file_name)
          other_socket.send(str(file_size).encode())
          file_des = open(file_name, 'rb')
          other_socket.recv(1024)
          while True:
            if file_size >= BUFFERSIZE: 
              content = file_des.read(BUFFERSIZE) 
              self.send_msg(other_socket, content)
              file_size -= BUFFERSIZE
            else:
              content = file_des.read(file_size)
              self.send_msg(other_socket, content)
              break

          file_des.close()
          other_socket.recv(3)

        if (intr_type == 'transfer'):
          file_des = open(file_name, 'wb')
          other_socket.send(("ACK").encode())
          file_size = int((other_socket.recv(1024)).decode())
          other_socket.send(("ACK").encode())
          while True:
            if file_size >= BUFFERSIZE:   
              content = self.recv_msg(other_socket) 
              file_des.write(content) 
              file_size -= BUFFERSIZE 
            else:
              content = self.recv_msg(other_socket) 
              file_des.write(content)
              break
        
          file_des.close()
          other_socket.send('ACK'.encode())
          if ('./node_files/' in file_name):
            messenger.send_put_ack(self.socket, 2001, self.get_master_ip(), file_name.split('/')[2], self.IP_ADDRESS)

        other_socket.close()
    
    
    def files_repair(self, sdfs_files_past:list, failed_ip : str):
      """
      Driver function to replace files when a machine crashes.

      Parameters : 
        sdfs_files_past : The list of sdfs files from the failed machine.
        failed_ip : The ip address of the failed machine
      
      Returns : None
      """

      time.sleep(3.5)
      mutex.acquire()
      all_ips = set(self.membership_dict.keys())
      mutex.release()
      for i in sdfs_files_past:
        temp = set(self.ips_have_filename(i))
        list_of_ips_without_file = list(all_ips - temp)
        if failed_ip in list_of_ips_without_file:
          list_of_ips_without_file.remove(failed_ip)
        list_of_ips_without_file.sort()
        if (self.IP_ADDRESS == list_of_ips_without_file[0]):
          messenger.send_get_request_to_master(self.socket, self.IP_ADDRESS, 2001, self.get_master_ip(), i, './node_files/' + i)
  

    def ips_have_filename(self, sdfs_file_name:str):
      """
      Checks if any of the machines have a particular file name.

      Parameters:
        sdfs_file_name : the file name to check for
      
      Returns : a list of ips that have the file
      """

      mutex.acquire()
      tmp_dict = self.membership_dict.copy()
      mutex.release()
      allips = list(tmp_dict.keys())
      ips_has_file = []
      for ip in allips:
        if (sdfs_file_name in tmp_dict[ip][4]):
          ips_has_file.append(ip)
      return ips_has_file.copy()

    def take_user_input(self):
        """
        Takes the user input for either quitting the machine from the group membership list or simulating a crash stop.

        Parameters:
          None
        Returns:
          None
        """
        #logging.info(f' PROCESS : {os.getpid()}  THREAD : {threading.currentThread().getName()} TIME : {datetime.now()} MESSAGE : Executing take_user_input')
        print("As a user you can input the following commands")
        print("q - quit")
        print("crash - crash machine")
        print("MSTable - print membershipTable")
        print("Enter gos for gossip_heatbeat or all for all-to-all_heatbeat")
        if self.all_to_all_OR_gossip_detection:
          logging.info(f'    Current failure detection: all-to-all')
        else:
          logging.info(f'    Current failure detection: gossip style')
        logging.info(f'ID: {self.IP_ADDRESS} {self.membership_dict[self.IP_ADDRESS][0]}\n')
        user_input = input("Enter User Command : ")
        file_user_input = deepcopy(user_input)
        user_input.strip()
        while (user_input != 'q' and user_input != 'crash'):
          if (user_input == 'MSTable'):
            self.print_memTable()
          elif (user_input == 'gos' and self.all_to_all_OR_gossip_detection):
            #to gos
            #1.change HeartBeat scheme 2.gossip the change, let others change as well(receiver should change as well)
            self.all_to_all_OR_gossip_detection = False
            change_heartBeat_request_dict = { 'Type' : "change_heartBeat_request", 
                  'scheme':False,
                  'Timestamp' : (str(datetime.now().isoformat(timespec='seconds'))).strip()
            }
            change_heartBeat_request_json = json.dumps(change_heartBeat_request_dict)
            data = (change_heartBeat_request_json).encode('utf-8')
            mutex_gossiping.acquire()
            gossiping_massages.append((data,datetime.now()))#add on list of messages to gossip
            #logging.info(f'Gossip {data} started')
            mutex_gossiping.release()
          elif (user_input == 'all' and (not self.all_to_all_OR_gossip_detection)):
            #to all
            #1.change HeartBeat scheme 2.gossip the change, let others change as well(receiver should change as well)
            self.all_to_all_OR_gossip_detection = True
            change_heartBeat_request_dict = { 'Type' : "change_heartBeat_request", 
                  'scheme':True,
                  'Timestamp' : (str(datetime.now().isoformat(timespec='seconds'))).strip()
            }
            change_heartBeat_request_json = json.dumps(change_heartBeat_request_dict)
            data = (change_heartBeat_request_json).encode('utf-8')
            mutex_gossiping.acquire()
            gossiping_massages.append((data,datetime.now()))#add on list of messages to gossip
            #logging.info(f'Gossip {data} started')
            mutex_gossiping.release()
          elif user_input == 'bytes':
            mutex.acquire()
            print(messenger.bytes_sent)
            mutex.release()

          elif user_input == 'store':
            mutex.acquire()
            for filename in self.membership_dict[self.IP_ADDRESS][4]:
              print(filename)
            mutex.release()
          
          elif ("ls" in user_input):
            temp = user_input.split()
            if (len(temp) < 2) :
              logging.info('Invalid-try again')
            else:
              sdfs_file_name = temp[1]
              if (len(self.ips_have_filename(sdfs_file_name)) == 0):
                print("File not present. \n")
              else:
                print(self.ips_have_filename(sdfs_file_name)) 

          elif ("put" in user_input):
            temp = user_input.split()
            if (len(temp) < 3):
              logging.info('Invalid-try again')
            else:
              local_file_name = temp[1]
              sdfs_file_name = temp[2]
              if (os.path.lexists(local_file_name)): 
                messenger.send_put_request_to_master(self.socket, 2001, self.get_master_ip(), self.IP_ADDRESS, sdfs_file_name, local_file_name)
              else:
                print('No such local file!\n')

          elif ("get" in user_input):
            temp = user_input.split()
            if (len(temp) < 3):
              logging.info('Invalid-try again')
            else:
              local_file_name = temp[2]
              sdfs_file_name = temp[1] 
              if (len(self.ips_have_filename(sdfs_file_name)) > 0):
                messenger.send_get_request_to_master(self.socket, self.IP_ADDRESS, 2001, self.get_master_ip(), sdfs_file_name, local_file_name)
              else:
                print('No such sdfs_file!\n')

          elif ("delete" in user_input):
            temp = user_input.split()
            if (len(temp) < 2):
              logging.info('Invalid-try again')
            else:
              sdfs_file_name = temp[1]
              if (len(self.ips_have_filename(sdfs_file_name)) > 0):
                messenger.send_delete_request_to_master(self.socket, 2001, self.get_master_ip(), sdfs_file_name)
              else:
                print('No such sdfs_file!\n')
          elif ("maple" in user_input):
            #maple <maple_exe> <num_maples>  <sdfs_intermediate_filename_prefix> <sdfs_src_directory>
            tmp = user_input.split()
            if (len(tmp) != 5):
              logging.info('Invalid-try again')
            else:
              maple_exe = tmp[1]
              num_maples = tmp[2]
              sdfs_intermediate_filename_prefix = tmp[3]
              sdfs_src_directory = tmp[4]
              if (os.path.isdir(sdfs_src_directory)):
                file_list = os.listdir(sdfs_src_directory)
                for this_file_name in file_list:#uploads all files to sdfs
                  this_file_path = './' + sdfs_src_directory + '/' + this_file_name
                  messenger.send_put_request_to_master(self.socket, 2001, self.get_master_ip(), self.IP_ADDRESS, this_file_name, this_file_path)
                messenger.send_maple_start_request_to_master(self.socket, 2001, self.get_master_ip(), maple_exe, num_maples, sdfs_intermediate_filename_prefix, file_list)
              else:
                print('No such sdfs_src_directory!\n')

          elif ("juice" in user_input):
            #juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={0,1}
            tmp = user_input.split(' ')
            if (len(tmp) != 6):
              logging.info('Invalid-try again')
            else:
              juice_exe = tmp[1]
              num_juices = tmp[2]
              sdfs_intermediate_filename_prefix = tmp[3]
              sdfs_dest_filename = tmp[4]
              delete_input = tmp[5]
              messenger.send_juice_start_request_to_master(self.socket, 2001, self.get_master_ip(), juice_exe, num_juices, sdfs_intermediate_filename_prefix, sdfs_dest_filename, delete_input)



          else:
            logging.info('Invalid-try again')
          print('')
          print('')
          print('')
          print('')
          print('')
          print("As a user you can input the following commands")
          print("q - quit")
          print("crash - crash machine")
          print("MSTable - print membershipTable")
          print("Enter gos for gossip_heatbeat or all for all-to-all_heatbeat")
          if self.all_to_all_OR_gossip_detection:
            logging.info(f'    Current failure detection: all-to-all')
          else:
            logging.info(f'    Current failure detection: gossip style')
          logging.info(f'ID: {self.IP_ADDRESS} {self.membership_dict[self.IP_ADDRESS][0]}\n')
          user_input = input("Enter User Command : ")
          user_input.strip()
  
        #Quit: if user enter 'q' : 1. send quit 2.quit(happen at the end of this function):
        if (user_input == 'q'):
          mutex.acquire()
          if (len(self.membership_dict.keys()) > 1):#if more than one machine present
            target_ip0, = random.sample(self.membership_dict.keys(), 1)
            while (target_ip0 == self.IP_ADDRESS):
              target_ip0, = random.sample(self.membership_dict.keys(), 1)
            target_port0 = int(self.membership_dict[target_ip0][3])
            messenger.send_quit_to_socket(target_ip0, target_port0, self.socket, self.IP_ADDRESS, self.PORT)
          mutex.release()

        self.socket.close()
        self.file_socket.close()
        process_end = True


    
    def print_memTable(self):
      for ip in self.membership_dict.keys():
        print(ip)

    def gossip_out(self):
      """
      Gossip the received information to the other nodes

      Parameters:
        None
      Return:
        None
      """
      while (not process_end):
        
        mutex_gossiping.acquire()
        mutex.acquire()
        if (len(gossiping_massages) != 0 and len(self.membership_dict.keys()) > 1):
          for entry in gossiping_massages:
            #number_of_memebers_to_gossip is the number of member to randomly selected to gossip
            number_of_memebers_to_gossip = 4
            for i in range(number_of_memebers_to_gossip):
              iprand, = random.sample(self.membership_dict.keys(), 1)
              while (iprand == self.IP_ADDRESS):#if iprand is current_machine_IP_address, reselect
                iprand, = random.sample(self.membership_dict.keys(), 1)
              target_ip_address = iprand
              target_port = int(self.membership_dict[target_ip_address][3])
              self.socket.sendto(entry[0], (target_ip_address, target_port))
            #check time if > O(log(N)), remove element from gossiping_massages
            elapsed_time = (datetime.now() - entry[1]).total_seconds()
            if (elapsed_time > math.log2(len(self.membership_dict.keys()))):
              #logging.info(f'Gossip {entry[0]} ended')
              gossiping_massages.remove(entry)
        elif (len(gossiping_massages) != 0 and len(self.membership_dict.keys()) == 1):
          gossiping_massages.clear()
        mutex.release()
        mutex_gossiping.release()
        time.sleep(0.1)

    def send_and_check_heartbeat(self):
      """
      Function to send and check heart beat or membershipTable     
      #time_stamp: time when machine joins
      #heartbeatcounter is blank in all to all, used in gossip style 
      Parameters:
        None
      Return:
        None

      """
      while (not process_end):#keep running until process ends
        if (self.all_to_all_OR_gossip_detection):#True if use alltoall
          #alltoall style
          messenger.send_all_to_all__heartbeat(self.socket, self.IP_ADDRESS, self.PORT, self.membership_dict)
          self.all_to_all_check_time_out()
        else:#False use gossip
          mutex.acquire()
          if (len(self.membership_dict.keys()) > 1):#if there are other nodes
            #update own heartbeat counter and localtime
            self.membership_dict[self.IP_ADDRESS][1]=datetime.now()
            self.membership_dict[self.IP_ADDRESS][2]=self.membership_dict[self.IP_ADDRESS][2] + 1
            mutex.release()
            #send to randomly selected k=3 members
            messenger.send_gossip__heartbeat(self.socket, self.IP_ADDRESS, self.PORT, self.membership_dict, 3)
            self.gossip_style_check_time_out()
          else:
            mutex.release()  
        time.sleep(0.1)#sleep for next heart beat

    def gossip_style_check_time_out(self):
        """
        This is a helper function of send_and_check_heartbeat to check for time outs during gossipping. 
        Uses the local time from self.membership_dict[key][1]
        Parameters: 
          None 
        Returns : 
          None
        """   
        #find failed then send the failure, set that failed heartbeat counter as -1, only delete failed at the next round(Tcleanup == 0.1)
        mutex.acquire()
        all_ip = list(self.membership_dict.keys())
        for checking_ip_address in all_ip:
          if (checking_ip_address == self.IP_ADDRESS):
            continue
          #0.if heartbeat_counter==-1, delete since failed, continue
          if (self.membership_dict[checking_ip_address][2] == -1):
            fail_ip = checking_ip_address
            repair = threading.Thread(target = self.files_repair, args=(self.membership_dict[fail_ip][4].copy(), fail_ip))#do files_repair
            repair.start()
            self.membership_dict.pop(fail_ip)
            continue


          #check failure
          local_time = datetime.now()
          last_heartbeat_time_at_index = self.membership_dict[checking_ip_address][1]
          if (type(last_heartbeat_time_at_index) == str):
            last_heartbeat_time_at_index = datetime.strptime(last_heartbeat_time_at_index, '%Y-%m-%d %H:%M:%S.%f')
          if ((local_time - last_heartbeat_time_at_index).total_seconds() > max(1.1*math.log2(len(self.membership_dict.keys())), 2)):#larger than gossiping time == time for any node to quite safly
            #failes,
            #1.set that failed heartbeat counter as -1,deltete at the next round 
            #2.append to known_gossiping_messages to avoid confusion 
            #3.gossip this failure of checking_ip_address, 
            fail_ip = checking_ip_address
            logging.info(f'Failure on ip:{fail_ip}')
            self.membership_dict[fail_ip][2] = -1
            #generate data of Type Failure to add on known_gossiping_messages and then gossip it
            failure_to_send = { 'Type' : "Failure", 
              'Failed_machine_ip' : fail_ip,
              'Timestamp' : (str(datetime.now().isoformat(timespec='seconds'))).strip()
            }
            failure_to_send_json = json.dumps(failure_to_send)
            data = (failure_to_send_json).encode('utf-8')
            known_gossiping_messages.append(data)
            mutex.release()
            #gossip the data
            mutex_gossiping.acquire()
            gossiping_massages.append((data,datetime.now()))#add on list of messages to gossip
            #logging.info(f'Gossip {data} started')
            mutex_gossiping.release()
            mutex.acquire()
        #else no failure
        mutex.release()

    def all_to_all_check_time_out(self): 
        """
        This is a helper function of send_and_check_heartbeat to check for time outs during all to all heartbeating. 
        Uses the local time from self.membership_dict[key][1]
        Parameters: 
          None 
        Returns : 
          None
        """            
        mutex.acquire()
        all_ip = list(self.membership_dict.keys())
        for checking_ip_address in all_ip:
          if (checking_ip_address == self.IP_ADDRESS):
            continue
          local_time = datetime.now()
          last_heartbeat_time_at_index = self.membership_dict[checking_ip_address][1]
          if (type(last_heartbeat_time_at_index) == str):
            last_heartbeat_time_at_index = datetime.strptime(last_heartbeat_time_at_index, '%Y-%m-%d %H:%M:%S.%f')
          if ((local_time - last_heartbeat_time_at_index).total_seconds() > max(1.1*math.log2(len(self.membership_dict.keys())), 2)):#larger than gossiping time == time for any node to quite safly
            #failes, 1.remove from membership list then 2.append to known_gossiping_messages to avoid confusion 3.gossip this failure of checking_ip_address
            fail_ip = checking_ip_address
            logging.info(f'Failure on ip:{fail_ip}')
            repair = threading.Thread(target = self.files_repair, args=(self.membership_dict[fail_ip][4].copy(),fail_ip))#do files_repair
            repair.start()
            self.membership_dict.pop(fail_ip)
            logging.info(f'update membership list : \n')
            self.print_memTable()
            #generate data of Type Failure to add on known_gossiping_messages and then gossip it
            failure_to_send = { 'Type' : "Failure", 
              'Failed_machine_ip' : fail_ip,
              'Timestamp' : (str(datetime.now().isoformat(timespec='seconds'))).strip()
            }
            failure_to_send_json = json.dumps(failure_to_send)
            data = (failure_to_send_json).encode('utf-8')
            known_gossiping_messages.append(data)
            mutex.release()
            #gossip the data
            mutex_gossiping.acquire()
            gossiping_massages.append((data,datetime.now()))#add on list of messages to gossip
            #logging.info(f'Gossip {data} started')
            mutex_gossiping.release()
            mutex.acquire()
        #else no failure
        mutex.release()
    
    def get_master_ip(self):
      #helper function to get the master ip_address.
      return sorted(list(self.membership_dict.keys()))[0]

    def get_ips_from_filename(self, filename:str):
      #helper function to get ips from a file
      hash_str = str(hash(filename))
      index = int(hash_str[len(hash_str) - 1])
      mutex.acquire()
      keys = list(self.membership_dict.keys())
      mutex.release()
      ips = sorted(keys)
      length = len(ips)
      return list([ips[(index + 0)%length], ips[(index + 1)%length], ips[(index + 2)%length], ips[(index + 3)%length]])

    #Functions below provide better/more stable data transfer than built-in functions, from stack-overflow      
    def send_msg(self, sock, msg):
      # Prefix each message with a 4-byte length (network byte order)
      msg = struct.pack('>I', len(msg)) + msg
      sock.sendall(msg)

    def recv_msg(self, sock):
      # Read message length and unpack it into an integer
      raw_msglen = self.recvall(sock, 4)
      if not raw_msglen:
          return None
      msglen = struct.unpack('>I', raw_msglen)[0]
      # Read the message data
      return self.recvall(sock, msglen)

    def recvall(self, sock, n):
      # Helper function to recv n bytes or return None if EOF is hit
      data = bytearray()
      while len(data) < n:
          packet = sock.recv(n - len(data))
          if not packet:
              return None
          data.extend(packet)
      return data

if __name__ == '__main__':
    try:
        args = parse()
        ip_address = args.ip
        # port = args.port
        port = 2001
        # intro_port = args.introducer_port
        intro_port = 2001
        if args.algo == "all":
          algo = True
        else:
          algo = False
        node_object = Node(str(ip_address), port, algo)
        logging.info(f'TIME : {datetime.now()} MESSAGE : Executing Node.py')
        t1 = threading.Thread(target = node_object.send_join_request, args=(intro_port,))
        t2 = threading.Thread(target = node_object.listen, args=())
        t3 = threading.Thread(target = node_object.send_and_check_heartbeat, args = ())
        t4 = threading.Thread(target = node_object.take_user_input, args= ())
        t5 = threading.Thread(target = node_object.gossip_out, args= ())
        t6 = threading.Thread(target = node_object.proces_file_instr, args= ())
        t7 = threading.Thread(target = node_object.file_transfer, args= ())
        #Start thread execution
        t1.start()
        t2.start()
        t3.start()
        t4.start()
        t5.start()
        t6.start()
        t7.start()
        #Join thread Execution for completion
        t4.join()
        os._exit(os.EX_OK)
    except Exception as e:
        logging.error(f'An error occured while trying to create node, make sure all flag values have been entered : {e}')
