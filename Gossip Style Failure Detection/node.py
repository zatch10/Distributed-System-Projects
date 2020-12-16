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
import sys
import time
import math
import os
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
    parser.add_argument('--intro_port', dest="introducer_port", type=int,
                        help='port number')
    parser.add_argument('--port', dest="port", type=int,
                        help='port number')
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
      #membership_dict : {key:<ip_address>, value:[time_stamp_of_last_join, last_heartbeat_time:datetime, heartbeatcounter, port:int]}
      self.membership_dict = {self.IP_ADDRESS:[time.time(), datetime.now(), 0, self.PORT]} # critical section variable
      self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
      self.socket.bind((self.IP_ADDRESS, self.PORT))
      self.has_joined = False # critical section variable
      self.all_to_all_OR_gossip_detection = algo



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
        logging.info(f'TIME : {datetime.now()} MESSAGE : Executing Introducer\'s listen')
        while (not process_end):
            data, addr = self.socket.recvfrom(1024) 
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
            logging.info(f'TIME : {datetime.now()} MESSAGE : current membership dict \n {self.membership_dict}')
            self.has_joined = True
            mutex.release()
            #no need to gossip

        elif (message_dict["Type"] == 'Join_req'):
            #1.update memebership list if not already exist 
            #2.gossip the Join_req
            logging.info(f'Received a gossiping join request')
            mutex.acquire() # critical section
            if(not (message_dict['IP_address'] in self.membership_dict.keys())):
              self.membership_dict[message_dict['IP_address']] = [time.time(), datetime.now(), 0, int(message_dict['Port'])]
              logging.info(f'update membership list : \n')
              self.print_memTable()
            mutex.release()
            #gossip the data
            mutex_gossiping.acquire()
            gossiping_massages.append((data,datetime.now()))#add on list of messages to gossip
            logging.info(f'Gossip {data} started')
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
            logging.info(f'Gossip {data} started')
            mutex_gossiping.release()

        elif (message_dict["Type"] == 'Failure'):
            #This indicates that another machine has failed 
            #1.update membershipTable 
            #2.gossip the Failure
            fail_ip = message_dict['Failed_machine_ip']
            logging.info(f'Received a gossiping Failure request of ip:{fail_ip}')
            mutex.acquire() # critical section
            if (fail_ip in self.membership_dict.keys()):
              self.membership_dict.pop(message_dict['Failed_machine_ip'])
              logging.info(f'update membership list : \n')
              self.print_memTable()
            mutex.release()
            #gossip the data
            mutex_gossiping.acquire()
            gossiping_massages.append((data,datetime.now()))#add on list of messages to gossip
            logging.info(f'Gossip {data} started')
            mutex_gossiping.release()

        elif (message_dict["Type"] == 'all_to_all_heart_beat'):
          other_ip = message_dict['IP_address']
          mutex.acquire()
          if (other_ip in self.membership_dict.keys()):#receive normal heartbeat
            self.membership_dict[other_ip][1] = datetime.now()#this is last_recieve_time, update
          else:#if heartbeat arrive sooner than join request
            self.membership_dict[message_dict['IP_address']] = [time.time(), datetime.now(), 0, int(message_dict['Port'])]
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
            self.membership_dict[message_dict['IP_address']] = [time.time(), datetime.now(), 0, int(message_dict['Port'])]
          mutex.release()

        elif (message_dict["Type"] == 'change_heartBeat_request'):
          #change scheme
          self.all_to_all_OR_gossip_detection = message_dict['scheme']
          #gossip the data
          mutex_gossiping.acquire()
          gossiping_massages.append((data,datetime.now()))#add on list of messages to gossip
          logging.info(f'Gossip {data} started')
          mutex_gossiping.release()
        #whenever adding new message at here, gossip it on condition



  
    def take_user_input(self):
        """
        Takes the user input for either quitting the machine from the group membership list or simulating a crash stop.

        Parameters:
          None
        Returns:
          None
        """
        logging.info(f' PROCESS : {os.getpid()}  THREAD : {threading.currentThread().getName()} TIME : {datetime.now()} MESSAGE : Executing take_user_input')
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
            logging.info(f'Gossip {data} started')
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
            logging.info(f'Gossip {data} started')
            mutex_gossiping.release()
          elif user_input == 'bytes':
            mutex.acquire()
            print(messenger.bytes_sent)
            mutex.release()
          else:
            logging.info('Invalid-try again')
          
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
              logging.info(f'Gossip {entry[0]} ended')
              gossiping_massages.remove(entry)
        elif (len(gossiping_massages) != 0 and len(self.membership_dict.keys()) == 1):
          logging.info(f'No one on memberShip Table, clear all redundent gossip messages')
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
            logging.info(f'Gossip {data} started')
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
            logging.info(f'Gossip {data} started')
            mutex_gossiping.release()
            mutex.acquire()
        #else no failure
        mutex.release()

if __name__ == '__main__':
    try:
        args = parse()
        ip_address = args.ip
        port = args.port
        intro_port = args.introducer_port
        if args.algo == "all":
          algo = True
        else:
          algo = False
        node_object = Node(str(ip_address), port, algo)
        logging.info(f'TIME : {datetime.now()} MESSAGE : Executing Node.py')
        t1 = threading.Thread(target=node_object.send_join_request, args=(intro_port,))
        t2 = threading.Thread(target = node_object.listen, args=())
        t3 = threading.Thread(target = node_object.send_and_check_heartbeat, args = ())
        t4 = threading.Thread(target = node_object.take_user_input, args= ())
        t5 = threading.Thread(target = node_object.gossip_out, args= ())
        #Start thread execution
        t1.start()
        t2.start()
        t3.start()
        t4.start()
        t5.start()
        #Join thread Execution for completion
        t4.join()
        os._exit(os.EX_OK)
    except Exception as e:
        logging.error(f'An error occured while trying to create node, make sure all flag values have been entered : {e}')
