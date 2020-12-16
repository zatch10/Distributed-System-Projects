import json
import socket
import os
import random
import sys
from datetime import datetime
#membership_dict key:<ip_address>, value:[0:time_stamp_of_last_join, 1:last_heartbeat_time:datetime, 2:heartbeatcounter, 3:port:int]
#date_time_obj = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S.%f')
class Message:
      def __init__(self):
        self.bytes_sent = 0

      def send_join_request_to_socket(self, target_ip_address:str, target_port:int, current_machine_socket:socket, current_machine_IP_address:str, current_port:int):
            """
            This function sends a join request to the introducer machine.

            Parameters:
                  target_ip_address (string) : The IP_address of the introducer machine to send the join request to.
                  target_port (int) : The introducer's port number
                  current_machine_socket (socket) : This machine's socket.
                  current_machine_IP_address (string) : the current machine's IP address
                  timestamp : The timestamp of the message sent.      
            """
            join_request_dict = { 'Type' : "Join_req", 
                  'Process_id' : (str(os.getpid())).strip(), 
                  'IP_address' : current_machine_IP_address,
                  'Timestamp' : (str(datetime.now().isoformat(timespec='seconds'))).strip(),
                  'Port' : str(current_port)
            }
            join_request_json = json.dumps(join_request_dict)
            self.bytes_sent += (sys.getsizeof(join_request_json) / 1024)
            current_machine_socket.sendto((join_request_json).encode('utf-8'), (target_ip_address, target_port))
  
      def send_ack_msg_to_socket(self,target_ip_address:str, target_port:int, current_machine_socket:socket, current_machine_IP_address:str, current_port:int, membership_dict:dict):
            """
            This function sends an acknowledge message to the specified machine.

            Parameters:
                  target_ip_address (string) : The IP_address of the specified machine to send the acknowledge message to.
                  target_port (int) : The specified machine's port number
                  current_machine_socket (socket) : This machine's socket.
                  current_machine_IP_address (string) : the current machine's IP address
                  timestamp : The timestamp of the message sent.        
            """
            for ip in membership_dict.keys():
              membership_dict[ip][1] = str(membership_dict[ip][1])
            ack_msg_dict = {'Type' : "Ack",
                  'Process_id' : (str(os.getpid())).strip(), 
                  'IP_address' : current_machine_IP_address,
                  'Timestamp' : (str(datetime.now().isoformat(timespec='seconds'))).strip(),
                  'Port' : str(current_port),
                  'Membership_dict' : json.dumps(membership_dict)
                  }
            ack_msg_json = json.dumps(ack_msg_dict)
            self.bytes_sent += (sys.getsizeof(ack_msg_json) / 1024)
            current_machine_socket.sendto((ack_msg_json).encode('utf-8'), (target_ip_address, target_port))
  
      def send_quit_to_socket(self,target_ip_address:str, target_port:int, current_machine_socket:socket, current_machine_IP_address:str, current_port:int):
            """ 
            This functions sends a quit message to the introducer machine.

            Parameters:
            target_ip_address (string) : The IP_address of the machine to send the quit request to.
            target_port (int) : The machine's port number
            current_machine_socket (socket) : This machine's socket.
            current_machine_IP_address (string) : the current machine's IP address
            timestamp : The timestamp of the message sent.
            """
            quit_to_send = { 'Type' : "Quit", 
                  'IP_address' : current_machine_IP_address,
                  'Timestamp' : (str(datetime.now().isoformat(timespec='seconds'))).strip()
            }
            quit_to_send_json = json.dumps(quit_to_send)
            self.bytes_sent += (sys.getsizeof(quit_to_send_json) / 1024)
            current_machine_socket.sendto((quit_to_send_json).encode('utf-8'), (target_ip_address, target_port))


      #TODO send_failure_to_socket, use 'Type' : "Failure",
      def send_failure_to_socket(self,target_ip_address:str, target_port:int, current_machine_socket:socket, current_machine_IP_address:str, current_port:int):
            """ 
            This functions sends a quit message to the introducer machine.

            Parameters:
            target_ip_address (string) : The IP_address of the machine to send the quit request to.
            target_port (int) : The machine's port number
            current_machine_socket (socket) : This machine's socket.
            current_machine_IP_address (string) : the current machine's IP address
            current_port : The current machine's port
            """
            failure_to_send = { 'Type' : "Failure", 
                  'Failed_machine_ip' : Failed_machine_ip,
                  'Timestamp' : (str(datetime.now().isoformat(timespec='seconds'))).strip()
            }
            failure_to_send_json = json.dumps(failure_to_send)
            self.bytes_sent += (sys.getsizeof(failure_to_send_json) / 1024)
            current_machine_socket.sendto((failure_to_send_json).encode('utf-8'), (target_ip_address, target_port))

      def send_all_to_all__heartbeat(self,current_machine_socket:socket, current_machine_IP_address:str, current_port:int, membership_dict:dict):
            """ 
            This functions sends all to all heartbeats.

            Parameters:
            current_machine_socket (socket) : This machine's socket.
            current_machine_IP_address (string) : the current machine's IP address
            current_port : The current machine's port number
            membership_dict : The membership dictionary of the system
            """
            heartbeat_dict = {'Type' : "all_to_all_heart_beat",
                  'IP_address' : current_machine_IP_address,
                  'Port' : current_port,
                  'Timestamp' : (str(datetime.now().isoformat(timespec='seconds'))).strip()
                  }
            heartbeat_json = json.dumps(heartbeat_dict)
            for target_ip_address in membership_dict.keys():
              target_port = membership_dict[target_ip_address][3]
              if (target_ip_address == current_machine_IP_address):
                continue
              self.bytes_sent += (sys.getsizeof(heartbeat_json) / 1024)
              current_machine_socket.sendto((heartbeat_json).encode('utf-8'), (target_ip_address, target_port))
      
      def send_gossip__heartbeat(self,current_machine_socket:socket, current_machine_IP_address:str, current_port:int, membership_dict:dict, number_of_members_to_gossip):
          """
          This function sends gossip heartbeats to the specified number

          Parameters:
          current_machine_socket (socket) : This machine's socket.
          current_machine_IP_address (string) : the current machine's IP address
          current_port : The current machine's port number
          membership_dict : The membership dictionary of the system
          """
          for ip in membership_dict.keys():
            membership_dict[ip][1] = str(membership_dict[ip][1])
          gossip__heartbeat_msg_dict = {'Type' : "gossip__heartbeat",
                'IP_address' : current_machine_IP_address,
                'Timestamp' : (str(datetime.now().isoformat(timespec='seconds'))).strip(),
                'Port' : str(current_port),
                'Membership_dict' : json.dumps(membership_dict)
                }
          gossip_heartbeat_msg_json = json.dumps(gossip__heartbeat_msg_dict)
          data = (gossip_heartbeat_msg_json).encode('utf-8')

          for i in range(number_of_members_to_gossip):
            iprand, = random.sample(membership_dict.keys(), 1)
            while (iprand == current_machine_IP_address):#if iprand is current_machine_IP_address, reselect
              iprand, = random.sample(membership_dict.keys(), 1)
            target_ip_address = iprand
            target_port = int(membership_dict[target_ip_address][3])
            self.bytes_sent += (sys.getsizeof(gossip_heartbeat_msg_json) / 1024)
            current_machine_socket.sendto(data, (target_ip_address, target_port))
        
    
