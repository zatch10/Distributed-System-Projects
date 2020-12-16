import json
import socket
import os
import random
import sys
from datetime import datetime
#membership_dict key:<ip_address>, value:[0:time_stamp_of_last_join, 1:last_heartbeat_time:datetime, 2:heartbeatcounter, 3:port:int, 4: list_of_files]
#date_time_obj = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S.%f')
#file_dict key:file_name, value:[(list of ips that have file)]
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

      def send_put_request_to_master(self, current_machine_socket:socket, master_port:int, master_IP_address:str, requestor_ip:str, sdfs_file_name:str, local_file_name_to_store:str):
        put_request = {'Type' : "put_request_to_master",
                  'sdfs_file_name' : sdfs_file_name,
                  'local_file_name_to_store':local_file_name_to_store,
                  'Timestamp' : (str(datetime.now().isoformat(timespec='seconds'))).strip(),
                  'requestor_ip' : requestor_ip
                  }
        put_request_json = json.dumps(put_request)
        current_machine_socket.sendto((put_request_json).encode('utf-8'), (master_IP_address, master_port))

      
      def send_put_ack(self, current_machine_socket:socket, master_port:int, master_IP_address:str, sdfs_file_name:str, current_machine_ip:str):
        put_ack = {'Type' : "put_ack",
                  'IP_putted' : current_machine_ip,
                  'sdfs_file_name' : sdfs_file_name,
                  'Timestamp' : (str(datetime.now().isoformat(timespec='seconds'))).strip(),
                  }
        put_ack_json = json.dumps(put_ack)
        current_machine_socket.sendto((put_ack_json).encode('utf-8'), (master_IP_address, master_port))
          
      def send_get_request_to_master(self, current_machine_socket:socket, requestor_ip:str, master_port:int, master_IP_address:str, sdfs_file_name:str, local_file_name_to_store:str):
        """
          This function sends a get request

          Parameters:
          current_machine_socket (socket) : This machine's socket.
          current_machine_IP_address (string) : the requester machine's IP address
          current_port (integer) : the requester machine's port
          master_IP_address (string): the master's ip address or the ip address of the node containing the file
          master_port (integer) : the master's port number or the port number of the node containing the file
          file_name (string) : name of the file to be deleted
          """
        get_request = {'Type' : "get_request_to_master",
                  'sdfs_file_name' : sdfs_file_name,
                  'local_file_name_to_store' : local_file_name_to_store,
                  'Timestamp' : (str(datetime.now().isoformat(timespec='seconds'))).strip(),
                  'requestor_ip' : requestor_ip
                  }
        get_request_json = json.dumps(get_request)
        current_machine_socket.sendto((get_request_json).encode('utf-8'), (master_IP_address, master_port))
        
      def send_delete_request_to_master(self, current_machine_socket:socket, master_port:int, master_IP_address:str, sdfs_file_name:str):
        """
          This function sends a delete request

          Parameters:
          current_machine_socket (socket) : This machine's socket
          master_IP_address (string): the master's ip address or the ip address of the node containing the file
          master_port (integer) : the master's port number or the port number of the node containing the file
          file_name (string) : name of the file to be deleted
          """
        delete_request = {'Type' : "delete_request_to_master",
                  'sdfs_file_name' : sdfs_file_name,
                  'Timestamp' : (str(datetime.now().isoformat(timespec='seconds'))).strip()
                  }
        delete_request_json = json.dumps(delete_request)
        current_machine_socket.sendto((delete_request_json).encode('utf-8'), (master_IP_address, master_port))
            
      def send_delete_ack(self, current_machine_socket:socket, master_port:int, master_IP_address:str, sdfs_file_name:str, current_machine_IP_address:str):
        """
          This function sends a delete acknowledgement to the master once file is deleted 

          Parameters:
          current_machine_socket (socket) : This machine's socket
          master_IP_address (string): the master's ip address or the ip address of the node containing the file
          master_port (integer) : the master's port number or the port number of the node containing the file
          file_name (string) : name of the file to be deleted
          """
        delete_ack = {'Type' : "delete_ack",
                  'IP_who_delete_file': current_machine_IP_address,
                  'sdfs_file_name' : sdfs_file_name,
                  'Timestamp' : (str(datetime.now().isoformat(timespec='seconds'))).strip()
                  }
        delete_ack_json = json.dumps(delete_ack)
        current_machine_socket.sendto((delete_ack_json).encode('utf-8'), (master_IP_address, master_port))
        
    
      def send_maple_start_request_to_master(self, current_machine_socket:socket, master_port:int, master_IP_address:str, maple_exe:str, num_maples:str, sdfs_intermediate_filename_prefix:str, file_list:list):
        maple_start_request_to_master = {'Type' : "maple_start_request_to_master",
                  'maple_exe' : maple_exe,
                  'num_maples' : num_maples,
                  'sdfs_intermediate_filename_prefix' : sdfs_intermediate_filename_prefix,
                  'file_list' : file_list,
                  'Timestamp' : (str(datetime.now().isoformat(timespec='seconds'))).strip()
                  }
        maple_start_request_to_master_json = json.dumps(maple_start_request_to_master)
        current_machine_socket.sendto((maple_start_request_to_master_json).encode('utf-8'), (master_IP_address, master_port))
      

      def send_maple_task_to_node(self, current_machine_socket:socket, target_ip_address:str, target_port:int, maple_exe:str, task_files:list):
        send_maple_task_to_node_dic = {'Type' : "send_maple_task_to_node",
                  'maple_exe' : maple_exe,
                  'task_files' : task_files,
                  'Timestamp' : (str(datetime.now().isoformat(timespec='seconds'))).strip()
                  }
        send_maple_task_to_node_json = json.dumps(send_maple_task_to_node_dic)
        current_machine_socket.sendto((send_maple_task_to_node_json).encode('utf-8'), (target_ip_address, target_port))
      

      def send_one_maple_task_complete(self, current_machine_socket:socket, master_IP_address:str, master_port:int, current_machine_IP_address:str):
        one_maple_task_complete = {'Type' : "one_maple_task_complete",
                      'finished_maple_ip' : current_machine_IP_address,
                      'Timestamp' : (str(datetime.now().isoformat(timespec='seconds'))).strip()   
        }
        one_maple_task_complete_json = json.dumps(one_maple_task_complete)
        current_machine_socket.sendto((one_maple_task_complete_json).encode('utf-8'), (master_IP_address, master_port))


      def send_juice_start_request_to_master(self, current_machine_socket:socket, master_port:int, master_IP_address:str, juice_exe:str, num_juices:str, sdfs_intermediate_filename_prefix:str, sdfs_dest_filename:str, delete_input:str):
        juice_start_request_to_master = {'Type' : "juice_start_request_to_master",
                      'juice_exe' : juice_exe,
                      'num_juices' : num_juices,
                      'sdfs_intermediate_filename_prefix' : sdfs_intermediate_filename_prefix,
                      'sdfs_dest_filename' : sdfs_dest_filename,
                      'delete_input' : delete_input,
                      'Timestamp' : (str(datetime.now().isoformat(timespec='seconds'))).strip()   
        }
        juice_start_request_to_master_json = json.dumps(juice_start_request_to_master)
        current_machine_socket.sendto((juice_start_request_to_master_json).encode('utf-8'), (master_IP_address, master_port))


      def send_juice_task_to_node(self, current_machine_socket:socket, target_ip_address:str, target_port:int, juice_exe:str, task_files:list):
        send_juice_task_to_node_dic = {'Type' : "send_juice_task_to_node",
                  'juice_exe' : juice_exe,
                  'task_files' : task_files,
                  'Timestamp' : (str(datetime.now().isoformat(timespec='seconds'))).strip()
                  }
        send_juice_task_to_nodee_json = json.dumps(send_juice_task_to_node_dic)
        current_machine_socket.sendto((send_juice_task_to_nodee_json).encode('utf-8'), (target_ip_address, target_port))

      def send_one_juice_task_complete(self, current_machine_socket:socket, master_IP_address:str, master_port:int, current_machine_IP_address:str):
        one_juice_task_complete = {'Type' : "one_juice_task_complete",
                      'finished_juice_ip' : current_machine_IP_address,
                      'Timestamp' : (str(datetime.now().isoformat(timespec='seconds'))).strip()   
        }
        one_juice_task_complete_json = json.dumps(one_juice_task_complete)
        current_machine_socket.sendto((one_juice_task_complete_json).encode('utf-8'), (master_IP_address, master_port))