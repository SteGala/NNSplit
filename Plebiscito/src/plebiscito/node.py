'''
This module impelments the behavior of a node
'''

import json
import queue
from datetime import datetime, timedelta
import copy
import logging
import time
import requests
from plebiscito.utils import *
import sys
from plebiscito.server import PlebiscitoServer
import numpy as np
import math
import threading
from kubernetes import client, config
import random
from queue import Empty

class InternalError(Exception):
    "Raised when the input value is less than 18"
    pass


# cose da implementare
# trasformare l'ID in una stringa in modo da poter prendere il nome del pod
# fare in modo di generare la topologia nel momento in cui si riceve la crd, e quindi far usare al sistema di bidding la topologia specifica (mappa?)
# potrebbe valer la pena fare la stessa cosa con gli IP

TRACE = 5

class node:

    def __init__(self, communication_port, alpha_value, utility, node_name, use_net_topology=False):
        self.id = get_container_ip()
        self.node_ip = get_container_ip()
        self.node_name = node_name
        self.use_net_topology = False

        n_GPU = get_total_gpu_cores()
        if n_GPU == None:
            # sys.exit(f"Failed to load GPU data on node. Exiting...")
            logging.info(
                f"Failed to get number of GPU core on the node. Setting the value to 0")
            n_GPU = 0.00001
        self.initial_gpu = float(n_GPU)
        self.updated_gpu = self.initial_gpu

        self.total_memory = get_total_memory()

        n_cpu = get_total_cpu_cores()
        if n_cpu == None:
            sys.exit(f"Failed to get number of CPU core on the node. Exiting...")
        self.initial_cpu = float(n_cpu)
        self.updated_cpu = self.initial_cpu

        self.q = queue.Queue()
        self.user_requests = []
        self.item = {}
        self.bids = {}
        self.tmp_item = {}
        self.layers = 0
        self.sequence = True
        self.alpha = alpha_value
        self.utility = utility

        self.available_cpu_per_task = {}
        self.available_gpu_per_task = {}
        self.available_bw_per_task = {}

        self.last_sent_msg = {}
        self.resource_remind = {}

        # it is not possible to have NN with more than 50 layers
        self.cum_cpu_reserved = 0
        self.cum_gpu_reserved = 0
        self.cum_bw_reserved = 0
        
        self.initial_bw = 100000000000
        self.updated_bw = self.initial_bw
                 
        self.use_net_topology = use_net_topology

        # -----
        # Shared variables among threads
        # -----
        self.__shared_resource_lock = threading.Lock()
        self.topology = {}
        self.ips = {}
        self.cpu_bid = {}
        self.gpu_bid = {}
        self.last_bid = {}
        #self.ongoing_bidding = False

        self.__enable_bidding_flag_lock = threading.Lock()
        self.bidding_enabled = True
        # END shared variables
        
        self.communication_port = communication_port
        self.server = PlebiscitoServer(self.q, communication_port)
        self.server.run_server()

        logging.info(
            f"Initialized plebiscito node instance with ID `{self.id}`. CPU: {self.initial_cpu}, GPU: {self.initial_gpu}, RAM: {self.total_memory}")

    def __generate_node_id_from_ips(self, ips):
        def is_ip_greater_than(ip1, ip2):
            octets1 = list(map(int, ip1.split('.')))
            octets2 = list(map(int, ip2.split('.')))

            for i in range(len(octets1)):
                if octets1[i] < octets2[i]:
                    return False
                elif octets1[i] > octets2[i]:
                    return True

            return False  # IP addresses are equal

        sorted_ips = sort_ip_addresses(ips)

        count = 0
        container_ip = get_container_ip()
        for ip in sorted_ips:
            if is_ip_greater_than(ip, container_ip):
                break
            count = count + 1

        return count

    def append_data(self, d):
        self.q.put(d)

    def join_queue(self):
        self.q.join()

    def utility_function(self, avail_bw, avail_cpu, avail_gpu):
        def f(x, alpha, beta):
            if beta == 0 and x == 0:
                return 1
            
            # shouldn't happen
            if beta == 0 and x != 0:
                return 0
            
            # if beta != 0 and x == 0 is not necessary
            return math.exp(-((alpha/100) * (x - beta))**2)
            #return math.exp(-(alpha/100)*(x-beta)**2)

        if (isinstance(avail_bw, float) and avail_bw == float('inf')):
            avail_bw = self.initial_bw
        
        # we assume that every job/node has always at least one CPU
        if self.utility == 'stefano':
            x = 0
            if self.item['NN_gpu'][0] == 0:
                x = 0
            else:
                x = self.item['NN_cpu'][0]/self.item['NN_gpu'][0]
                
            beta = 0
            if avail_gpu == 0:
                beta = 0
            else:
                beta = avail_cpu/avail_gpu
            if self.alpha == 0:
                return f(x, 0.01, beta)
            else:
                return f(x, self.alpha, beta)
        elif self.utility == 'alpha_BW_CPU':
            return (self.alpha*(avail_bw/self.initial_bw))+((1-self.alpha)*(avail_cpu/self.initial_cpu)) #BW vs CPU
        elif self.utility == 'alpha_GPU_CPU':
            return (self.alpha*(avail_gpu/self.initial_gpu))+((1-self.alpha)*(avail_cpu/self.initial_cpu)) #GPU vs CPU
        elif self.utility == 'alpha_GPU_BW':
            return (self.alpha*(avail_gpu/self.initial_gpu))+((1-self.alpha)*(avail_bw/self.initial_bw))
    
    def forward_to_neighbohors(self):
        with self.__shared_resource_lock:
            for ip in (self.ips[self.item['job_id']]):
                if self.id != ip:
                    data = {
                        "job_id": self.item['job_id'],
                        "user": self.item['user'],
                        "edge_id": self.id,
                        "auction_id": self.bids[self.item['job_id']]['auction_id'],
                        "NN_gpu": list(self.item['NN_gpu']),
                        "NN_cpu": list(self.item['NN_cpu']),
                        "NN_data_size": list(self.item['NN_data_size']),
                        "bid": self.bids[self.item['job_id']]['bid'],
                        #"x": self.bids[self.item['job_id']]['x'],
                        "N_layer": self.item["N_layer"],
                        "N_layer_min": self.item["N_layer_min"],
                        "N_layer_max": self.item["N_layer_max"],
                        "N_layer_bundle": self.item["N_layer_bundle"],
                        "ram":self.item["ram"],
                        "timestamp": datetime_list_to_string_list(self.bids[self.item['job_id']]['timestamp']),
                        "duration": self.item["duration"],
                        "ips": self.item["ips"],
                    }

                    # Send POST request to the server
                    url = f"http://{ip}:{self.communication_port}"
                    headers = {'Content-type': 'application/json'}

                    try:
                        response = requests.post(
                            url, data=json.dumps(data), headers=headers)
                        response.raise_for_status()  # Raise exception for non-2xx status codes

                        # Check the return code
                        if response.status_code != 200:
                            logging.info("Server response:", response.json())
                    except requests.exceptions.RequestException as e:
                        logging.info(
                            "An error occurred while sending the request:", e)

                    logging.debug("FORWARD " + str(self.id) + " to " + str(ip) +
                                " " + str(self.bids[self.item['job_id']]['auction_id']))

    def print_node_state(self, msg, bid=False, type='debug'):
        # logger_method = getattr(logging, type)
        logging.info(str(msg) +
                     " - edge_id:" + str(self.id) +
                     " job_id:" + str(self.item['job_id']) +
                     " from_edge:" + str(self.item['edge_id']) +
                     " available GPU:" + str(self.updated_gpu) +
                     " available CPU:" + str(self.updated_cpu) +
                     (("\n"+str(self.bids[self.item['job_id']]['auction_id']) if bid else "") +
                      ("\n"+str(self.item['auction_id']) if bid else "\n")))
        
    def save_node_state(self):
        with self.__shared_resource_lock:
            self.last_bid[self.item['job_id']] = copy.deepcopy(self.bids[self.item['job_id']]['auction_id'])


    def update_local_val(self, tmp, index, id, bid, timestamp, lst):
        tmp['job_id'] = self.item['job_id']
        tmp['auction_id'][index] = id
        tmp['bid'][index] = bid
        tmp['timestamp'][index] = timestamp
        return index + 1

    def reset(self, index, dict, bid_time):
        dict['auction_id'][index] = float('-inf')
        dict['bid'][index]= float('-inf')
        dict['timestamp'][index] = bid_time # - timedelta(days=1)
        return index + 1

    def init_null(self):
        # print(self.item['duration'])
        self.bids[self.item['job_id']]={
            "count":0,
            "consensus_count":0,
            "forward_count":0,
            "deconflictions":0,
            "job_id": self.item['job_id'], 
            "user": int(), 
            "auction_id": list(), 
            "NN_gpu": self.item['NN_gpu'], 
            "NN_cpu": self.item['NN_cpu'], 
            "NN_data_size": self.item['NN_data_size'],
            "bid": list(), 
            "bid_gpu": list(), 
            "bid_cpu": list(), 
            "bid_bw": list(), 
            "timestamp": list(),
            "arrival_time":datetime.now(),
            "start_time": 0, #datetime.now(),
            "progress_time": 0, #datetime.now(),
            "duration": random.randint(10, 100), #self.item['duration'],
            "complete":False,
            "complete_timestamp":None,
            "N_layer_min": self.item["N_layer_min"],
            "N_layer_max": self.item["N_layer_max"],
            "edge_id": self.id, 
            "N_layer": self.item["N_layer"],
            'consensus':False,
            'clock':False,
            'rebid':False,
            "N_layer_bundle": self.item["N_layer_bundle"]
            }
        
        self.available_gpu_per_task[self.item['job_id']] = [self.updated_gpu]
        self.available_cpu_per_task[self.item['job_id']] = [self.updated_cpu]
        if not self.use_net_topology:
            self.available_bw_per_task[self.item['job_id']] = self.updated_bw
        else:
            self.bw_with_nodes[self.item['job_id']] = {}
            self.bw_with_client[self.item['job_id']] = self.network_topology.get_available_bandwidth_with_client(self.id)
        
        NN_len = self.item['N_layer']
        
        for _ in range(0, NN_len):
            self.bids[self.item['job_id']]['bid'].append(float('-inf'))
            self.bids[self.item['job_id']]['bid_gpu'].append(float('-inf'))
            self.bids[self.item['job_id']]['bid_cpu'].append(float('-inf'))
            self.bids[self.item['job_id']]['bid_bw'].append(float('-inf'))
            self.bids[self.item['job_id']]['auction_id'].append(float('-inf'))
            self.bids[self.item['job_id']]['timestamp'].append(datetime.now() - timedelta(days=1))

        with self.__shared_resource_lock:
            self.ips[self.item['job_id']] = self.item['ips']

            self.last_bid[self.item['job_id']] = {}

        threading.Thread(target=self.check_bidding, args=[copy.deepcopy(self.item)], daemon=True).start()

    def bid(self, enable_forward=True):
        bid_on_layer = False
        NN_len = len(self.bids[self.item["job_id"]]["auction_id"])
        if self.use_net_topology:
            avail_bw = None
        else:
            avail_bw = 10000000
        tmp_bid = copy.deepcopy(self.bids[self.item['job_id']])
        gpu_=0
        cpu_=0
        first = False
        first_index = None
        layers = 0
        bw_with_client = False
        previous_winner_id = 0
        bid_ids = []
        bid_ids_fail = []
        bid_round = 0
        i = 0
        bid_time = datetime.now()

        if self.item['job_id'] in self.bids:                  
            while i < NN_len:
                if self.use_net_topology:
                    with self.__layer_bid_lock:
                        if bid_round >= self.__layer_bid_events[self.item["job_id"]]:
                            break

                if len(self.available_cpu_per_task[self.item['job_id']]) <= bid_round:
                    # for j in range(len(self.available_cpu_per_task[self.item['job_id']]), bid_round):
                    self.available_cpu_per_task[self.item['job_id']].append(min(self.available_cpu_per_task[self.item['job_id']][bid_round-1], self.updated_cpu))
                    self.available_gpu_per_task[self.item['job_id']].append(min(self.available_gpu_per_task[self.item['job_id']][bid_round-1], self.updated_gpu))

                res_cpu, res_gpu, res_bw = self.get_reserved_resources(self.item['job_id'], i)
                NN_data_size = 1
                
                if i == 0:
                    if self.use_net_topology:
                        avail_bw = self.bw_with_client[self.item['job_id']]
                        res_bw = 0
                    first_index = i
                else:
                    if self.use_net_topology and not bw_with_client and not first:
                        if tmp_bid['auction_id'][i-1] not in self.bw_with_nodes[self.item['job_id']]:                            
                            self.bw_with_nodes[self.item['job_id']][tmp_bid['auction_id'][i-1]] = self.network_topology.get_available_bandwidth_between_nodes(self.id, tmp_bid['auction_id'][i-1])
                        previous_winner_id = tmp_bid['auction_id'][i-1]
                        avail_bw = self.bw_with_nodes[self.item['job_id']][tmp_bid['auction_id'][i-1]]
                        res_bw = 0
                    first = True
                
                if  self.item['NN_gpu'][i] <= self.updated_gpu - gpu_ + res_gpu and \
                    self.item['NN_cpu'][i] <= self.updated_cpu - cpu_ + res_cpu and \
                    NN_data_size <= avail_bw + res_bw and \
                    tmp_bid['auction_id'].count(self.id)<self.item["N_layer_max"] and \
                    (self.item['N_layer_bundle'] is None or (self.item['N_layer_bundle'] is not None and layers < self.item['N_layer_bundle'])) :

                    if tmp_bid['auction_id'].count(self.id) == 0 or \
                        (tmp_bid['auction_id'].count(self.id) != 0 and i != 0 and tmp_bid['auction_id'][i-1] == self.id):
                        
                        bid = self.utility_function(avail_bw, self.available_cpu_per_task[self.item['job_id']][bid_round], self.available_gpu_per_task[self.item['job_id']][bid_round])

                        # if bid == 1:
                        #     bid -= self.id * 0.000000001
                        
                        if bid > tmp_bid['bid'][i]:# or (bid == tmp_bid['bid'][i] and self.id < tmp_bid['auction_id'][i]):
                            bid_on_layer = True

                            tmp_bid['bid'][i] = bid
                            tmp_bid['bid_gpu'][i] = self.updated_gpu
                            tmp_bid['bid_cpu'][i] = self.updated_cpu
                            tmp_bid['bid_bw'][i] = avail_bw
                                
                            gpu_ += self.item['NN_gpu'][i]
                            cpu_ += self.item['NN_cpu'][i]

                            tmp_bid['auction_id'][i]=(self.id)
                            tmp_bid['timestamp'][i] = bid_time
                            layers += 1

                            bid_ids.append(i)
                            if i == 0:
                                bw_with_client = True

                            if layers >= self.item["N_layer_bundle"]:
                                break
                            
                            i += 1
                        else:
                            bid_ids_fail.append(i)
                            if self.use_net_topology or self.item["N_layer_bundle"] is not None:
                                i += self.item["N_layer_bundle"]
                                bid_round += 1
                                first = False                           
                    else:
                        if self.use_net_topology or self.item["N_layer_bundle"] is not None:
                            i += self.item["N_layer_bundle"]
                            bid_round += 1
                            first = False            
                else:
                    if bid_on_layer:
                        break
                    if self.use_net_topology or self.item["N_layer_bundle"] is not None:
                        i += self.item["N_layer_bundle"]
                        bid_round += 1
                        first = False

            if self.id in tmp_bid['auction_id'] and \
                (first_index is None or avail_bw - self.item['NN_data_size'][first_index] >= 0) and \
                tmp_bid['auction_id'].count(self.id)>=self.item["N_layer_min"] and \
                tmp_bid['auction_id'].count(self.id)<=self.item["N_layer_max"] and \
                self.integrity_check(tmp_bid['auction_id'], 'bid') and \
                (self.item['N_layer_bundle'] is None or (self.item['N_layer_bundle'] is not None and layers == self.item['N_layer_bundle'])):

                # logging.log(TRACE, "BID NODEID:" + str(self.id) + ", auction: " + str(tmp_bid['auction_id']))
                success = False
                if self.use_net_topology:
                    if bw_with_client and self.network_topology.consume_bandwidth_node_and_client(self.id, self.item['NN_data_size'][0], self.item['job_id']):
                        success = True
                    elif not bw_with_client and self.network_topology.consume_bandwidth_between_nodes(self.id, previous_winner_id, self.item['NN_data_size'][0], self.item['job_id']):
                        success = True
                else:
                    success = True
                
                if success:
                    #self.print_node_state(f"Bid succesful {tmp_bid['auction_id']}")
                    first_index = tmp_bid['auction_id'].index(self.id)
                    if not self.use_net_topology:
                        self.updated_bw -= self.item['NN_data_size'][first_index] 
                        # self.available_bw_per_task[self.item['job_id']] -= self.item['NN_data_size'][first_index] 

                    self.bids[self.item['job_id']] = copy.deepcopy(tmp_bid)

                    for i in bid_ids_fail:
                        self.release_reserved_resources(self.item["job_id"], i)
                    
                    for i in bid_ids:
                        self.release_reserved_resources(self.item["job_id"], i)
                        self.updated_gpu -= self.item['NN_gpu'][i]
                        self.updated_cpu -= self.item['NN_cpu'][i]

                    # self.available_cpu_per_task[self.item['job_id']] -= cpu_
                    # self.available_gpu_per_task[self.item['job_id']] -= gpu_

                    # if self.available_cpu_per_task[self.item['job_id']] < 0 or self.available_gpu_per_task[self.item['job_id']] < 0:
                    #     print("mannaggia la miseria")

                    
                    if enable_forward:
                        self.forward_to_neighbohors()
                    
                    if self.use_net_topology:
                        with self.__layer_bid_lock:
                            self.__layer_bid[self.item["job_id"]] = sum(1 for i in self.bids[self.item['job_id']]["auction_id"] if i != float('-inf'))
                            
                    return True
                else:
                    return False
            else:
                pass
                # self.print_node_state("bid failed " + str(tmp_bid['auction_id']), True)
        else:
            self.print_node_state('Value not in dict (first_msg)', type='error')
            return False
    
    def deconfliction(self):
        rebroadcast = False
        k = self.item['edge_id'] # sender
        i = self.id # receiver
        self.bids[self.item['job_id']]['deconflictions']+=1
        release_to_client = False
        previous_winner_id = float('-inf')
        job_id = self.item["job_id"]
        
        tmp_local = copy.deepcopy(self.bids[self.item['job_id']])
        prev_bet = copy.deepcopy(self.bids[self.item['job_id']])
        index = 0
        reset_flag = False
        reset_ids = []
        bid_time = datetime.now()
        
        if self.use_net_topology:
            initial_count = 0
            for j in tmp_local["auction_id"]:
                if j != float('-inf'):
                    initial_count += 1

        while index < min(len(tmp_local["auction_id"]), len(self.item['auction_id'])):
            if self.item['job_id'] in self.bids:
                z_kj = self.item['auction_id'][index]
                z_ij = tmp_local['auction_id'][index]
                y_kj = self.item['bid'][index]
                y_ij = tmp_local['bid'][index]
                t_kj = self.item['timestamp'][index]
                t_ij = tmp_local['timestamp'][index]

                logging.log(TRACE,'DECONFLICTION - NODEID(i):' + str(i) +
                                ' sender(k):' + str(k) +
                                ' z_kj:' + str(z_kj) +
                                ' z_ij:' + str(z_ij) +
                                ' y_kj:' + str(y_kj) +
                                ' y_ij:' + str(y_ij) +
                                ' t_kj:' + str(t_kj) +
                                ' t_ij:' + str(t_ij)
                                )
                if z_kj==k : 
                    if z_ij==i:
                        if (y_kj>y_ij): 
                            rebroadcast = True
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #1')
                            if index == 0:
                                release_to_client = True
                            elif previous_winner_id == float('-inf'):
                                previous_winner_id = prev_bet['auction_id'][index-1]
                            index = self.update_local_val(tmp_local, index, z_kj, y_kj, t_kj, self.bids[self.item['job_id']])

                        elif (y_kj==y_ij and z_kj<z_ij):
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #3')
                            rebroadcast = True
                            if index == 0:
                                release_to_client = True
                            elif previous_winner_id == float('-inf'):
                                previous_winner_id = prev_bet['auction_id'][index-1]
                            index = self.update_local_val(tmp_local, index, z_kj, y_kj, t_kj, self.bids[self.item['job_id']])

                        else:# (y_kj<y_ij):
                            rebroadcast = True
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #2')
                            index = self.update_local_val(tmp_local, index, z_ij, tmp_local['bid'][index], bid_time, self.item)
                        
                        # else:
                        #     if config.enable_logging:
                        #         logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #3else')
                        #     index+=1
                        #     rebroadcast = True

                    elif z_ij==k:
                        if  t_kj>t_ij:
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  '#4')
                            index = self.update_local_val(tmp_local, index, z_kj, y_kj, t_kj, self.bids[self.item['job_id']])
                            rebroadcast = True 
                        else:
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #5 - 6')
                            index+=1
                    
                    elif z_ij == float('-inf'):
                        logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #12')
                        index = self.update_local_val(tmp_local, index, z_kj, y_kj, t_kj, self.bids[self.item['job_id']])
                        rebroadcast = True

                    elif z_ij!=i and z_ij!=k:
                        if y_kj>y_ij and t_kj>=t_ij:
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #7')
                            index = self.update_local_val(tmp_local, index, z_kj, y_kj, t_kj, self.bids[self.item['job_id']])
                            rebroadcast = True
                        elif y_kj<y_ij and t_kj<=t_ij:
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #8')
                            index += 1
                            rebroadcast = True
                        elif y_kj==y_ij:
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #9')
                            rebroadcast = True
                            index+=1
                        # elif y_kj==y_ij and z_kj<z_ij:
                            # if config.enable_logging:
                                # logging.log(TRACE, 'NODEID:'+str(self.id) +  '#9-new')
                            # index = self.update_local_val(tmp_local, index, z_kj, y_kj, t_kj, self.bids[self.item['job_id']])                  
                            # rebroadcast = True
                        elif y_kj<y_ij and t_kj>t_ij:
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #10reset')
                            # index, reset_flag = self.reset(index, tmp_local)
                            index += 1
                            rebroadcast = True
                        elif y_kj>y_ij and t_kj<t_ij:
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #11rest')
                            # index, reset_flag = self.reset(index, tmp_local)
                            index = self.update_local_val(tmp_local, index, z_kj, y_kj, t_kj, self.bids[self.item['job_id']])
                            rebroadcast = True  
                        else:
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #11else')
                            index += 1  
                            rebroadcast = True  
                    
                    else:
                        index += 1   
                        logging.log(TRACE, "eccoci")    
                
                elif z_kj==i:                                
                    if z_ij==i:
                        if t_kj>t_ij:
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #13Flavio')
                            index = self.update_local_val(tmp_local, index, z_kj, y_kj, t_kj, self.bids[self.item['job_id']])
                            rebroadcast = True 
                            
                        else:
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #13elseFlavio')
                            index+=1
                            rebroadcast = True

                    elif z_ij==k:
                        logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #14reset')
                        reset_ids.append(index)
                        # index = self.reset(index, self.bids[self.item['job_id']])
                        index += 1
                        reset_flag = True
                        rebroadcast = True                        

                    elif z_ij == float('-inf'):
                        logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #16')
                        rebroadcast = True
                        index+=1
                    
                    elif z_ij!=i and z_ij!=k:
                        logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #15')
                        rebroadcast = True
                        index+=1
                    
                    else:
                        logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #15else')
                        rebroadcast = True
                        index+=1                
                
                elif z_kj == float('-inf'):
                    if z_ij==i:
                        logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #31')
                        rebroadcast = True
                        index+=1
                        
                    elif z_ij==k:
                        logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #32')
                        index = self.update_local_val(tmp_local, index, z_kj, y_kj, t_kj, self.bids[self.item['job_id']])
                        rebroadcast = True
                        
                    elif z_ij == float('-inf'):
                        logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #34')
                        index+=1
                        
                    elif z_ij!=i and z_ij!=k:
                        if t_kj>t_ij:
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #33')
                            index = self.update_local_val(tmp_local, index, z_kj, y_kj, t_kj, self.bids[self.item['job_id']])
                            rebroadcast = True
                        else: 
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #33else')
                            index+=1
                        
                    else:
                        logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #33elseelse')
                        index+=1
                        rebroadcast = True

                elif z_kj!=i or z_kj!=k:   
                                     
                    if z_ij==i:
                        if (y_kj>y_ij):
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  '#17')
                            rebroadcast = True
                            if index == 0:
                                release_to_client = True
                            elif previous_winner_id == float('-inf'):
                                previous_winner_id = prev_bet['auction_id'][index-1]
                            index = self.update_local_val(tmp_local, index, z_kj, y_kj, t_kj, self.bids[self.item['job_id']])
                        elif (y_kj==y_ij and z_kj<z_ij):
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  '#17')
                            rebroadcast = True
                            if index == 0:
                                release_to_client = True
                            elif previous_winner_id == float('-inf'):
                                previous_winner_id = prev_bet['auction_id'][index-1]
                            index = self.update_local_val(tmp_local, index, z_kj, y_kj, t_kj, self.bids[self.item['job_id']])
                        else:# (y_kj<y_ij):
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  '#19')
                            rebroadcast = True
                            index = self.update_local_val(tmp_local, index, z_ij, tmp_local['bid'][index], bid_time, self.item)
                        # else:
                        #     if config.enable_logging:
                        #         logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #19else')
                        #     index+=1
                        #     rebroadcast = True

                    elif z_ij==k:
                        if y_kj<y_ij:
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #20Flavio')
                            index = self.update_local_val(tmp_local, index, z_kj, y_kj, t_kj, self.bids[self.item['job_id']])
                            rebroadcast = True 
                        # elif (y_kj==y_ij and z_kj<z_ij):
                        #     if config.enable_logging:
                        #         logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #3stefano')
                        #     rebroadcast = True
                        #     index = self.update_local_val(tmp_local, index, z_kj, y_kj, t_kj, self.bids[self.item['job_id']])
                        elif t_kj>=t_ij:
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  '#20')
                            index = self.update_local_val(tmp_local, index, z_kj, y_kj, t_kj, self.bids[self.item['job_id']])
                            rebroadcast = True
                        elif t_kj<t_ij:
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  '#21reset')
                            # index, reset_flag = self.reset(index, tmp_local)
                            index += 1
                            rebroadcast = True

                    elif z_ij == z_kj:
                    
                        if t_kj>t_ij:
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  '#22')
                            index = self.update_local_val(tmp_local, index, z_kj, y_kj, t_kj, self.bids[self.item['job_id']])
                        else:
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #23 - 24')
                            index+=1
                    
                    elif z_ij == float('-inf'):
                        logging.log(TRACE, 'NODEID:'+str(self.id) +  '#30')
                        index = self.update_local_val(tmp_local, index, z_kj, y_kj, t_kj, self.bids[self.item['job_id']])
                        rebroadcast = True

                    elif z_ij!=i and z_ij!=k and z_ij!=z_kj:
                        if y_kj>=y_ij and t_kj>=t_ij:
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  '#25')
                            index = self.update_local_val(tmp_local, index, z_kj, y_kj, t_kj, self.bids[self.item['job_id']])                   
                            rebroadcast = True
                        elif y_kj<y_ij and t_kj<=t_ij:
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  '#26')
                            rebroadcast = True
                            index+=1
                        # elif y_kj==y_ij:# and z_kj<z_ij:
                        #     if config.enable_logging:
                        #         logging.log(TRACE, 'NODEID:'+str(self.id) +  '#27')
                        #     index = self.update_local_val(tmp_local, index, z_kj, y_kj, t_kj, self.bids[self.item['job_id']])                   
                        #     rebroadcast = True
                        # elif y_kj==y_ij:
                        #     if config.enable_logging:
                        #         logging.log(TRACE, 'NODEID:'+str(self.id) +  '#27bis')
                        #     index+=1
                        #     rebroadcast = True
                        elif y_kj<y_ij and t_kj>t_ij:
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  '#28')
                            index = self.update_local_val(tmp_local, index, z_kj, y_kj, t_kj, self.bids[self.item['job_id']])                   
                            rebroadcast = True
                        elif y_kj>y_ij and t_kj<t_ij:
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  '#29')
                            # index, reset_flag = self.reset(index, tmp_local)
                            index += 1
                            rebroadcast = True
                        else:
                            logging.log(TRACE, 'NODEID:'+str(self.id) +  '#29else')
                            index+=1
                            rebroadcast = True
                    
                    else:
                        logging.log(TRACE, 'NODEID:'+str(self.id) +  ' #29else2')
                        index+=1
                
                else:
                    self.print_node_state('smth wrong?', type='error')

            else:
                self.print_node_state('Value not in dict (deconfliction)', type='error')

        if self.integrity_check(tmp_local['auction_id'], 'deconfliction'):
            # tmp_local['auction_id']!=self.bids[self.item['job_id']]['auction_id']:
            # tmp_local['bid'] != self.bids[self.item['job_id']]['bid'] and \
            # tmp_local['timestamp'] != self.bids[self.item['job_id']]['timestamp']:
            # self.print_node_state(f'Deconfliction checked pass {self.id}', True)

            if reset_flag:
                self.forward_to_neighbohors(tmp_local)
                for i in reset_ids:
                    _ = self.reset(i, tmp_local, bid_time)
                self.bids[self.item['job_id']] = copy.deepcopy(tmp_local)
                return False, False             

            cpu = 0
            gpu = 0
            bw = 0

            first_1 = False
            first_2 = False
            for i in range(len(tmp_local["auction_id"])):
                if tmp_local["auction_id"][i] == self.id and prev_bet["auction_id"][i] == self.id:
                    if i != 0 and tmp_local["auction_id"][i-1] != prev_bet["auction_id"][i-1]: 
                        if self.use_net_topology:
                            print(f"Failure in node {self.id} job_bid {job_id}. Deconfliction failed. Exiting ...")
                            raise InternalError
                elif tmp_local["auction_id"][i] == self.id and prev_bet["auction_id"][i] != self.id:
                    # self.release_reserved_resources(self.item['job_id'], i)
                    cpu -= self.item['NN_cpu'][i]
                    gpu -= self.item['NN_gpu'][i]
                    if not first_1:
                        bw -= self.item['NN_data_size'][i]
                        first_1 = True
                elif tmp_local["auction_id"][i] != self.id and prev_bet["auction_id"][i] == self.id:
                    cpu += self.item['NN_cpu'][i]
                    gpu += self.item['NN_gpu'][i]
                    if not first_2:
                        bw += self.item['NN_data_size'][i]
                        first_2 = True
                    
            self.updated_cpu += cpu
            self.updated_gpu += gpu

            if self.use_net_topology:
                if release_to_client:
                    self.network_topology.release_bandwidth_node_and_client(self.id, bw, self.item['job_id'])
                elif previous_winner_id != float('-inf'):
                    self.network_topology.release_bandwidth_between_nodes(previous_winner_id, self.id, bw, self.item['job_id'])      
            else:
                self.updated_bw += bw

            self.bids[self.item['job_id']] = copy.deepcopy(tmp_local)
            
            if self.use_net_topology:
                with self.__layer_bid_lock:
                    self.__layer_bid[self.item["job_id"]] = sum(1 for i in self.bids[self.item['job_id']]["auction_id"] if i != float('-inf'))

            return rebroadcast, False
        else:
            if self.use_net_topology:
                print(f"Failure in node {self.id}. Deconfliction failed. Exiting ...")
                raise InternalError

            cpu = 0
            gpu = 0
            bw = 0
            first_1 = False

            for i in range(len(self.item["auction_id"])):
                if self.item["auction_id"][i] == self.id and prev_bet["auction_id"][i] == self.id:
                    pass
                elif self.item["auction_id"][i] == self.id and prev_bet["auction_id"][i] != self.id:
                    cpu -= self.item['NN_cpu'][i]
                    gpu -= self.item['NN_gpu'][i]
                    if not first_1:
                        bw -= self.item['NN_data_size'][i]
                        first_1 = True
                elif self.item["auction_id"][i] != self.id and prev_bet["auction_id"][i] == self.id:
                    self.remind_to_release_resources(self.item["job_id"], self.item['NN_cpu'][i], self.item['NN_gpu'][i], bw, [i])
       
            self.updated_cpu += cpu
            self.updated_gpu += gpu
            self.updated_bw += bw            
            
            for key in self.item:
                self.bids[self.item['job_id']][key] = copy.deepcopy(self.item[key])
                
            self.forward_to_neighbohors()
                    
            return False, True       


    def remind_to_release_resources(self, job_id, cpu, gpu, bw, idx):
        if job_id not in self.resource_remind:
            self.resource_remind[job_id] = {}
            self.resource_remind[job_id]["idx"] = []

        self.resource_remind[job_id]["cpu"] = cpu
        self.resource_remind[job_id]["gpu"] = gpu
        self.resource_remind[job_id]["bw"] = bw
        self.resource_remind[job_id]["idx"] += idx

        for id in idx:
            self.cum_cpu_reserved += cpu
            self.cum_gpu_reserved += gpu
            self.cum_bw_reserved += bw

    def get_reserved_resources(self, job_id, index):
        if job_id not in self.resource_remind:
            return 0, 0, 0
        
        found = 0
        for id in self.resource_remind[job_id]["idx"]:
            if id == index:
                found += 1
        
        if found == 0:
            return 0, 0, 0

        return math.ceil(self.cum_cpu_reserved), math.ceil(self.cum_gpu_reserved), math.ceil(self.cum_bw_reserved)


    def release_reserved_resources(self, job_id, index):
        if job_id not in self.resource_remind:
            return False
        found = 0
        for id in self.resource_remind[job_id]["idx"]:
            if id == index:
                # print(self.resource_remind[job_id]["cpu"])
                self.updated_cpu += self.resource_remind[job_id]["cpu"]
                self.updated_gpu += self.resource_remind[job_id]["gpu"]
                self.updated_bw += self.resource_remind[job_id]["bw"]

                self.cum_cpu_reserved -= self.resource_remind[job_id]["cpu"]
                self.cum_gpu_reserved -= self.resource_remind[job_id]["gpu"]
                self.cum_bw_reserved -= self.resource_remind[job_id]["bw"]

                found += 1
                
        if found > 0:
            for _ in range(found):
                self.resource_remind[job_id]["idx"].remove(index)
            return True
        else:
            return False

    def update_bid(self):
        if self.item['job_id'] in self.bids:
        
            # Consensus check
            if  self.bids[self.item['job_id']]['auction_id']==self.item['auction_id'] and \
                self.bids[self.item['job_id']]['bid'] == self.item['bid'] and \
                self.bids[self.item['job_id']]['timestamp'] == self.item['timestamp']:
                    
                    if float('-inf') in self.bids[self.item['job_id']]['auction_id']:
                        if self.id not in self.bids[self.item['job_id']]['auction_id']: 
                            self.bid()
                        else:
                            self.forward_to_neighbohors()
                    else:
                        #self.print_node_state('Consensus -', True)
                        self.bids[self.item['job_id']]['consensus_count']+=1
                    
            else:
                #self.print_node_state('BEFORE', True)
                rebroadcast, integrity_fail = self.deconfliction()

                success = False

                if not integrity_fail and self.id not in self.bids[self.item['job_id']]['auction_id']:
                    success = self.bid()
                    
                if not success and rebroadcast:
                    self.forward_to_neighbohors()
                elif float('-inf') in self.bids[self.item['job_id']]['auction_id']:
                    self.forward_to_neighbohors()
        else:
            self.print_node_state('Value not in dict (update_bid)', type='error')


    def new_msg(self):
        if str(self.id) == str(self.item['edge_id']):
            self.print_node_state(
                'This was not supposed to be received', type='error')

        elif self.item['job_id'] in self.bids:
            if self.integrity_check(self.item['auction_id']):
                self.update_bid()
        else:
            self.print_node_state('Value not in dict (new_msg)', type='error')

    def integrity_check(self, bid, msg):
        min_ = self.item["N_layer_min"]
        max_ = self.item["N_layer_max"]
        curr_val = bid[0]
        curr_count = 1
        prev_values = [curr_val]  # List to store previously encountered values

        for i in range(1, len(bid)):
            if bid[i] == curr_val:
                curr_count += 1
            else:
                if (curr_count < min_ or curr_count > max_) and curr_val != float('-inf'):
                    return False

                if bid[i] in prev_values:  # Check if current value is repeated
                   return False

                curr_val = bid[i]
                curr_count = 1
                prev_values.append(curr_val)  # Add current value to the list

        if curr_count < min_ or curr_count > max_ and curr_val != float('-inf'):
            return False

        return True

    def work(self):        
        while True:
            self.item = self.q.get()

            flag = False

            # new request from client
            if self.item['edge_id'] is None:
                logging.info(f"Received first message for job {self.item['job_id']}. Starting the bidding process.")
                logging.info(f"Agents participating to the bidding session: {self.item['ips']}")
                flag = True

            if self.item['job_id'] not in self.bids:
                logging.info(f"Received first message for job {self.item['job_id']}. Starting the bidding process.")
                logging.info(f"Agents participating to the bidding session: {self.item['ips']}")
                #self.print_node_state('IF1 q:' + str(self.q.qsize()))

                self.init_null()
                #self.save_node_state()
                
                self.bid(flag)
                self.save_node_state()

            if not flag:
                #self.print_node_state('IF2 q:' + str(self.q.qsize()))
                # if not self.bids[self.item['job_id']]['complete'] and \
                #    not self.bids[self.item['job_id']]['clock'] :
                if self.id not in self.item['auction_id']:
                    self.bid(False)
                    self.save_node_state()

                self.update_bid()
                self.save_node_state()

                # else:
                #     print('kitemmuorten!')

            self.bids[self.item['job_id']]['start_time'] = 0                            
            self.bids[self.item['job_id']]['count'] += 1
            

            self.q.task_done()
                    
    def disable_bidding(self):
        logging.info("Network pressure exceeded the threashold. Will not participate to any further bidding process.")
        with self.__enable_bidding_flag_lock:
            self.bidding_enabled = False

    def enable_bidding(self):
        logging.info("Network pressure under the threashold. Will participate to any further bidding process.")
        with self.__enable_bidding_flag_lock:
            self.bidding_enabled = True        

    def __create_pod(self, node_name, n_cpu, n_ram, id, duration, job_id):
        logging.info(f"Creating pod 'nn-pod-{str(id)}' on node {str(node_name)}")

        config.load_incluster_config()

        pod = client.V1Pod()
        pod.metadata = client.V1ObjectMeta(name="nn-pod-" + str(job_id) + "-" + str(id))
        pod.spec = client.V1PodSpec(
            containers=[
                client.V1Container(
                    name="example-container",
                    image="nginx:latest",
                    resources=client.V1ResourceRequirements(
                    requests={"cpu": str(n_cpu), "memory": str(n_ram)},
                    limits={"cpu": str(n_cpu), "memory": str(n_ram)},
                ),
                )
            ],
            node_selector={"kubernetes.io/hostname": str(node_name)},
            termination_grace_period_seconds=int(duration),
            volumes=[]
        )

        
        #volumes = pod.spec.get('volumes', [])

        # # Filter out the 'default-token' volume, which is the service account token volume
        # filtered_volumes = [volume for volume in volumes if volume.get('name') != 'default-token']

        # # Update the pod spec with the filtered volumes
        #pod.spec['volumes'] = []

        # Create the Pod in the Kubernetes cluster
        api_instance = client.CoreV1Api()
        api_instance.create_namespaced_pod(namespace="default", body=pod)


        # api = client.CoreV1Api()

        # pod_manifest = {
        # "apiVersion": "v1",
        # "kind": "Pod",
        # "metadata": {
        #     "name": "nn-pod-" + str(job_id) + "-" + str(id),
        #     "labels": {
        #         "app": "my-app"
        #     }
        # },
        # "spec": {
        #     "affinity": {
        #         "nodeAffinity": {
        #             "requiredDuringSchedulingIgnoredDuringExecution": {
        #                 "nodeSelectorTerms": [
        #                     {
        #                         "matchExpressions": [
        #                             {
        #                                 "key": "kubernetes.io/hostname",
        #                                 "operator": "In",
        #                                 "values": [str(node_name)]  # Adjust node names as needed
        #                             }
        #                         ]
        #                     }
        #                 ]
        #             }
        #         }
        #     },
        #     "volumes" : [],
        #     "terminationGracePeriodSeconds": int(duration),
        #     "containers": [
        #         {
        #             "name": "my-container",
        #             "image": "nginx:latest",
        #             "volumeMounts": [],
        #             "ports": [
        #                 {
        #                     "containerPort": 80
        #                 }
        #             ],
        #             "resources": {
        #                 "requests": {
        #                     "cpu": str(n_cpu),  # Adjust CPU request as needed
        #                     "memory": str(n_ram)
        #                 },
        #                 "limits": {
        #                     "cpu": str(n_cpu),  # Adjust CPU limit as needed
        #                     "memory": str(n_ram)
        #                 }
        #             }
        #         }
        #     ]
        # }}

        # _ = api.create_namespaced_pod(
        #     body=pod_manifest,
        #     namespace="default"
        # )

    def check_bidding(self, job):
        time.sleep(5)
        job_id = job["job_id"]

        logging.info(f"Bidding process completed for job '{job_id}'")
        logging.info(self.last_bid[job_id])

        with self.__shared_resource_lock:
            if float('-inf') not in self.last_bid[job_id]:
                count = 0
                n_layers = 0
                for ip in self.last_bid[job_id]:
                    if ip == self.node_ip:
                        n_layers = n_layers + 1
                
                for id, ip in enumerate(self.last_bid[job_id]):
                    if ip == self.node_ip:
                        self.__create_pod(self.node_name, job["NN_cpu"][id], job["ram"]/len(self.last_bid[job_id]), count, job["duration"], job_id)
                    count = count + 1
                
            else:
                logging.info(f"No layers assinged to this node in the bidding session.")

def message_data(job_id, user, num_gpu, num_cpu, duration, bandwidth, ips, infra_topology, ram):
    
    layer_number = random.choice([6])
    
    gpu = round(num_gpu / layer_number, 6)
    cpu = round(num_cpu / layer_number, 6)
    bw = round(float(bandwidth) / 2, 6)
    # bw = round(float(bandwidth) / min_layer_number, 2)

    NN_gpu = np.ones(layer_number) * gpu
    NN_cpu = np.ones(layer_number) * cpu
    NN_data_size = np.ones(layer_number) * bw

    max_layer_bid = random.choice([4])
    bundle_size = 2
    
    data = {
        "job_id": int(),
        "user": int(),
        "num_gpu": int(),
        "num_cpu": int(),
        "duration": int(),
        "N_layer": layer_number,
        "N_layer_min": 1, # Do not change!! This could be either 1 or = to N_layer_max
        "N_layer_max": max_layer_bid,
        "N_layer_bundle": bundle_size, 
        # "job_name": int(),
        # "submit_time": int(),
        # "gpu_type": int(),
        # "num_inst": int(),
        # "size": int(),
        "edge_id":int(),
        "NN_gpu": NN_gpu,
        "NN_cpu": NN_cpu,
        "NN_data_size": NN_data_size
        }


    data['edge_id']=None
    data['job_id']=job_id
    data['user']=user
    data['num_gpu']=num_gpu
    data['num_cpu']=num_cpu
    data['duration']=duration
    # data['job_name']=job_name
    # data['submit_time']=submit_time
    # data['gpu_type']=gpu_type
    # data['num_inst']=num_inst
    # data['size']=size
    data["ram"] = ram
    data['topology'] = infra_topology
    data['job_id']=job_id
    data['ips'] = ips

    return data
