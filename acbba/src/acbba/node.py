'''
This module impelments the behavior of a node
'''

import json
import queue
from datetime import datetime, timedelta
import copy
import logging
import requests
from acbba.utils import *
import sys
from acbba.server import ACBBAServer
from acbba.topology import topology
import numpy as np
from acbba.utils import *
import math
import threading

# cose da implementare
# trasformare l'ID in una stringa in modo da poter prendere il nome del pod
# fare in modo di generare la topologia nel momento in cui si riceve la crd, e quindi far usare al sistema di bidding la topologia specifica (mappa?)
# potrebbe valer la pena fare la stessa cosa con gli IP

TRACE = 5

layer_number = 6.0
min_layer_number = 1.0  # Min number of layers per node
max_layer_number = layer_number/2.0  # Max number of layers per node
counter = 0  # Messages counter

tot_GPU = 1000
tot_CPU = 1000
tot_BW = 1000000000


class node:

    def __init__(self, communication_port, ips, alpha_value, node_bw, num_clients, utility):
        self.id = self.__generate_node_id_from_ips(ips)
        self.node_ip = get_container_ip()

        n_GPU = get_total_gpu_cores()
        if n_GPU == None:
            # sys.exit(f"Failed to load GPU data on node. Exiting...")
            logging.info(
                f"Failed to get number of GPU core on the node. Setting the value to 0")
            n_GPU = 0
        self.initial_gpu = float(n_GPU)
        self.updated_gpu = self.initial_gpu

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
        self.ips = sort_ip_addresses(ips)
        self.ips.insert(self.id, self.node_ip)
        self.utility = utility

        self.topology = topology(func_name='complete_graph', max_bandwidth=node_bw,
                                 min_bandwidth=node_bw/2, num_clients=num_clients, num_edges=len(self.ips))

        self.initial_bw = self.topology.b
        self.updated_bw = self.initial_bw

        self.communication_port = communication_port
        self.server = ACBBAServer(self.q, communication_port)
        self.server.run_server()

        logging.info(
            f"Initialized acbba node instance with ID {self.id}. CPU: {self.initial_cpu}, GPU: {self.initial_gpu}")

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

    def utility_function(self):
        def f(x, alpha, beta):
            return math.exp(-(alpha/2)*(x-beta)**2)

        if self.utility == 'stefano':
            return f(self.item['NN_cpu'][0]/self.item['NN_gpu'][0], self.alpha, self.initial_cpu/self.initial_gpu)
        elif self.utility == 'alpha_BW_CPU':
            # return (config.a*(self.updated_bw[self.item['user']][self.id]/config.tot_bw))+((1-config.a)*(self.updated_cpu/config.tot_cpu)) #BW vs CPU
            return (self.alpha*(self.updated_bw[self.item['user']][self.id]/tot_BW))+((1-self.alpha)*(self.updated_cpu/tot_CPU))
        elif self.utility == 'alpha_GPU_CPU':
            return (self.alpha*(self.updated_gpu/tot_GPU))+((1-self.alpha)*(self.updated_cpu/tot_CPU))
        elif self.utility == 'alpha_GPU_BW':
            return (self.alpha*(self.updated_gpu/tot_GPU))+((1-self.alpha)*(self.updated_bw[self.item['user']][self.id]/tot_BW))
        else:
            sys.exit(
                f"Unsupported utility function {self.utility}. Exiting...")

    def forward_to_neighbohors(self):
        for i in range(len(self.ips)):
            if self.topology.to()[i][self.id] and self.id != i:
                data = {
                    "job_id": self.item['job_id'],
                    "user": self.item['user'],
                    "edge_id": self.id,
                    "auction_id": self.bids[self.item['job_id']]['auction_id'],
                    "NN_gpu": self.item['NN_gpu'].tolist() if type(self.item['NN_gpu']) is np.ndarray else self.item['NN_gpu'],
                    "NN_cpu": self.item['NN_cpu'].tolist() if type(self.item['NN_cpu']) is np.ndarray else self.item['NN_cpu'],
                    "NN_data_size": self.item['NN_data_size'].tolist() if type(self.item['NN_data_size']) is np.ndarray else self.item['NN_data_size'],
                    "bid": self.bids[self.item['job_id']]['bid'],
                    "x": self.bids[self.item['job_id']]['x'],
                    "timestamp": datetime_list_to_string_list(self.bids[self.item['job_id']]['timestamp'])
                }

                # Send POST request to the server
                url = f"http://{self.ips[i]}:{self.communication_port}"
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

                logging.debug("FORWARD " + str(self.id) + " to " + str(i) +
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

    def update_local_val(self, index, id, bid, timestamp):
        self.bids[self.item['job_id']]['job_id'] = self.item['job_id']
        self.bids[self.item['job_id']]['auction_id'][index] = id
        self.bids[self.item['job_id']]['x'][index] = 1
        self.bids[self.item['job_id']]['bid'][index] = bid
        self.bids[self.item['job_id']]['timestamp'][index] = timestamp
        return index + 1

    def reset(self, index):
        self.bids[self.item['job_id']]['auction_id'][index] = float('-inf')
        self.bids[self.item['job_id']]['x'][index] = 0
        self.bids[self.item['job_id']]['bid'][index] = float('-inf')
        self.bids[self.item['job_id']
                  ]['timestamp'][index] = datetime.now() - timedelta(days=1)
        return index + 1

    def unbid(self):
        for i, id in enumerate(self.bids[self.item['job_id']]['auction_id']):
            if id == self.id:
                self.updated_gpu += self.bids[self.item['job_id']]['NN_gpu'][i]
                self.updated_cpu += self.bids[self.item['job_id']]['NN_cpu'][i]
                self.updated_bw += self.item['NN_data_size'][i]
                # self.updated_bw[self.item['user']][self.id] += self.item['NN_data_size'][i]
                self.bids[self.item['job_id']]['auction_id'][i] = float('-inf')
                self.bids[self.item['job_id']]['x'][i] = 0
                self.bids[self.item['job_id']]['bid'][i] = float('-inf')

    def init_null(self):
        self.print_node_state('INITNULL')

        self.bids[self.item['job_id']] = {
            "job_id": self.item['job_id'],
            "user": int(),
            "auction_id": list(),
            "NN_gpu": self.item['NN_gpu'],
            "NN_cpu": self.item['NN_cpu'],
            "bid": list(),
            "bid_gpu": list(),
            "bid_cpu": list(),
            "bid_bw": list(),
            "x": list(),
            "timestamp": list()
        }

        NN_len = len(self.item['NN_gpu'])

        for _ in range(0, NN_len):
            self.bids[self.item['job_id']]['x'].append(float('-inf'))
            self.bids[self.item['job_id']]['bid'].append(float('-inf'))
            self.bids[self.item['job_id']]['bid_gpu'].append(float('-inf'))
            self.bids[self.item['job_id']]['bid_cpu'].append(float('-inf'))
            self.bids[self.item['job_id']]['bid_bw'].append(float('-inf'))
            self.bids[self.item['job_id']]['auction_id'].append(float('-inf'))
            self.bids[self.item['job_id']]['timestamp'].append(
                datetime.now() - timedelta(days=1))

    def first_msg(self):

        if self.item['job_id'] in self.bids:
            self.sequence = True
            self.layers = 0
            NN_len = len(self.item['NN_gpu'])
            required_bw = self.item['NN_data_size'][0]
            avail_bw = self.updated_bw[self.item['user']][self.id]

            if required_bw <= avail_bw:  # TODO node id needed
                for i in range(0, NN_len):
                    if self.sequence == True and \
                            self.item['NN_gpu'][i] <= self.updated_gpu and \
                            self.item['NN_cpu'][i] <= self.updated_cpu and \
                            self.layers < max_layer_number:

                        logging.info(self.item['NN_gpu'])
                        logging.info(self.updated_gpu)
                        logging.info(self.item['NN_cpu'])
                        logging.info(self.updated_cpu)

                        self.bids[self.item['job_id']
                                  ]['bid'][i] = self.utility_function()
                        self.bids[self.item['job_id']
                                  ]['bid_gpu'][i] = self.updated_gpu
                        self.bids[self.item['job_id']
                                  ]['bid_cpu'][i] = self.updated_cpu
                        # TODO update with BW
                        self.bids[self.item['job_id']
                                  ]['bid_bw'][i] = self.updated_gpu
                        self.updated_gpu = self.updated_gpu - \
                            self.item['NN_gpu'][i]
                        self.updated_cpu = self.updated_cpu - \
                            self.item['NN_cpu'][i]
                        self.bids[self.item['job_id']]['x'][i] = 1
                        self.bids[self.item['job_id']
                                  ]['auction_id'][i] = self.id
                        self.bids[self.item['job_id']
                                  ]['timestamp'][i] = datetime.now()
                        self.layers += 1
                    else:
                        self.sequence = False
                        self.bids[self.item['job_id']]['x'][i] = float('-inf')
                        self.bids[self.item['job_id']
                                  ]['bid'][i] = float('-inf')
                        self.bids[self.item['job_id']
                                  ]['auction_id'][i] = float('-inf')
                        self.bids[self.item['job_id']]['timestamp'][i] = datetime.now(
                        ) - timedelta(days=1)

            if self.bids[self.item['job_id']]['auction_id'].count(self.id) < min_layer_number:
                self.unbid()
            else:
                # TODO else update bandwidth
                self.updated_bw[self.item['user']
                                ][self.id] -= self.item['NN_data_size'][0]

                self.forward_to_neighbohors()
        else:
            self.print_node_state(
                'Value not in dict (first_msg)', type='error')

    def rebid(self):

        self.print_node_state('REBID')

        if self.item['job_id'] in self.bids:

            self.sequence = True
            self.layers = 0
            NN_len = len(self.item['NN_gpu'])
            avail_bw = self.updated_bw[self.item['user']][self.id]

            for i in range(0, NN_len):
                if self.bids[self.item['job_id']]['auction_id'][i] == float('-inf'):
                    logging.log(TRACE, "RIF1: " + str(self.id) + ", avail_gpu:" + str(self.updated_gpu) + ", avail_cpu:" + str(
                        self.updated_cpu) + ", BIDDING on: " + str(i) + ", NN_len:" + str(NN_len) + ", Layer_resources" + str(self.item['NN_gpu'][i]))
                    required_bw = self.item['NN_data_size'][i]

                    if self.sequence == True and\
                            self.item['NN_gpu'][i] <= self.updated_gpu and \
                            self.item['NN_cpu'][i] <= self.updated_cpu and \
                            self.item['NN_data_size'][i] <= avail_bw and \
                            self.layers < max_layer_number:

                        logging.log(TRACE, "RIF2 NODEID: " + str(self.id) + ", avail_gpu:" + str(self.updated_gpu) + ", avail_cpu:" + str(
                            self.updated_cpu) + ", BIDDING on: " + str(i) + ", NN_len:" + str(NN_len) + ", Layer_resources" + str(self.item['NN_gpu'][i]))
                        self.bids[self.item['job_id']
                                  ]['bid'][i] = self.utility_function()
                        self.bids[self.item['job_id']
                                  ]['bid_gpu'][i] = self.updated_gpu
                        self.bids[self.item['job_id']
                                  ]['bid_cpu'][i] = self.updated_cpu
                        # TODO update with BW
                        self.bids[self.item['job_id']
                                  ]['bid_bw'][i] = self.updated_bw[self.item['user']][self.id]
                        self.updated_gpu = self.updated_gpu - \
                            self.item['NN_gpu'][i]
                        self.updated_cpu = self.updated_cpu - \
                            self.item['NN_cpu'][i]
                        self.bids[self.item['job_id']]['x'][i] = (1)
                        self.bids[self.item['job_id']
                                  ]['auction_id'][i] = (self.id)
                        self.bids[self.item['job_id']
                                  ]['timestamp'][i] = datetime.now()
                        self.layers += 1
                    else:
                        self.sequence = False
                        self.bids[self.item['job_id']
                                  ]['x'][i] = (float('-inf'))
                        self.bids[self.item['job_id']
                                  ]['bid'][i] = (float('-inf'))
                        self.bids[self.item['job_id']
                                  ]['auction_id'][i] = (float('-inf'))
                        self.bids[self.item['job_id']]['timestamp'][i] = (
                            datetime.now() - timedelta(days=1))

            if self.bids[self.item['job_id']]['auction_id'].count(self.id) < min_layer_number:
                self.unbid()
            else:
                self.updated_bw[self.item['user']][self.id] -= self.item['NN_data_size'][self.bids[self.item['job_id']]
                                                                                         ['auction_id'].index(self.id)]  # TODO else update bandwidth

                self.forward_to_neighbohors()
        else:
            self.print_node_state('Value not in dict (rebid)', type='error')

    def deconfliction(self):
        rebroadcast = False
        k = self.item['edge_id']  # sender
        i = self.id  # receiver
        index = 0
        while index < layer_number:
            if self.item['job_id'] in self.bids:
                z_kj = self.item['auction_id'][index]
                z_ij = self.bids[self.item['job_id']]['auction_id'][index]
                y_kj = self.item['bid'][index]
                y_ij = self.bids[self.item['job_id']]['bid'][index]
                t_kj = self.item['timestamp'][index]
                t_ij = self.bids[self.item['job_id']]['timestamp'][index]

                logging.log(TRACE, ' edge_id(i):' + str(i) +
                            ' sender(k):' + str(k) +
                            ' z_kj:' + str(z_kj) +
                            ' z_ij:' + str(z_ij) +
                            ' y_kj:' + str(y_kj) +
                            ' y_ij:' + str(y_ij) +
                            ' t_kj:' + str(t_kj) +
                            ' t_ij:' + str(t_ij)
                            )
                if z_kj == k:
                    if z_ij == i:
                        if (y_kj > y_ij) or (y_kj == y_ij and z_kj < z_ij):
                            rebroadcast = True
                            logging.log(TRACE, 'edge_id:' +
                                        str(self.id) + ' #1-#2')

                            while index < layer_number and self.item['auction_id'][index] == z_kj:
                                self.updated_gpu = self.updated_gpu + \
                                    self.item['NN_gpu'][index]
                                self.updated_cpu = self.updated_cpu + \
                                    self.item['NN_cpu'][index]
                                self.updated_bw[self.item['user']][self.id] = self.updated_bw[self.item['user']
                                                                                              ][self.id] + self.item['NN_data_size'][index]
                                index = self.update_local_val(
                                    index, z_kj, self.item['bid'][index], self.item['timestamp'][index])
                        elif (y_kj < y_ij):
                            logging.log(TRACE, 'edge_id:'+str(self.id) + ' #3')
                            rebroadcast = True
                            while index < layer_number and self.bids[self.item['job_id']]['auction_id'][index] == z_ij:
                                index = self.update_local_val(
                                    index, z_ij, self.bids[self.item['job_id']]['bid'][index], datetime.now())

                        else:
                            logging.log(TRACE, 'edge_id:' +
                                        str(self.id) + ' #3else')
                            index += 1

                    elif z_ij == k:
                        if t_kj > t_ij:
                            logging.log(TRACE, 'edge_id:'+str(self.id) + '#4')
                            index = self.update_local_val(
                                index, k, self.item['bid'][index], t_kj)

                        else:
                            logging.log(TRACE, 'edge_id:' +
                                        str(self.id) + ' #4else')
                            index += 1

                    elif z_ij == float('-inf'):
                        logging.log(TRACE, 'edge_id:'+str(self.id) + ' #12')
                        index = self.update_local_val(
                            index, z_kj, self.item['bid'][index], t_kj)
                        rebroadcast = True

                    elif z_ij != i and z_ij != k:
                        if y_kj > y_ij and t_kj >= t_ij:
                            logging.log(TRACE, 'edge_id:'+str(self.id) + ' #7')
                            while index < layer_number and self.item['auction_id'][index] == z_kj:
                                index = self.update_local_val(
                                    index, z_kj, self.item['bid'][index], self.item['timestamp'][index])
                            rebroadcast = True
                        elif y_kj < y_ij and t_kj > t_ij:
                            logging.log(TRACE, 'edge_id:'+str(self.id) + ' #8')
                            rebroadcast = True
                            index += 1
                        elif y_kj == y_ij:
                            logging.log(TRACE, 'edge_id:' +
                                        str(self.id) + ' #9else')
                            rebroadcast = True
                            index += 1
                        elif y_kj < y_ij and t_kj < t_ij:
                            logging.log(TRACE, 'edge_id:' +
                                        str(self.id) + ' #10')
                            index += 1
                            rebroadcast = True
                        elif y_kj > y_ij and t_kj < t_ij:
                            logging.log(TRACE, 'edge_id:' +
                                        str(self.id) + ' #11')
                            while index < layer_number and self.item['auction_id'][index] == z_kj:
                                index = self.update_local_val(
                                    index, z_kj, self.item['bid'][index], self.item['timestamp'][index])
                            rebroadcast = True
                        else:
                            logging.log(TRACE, 'edge_id:' +
                                        str(self.id) + ' #11else')
                            index += 1

                elif z_kj == i:
                    if z_ij == i:
                        logging.log(TRACE, 'edge_id:'+str(self.id) + ' #13')
                        index += 1
                    elif z_ij == k:
                        logging.log(TRACE, 'edge_id:'+str(self.id) + ' #14')
                        index = self.reset(index)

                    elif z_ij == float('-inf'):
                        logging.log(TRACE, 'edge_id:'+str(self.id) + ' #16')
                        rebroadcast = True
                        index += 1
                    elif z_ij != i and z_ij != k:
                        logging.log(TRACE, 'edge_id:'+str(self.id) + ' #15')
                        rebroadcast = True
                        index += 1
                    else:
                        logging.log(TRACE, 'edge_id:' +
                                    str(self.id) + ' #15else')
                        rebroadcast = True
                        index += 1

                elif z_kj == float('-inf'):
                    if z_ij == i:
                        logging.log(TRACE, 'edge_id:'+str(self.id) + ' #31')
                        rebroadcast = True
                        index += 1
                    elif z_ij == k:
                        logging.log(TRACE, 'edge_id:'+str(self.id) + ' #32')
                        index = self.reset(index)
                        rebroadcast = True
                    elif z_ij == float('-inf'):
                        logging.log(TRACE, 'edge_id:'+str(self.id) + ' #34')
                        index += 1
                    elif z_ij != i and z_ij != k:
                        logging.log(TRACE, 'edge_id:'+str(self.id) + ' #33')
                        if t_kj > t_ij:
                            index = self.reset(index)
                            rebroadcast = True
                        else:
                            index += 1
                    else:
                        logging.log(TRACE, 'edge_id:' +
                                    str(self.id) + ' #33else')
                        index += 1

                elif z_kj != i or z_kj != k:
                    if z_ij == i:
                        if (y_kj > y_ij) or (y_kj == y_ij and z_kj < z_ij):
                            logging.log(TRACE, 'edge_id:'+str(self.id) + '#17')
                            rebroadcast = True
                            while index < layer_number and self.item['auction_id'][index] == z_kj:
                                self.updated_gpu = self.updated_gpu + \
                                    self.item['NN_gpu'][index]
                                self.updated_cpu = self.updated_cpu + \
                                    self.item['NN_cpu'][index]
                                self.updated_bw[self.item['user']][self.id] = self.updated_bw[self.item['user']
                                                                                              ][self.id] + self.item['NN_data_size'][index]
                                index = self.update_local_val(
                                    index, z_kj, self.item['bid'][index], self.item['timestamp'][index])
                        elif (y_kj < y_ij):
                            logging.log(TRACE, 'edge_id:'+str(self.id) + '#19')
                            rebroadcast = True
                            while index < layer_number and self.bids[self.item['job_id']]['auction_id'][index] == z_ij:
                                index = self.update_local_val(
                                    index, z_ij, self.bids[self.item['job_id']]['bid'][index], datetime.now())
                        else:
                            logging.log(TRACE, 'edge_id:' +
                                        str(self.id) + ' #19else')

                            index += 1

                    elif z_ij == k:

                        if t_kj > t_ij:
                            logging.log(TRACE, 'edge_id:'+str(self.id) + '#20')
                            while index < layer_number and self.item['auction_id'][index] == z_kj:
                                index = self.update_local_val(
                                    index, z_kj, self.item['bid'][index], self.item['timestamp'][index])
                            rebroadcast = True
                        elif t_kj < t_ij:
                            logging.log(TRACE, 'edge_id:'+str(self.id) + '#21')
                            index = self.reset(index)
                            rebroadcast = True
                        else:
                            logging.log(TRACE, 'edge_id:' +
                                        str(self.id) + ' #21else')
                            index += 1

                    elif z_ij == z_kj:

                        if t_kj > t_ij:
                            logging.log(TRACE, 'edge_id:'+str(self.id) + '#22')
                            while index < layer_number and self.item['auction_id'][index] == z_kj:
                                index = self.update_local_val(
                                    index, z_kj, self.item['bid'][index], self.item['timestamp'][index])
                        else:
                            logging.log(TRACE, 'edge_id:' +
                                        str(self.id) + ' #22else')
                            index += 1
                    elif z_ij == float('-inf'):
                        logging.log(TRACE, 'edge_id:'+str(self.id) + '#30')
                        index = self.update_local_val(
                            index, z_kj, self.item['bid'][index], self.item['timestamp'][index])
                        rebroadcast = True

                    elif z_ij != i and z_ij != k and z_ij != z_kj:
                        if y_kj > y_ij and t_kj >= t_ij:
                            logging.log(TRACE, 'edge_id:'+str(self.id) + '#25')
                            while index < layer_number and self.item['auction_id'][index] == z_kj:
                                index = self.update_local_val(
                                    index, z_kj, self.item['bid'][index], self.item['timestamp'][index])
                            rebroadcast = True
                        elif y_kj < y_ij and t_kj <= t_ij:
                            logging.log(TRACE, 'edge_id:'+str(self.id) + '#26')
                            rebroadcast = True
                            index += 1
                        elif y_kj == y_ij and z_kj < z_ij:
                            logging.log(TRACE, 'edge_id:'+str(self.id) + '#27')
                            while index < layer_number and self.item['auction_id'][index] == z_kj:
                                index = self.update_local_val(
                                    index, z_kj, self.item['bid'][index], self.item['timestamp'][index])
                            rebroadcast = True
                        elif y_kj == y_ij:
                            logging.log(TRACE, 'edge_id:'+str(self.id) + '#27')
                            index += 1
                        elif y_kj < y_ij and t_kj > t_ij:
                            logging.log(TRACE, 'edge_id:'+str(self.id) + '#28')
                            while index < layer_number and self.item['auction_id'][index] == z_kj:
                                index = self.update_local_val(
                                    index, z_kj, self.item['bid'][index], self.item['timestamp'][index])
                            rebroadcast = True
                        elif y_kj > y_ij and t_kj < t_ij:
                            logging.log(TRACE, 'edge_id:'+str(self.id) + '#29')
                            index += 1
                            rebroadcast = True
                        else:
                            logging.log(TRACE, 'edge_id:' +
                                        str(self.id) + '#29else')
                            index += 1
                    else:
                        logging.log(TRACE, 'edge_id:' +
                                    str(self.id) + ' #29else2')
                        index += 1
                else:
                    self.print_node_state('smth wrong?', type='error')
                    pass

            else:
                self.print_node_state(
                    'Value not in dict (deconfliction)', type='error')

                # logging.error("Value not in dict (deconfliction) - user:"+ str(self.id) + " " + str(self.item['job_id'])+"job_id: "  + str(self.item['job_id']) + " node: " + str(self.id) + " from " + str(self.item['user']))

        # print("End deconfliction - user:"+ str(self.id) + " job_id: "  + str(self.item['job_id'])  + " from " + str(self.item['user']))

        return rebroadcast

    def update_bid(self):

        if self.item['job_id'] in self.bids:

            # check consensus
            if self.bids[self.item['job_id']]['auction_id'] == self.item['auction_id'] and self.bids[self.item['job_id']]['bid'] == self.item['bid'] and self.bids[self.item['job_id']]['timestamp'] == self.item['timestamp']:
                if self.id not in self.bids[self.item['job_id']]['auction_id'] and float('-inf') in self.bids[self.item['job_id']]['auction_id']:
                    self.rebid()
                else:
                    self.print_node_state('Consensus -')
                    pass

            else:
                self.print_node_state('BEFORE', True)
                rebroadcast = self.deconfliction()

                if self.id not in self.bids[self.item['job_id']]['auction_id'] and float('-inf') in self.bids[self.item['job_id']]['auction_id']:
                    self.rebid()

                elif rebroadcast:
                    self.forward_to_neighbohors()
                self.print_node_state('AFTER', True)

        else:
            self.print_node_state(
                'Value not in dict (update_bid)', type='error')

    def new_msg(self):
        if str(self.id) == str(self.item['edge_id']):
            self.print_node_state(
                'This was not supposed to be received', type='error')

        elif self.item['job_id'] in self.bids:
            if self.integrity_check(self.item['auction_id']):
                self.update_bid()
        else:
            self.print_node_state('Value not in dict (new_msg)', type='error')

    def integrity_check(self, bid):
        curr_val = bid[0]
        curr_count = 1
        for i in range(1, len(bid)):

            if bid[i] == curr_val:
                curr_count += 1
            else:
                if curr_count < min_layer_number or curr_count > max_layer_number:
                    self.print_node_state('DISCARD BROKEN MSG' + str(bid))
                    return False

                curr_val = bid[i]
                curr_count = 1

        return True

    def work(self):
        while True:
            if (self.q.empty() == False):

                global counter
                counter += 1
                self.item = None
                self.item = copy.deepcopy(self.q.get())

                if self.item['job_id'] not in self.bids:
                    self.init_null()

                # check msg type
                if self.item['edge_id'] is not None and self.item['user'] in self.user_requests:
                    # edge to edge request
                    self.print_node_state('IF1 q:' + str(self.q.qsize()), True)
                    self.new_msg()

                elif self.item['edge_id'] is None and self.item['user'] not in self.user_requests:
                    # brand new request from client
                    self.print_node_state('IF2 q:' + str(self.q.qsize()))
                    self.user_requests.append(self.item['user'])
                    self.first_msg()

                elif self.item['edge_id'] is not None and self.item['user'] not in self.user_requests:
                    # edge anticipated client request
                    self.print_node_state('IF3 q:' + str(self.q.qsize()))
                    self.user_requests.append(self.item['user'])
                    self.new_msg()

                elif self.item['edge_id'] is None and self.item['user'] in self.user_requests:
                    # client after edge request
                    self.print_node_state('IF4 q:' + str(self.q.qsize()))
                    self.rebid()

                self.q.task_done()

                # print(str(self.q.qsize()) +" polpetta - user:"+ str(self.id) + " job_id: "  + str(self.item['job_id'])  + " from " + str(self.item['user']))


def message_data(job_id, user, num_gpu, num_cpu, duration, job_name, submit_time, gpu_type, num_inst, size, bandwidth):

    gpu = int(num_gpu / layer_number)
    cpu = int(num_cpu / layer_number)
    bw = int(float(bandwidth) / layer_number)

    NN_gpu = np.ones(int(layer_number)) * gpu
    NN_cpu = np.ones(int(layer_number)) * cpu
    NN_data_size = np.ones(int(layer_number)) * bw

    data = {
        "job_id": int(),
        "user": int(),
        "num_gpu": int(),
        "num_cpu": int(),
        "duration": int(),
        "job_name": int(),
        "submit_time": int(),
        "gpu_type": int(),
        "num_inst": int(),
        "size": int(),
        "edge_id": int(),
        "NN_gpu": NN_gpu,
        "NN_cpu": NN_cpu,
        "NN_data_size": NN_data_size
    }

    data['edge_id'] = None
    data['job_id'] = job_id
    data['user'] = user
    data['num_gpu'] = num_gpu
    data['num_cpu'] = num_cpu
    data['duration'] = duration
    data['job_name'] = job_name
    data['submit_time'] = submit_time
    data['gpu_type'] = gpu_type
    data['num_inst'] = num_inst
    data['size'] = size
    data['job_id'] = job_id

    return data
