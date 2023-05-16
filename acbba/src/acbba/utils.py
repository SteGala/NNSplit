"""
This module contains utils functions to calculate all necessary stats
"""
from csv import DictWriter
from datetime import datetime
import os
import socket
import subprocess
import psutil

datetime_format = "%Y-%m-%d %H:%M:%S"

#def calculate_utility(nodes, num_edges, msg_count, time, n_req, job_ids, alpha):
#    stats = {}
#    stats['nodes'] = {}
#    stats['tot_utility'] = 0
#
#    field_names = ['n_nodes', 'n_req', 'n_msg',
#                   'exec_time', 'tot_utility', 'jaini', 'tot_gpu']
#    dictionary = {'n_nodes': num_edges, 'n_req': n_req,
#                  'n_msg': msg_count, 'exec_time': time, 'tot_gpu': c.tot_gpu}
#
#    # ---------------------------------------------------------
#    # alpha utility factor
#    # ---------------------------------------------------------
#    field_names.append('alpha')
#    dictionary['alpha'] = alpha
#
#    # ---------------------------------------------------------
#    # calculate assigned jobs
#    # ---------------------------------------------------------
#
#    count_assigned = 0
#    count_unassigned = 0
#
#    for j in job_ids:
#
#        flag = True
#        for i in range(c.num_edges):
#            if not all(x == 1 for x in c.nodes[i].bids[j]['x']):
#                flag = False
#
#        if flag:
#            count_assigned += 1
#        else:
#            count_unassigned += 1
#
#    field_names.append('count_assigned')
#    field_names.append('count_unassigned')
#    dictionary['count_assigned'] = count_assigned
#    dictionary['count_unassigned'] = count_unassigned
#
#    # ---------------------------------------------------------
#    # calculate node utility and assigned jobs
#    # ---------------------------------------------------------
#    for i in range(num_edges):
#        field_names.append('node_'+str(i)+'_jobs')
#        field_names.append('node_'+str(i)+'_utility')
#
#        stats['nodes'][nodes[i].id] = {
#            "utility": float(),
#            "assigned_count": int()
#        }
#
#        stats['nodes'][nodes[i].id]['utility'] = 0
#        stats['nodes'][nodes[i].id]['assigned_count'] = 0
#
#        for j, job_id in enumerate(nodes[i].bids):
#
#            if nodes[i].id in nodes[i].bids[job_id]['auction_id']:
#                stats['nodes'][nodes[i].id]['assigned_count'] += 1
#
#            # print(nodes[i].bids[job_id])
#            for k, auctioner in enumerate(nodes[i].bids[job_id]['auction_id']):
#                # print(nodes[i].id)
#                if auctioner == nodes[i].id:
#                    # print(nodes[i].bids[job_id]['bid'])
#                    stats['nodes'][nodes[i].id]['utility'] += nodes[i].bids[job_id]['bid'][k]
#        print(str(nodes[i].id) + ' utility: ' +
#              str(stats['nodes'][nodes[i].id]['utility']))
#        dictionary['node_' +
#                   str(i)+'_utility'] = stats['nodes'][nodes[i].id]['utility']
#        stats["tot_utility"] += stats['nodes'][nodes[i].id]['utility']
#
#    for i in stats['nodes']:
#
#        print('node: ' + str(i) + ' assigned jobs count: ' +
#              str(stats['nodes'][i]['assigned_count']))
#        dictionary['node_' +
#                   str(i)+'_jobs'] = stats['nodes'][i]['assigned_count']
#
#    dictionary['tot_utility'] = stats["tot_utility"]
#    print(stats["tot_utility"])
#
#    # ---------------------------------------------------------
#    # calculate fairness
#    # ---------------------------------------------------------
#    dictionary['jaini'] = jaini_index(dictionary, num_edges)
#
#    write_data(field_names, dictionary)
#
#
#def write_data(field_names, dictionary):
#    filename = 'bidding_results.csv'
#    file_exists = os.path.isfile(filename)
#
#    with open(filename, 'a', newline='') as f:
#        writer = DictWriter(f, fieldnames=field_names)
#
#        # Pass the dictionary as an argument to the Writerow()
#        if not file_exists:
#            writer.writeheader()  # write the column headers if the file doesn't exist
#
#        writer.writerow(dictionary)
#
#
#def jaini_index(dictionary, num_nodes):
#    data = []
#    for i in range(num_nodes):
#        data.append(dictionary['node_'+str(i)+'_jobs'])
#
#    sum_normal = 0
#    sum_square = 0
#
#    for arg in data:
#        sum_normal += arg
#        sum_square += arg**2
#"%Y-%m-%d %H:%M:%S"
#"%Y-%m-%d %H:%M:%S"r sum_square == 0:
#"%Y-%m-%d %H:%M:%S"
#"%Y-%m-%d %H:%M:%S"
#    return sum_normal ** 2 / (len(data) * sum_square)


def get_total_cpu_cores():
    return psutil.cpu_count(logical=True)


def get_total_gpu_cores():
    try:
        output = subprocess.check_output(
            ['nvidia-smi', '--query-gpu=index', '--format=csv,noheader']).decode().strip()
        gpu_indices = output.split('\n')
        return len(gpu_indices)
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None

def get_container_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(0)
    try:
        # doesn't even have to be reachable
        s.connect(('10.254.254.254', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

def sort_ip_addresses(ip_addresses):
    # Split the IP addresses into octets and convert them to integers
    ip_integers = [list(map(int, ip.split('.'))) for ip in ip_addresses]
    # Sort the IP addresses based on the octets
    sorted_ip_integers = sorted(ip_integers)
    # Convert the sorted octets back to IP address strings
    sorted_ip_addresses = ['.'.join(map(str, ip)) for ip in sorted_ip_integers]
    return sorted_ip_addresses

def datetime_list_to_string_list(datetime_list, format_str=datetime_format):
    string_list = [dt.strftime(format_str) for dt in datetime_list]
    return string_list

def string_list_to_datetime_list(string_list, format_str=datetime_format):
    datetime_list = [datetime.strptime(string, format_str) for string in string_list]
    return datetime_list