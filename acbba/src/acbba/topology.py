"""
Topology building module
"""

import numpy as np


# metti qua la topologia

class topology:
    def __init__(self, func_name, max_bandwidth, min_bandwidth, num_clients, num_edges):
        self.nax_bandwidth = max_bandwidth
        self.nin_bandwidth = min_bandwidth
        self.n = num_edges  # asjacency matrix

        self.to = getattr(self, func_name)

        self.b = np.random.uniform(min_bandwidth, max_bandwidth, size=(
            num_clients, num_edges))  # bandwidth matrix

    def call_func(self):
        return self.to()

    def linear_topology(self):
        """
        This function returns the adjacency matrix for a linear topology of n nodes.
        """
        # Create an empty adjacency matrix of size n x n
        adjacency_matrix = np.zeros((self.n, self.n))

        # Add edges to the adjacency matrix
        for i in range(self.n):
            if i == 0:
                # Connect node 0 to node 1
                adjacency_matrix[i][i+1] = 1
            elif i == self.n-1:
                # Connect node n-1 to node n-2
                adjacency_matrix[i][i-1] = 1
            else:
                # Connect node i to nodes i-1 and i+1
                adjacency_matrix[i][i-1] = 1
                adjacency_matrix[i][i+1] = 1

        return adjacency_matrix

    def complete_graph(self):
        return np.ones((self.n, self.n)) - np.eye(self.n)

    def ring_graph(self):
        adjacency_matrix = np.zeros((self.n, self.n))
        for i in range(self.n):
            adjacency_matrix[i][(i-1) % self.n] = 1
            adjacency_matrix[i][(i+1) % self.n] = 1
        return adjacency_matrix

    def star_graph(self):
        adjacency_matrix = np.zeros((self.n, self.n))
        adjacency_matrix[0, :] = 1
        adjacency_matrix[:, 0] = 1
        adjacency_matrix[0, 0] = 0
        return adjacency_matrix

    def grid_graph(self):
        adjacency_matrix = np.zeros((self.n*self.n, self.n*self.n))
        for i in range(self.n):
            for j in range(self.n):
                node = i*self.n + j
                if i > 0:
                    adjacency_matrix[node][node-self.n] = 1
                if i < self.n-1:
                    adjacency_matrix[node][node+self.n] = 1
                if j > 0:
                    adjacency_matrix[node][node-1] = 1
                if j < self.n-1:
                    adjacency_matrix[node][node+1] = 1
        return adjacency_matrix

# t = topo(func_name='grid_graph', max_bandwidth=100, min_bandwidth=10,num_clients=10, num_edges=10)
# print(t.call_func())
