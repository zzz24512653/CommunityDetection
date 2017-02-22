import networkx as nx

import sys
sys.path.append('../')

from util.graph_helper import load_graph
from util.graph_helper import clone_graph
from util.modularity import cal_Q

#paper: Community structure in social and biological networks

class GN:
    
    def __init__(self, G):
        self._G_cloned = clone_graph(G)
        self._G = G
        self._partition = [[n for n in G.nodes()]]
        self._max_Q = 0.0
        
    def execute(self):
        while len(self._G.edges()) != 0:
            edge = max(nx.edge_betweenness(self._G).items(),key=lambda item:item[1])[0]
            self._G.remove_edge(edge[0], edge[1])
            components = [list(c) for c in list(nx.connected_components(self._G))]
            if len(components) != len(self._partition):
                cur_Q = cal_Q(components, self._G_cloned)
                if cur_Q > self._max_Q:
                    self._max_Q = cur_Q
                    self._partition = components
        print self._max_Q
        print self._partition
        return self._partition
        
    
if __name__ == '__main__':
    G = load_graph('../network/club.txt')
    algorithm = GN(G)
    algorithm.execute()
    #print len(G.edges(None, False))

