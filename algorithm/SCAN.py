import networkx as nx
import math
import random

'''
paper : <<SCAN:A Structural Clustering Algorithm for Networks>>
'''

class SCAN():
    
    def __init__(self, G, epsilon=0.5, mu=3):
        self._G = G
        self._epsilon = epsilon
        self._mu = mu

    def cal_similarity(self, node_i, node_j):
        s1 = set(self._G.neighbors(node_i))
        s1.add(node_i)
        s2 = set(self._G.neighbors(node_j))
        s2.add(node_j)
        return len(s1 & s2) / math.sqrt(len(s1) * len(s2))

    def get_epsilon_neighbor(self, node):
        return [neighbor for neighbor in self._G.neighbors(node) if self.cal_similarity(node, neighbor) >= self._epsilon]        

    def is_core(self, node):
        return len(self.get_epsilon_neighbor(node)) >= self._mu
    
    def get_hubs_outliers(self, communities):
        other_nodes = set(self._G.node.keys())
        node_community = {}
        for i,c in enumerate(communities):
            for node in c:
                other_nodes.discard(node)
                node_community[node] = i
        hubs = []
        outliers = []
        for node in other_nodes:
            neighbors = self._G.neighbors(node)
            neighbor_community = set()
            for neighbor in neighbors:
                neighbor_community.add(node_community[neighbor])
            if len(neighbor_community) > 1:
                hubs.append(node)
            else:
                outliers.append(node)
        return hubs,outliers

    def execute(self):
        # random scan nodes
        visit_sequence = self._G.node.keys()
        random.shuffle(visit_sequence)
        communities = []
        for node_name in visit_sequence:
            node = self._G.node[node_name]
            if(node.get("classified") == True):
                continue
            if(self.is_core(node_name)):  # a new community
                community = [node_name]
                communities.append(community)
                node["type"] = "core"
                node["classified"] = True
                queue = self.get_epsilon_neighbor(node_name)
                while(len(queue) != 0):
                    temp = queue.pop(0)
                    if(self._G.node[temp].get("classified") != True):
                        self._G.node[temp]["classified"] = True
                        community.append(temp)
                    if(not self.is_core(temp)):
                        continue
                    R = self.get_epsilon_neighbor(temp)
                    for r in R:
                        node_r = self._G.node[r]
                        is_classified = node_r.get("classified")
                        if(is_classified):
                            continue
                        node_r["classified"] = True
                        community.append(r)
                        if(node_r.get("type") != "non-member"):
                            queue.append(r)
                        else:
                            node["type"] = "non-member"
        return communities

if __name__ == '__main__':
    G = nx.Graph()
    for i in range(14):
        G.add_node(i)
    edges = [(13, 9), (9, 12), (9, 8), (9, 10), (8, 7), (8, 12), (10, 11), (10, 12), (11, 7), (12, 7), (12, 11), (7, 6), (11, 6), (10, 6), (6, 3), (6, 4), (6, 0), (3, 5), (4, 5), (0, 5), (3, 2), (0, 1), (5, 2), (5, 1), (1, 2), (3, 4), (4, 0)]
    G.add_edges_from(edges)
    
    #print G.node.keys()
    
    #G = nx.karate_club_graph()
    
    algorithm = SCAN(G, 0.7, 3)
    communities = algorithm.execute()
    for community in communities:
        print 'community: ', sorted(community)
    hubs_outliers = algorithm.get_hubs_outliers(communities)
    print 'hubs: ', hubs_outliers[0]
    print 'outliers: ', hubs_outliers[1]
