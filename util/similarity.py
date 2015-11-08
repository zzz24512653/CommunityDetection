import math
import networkx as nx

def coefficient(node_i,node_j,G):
    '''
        paper : <<Defining and identifying communities in networks>>
    '''
    neighbors_i = set(nx.neighbors(G, node_i))
    neighbors_j = set(nx.neighbors(G, node_j))
    common = len(set(neighbors_i & neighbors_j))
    min_k = min(len(neighbors_i),len(neighbors_j))-1
    if(min_k == 0):
        return 1
    else:
        return common / min_k
    
def cal_similarity(G, node_i, node_j):
    '''
        paper : <<SCAN:A Structural Clustering Algorithm for Networks>>
    '''
    s1 = set(G.neighbors(node_i))
    s1.add(node_i)
    s2 = set(G.neighbors(node_j))
    s2.add(node_j)
    return len(s1 & s2) / math.sqrt(len(s1) * len(s2))