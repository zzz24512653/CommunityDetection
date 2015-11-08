import math
from collections import defaultdict
import numpy as np


def cal_NMI(X,Y,all_nodes):
    
    def _h(w,n):
        if w==0:
            return 0
        return -w*math.log(float(w)/n,2)
    
    def _H(X,n):
        res = 0.0
        for xi in X:
            res += _h(len(xi),n)
            res += _h(n-len(xi),n)
        return res
    
    def _info_loss_i(Xi,Yi,all_nodes):
        ''' Xi,Yi means a communtiy '''
        x_vector = np.zeros(n)
        y_vector = np.zeros(n)
        for node in Xi:
            x_vector[node_index[node]] = 1
        for node in Yi:
            y_vector[node_index[node]] = 1
          
        a=b=c=d=0
        for i,j in zip(x_vector,y_vector):
            if i==0:
                if j==0:
                    a += 1
                else:
                    b += 1
            else:
                if j==0:
                    c += 1
                else:
                    d += 1
        
        if _h(a,n)+_h(d,n)-_h(b,n)-_h(c,n) >= 0:
            H_xi_yi = _h(a,n)+_h(b,n)+_h(c,n)+_h(d,n)-_h(a+c,n)-_h(b+d,n)
        else:
            H_xi_yi = _h(c+d,n)+_h(a+b,n)
        return H_xi_yi
        
    def _info_loss(Xi,Y):
        ''' Y means a cover '''
        info_losses = []
        for Yi in Y:
            info_losses.append(_info_loss_i(Xi, Yi, all_nodes))
        return min(info_losses)

    node_index = {}
    for index,node in enumerate(all_nodes):
        node_index[node] = index
    n = len(node_index)
    
    H_X_Y = 0.0
    H_X = 0.0
    for Xi in X:
        H_X_Y += _info_loss(Xi, Y)
    H_X += _H(X,n)
    H_Y_X = 0.0
    H_Y = 0.0
    for Yi in Y:
        H_Y_X += _info_loss(Yi, X)
    H_Y += _H(Y,n)
        
    res = 1-(H_X_Y/H_X+H_Y_X/H_Y)/2
    return res
       
def cal_Modularity(partition,G):
    m = len(G.edges(None, False))
    a = []
    e = []
    
    for community in partition:
        t = 0.0
        for node in community:
            t += len(G.neighbors(node))
        a.append(t/(2*m))
        
    for community in partition:
        t = 0.0
        for i in range(len(community)):
            for j in range(len(community)):
                if(G.has_edge(community[i], community[j])):
                    t += 1.0
        e.append(t/(2*m))
        
    q = 0.0
    for ei,ai in zip(e,a):
        q += (ei - ai**2) 
        
    return q 

def cal_EQ(cover,G):
    """
        reference:Detect overlapping and hierarchical community structure in networks
        G:vertex-neighbors
    """
    vertex_community = defaultdict(lambda:set())
    for i,c in enumerate(cover):
        for v in c:
            vertex_community[v].add(i)
            
    m = 0.0  
    for v,neighbors in G.items():
        for n in neighbors:
            if v > n:
                m += 1
    
    total = 0.0
    for c in cover:
        for i in c:
            o_i = len(vertex_community[i])
            k_i = len(G[i])
            for j in c:
                o_j = len(vertex_community[j])
                k_j = len(G[j])
                if i > j:
                    continue
                t = 0.0
                if j in G[i]:
                    t += 1.0/(o_i*o_j)
                t -= k_i*k_j/(2*m*o_i*o_j)
                if i == j:
                    total += t
                else:
                    total += 2*t
    
    return round(total/(2*m),4)
