from collections import defaultdict

"""
    paper:<<Detect overlapping and hierarchical community structure in networks>>
    G:vertex-neighbors {vertex:list(neighbors)}
"""

def cal_EQ(cover,G):
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