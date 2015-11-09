import collections
import string
import random

'''
    paper : <<Fast unfolding of communities in large networks>>
'''

def load_graph(path):
    G = collections.defaultdict(dict)
    with open(path) as text:
        for line in text:
            vertices = line.strip().split()
            v_i = string.atoi(vertices[0])
            v_j = string.atoi(vertices[1])
            G[v_i][v_j] = 1.0
            G[v_j][v_i] = 1.0
    return G

class Vertex():
    
    def __init__(self, vid, cid, nodes, k_in=0):
        self._vid = vid
        self._cid = cid
        self._nodes = nodes
        self._kin = k_in  #结点内部的边的权重

class Louvain():
    
    def __init__(self, G):
        self._G = G
        self._m = 0　　#边数量
        self._cid_vertices = {}　　#需维护的关于社区的信息(社区编号,其中包含的结点编号的集合)
        self._vid_vertex = {}　　#需维护的关于结点的信息(结点编号，相应的Vertex实例)
        for vid in self._G.keys():
            self._cid_vertices[vid] = set([vid])
            self._vid_vertex[vid] = Vertex(vid, vid, set([vid]))
            self._m += sum([1 for neighbor in self._G[vid].keys() if neighbor>vid])
        
    def first_stage(self):
        print '---first stage---'
        mod_inc = False  #用于判断算法是否可终止
        visit_sequence = self._G.keys()
        random.shuffle(visit_sequence)
        while True:
            can_stop = True　　#第一阶段是否可终止
            for v_vid in visit_sequence:
                v_cid = self._vid_vertex[v_vid]._cid
                k_v = sum(self._G[v_vid].values()) + self._vid_vertex[v_vid]._kin
                cid_Q = {}
                for w_vid in self._G[v_vid].keys():
                    w_cid = self._vid_vertex[w_vid]._cid
                    if w_cid in cid_Q:
                        continue
                    else:
                        tot = sum([sum(self._G[k].values())+self._vid_vertex[k]._kin for k in self._cid_vertices[w_cid]])
                        if w_cid == v_cid:
                            tot -= k_v
                        k_v_in = sum([v for k,v in self._G[v_vid].items() if k in self._cid_vertices[w_cid]])
                        delta_Q = k_v_in - k_v * tot / self._m　　#由于只需要知道delta_Q的正负，所以少乘了1/(2*self._m)
                        cid_Q[w_cid] = delta_Q
                    
                cid,max_delta_Q = sorted(cid_Q.items(),key=lambda item:item[1],reverse=True)[0]
                '''
                print '===================='
                print 'v_vid: ', v_vid, 'c: ',self._cid_vertices[v_cid],' neighbors: ', self._G[v_vid].keys()
                print 'k_v: ', k_v
                print 'cid: ', cid , ' c: ', self._cid_vertices[cid]
                print 'tot: ', tot
                print 'k_v_in: ', k_v_in
                print 'delta_Q: ', delta_Q
                '''
                if max_delta_Q > 0.0 and cid!=v_cid:
                    
                    self._vid_vertex[v_vid]._cid = cid
                    self._cid_vertices[cid].add(v_vid)
                    self._cid_vertices[v_cid].remove(v_vid)
                    can_stop = False
                    mod_inc = True
            if can_stop:
                break
        return mod_inc
        
    def second_stage(self):
        print '---second stage---'
        cid_vertices = {}
        vid_vertex = {}
        for cid,vertices in self._cid_vertices.items():
            if len(vertices) == 0:
                continue
            new_vertex = Vertex(cid, cid, set())
            for vid in vertices:
                new_vertex._nodes.update(self._vid_vertex[vid]._nodes)
                new_vertex._kin += self._vid_vertex[vid]._kin
                for k,v in self._G[vid].items():
                    if k in vertices:
                        new_vertex._kin += v/2.0
            #print 'cid: ', cid , ' kin: ', new_vertex._kin
            #print new_vertex._nodes
            cid_vertices[cid] = set([cid])
            vid_vertex[cid] = new_vertex
        
        G = collections.defaultdict(dict)   
        for cid1,vertices1 in self._cid_vertices.items():
            if len(vertices1) == 0:
                continue
            for cid2,vertices2 in self._cid_vertices.items():
                if cid2<=cid1 or len(vertices2)==0:
                    continue
                edge_weight = 0.0
                for vid in vertices1:
                    for k,v in self._G[vid].items():
                        if k in vertices2:
                            edge_weight += v
                #print 's1: ', vertices1, ' s2: ',vertices2, ' weight: ', edge_weight
                if edge_weight != 0:
                    G[cid1][cid2] = edge_weight
                    G[cid2][cid1] = edge_weight
        
        self._cid_vertices = cid_vertices
        self._vid_vertex = vid_vertex
        self._G = G
        
        #print '==========new Graph=========='
        #for k,v in G.items():
            #print k,v
    
    def get_communities(self):
        #print '---communities---'
        communities = []
        for vertices in self._cid_vertices.values():
            if len(vertices) != 0:
                c = set()
                #print cid,vertices
                for vid in vertices:
                    c.update(self._vid_vertex[vid]._nodes)
                communities.append(c)
                #print 'cid: ', cid, ' len: ', len(c), c
        return communities
    
    def execute(self):
        '''
        for i in range(30):
            print '-----------------------------------------'+str(i)+'--------------------------------------------'
            print self.first_stage()
            self.second_stage()
            self.get_communities()
        '''
        iter_time = 1
        while True:
            print '---'+str(iter_time)+'---'
            iter_time += 1
            mod_inc = self.first_stage()
            if mod_inc:
                self.second_stage()
            else:
                break
        return self.get_communities()


if __name__ == '__main__':
    #G = load_graph('./demo.txt')
    G = load_graph('./club.txt')
    algorithm = Louvain(G)
    communities = algorithm.execute()
    for c in communities:
        print c
    #for k,v in G.items():
        #print k,v
        