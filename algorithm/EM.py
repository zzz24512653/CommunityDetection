import random 
import collections
import networkx as nx

'''
    paper : <<Mixture models and exploratory analysis in networks>>
'''

class EM():

    def __init__(self, G, k, max_iter = 100):
        self._G = G
        self._n = len(self._G.node)
        self._k = k
        self._pi = []
        self._theta = []
        self._max_iter = max_iter
        
    def e_step(self, q):
        for i in range(self._n):
            q.append([])
            norm = 0.0
            for g in range(self._k):
                x = self._pi[g]
                for j in self._G.neighbors(i):
                    x *= self._theta[g][j]
                q[i].append(x)
                norm += x
            for g in range(self._k):
                q[i][g] /= norm
    
    def m_step(self, q):
        for g in range(self._k):
            sum1 = 0.0
            sum3 = 0.0
            for i in range(self._n):
                sum1 += q[i][g]
                sum2 = 0.0
                for j in self._G.neighbors(i):
                    sum2 += q[j][g]
                self._theta[g][i] = sum2  # update theta
                sum3 += q[i][g]*len(self._G.neighbors(i))
            self._pi[g] = sum1/self._n  # update pi
            for i in range(self._n):
                self._theta[g][i] /= sum3 # norm
        
    def execute(self):
        # initial parameters
        X = [1.0+random.random() for i in range(self._k)]
        norm = sum(X)
        self._pi = [x/norm for x in X]
        
        for i in range(self._k):
            Y = [1.0+random.random() for j in range(self._n)]
            norm = sum(Y)
            self._theta.append([y/norm for y in Y])
        
        q_old = []
        for iter_time in range(self._max_iter):
            q = []
            # E-step
            self.e_step(q)
            # M-step
            self.m_step(q)
                    
            if(iter_time != 0):
                deltasq = 0.0
                for i in range(self._n):
                    for g in range(self._k):
                        deltasq += (q_old[i][g]-q[i][g])**2
                #print "delta: ", deltasq
                if(deltasq < 0.05):
                    #print "iter_time: ", iter_time
                    break
            
            q_old = []
            for i in range(self._n):
                q_old.append([])
                for g in range(self._k):
                    q_old[i].append(q[i][g])
        
        communities = collections.defaultdict(lambda:set())
        for i in range(self._n):
            c_id = 0
            cur_max = q[i][0]
            for j in range(1,self._k):
                if q[i][j] > cur_max:
                    cur_max = q[i][j]
                    c_id = j
            communities[c_id].add(i)
        return communities.values()

if __name__ == '__main__':
    G = nx.karate_club_graph()
    algorithm = EM(G, 2)
    communities = algorithm.execute()
    for c in communities:
        print len(c), sorted(c)