import math
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