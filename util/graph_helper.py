import networkx as nx


def load_graph(path):
    G = nx.Graph()
    with open(path) as text:
        for line in text:
            vertices = line.strip().split(" ")
            source = int(vertices[0])
            target = int(vertices[1])
            G.add_edge(source, target)
    return G


def clone_graph(G):
    cloned_g = nx.Graph()
    for edge in G.edges():
        cloned_g.add_edge(edge[0], edge[1])
    return cloned_g

if __name__ == "__main__":
    G = load_graph('../network/club.txt')
    print len(G.nodes(False))
    print len(G.edges(None, False))

    g2 = clone_graph(G)
    print g2.nodes()


#     for edge in G.edges(None, False):
#         print edge
