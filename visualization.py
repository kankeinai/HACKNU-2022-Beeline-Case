import networkx as nx
import igviz as ig

def generate_edges(graph):

    #----------Function-Description-----------------
    # INPUT: graph  - graph generated in the previous part
    # OUTPUT: edges - list of edges written as tuples
    
    # aggregator for edges
    edges = []
    for key in graph.keys():

        # if column is in dependancies of the other, then there is 
        # an edge directed from parent column to child column

        for dependence in graph[key]["cols_dependencies"]:
            name = key
            if graph[key]['data_sources']!=None:
                # format name 
                name = graph[key]['data_sources'][0].split(".")[0]+key
            # add parent -> child tuple
            edges.append((dependence, name))

    return edges

def createDiGraph(edges):
    G = nx.DiGraph()
    G.add_edges_from(edges)
    return G

edges = generate_edges(graph)
DG = createDiGraph(edges)

color_list = []
sizing_list = []

for node in DG.nodes():
    size = DG.degree(node) * 15
    sizing_list.append(size)

ig.plot(DG,
        title = "Column dependences",
        layout = "planar",
        size_method=sizing_list, 
        arrow_size = 2,
        node_opacity = 0.2
        )

