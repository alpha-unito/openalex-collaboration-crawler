import networkx as nx
import numpy as np
import pandas as pd
import netbone as nb
from netbone.filters import threshold_filter
import os


#CONFIGURATION PARAMETERS

input_networks_path = "./nets_weighted"
output_networks_path = "./backbones"


def generate_bacbone(graph):
    """
    Generate the backbone of a graph using the netbone library.
    :param graph: NetworkX graph
    :return: Backbone graph
    """
    backbone_net = nb.disparity(graph)
    filtered_backbone_net = threshold_filter(backbone_net, 0.05)
    
    return filtered_backbone_net

if __name__ == "__main__":

    graphs_to_process = []
    
    for path in os.listdir(input_networks_path):
        if path.endswith(".csv"):
            graphs_to_process.append(path)


    for path in graphs_to_process:
        filename = input_networks_path + "/" + path.split("/")[-1].split(".")[0] + ".csv"
        print(f"Processing graph: {filename}")
        
        output_file_name = f"{output_networks_path}/backbone_{path.split("/")[-1].split(".")[0]}.csv"
        print(f"Output path: {output_file_name}")
        data = pd.read_csv(filename)
        column_names = ['author1', 'author2', 'weight']
        data.columns = column_names

        graph = nx.from_pandas_edgelist(data, source='author1', target='author2', edge_attr='weight', create_using=nx.Graph())

        backbone_graph = generate_bacbone(graph)

        g_data = nx.to_pandas_edgelist(backbone_graph)
        g_data.to_csv(output_file_name, index=False)
    