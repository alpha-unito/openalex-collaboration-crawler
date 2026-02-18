import os, sys, tomllib

toml_config_path = sys.argv[1] if len(sys.argv) > 1 else "default.toml"

print("Parsing {} configuration file".format(toml_config_path))
with open(toml_config_path, 'rb') as f:
    configuration = tomllib.load(f)

try:
    input_networks_path     = configuration["workflow_data"] + "/" + configuration["country"] + "/" + configuration["backbones"]["inputs"]["graph_directory"]
    output_networks_path    = configuration["workflow_data"] + "/" + configuration["country"] + "/" + configuration["backbones"]["outputs"]["backbone_directory"]
except Exception as e:
    print("Error: key {} not found".format(e))
    exit(-1)

print(f"\n{'=' * 60}")
print(f"{" BACKBONE CONFIGURATION ".center(60, ' ')}")
print(f"{'=' * 60}")

# --- Inputs ---
print(f"\n[DATA SOURCE]")
print(f"  Weighted Graph Directory:          {input_networks_path}")

# --- Outputs ---
print(f"\n[OUTPUTS]")
print(f"  Bacbones output directory:    {output_networks_path}")

import networkx as nx
import numpy as np
import pandas as pd
import netbone as nb
from netbone.filters import threshold_filter
from pathlib import Path

Path(output_networks_path).mkdir(parents=True, exist_ok=True)

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

    graphs_to_process = sorted(graphs_to_process, key=lambda x: os.path.getsize(f"{input_networks_path}/{x}"))

    for path in graphs_to_process:
        filename = input_networks_path + "/" + path.split("/")[-1].split(".")[0] + ".csv"
        print(f"Processing graph: {filename}")
        
        output_file_name = f"{output_networks_path}/backbone_{path.split("/")[-1].split(".")[0]}.csv"

        if os.path.exists(output_file_name):
            print(f"Backbone already computed for path {filename}")
            continue


        print(f"Output path: {output_file_name}")
        data = pd.read_csv(filename)
        column_names = ['author1', 'author2', 'weight']
        data.columns = column_names

        graph = nx.from_pandas_edgelist(data, source='author1', target='author2', edge_attr='weight', create_using=nx.Graph())

        backbone_graph = generate_bacbone(graph)

        g_data = nx.to_pandas_edgelist(backbone_graph)
        g_data.to_csv(output_file_name, index=False)
    