# Validate structural graph properties against synthetic graphs

import pandas as pd
import matplotlib.pyplot as plt
import tomllib
import re, sys, os
from pathlib import Path
import networkx as nx
import rustworkx as rx
import alive_progress
from compute_structural_statistics import compute_structural_stats

tmp_path = "/tmp/synthetic_graph.edgelist"

toml_config_path = sys.argv[1] if len(sys.argv) > 1 else "default.toml"
print(f"Parsing {toml_config_path} configuration file")


with open(toml_config_path, "rb") as f:
    cfg = tomllib.load(f)

try:
    analyzed_country = cfg["analized_country_full"]
    base_dir = Path(cfg["statistics_out_basedir"])

    bacbones_path = cfg["workflow_data"] + "/" + cfg["country"] + "/" + cfg["backbones"]["outputs"]["backbone_directory"]
    
    output_stats_filename = cfg["statistics_out_basedir"] + "/" + cfg["graph_property_validation"]["outputs"]["stats_out"]
    iterations = cfg["graph_property_validation"]["iterations"]


except KeyError as e:
    raise RuntimeError(f"Missing config key: {e}")

def load_collaboration_graph(path: str) -> nx.Graph:
    print("Loading file: ", path)
    collab_graph = nx.Graph()
    with open (path, 'r') as f:
        next(f)
        for line in f:
            source, target, weight, _ = line.strip().split(',')
            collab_graph.add_edge(source, target, weight=float(weight))
    print("\tGraph has: ", collab_graph.number_of_nodes(), " nodes and ", collab_graph.number_of_edges(), " edges")
    return collab_graph


for bacbone_name in os.listdir(bacbones_path):
    print(f"Analizing backbone {bacbone_name}")
    
    bacbone = bacbones_path + "/" + bacbone_name
    graph = load_collaboration_graph(bacbone)
    degree_sequence = [d for _,d in graph.degree()]
    
    print(f"Executing analisis on {bacbone_name} with {iterations} iterations")
    all_stats = []

    with alive_progress.alive_bar(iterations, title="bacbone_name") as bar:
        for i in range(iterations):
            grx = rx.PyGraph()
    
            g = nx.expected_degree_graph(degree_sequence, seed=42 + i)
            
        
            nodemap = {node: grx.add_node(node) for node in g.nodes()}
            
            for u, v in g.edges():
                grx.add_edge(nodemap[u], nodemap[v], 1)
            
            stats = compute_structural_stats(graph=grx, graph_name=f"{bacbone_name}.{i}")
            all_stats.append(stats)
            
            bar()

    # 3. Create a single DataFrame from the list and compute the mean
    results_df = pd.DataFrame(all_stats)
    average_stats = results_df.mean(numeric_only=True).to_frame().T
    average_stats["graph_name"] = bacbone_name
    cols = ["graph_name"] + [c for c in average_stats.columns if c != "graph_name"]
    average_stats = average_stats[cols]
            
    if not os.path.exists(output_stats_filename):
        average_stats.to_csv(output_stats_filename, index=False)
    else:
        average_stats.to_csv(output_stats_filename, mode='a', header=False, index=False)
    

    
print(f"Stored stats to {output_stats_filename}")
        
        

        

