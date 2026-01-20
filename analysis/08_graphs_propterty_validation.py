# Validate structural graph properties against synthetic graphs

import pandas as pd
import matplotlib.pyplot as plt
import tomllib
import re, sys, os
from pathlib import Path
import networkx as nx
import alive_progress
import random
import numpy as np


toml_config_path = sys.argv[1] if len(sys.argv) > 1 else "default.toml"
print(f"Parsing {toml_config_path} configuration file")


with open(toml_config_path, "rb") as f:
    cfg = tomllib.load(f)

try:
    analyzed_country = cfg["analized_country_full"]
    base_dir = Path(cfg["statistics_out_basedir"])

    bacbones_path = cfg["workflow_data"] + "/" + cfg["country"] + "/" + cfg["backbones"]["outputs"]["backbone_directory"]
    
    output_stats_filename_random = cfg["statistics_out_basedir"] + "/" + cfg["graph_property_validation"]["outputs"]["stats_out_random"]
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
            parts  = line.strip().split(',')
            source = parts[0]
            target = parts[1]
            weight = parts[2]
            collab_graph.add_edge(source, target, weight=float(weight))
    print("\tGraph has: ", collab_graph.number_of_nodes(), " nodes and ", collab_graph.number_of_edges(), " edges")
    return collab_graph


def compute_structural_stats(graph, graph_name):
    """
    Compute structural statistics of a graph.
    :param graph: NetworkX graph
    :return: Dictionary of structural statistics
    """

    # degree distribution
    degree_sequence = sorted([graph.degree(n) for n in graph.nodes()], reverse=True)  # degree sequence
    min_degree = min(degree_sequence)
    max_degree = max(degree_sequence)
    mean_degree = np.mean(degree_sequence)
    median_degree = np.median(degree_sequence)
    degree_std = np.std(degree_sequence)

    try:
        density = len(graph.edges()) / (len(graph.nodes()) * (len(graph.nodes()) - 1) / 2)
    except:
        density = -1


    stats = {
        'graph_name': graph_name,
        'number_of_nodes': len(graph.nodes()),
        'number_of_edges': len(graph.edges()),
        'min_degree': min_degree,
        'max_degree': max_degree,
        'mean_degree': mean_degree,
        'median_degree': median_degree,
        'degree_std': degree_std,
        'density': density ,
        'clustering_coefficent' : nx.average_clustering(graph),
        'degree_assortativity' : nx.degree_assortativity_coefficient(graph),
        'transitivity': nx.transitivity(graph),
        'n_connected_components': nx.number_connected_components(graph)
    }

    return stats


for bacbone_name in os.listdir(bacbones_path):
    print(f"Analizing backbone {bacbone_name}")
    
    bacbone = bacbones_path + "/" + bacbone_name
    graph = load_collaboration_graph(bacbone)
    degree_sequence = [d for _,d in graph.degree()]
    
    print(f"Executing analisis on {bacbone_name} with {iterations} iterations")
    
    stats =  pd.DataFrame([compute_structural_stats(graph=graph, graph_name=bacbone_name)])

    if not os.path.exists(output_stats_filename):
        stats.to_csv(output_stats_filename, index=False)
    else:
        stats.to_csv(output_stats_filename, mode='a', header=False, index=False)
    
    all_stats = []

    with alive_progress.alive_bar(iterations, title="bacbone_name") as bar:
        for i in range(iterations):
    
            g = nx.expected_degree_graph(degree_sequence, seed=random.randint(0, 1000000))
            
            stats = compute_structural_stats(graph=g, graph_name=f"{bacbone_name}.{i}")
            all_stats.append(stats)
            
            bar()

    # 3. Create a single DataFrame from the list and compute the mean
    results_df = pd.DataFrame(all_stats)

    mean_df = results_df.mean(numeric_only=True)
    var_df = results_df.var(numeric_only=True)

    average_stats = pd.concat(
        [mean_df, var_df.add_suffix("_var")],
        axis=0
    ).to_frame().T
    
    average_stats["graph_name"] = bacbone_name
    cols = ["graph_name"] + [c for c in average_stats.columns if c != "graph_name"]
    average_stats = average_stats[cols]
            
    if not os.path.exists(output_stats_filename_random):
        average_stats.to_csv(output_stats_filename_random, index=False)
    else:
        average_stats.to_csv(output_stats_filename_random, mode='a', header=False, index=False)
    

    
print(f"Stored random generated stats to {output_stats_filename_random}")
print(f"Stored bacbone stats to {output_stats_filename}")

        
        

        

