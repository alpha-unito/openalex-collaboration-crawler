# -----------------------------
# Configuration parameters
# -----------------------------

# Input directory containing the graph CSV files
graph_input_directory = "/beegfs/home/msantima/OpenAlexCollaborations/IT/nets_weighted"

# Output file for structural statistics
output_stats_file = "./structural_stats.csv"

# Output file for structural statistics of the largest connected component
output_stats_file_largest_cc = "./largestCC_structural.csv"

# Whether to skip the header row in the CSV files (set false if no header)
skip = False

# -----------------------------
# END Configuration parameters
# -----------------------------

import rustworkx as rwx
import alive_progress, subprocess, os, sys
import numpy as np
import pandas as pd


def compute_structural_stats(graph, graph_name):
    """
    Compute structural statistics of a graph.
    :param graph: NetworkX graph
    :return: Dictionary of structural statistics
    """

    # degree distribution
    degree_sequence = sorted([graph.degree(n) for n in graph.node_indices()], reverse=True)  # degree sequence
    min_degree = min(degree_sequence)
    max_degree = max(degree_sequence)
    mean_degree = np.mean(degree_sequence)
    median_degree = np.median(degree_sequence)
    degree_std = np.std(degree_sequence)

    #weighted degree distribution
    weighted_degree_sequence = sorted([ sum([v for(k,v) in graph.adj(n).items()]) for n in graph.node_indices()], reverse=True)
    w_min_degree = min(weighted_degree_sequence)
    w_max_degree = max(weighted_degree_sequence)
    w_mean_degree = np.mean(weighted_degree_sequence)
    w_median_degree = np.median(weighted_degree_sequence)
    w_degree_std = np.std(weighted_degree_sequence)


    stats = {
        'graph_name': graph_name,
        'number_of_nodes': len(graph.nodes()),
        'number_of_edges': len(graph.edges()),
        'min_degree': min_degree,
        'max_degree': max_degree,
        'mean_degree': mean_degree,
        'median_degree': median_degree,
        'degree_std': degree_std,
        'w_min_degree': w_min_degree,
        'w_max_degree': w_max_degree,
        'w_mean_degree': w_mean_degree,
        'w_median_degree': w_median_degree,
        'w_degree_std': w_degree_std,
        'density': len(graph.edges()) / (len(graph.nodes()) * (len(graph.nodes()) - 1) / 2),
        'transitivity': rwx.transitivity(graph),
        'n_connected_components': rwx.number_connected_components(graph)
    }

    return stats

if __name__ == "__main__":
    
    for path in os.listdir(graph_input_directory):
        if not path.endswith(".csv"):
            continue
        
        
        graph_name = path.split("/")[-1].split(".")[0]
        graph_path = f"{graph_input_directory}/{path}"
        
        print(f"Loading graph {graph_name} from {graph_path}")
        # load the graph
        node_map = {}
        graph = rwx.PyGraph()
        
        num_lines = int(subprocess.run("wc -l " + graph_path, shell=True, text=True, capture_output=True).stdout.split(' ')[0])
        
        with alive_progress.alive_bar(num_lines) as bar:
            with open(graph_path, 'r') as f:
                for data in f.readlines():
                    bar()
                    if skip:
                        cols = data.strip().split(",")
                        author_1_col = cols.index("source")
                        author_2_col = cols.index("target")
                        weight_col = cols.index("weight")
                        skip = False
                        continue
                    else:
                        author_1_col = 0
                        author_2_col = 1
                        weight_col = 2
                    parts   = data.strip().split(",")
                    author1 = parts[author_1_col]
                    author2 = parts[author_2_col]
                    weight  = parts[weight_col]
            
                    # add the nodes to the graph
                    if author1 not in node_map:
                        node_map[author1] = graph.add_node(author1)
                    if author2 not in node_map:
                        node_map[author2] = graph.add_node(author2)
                        
                    # add the edge to the graph
                    graph.add_edge(node_map[author1], node_map[author2], int(weight))
                

        print(f"Graph {graph_name} loaded with {len(graph.nodes())} nodes and {len(graph.edges())} edges")
        
        print(f"Computing statistics for graph {graph_name}")
        # # compute the structural statistics
        stats = compute_structural_stats(graph, graph_name)
        
        # # dump the dict stats to a csv file
        df = pd.DataFrame.from_dict(stats, orient='index').T

        # # if output_path does not exist
        if not os.path.exists(output_stats_file):
            df.to_csv(output_stats_file, index=False)
        else:
            # append the new stats to the existing csv file
            df.to_csv(output_stats_file, mode='a', header=False, index=False)
            
        print(f"Computing statistics for the largest connected component of graph {graph_name}")
        # get the largest connected component
        largest_cc = list(max(rwx.connected_components(graph), key=len))
        graph = graph.subgraph(largest_cc)
        
        
        # compute the structural statistics
        stats = compute_structural_stats(graph, graph_name)
        
        # dump the dict stats to a csv file
        df = pd.DataFrame.from_dict(stats, orient='index').T
        
        if not os.path.exists(output_stats_file_largest_cc):
            df.to_csv(output_stats_file_largest_cc, index=False)
        else:
            # append the new stats to the existing csv file
            df.to_csv(output_stats_file_largest_cc, mode='a', header=False, index=False)

    



