import networkx as nx
import os
import numpy as np
import pickle
import csv
from pathlib import Path


# -----------------------------
# Configuration parameters
# -----------------------------

# Input folder containing the backbone graph CSV files
input_graph_folder = "/beegfs/home/msantima/OpenAlexCollaborations/IT/backbones"

# Output folder for the extracted communities. If None, it will be set to input folder with "backbones" replaced by "communities"
output_graph_folder = None

# Output file for the community statistics
statistics_output_file = "communities_statistics.csv"

# -----------------------------
# END Configuration parameters
# -----------------------------



def load_collaboration_graph(path: str) -> nx.Graph:
    print("Loading file: ", path)
    collab_graph = nx.Graph()
    with open(path, "r") as f:
        next(f)
        for line in f:
            source, target, weight, _ = line.strip().split(",")
            collab_graph.add_edge(source, target, weight=float(weight))
    print(
        "\tGraph has: ",
        collab_graph.number_of_nodes(),
        " nodes and ",
        collab_graph.number_of_edges(),
        " edges",
    )
    return collab_graph


def find_communities(graph: nx.Graph, seed: int = 42) -> list:
    communities = nx.community.louvain_communities(graph, weight="weight", seed=seed)
    print("\tFound ", len(communities), " communities")
    return communities


def dump_communities(communities: list, output_path: str):

    with open(output_path, "wb") as f:
        pickle.dump(communities, f)
    print("\tCommunities dumped to: ", output_path)


def dump_statistics(filename: str, statistics: list, output_path: str):
    dataset_name = filename.split("/")[-1].replace("_dataset_backbone.csv", "").replace(
        "weighted_", ""
    )
    
    if not os.path.exists(output_path):
        with open(output_path, "w") as f:
            csv.writer(f).writerow(
                ["dataset", "modularity", "coverage", "performance", "conductance"]
            )
            f.write("")

    with open(output_path, "a") as f:
        csv.writer(f).writerow([dataset_name] + statistics)
        f.write("")
    print("\tStatistics dumped to: ", output_path)


def eval_conductance(graph, communities):
    conductance = np.zeros([len(communities), len(communities)])
    for i, comm_i in enumerate(communities):
        for j, comm_j in enumerate(communities):
            conductance[i, j] = nx.algorithms.cuts.conductance(
                graph, comm_i, comm_j, weight="weight"
            )
    return conductance


def compute_statistics(graph, communities):
    modularity = nx.community.modularity(graph, communities)
    coverage, performance = nx.community.partition_quality(graph, communities)
    conductance = np.mean(eval_conductance(graph, communities)).item()
    print(
        f"\tModularity: {modularity} - coverage: {coverage} - performance: {performance} - conductance: {conductance}"
    )
    return [modularity, coverage, performance, conductance]


if __name__ == "__main__":
    
    files = sorted(
        os.listdir(input_graph_folder), key=lambda x: os.stat(os.path.join(input_graph_folder, x)).st_size
    )
    
    if output_graph_folder is None:
        output_graph_folder = input_graph_folder.replace("backbones", "communities")
    
    if not os.path.exists(output_graph_folder):
        os.makedirs(output_graph_folder)
    

    for file in files:
        if not file.endswith(".csv"):
            continue
        print("\n\n")
        file = input_graph_folder + "/" + file
        print(f"Processing file: {file}")
        collab_graph = load_collaboration_graph(file)
        communities = find_communities(collab_graph)
        output_path = output_graph_folder + "/" + file.split("/")[-1].replace(".csv", "_communities.pkl")
        dump_communities(communities, output_path)

        print("Starting statistics computation...")
        statistics = compute_statistics(collab_graph, communities)
        dump_statistics(file, statistics=statistics, output_path=statistics_output_file)
