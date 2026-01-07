
import networkx as nx
import numpy as np
import os
import pickle
from pathlib import Path
import csv
import datetime
from sklearn.metrics import adjusted_mutual_info_score, normalized_mutual_info_score

# -----------------------------
# Configuration parameters
# -----------------------------

# Input folder containing the backbone graph CSV files
input_graph_folder = "/beegfs/home/msantima/OpenAlexCollaborations/IT/backbones"

# Communities output folder. If None, it will be set to input folder with "backbones" replaced by "stability"
communities_output_folder = None

# Output file for the community statistics
statistics_output_file = "communities_stability_statistics.csv"

# number of runs for stability evaluation
RUNS = 10

# -----------------------------
# END Configuration parameters
# -----------------------------


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

def dump_communities(communities: list, output_path: str):
    
    if not os.path.exists(os.path.dirname(output_path)):
        os.makedirs(os.path.dirname(output_path))
        
    with open(output_path, 'wb') as f:
        pickle.dump(communities, f)
    print("\tCommunities dumped to: ", output_path)
    
def find_communities(graph, runs):
    communities = []
    for i in range(runs):
        i % 5 == 0 and print("\tRun ", i)
        communities.append(nx.community.louvain_communities(graph,  weight='weight', seed=datetime.datetime.now().microsecond))
    return communities

def eval_stability(communities):
    """
    TL;DR:NMI requires that communities are node labels (i.e., assignments) rather than lists of nodes
    """
    # First, we convert communities to "node labels"
    community_labels = []
    # For each run, we convert the communities to a dictionary of node labels
    for run in communities:
        labels = {}
        # For each community, we convert the list of nodes to a dictionary of node labels
        for comm_id, community in enumerate(run):
            for node in community:
                labels[node] = comm_id
        community_labels.append(labels)

    # Pairwise NMI between runs (it is symmetric)
    nmi_values = []
    adj_nmi_values = []
    for i in range(len(community_labels)):
        for j in range(i+1, len(community_labels)):
            # We first keep only nodes that are present in both runs
            common_nodes = set(community_labels[i].keys()) & set(community_labels[j].keys())
            # We then filter the communities to only include the common nodes
            labels_i = [community_labels[i][node] for node in common_nodes]
            labels_j = [community_labels[j][node] for node in common_nodes]
            # We then evaluate the NMI
            nmi_values.append(normalized_mutual_info_score(labels_i, labels_j))
            # We also evaluate the adjusted NMI
            adj_nmi_values.append(adjusted_mutual_info_score(labels_i, labels_j))

    return nmi_values, adj_nmi_values

def get_bigger_communities(communities, min_size = 20):
    # first the bigger communities
    bigger_communities_per_partition = [list(filter(lambda x: len(x) > min_size, part)) for part in communities]

    # then get the minimum number of communities
    min_partition_size = min([len(part) for part in bigger_communities_per_partition])
    
    # finally, for each partition, only save the bigger communities
    # up to min_partition_size communities
    to_return = []

    for partition in bigger_communities_per_partition:
        comms_sorted_by_size = sorted(partition, key=lambda x: len(x), reverse=True)
        to_return.append(comms_sorted_by_size[:min_partition_size])

    return to_return

def dump_statistics(filename: str, statistics: list, output_path: str):
    dataset_name = filename.replace("_dataset_backbone.csv", "").replace("weighted_", "")
    
    if not os.path.exists(output_path):
        with open(output_path, 'w') as f:
            csv.writer(f).writerow(['dataset', 'NMI', 'ADJ_NMI'])
            f.write("")
    
    with open(output_path, 'a') as f:
        csv.writer(f).writerow([dataset_name] + statistics)
        f.write("")
    print("\tStatistics dumped to: ", output_path)
    

if __name__ == "__main__":
    files = os.listdir(input_graph_folder)
    files.sort()
    
    communities_output_folder = communities_output_folder or input_graph_folder.replace("backbones", "stability")
    
    if not os.path.exists(communities_output_folder):
        os.makedirs(communities_output_folder)
    
    for file in files:
        file = Path(file)
        if not file.name.endswith(".csv"):
            continue
        path = os.path.join(input_graph_folder, file) 
        collab_graph = load_collaboration_graph(path)
        collab_graph.__networkx_cache__ = None

        partitions = find_communities(collab_graph, RUNS)
        
        output_file_name = f"{communities_output_folder}/{file.name.replace('.csv', '_multiple_communities.pkl')}"
        
        dump_communities(partitions, output_file_name)
        
        filtered_partitions = get_bigger_communities(partitions, min_size=1)
        nmis, adj_nmis = eval_stability(filtered_partitions)
        dump_statistics(file.name, statistics=[np.mean(nmis).item(), np.mean(adj_nmis).item()], output_path=statistics_output_file)
        


