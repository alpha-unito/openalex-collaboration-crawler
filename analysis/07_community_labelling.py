import json
import networkx as nx
import os, pathlib
import pickle
import glob
import alive_progress


# -----------------------------
# Configuration parameters
# -----------------------------

# Location of oiginal graphs, as returned by the papers extraction process (not the backbones nor the weighted versions)
graph_paths = "/beegfs/home/msantima/OpenAlexCollaborations/IT/"

# Directory containing the community pickle files generated during the stability analysis
community_pickle_directory = "/beegfs/home/msantima/OpenAlexCollaborations/IT/stability/"

# Time intervals for which to compute the community labels, as used in the graph generation phase
community_time_intervals = [("*", 1969),(1970, 1989), (1990, 1999), (2000, 2009), (2010, 2011), (2012, 2014), (2015, 2024), (2025, "*")]

# Input folder containing the backbone graph CSV files
dataset_metadata_file_path = "/beegfs/home/msantima/OpenAlexCollaborations/IT/metadata_dataset.csv"

# Output directory for the community labels
community_labels_output_directory = "/beegfs/home/msantima/OpenAlexCollaborations/IT/communities"

# -----------------------------
# END Configuration parameters
# -----------------------------

def get_works_from_community(community, graph_source_file):
    works = set()
    
    # Load the graph source file to find works associated with the community
    with open(graph_source_file, "r") as f:
        next(f)
        for line in f:
            parts = line.strip().split(",")
            work_id = parts[1]
            author1 = parts[2]
            author2 = parts[3]
            
            if author1 in community and author2 in community:
                works.add(work_id)
    
    return works


def load_backbone(backbone_path):
    
    print(f"Loading backbone from file: {backbone_path}")
    collab_graph = nx.Graph()
    with open(backbone_path, "r") as f:
        next(f)
        for line in f:
            source, target, weight, _ = line.strip().split(",")
            collab_graph.add_edge(source, target, weight=float(weight))
    print(
        "\tGraph has: ", collab_graph.number_of_nodes(), " nodes and ", collab_graph.number_of_edges(), " edges",
    )
    return collab_graph



def match_community_works_to_topics(community_works, metadata_file):
    topics = {}
    with open(metadata_file, "r") as f:
        for line in f:
            parts = line.strip().split(",")
            if parts[0] in community_works:
                topic = parts[3].split(";")
                for topic_item in topic:
                    if topic_item not in topics:
                        topics[topic_item] = 0
                    topics[topic_item] += 1
                    
    import mappings
    for topic in mappings.application_domains_to_delete:
            if topic in topics:
                del topics[topic]
           
    return dict(sorted(topics.items(), key=lambda item: item[1], reverse=True))


def compute_top_categories_for_communities(graph_paths, community_pickles_directory, start_year, end_year):
    
    tmp = glob.glob(f"{graph_paths}/{start_year}_{end_year}*.csv")
    if len(tmp) == 0:
        print(f"No graph found for time interval {start_year}-{end_year}, skipping...")
        return

    community_graph_file_path = tmp[0]
    
    tmp = glob.glob(f"{community_pickles_directory}/*{start_year}_{end_year}*.pkl")
    if len(tmp) == 0:
        print(f"No community pickle found for time interval {start_year}-{end_year}, skipping...")
        return
    community_pickle_path = tmp[0]
    communities_works={}
    
    #from the multiple communities pickle file, load the first run ot uf the several one generated dunring stability analysis
    communities = pickle.load(open(community_pickle_path, "rb"))[0]
    
    with alive_progress.alive_bar(len(communities), title=f"Processing community for dataset {start_year}-{end_year}") as bar:
            
        for community_id, community_authors in enumerate(communities):
            works = get_works_from_community(community_authors, community_graph_file_path)
            communities_works[community_id] = match_community_works_to_topics(works, dataset_metadata_file_path)
            bar()
        
    output_file = f"{community_labels_output_directory}/topic_distribution_{start_year}_{end_year}.json".replace("*", "")
    json.dump(communities_works, open(output_file, "w"))
    print(f"Community labels dumped to: {output_file}\n")
    
if __name__ == "__main__":
    
    for (start_year, end_year) in community_time_intervals:
        compute_top_categories_for_communities(graph_paths, community_pickle_directory, start_year, end_year)