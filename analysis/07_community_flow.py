import pickle
import os
import re
import numpy as np
import csv
import glob
import json
import alive_progress
import matplotlib.pyplot as plt
from functools import reduce 
import tomllib
import sys


toml_config_path = sys.argv[1] if len(sys.argv) > 1 else "default.toml"
print("Parsing {} configuration file".format(toml_config_path))
with open(toml_config_path, 'rb') as f:
    configuration = tomllib.load(f)

try:
    statistics_out_basedir          = configuration["statistics_out_basedir"]
    display_sink_community          = configuration["community_flow"]["display_sink_community"]
    size_statistics_path            = configuration["statistics_out_basedir"] + "/" + configuration["community_flow"]["outputs"]["size_statistics_path"]
    quantiles                       = configuration["community_flow"]["quantiles"]
    flow_percentile                 = configuration["community_flow"]["flow_percentile"]
    dataset_metadata_file_path      = configuration["workflow_data"] + "/" + configuration["country"] + "/" + configuration["metadata_analisys"]["inputs"]["metadata_path"]
    graph_paths                     = configuration["workflow_data"] + "/" + configuration["country"] + "/" + configuration["metadata_analisys"]["inputs"]["graph_directory"]
    community_pickle_directory      = configuration["workflow_data"] + "/" + configuration["country"] + "/" + configuration["community_stability"]["outputs"]["communities_output_folder"]
    comm_labels_out_path            = configuration["workflow_data"] + "/" + configuration["country"] + "/" + configuration["community_extraction"]["outputs"]["communities_folder"]
except Exception as e:
    print("Error: key {} not found".format(e))
    exit(-1)

community_time_intervals = []
try:
    for interval in configuration["time_intervals"]:
        community_time_intervals.append((interval[0], interval[1]))
except KeyError :
    exit(-1)
    
print(f"\n{'=' * 60}")
print(f"{' COMMUNITY FLOW CONFIGURATION SUMMARY '.center(60, ' ')}")
print(f"{'=' * 60}")

print(f"\n[INPUTS]")
print(f"  Dataset Metadata:     {dataset_metadata_file_path}")
print(f"  Graphs Directory:     {graph_paths}")
print(f"  Communities Pickle:   {community_pickle_directory}")

print(f"\n[SETTINGS]")
print(f"  Display Sink Comm.:   {display_sink_community}")
print(f"  Flow Percentile:      {flow_percentile}")
print(f"  Quantiles:            {quantiles}")

print(f"\n[OUTPUT FILES]")
print(f"  Size Statistics Path: {size_statistics_path}")
print(f"  Community Labels Out: {comm_labels_out_path}")

print(f"{'=' * 60}\n")

    

def load_works(start_year, end_year, graph_paths):
    works = dict()
    end_year = end_year if end_year != "*" else ""
    tmp = glob.glob(f"{graph_paths}/{start_year}_{end_year}*.csv")
    if len(tmp) == 0:
        print(f"No graph found for time interval starting with {start_year}, skipping...")
        return

    community_graph_file_path = tmp[0]
    
    # Load the graph source file to find works associated with the community
    with open(community_graph_file_path, "r") as f:
        next(f)
        for line in f:
            parts = line.strip().split(",")
            work_id = parts[1]
            author1 = parts[2]
            author2 = parts[3]
            
            first_author = author1 if author1 < author2 else author2
            second_author = author2 if author1 < author2 else author1
  
            
            if first_author not in works:
                works[first_author] = dict()
            
            works[first_author][second_author] = work_id 
    print("Loaded works for community.")
    return works

def get_works_from_community(community, works):
    community_works = set()
    for author1 in community:
        for author2 in community:
            first_author = author1 if author1 < author2 else author2
            second_author = author2 if author1 < author2 else author1
            if first_author != second_author and first_author in works and second_author in works[first_author]:
                community_works.add(works[first_author][second_author])
                
    return community_works

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


def load_communities(community_directory: str) -> dict:
        
    loaded_communities = {}
    pattern = re.compile(r'_(\d{4})?_(\d{4})?_')
    print(f"Loading communities from directory: {community_directory}")
    for file in os.listdir(community_directory):
        if file.endswith(".pkl"):
            with open(os.path.join(community_directory, file), "rb") as f:
                match = pattern.search(file)
                if match:
                    start_year, end_year = match.groups()
                    key = f"{start_year if start_year else '*'}-{end_year if end_year else '*'}"
                    loaded_data = pickle.load(f)
                    # if loading communities from stability computatino get the first computed communities, otherwise load directly the data
                    if isinstance(loaded_data, list):
                        loaded_communities[key] = loaded_data[0]
                    else:
                        loaded_communities[key] = loaded_data
                    print(f"\t {key}: {len(loaded_communities[key])} communities.")
    print("All communities loaded.")
    return loaded_communities

def community_size_distribution(communities: dict, quantiles: list, output_file: str) -> dict:
    print("")
    sorted_keys = sorted(communities.keys())

    year_col_width = 8
    value_col_width = 10

    print("Community size distributions:")

    with open(output_file, "w") as f:
        # header: include an empty year cell
        writer = csv.writer(f)
        writer.writerow(["year"] + quantiles)

        print(
            f"{'':<{year_col_width}}"
            + "".join(f"{q:>{value_col_width}.0f} " for q in quantiles)
        )
        
        for year in sorted_keys:
            sizes = [len(c) for c in communities[year]]
            qs = np.percentile(sizes, quantiles)
            writer.writerow([year] + qs.tolist())
            print(
                f"{year:<{year_col_width}}"
                + "".join(f"{q:>{value_col_width}.2f} " for q in qs)
            )
    print("\n")

def get_commununity_over_percentile(community: dict, start_year: str, end_year: str, percentile: int = 99) -> dict:
    filtered_communities = {}
    sink_communities = {}
    sizes = [len(c) for c in community]
    threshold = np.percentile(sizes, percentile)
    filtered_communities = [c for c in community if len(c) >= threshold]
    sink_communities = [c for c in community if len(c) < threshold]
    print(f"Number of communities over {percentile}th percentile: {start_year}-{end_year} ~ {len(filtered_communities)}")
    
    return filtered_communities, sink_communities

def find_overlap(comm1, comm2, normalized=False):
    if normalized:
        return len(set(comm1).intersection(set(comm2))) / len(set(comm1)) #len(set(comm1).union(set(comm2)))

    return len(set(comm1).intersection(set(comm2)))

if __name__ == "__main__":
    communities = load_communities(community_pickle_directory)
    community_size_distribution(communities, quantiles, size_statistics_path)
    
    flow_communities = dict()
    
    for start_year, end_year in community_time_intervals:
        community = communities[f"{start_year}-{end_year}"]
    
        percentile_communities, sink_communities = get_commununity_over_percentile(community, start_year, end_year, percentile=flow_percentile)
        communities_works={}
        loaded_works = load_works(start_year, end_year, graph_paths)
        with alive_progress.alive_bar(len(percentile_communities), title=f"Processing community for dataset starting at {start_year}") as bar:
            for community_id, community_authors in enumerate(percentile_communities):
                works = get_works_from_community(community_authors, loaded_works)
                communities_works[community_id] = match_community_works_to_topics(works, dataset_metadata_file_path)
                bar()
            
        output_file = f"{comm_labels_out_path}/topic_distribution_{start_year}_{end_year}.json".replace("*", "")
        json.dump(communities_works, open(output_file, "w"))
        print(f"Community labels dumped to: {output_file}\n")

        if display_sink_community and len(sink_communities) > 0:
            flow_communities[f"{start_year}-{end_year}"] = percentile_communities + [reduce(set.union, sink_communities, set())]
        else:
            flow_communities[f"{start_year}-{end_year}"] = percentile_communities

    # Flow analysis
    migration_matrices = {}
    sorted_before_data_dict = {}
    sorted_during_data_dict = {}
    lost_nodes_dict = {}
    lost_nodes_global_dict = {}

    for i in range(0, len(community_time_intervals)-1):
        year_before = f"{community_time_intervals[i][0]}-{community_time_intervals[i][1]}"
        year_during = f"{community_time_intervals[i+1][0]}-{community_time_intervals[i+1][1]}"

        print(f"Analyzing migration from {year_before} to {year_during}")

        before_comms_data = [(idx, comm) for idx, comm in enumerate(flow_communities[year_before])]
        during_comms_data = [(idx, comm) for idx, comm in enumerate(flow_communities[year_during])]

        sorted_before_comms_data = sorted(before_comms_data, key=lambda x: len(x[1]), reverse=True)
        sorted_during_comms_data = sorted(during_comms_data, key=lambda x: len(x[1]), reverse=True)

        migration_matrices[year_during] = np.zeros((len(sorted_before_comms_data), len(sorted_during_comms_data)))

        sorted_before_data_dict[year_before] = sorted_before_comms_data
        sorted_during_data_dict[year_during] = sorted_during_comms_data
        
        for i in range(0, len(sorted_before_comms_data)):
            for j in range(0, len(sorted_during_comms_data)):
                migration_matrices[year_during][i][j] = find_overlap(
                                                    sorted_before_comms_data[i][1], 
                                                    sorted_during_comms_data[j][1], 
                                                    normalized=True
                                                )

        #Compute lost nodes relative to filtered communities
        all_authors_before = set()
        for _, comm in sorted_before_comms_data:
            all_authors_before.update(comm)
        all_authors_during = set()
        for _, comm in sorted_during_comms_data:
            all_authors_during.update(comm)

        lost_nodes = all_authors_before.difference(all_authors_during)
        lost_nodes_dict[year_during] = len(lost_nodes) / len(all_authors_before) * 100
            
        #compute lost nodes relative to the complete communities (i.e. not filtered)
        all_non_filtered_authors_before = set()
        for comm in flow_communities[year_before]:
            all_non_filtered_authors_before.update(comm)
        all_non_filtered_authors_during = set()
        for comm in flow_communities[year_during]:
            all_non_filtered_authors_during.update(comm)    
        
        lost_nodes_unfiltered = all_non_filtered_authors_before.difference(all_non_filtered_authors_during)
        lost_nodes_global_dict[year_during] = len(lost_nodes_unfiltered) / len(all_non_filtered_authors_before) * 100
        
        print(f"\t\tLost nodes from {year_before} to {year_during}: {lost_nodes_dict[year_during]:.2f}% (filtered) / {lost_nodes_global_dict[year_during]:.2f}% (global)")

        # Plot migration heatmap
        
        n_rows = len(sorted_before_data_dict[year_before])
        n_cols = len(sorted_during_data_dict[year_during])

        # Scale figure with matrix size
        fig_width  = max(6, 0.6 * n_cols)
        fig_height = max(6, 0.6 * n_rows)

        fig, ax = plt.subplots(
            figsize=(fig_width, fig_height),
            dpi=200,
            constrained_layout=True
        )

        text_colors = {True: 'white', False: 'black'}

        im = ax.imshow(
            migration_matrices[year_during],
            cmap='Blues',
            aspect='auto',
            vmin=0,
            vmax=1
        )

        # Axis labels
        ax.xaxis.set_label_position('top')
        ax.xaxis.tick_top()
        ax.set_ylabel(f'Communities in range {year_before}')
        ax.set_xlabel(f'Communities in range {year_during}')
        
        # Tick positions
        xticks = np.arange(n_cols)
        yticks = np.arange(n_rows)

        ax.set_xticks(xticks)
        ax.set_yticks(yticks)

        # Tick labels
        xlabels = [str(i) for i in range(n_cols)]
        xlabels[-1] = "Other"

        ylabels = [str(i) for i in range(n_rows)]
        ylabels[-1] = "Other"

        ax.set_xticklabels(xlabels)
        ax.set_yticklabels(ylabels)

        # Adaptive font size
        font_size = max(8, 14 - max(n_rows, n_cols) // 2)

        for i in range(n_rows):
            for j in range(n_cols):
                val = round(migration_matrices[year_during][i, j] * 100, 2)
                ax.text(
                    j, i, f"{val}%",
                    ha="center",
                    va="center",
                    fontsize=font_size,
                    color=text_colors[val > 49]
                )

        # Ticks
        ax.set_yticks(np.arange(n_rows))
        ax.set_xticks(np.arange(n_cols))

        # Rotate x labels if needed
        if n_cols > 10:
            plt.setp(ax.get_xticklabels(), rotation=45, ha="left")

        fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)

        # Save
        out = f"{statistics_out_basedir}/community_migration_{year_before}_to_{year_during}.pdf"
        plt.savefig(out, bbox_inches="tight")
        plt.close()

        print(f"Migration heatmap saved to {out}\n")
        
    