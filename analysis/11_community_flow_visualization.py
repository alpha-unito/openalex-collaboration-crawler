import pickle
import os
import re
import csv
import colorsys
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.path import Path
import matplotlib.patches as patches
import tomllib
import sys

# --- CONFIGURATION LOADING ---
toml_config_path = sys.argv[1] if len(sys.argv) > 1 else "default.toml"
print(f"Parsing {toml_config_path} configuration file")
with open(toml_config_path, 'rb') as f:
    configuration = tomllib.load(f)

try:
    statistics_out_basedir          = configuration["statistics_out_basedir"]
    flow_percentile                 = configuration["community_flow"]["flow_percentile"]
    display_other                   = configuration["community_flow"]["display_sink_community"]
    community_pickle_directory      = os.path.join(configuration["workflow_data"], configuration["country"], configuration["community_stability"]["outputs"]["communities_output_folder"])
except Exception as e:
    print(f"Error: key {e} not found")
    exit(-1)

community_time_intervals = [tuple(interval) for interval in configuration.get("time_intervals", [])]
if not community_time_intervals:
    exit(-1)

# --- HELPER FUNCTIONS ---
def load_communities(community_directory: str) -> dict:
    loaded_communities = {}
    pattern = re.compile(r'_(\d{4})?_(\d{4})?_')
    for file in os.listdir(community_directory):
        if file.endswith(".pkl"):
            with open(os.path.join(community_directory, file), "rb") as f:
                match = pattern.search(file)
                if match:
                    start_year, end_year = match.groups()
                    key = f"{start_year if start_year else '*'}-{end_year if end_year else '*'}"
                    data = pickle.load(f)
                    loaded_communities[key] = data[0] if isinstance(data, list) else data
    return loaded_communities

def get_label_for_community(start, end, rank_idx):
    filename = f"community_names_{start}_{end}.csv"
    file_path = os.path.join(statistics_out_basedir, filename)

    try:
        with open(file_path, newline="") as f:
            for row in csv.DictReader(f):
                if row["community_id"] == str(rank_idx):
                    return row.get("selected_name", "").strip() or row["candidate_1"]
    except FileNotFoundError:
        pass
    return f"Rank {rank_idx}"


def distinct_color(index):
    hue = (index * 0.618033988749895) % 1
    return colorsys.hsv_to_rgb(hue, 0.65, 0.85)


def has_flow(interval_idx, members, communities):
    neighbours = []
    if interval_idx > 0:
        neighbours.extend(communities[interval_idx - 1])
    if interval_idx + 1 < len(communities):
        neighbours.extend(communities[interval_idx + 1])
    return any(not members.isdisjoint(other_members) for _, other_members, _ in neighbours)

def draw_sankey_flow(ax, x1, x2, y1_start, y1_end, y2_start, y2_end, color):
    verts = [
        (x1, y1_start), (x1 + (x2 - x1) / 2, y1_start),
        (x1 + (x2 - x1) / 2, y2_start), (x2, y2_start),
        (x2, y2_end), (x1 + (x2 - x1) / 2, y2_end),
        (x1 + (x2 - x1) / 2, y1_end), (x1, y1_end),
        (x1, y1_start)
    ]
    codes = [Path.MOVETO, Path.CURVE4, Path.CURVE4, Path.CURVE4, 
             Path.LINETO, Path.CURVE4, Path.CURVE4, Path.CURVE4, Path.CLOSEPOLY]
    path = Path(verts, codes)
    patch = patches.PathPatch(path, facecolor=color, edgecolor='none')
    ax.add_patch(patch)

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    all_communities = load_communities(community_pickle_directory)
    flow_communities = []
    
    # Pre-process: Filter and Rank
    for start, end in community_time_intervals:
        key = f"{start}-{end}"
        raw_list = all_communities[key]
        sizes = [len(c) for c in raw_list]
        threshold = np.percentile(sizes, flow_percentile)
        
        # Keep the original ID while sorting nodes by size for display.
        filtered_comms = enumerate(set(c) for c in raw_list if len(c) >= threshold)
        communities = [
            (rank, members, False)
            for rank, members in sorted(filtered_comms, key=lambda item: len(item[1]), reverse=True)
        ]
        if display_other:
            other = set().union(*(set(c) for c in raw_list if len(c) < threshold))
            if other:
                communities.append((None, other, True))
        flow_communities.append(communities)

    flow_communities = [
        [community for community in communities if has_flow(i, community[1], flow_communities)]
        for i, communities in enumerate(flow_communities)
    ]

    fig, ax = plt.subplots(figsize=(24, 14)) # Wider for long text labels
    ax.set_xlim(-0.7, len(community_time_intervals) + 0.5)
    ax.set_ylim(-0.1, 1.1)
    ax.axis('off')

    v_gap = 0.025
    pos_map = {} 

    # 1. Draw Nodes & use RANK as the label lookup key
    for t_idx, (start, end) in enumerate(community_time_intervals):
        comms = flow_communities[t_idx]
        total_authors = sum(len(members) for _, members, _ in comms)
        
        current_y = 1.0
        for display_idx, (rank, members, is_other) in enumerate(comms):
            height = (len(members) / total_authors) * (1.0 - (len(comms) * v_gap))
            y_top = current_y
            y_bottom = current_y - height
            
            node_color = distinct_color(sum(len(c) for c in flow_communities[:t_idx]) + display_idx)
            rect = patches.Rectangle((t_idx - 0.05, y_bottom), 0.1, height, 
                                     color=node_color, zorder=3)
            ax.add_patch(rect)
            
            label_str = "Other" if is_other else get_label_for_community(start, end, rank)
            
            ax.text(t_idx + 0.07, (y_top + y_bottom)/2, label_str, 
                    ha='left', va='center', fontsize=11, color='black',
                    linespacing=1.1, zorder=4)
            
            pos_map[(t_idx, display_idx)] = (y_top, y_bottom, node_color)
            current_y = y_bottom - v_gap
        
        ax.text(t_idx, -0.06, f"{start}-{end}", ha='center', fontweight='bold', fontsize=15)

    # 2. Draw Ribbons
    for t_idx in range(len(community_time_intervals) - 1):
        comms_curr = flow_communities[t_idx]
        comms_next = flow_communities[t_idx + 1]
        
        curr_offset = {i: pos_map[(t_idx, i)][0] for i in range(len(comms_curr))}
        next_offset = {j: pos_map[(t_idx+1, j)][0] for j in range(len(comms_next))}

        for i, (_, members_curr, _) in enumerate(comms_curr):
            y_top_c, y_bot_c, flow_color = pos_map[(t_idx, i)]
            
            for j, (_, members_next, _) in enumerate(comms_next):
                overlap_count = len(members_curr & members_next)
                if overlap_count > 0:
                    y_top_n, y_bot_n, _ = pos_map[(t_idx+1, j)]
                    
                    h_curr = (overlap_count / len(members_curr)) * (y_top_c - y_bot_c)
                    h_next = (overlap_count / len(members_next)) * (y_top_n - y_bot_n)
                    
                    draw_sankey_flow(ax, t_idx + 0.05, t_idx + 1 - 0.05, 
                                     curr_offset[i], curr_offset[i] - h_curr,
                                     next_offset[j], next_offset[j] - h_next,
                                     color=flow_color)
                    
                    curr_offset[i] -= h_curr
                    next_offset[j] -= h_next

    pdf_out = os.path.join(statistics_out_basedir, "community_flow_visualization.pdf")
    plt.title("Semantic Evolution of Communities", fontsize=22, pad=40)
    plt.savefig(pdf_out, bbox_inches='tight', dpi=300)
    plt.close()
    print(f"Sankey plot saved to: {pdf_out}")
