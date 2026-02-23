import pickle
import os
import re
import numpy as np
import json
import matplotlib.pyplot as plt
from matplotlib.path import Path
import matplotlib.patches as patches
from functools import reduce 
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
    """Loads label where the key is the OFFSET/RANK in the filtered list."""
    # Match the logic: file.stem.replace("topic_distribution", "community_labelling")
    filename = f"community_labelling_{start}_{end}.json"
    file_path = os.path.join(statistics_out_basedir, filename)
    
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as f:
                labels_dict = json.load(f)
                # Key is the string of the index/rank
                comm_labels = labels_dict.get(str(rank_idx), [])
                if comm_labels and len(comm_labels) > 0:
                    return str(comm_labels[0])
        except Exception:
            pass
    return f"Rank {rank_idx}"

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
    patch = patches.PathPatch(path, facecolor=color, edgecolor='none', alpha=0.25)
    ax.add_patch(patch)

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    all_communities = load_communities(community_pickle_directory)
    flow_communities = {}
    
    # Pre-process: Filter and Rank
    for start, end in community_time_intervals:
        key = f"{start}-{end}"
        raw_list = all_communities[key]
        sizes = [len(c) for c in raw_list]
        threshold = np.percentile(sizes, flow_percentile)
        
        # We MUST sort descending by size BEFORE assigning the index
        # to match the rank-based labeling in your JSONs
        filtered_comms = [c for c in raw_list if len(c) >= threshold]
        flow_communities[key] = sorted(filtered_comms, key=len, reverse=True)

    colors = plt.cm.tab20.colors 
    fig, ax = plt.subplots(figsize=(24, 14)) # Wider for long text labels
    ax.set_xlim(-0.7, len(community_time_intervals) + 0.5)
    ax.set_ylim(-0.1, 1.1)
    ax.axis('off')

    v_gap = 0.025
    pos_map = {} 

    # 1. Draw Nodes & use RANK as the label lookup key
    for t_idx, (start, end) in enumerate(community_time_intervals):
        key = f"{start}-{end}"
        comms = flow_communities[key] 
        total_authors = sum(len(c) for c in comms)
        
        current_y = 1.0
        for rank, members in enumerate(comms):
            height = (len(members) / total_authors) * (1.0 - (len(comms) * v_gap))
            y_top = current_y
            y_bottom = current_y - height
            
            node_color = colors[rank % len(colors)]
            rect = patches.Rectangle((t_idx - 0.05, y_bottom), 0.1, height, 
                                     color=node_color, alpha=0.85, zorder=3)
            ax.add_patch(rect)
            
            # Use 'rank' as the ID because labeling script used enumerate()
            label_str = get_label_for_community(start, end, rank)
            
            ax.text(t_idx + 0.07, (y_top + y_bottom)/2, label_str, 
                    ha='left', va='center', fontsize=7, color='black', 
                    linespacing=1.1, zorder=4)
            
            pos_map[(t_idx, rank)] = (y_top, y_bottom, node_color)
            current_y = y_bottom - v_gap
        
        ax.text(t_idx, -0.06, f"{start}-{end}", ha='center', fontweight='bold', fontsize=12)

    # 2. Draw Ribbons
    for t_idx in range(len(community_time_intervals) - 1):
        key_curr = f"{community_time_intervals[t_idx][0]}-{community_time_intervals[t_idx][1]}"
        key_next = f"{community_time_intervals[t_idx+1][0]}-{community_time_intervals[t_idx+1][1]}"
        
        comms_curr = flow_communities[key_curr]
        comms_next = flow_communities[key_next]
        
        curr_offset = {i: pos_map[(t_idx, i)][0] for i in range(len(comms_curr))}
        next_offset = {j: pos_map[(t_idx+1, j)][0] for j in range(len(comms_next))}

        for i, members_curr in enumerate(comms_curr):
            y_top_c, y_bot_c, flow_color = pos_map[(t_idx, i)]
            
            for j, members_next in enumerate(comms_next):
                overlap_count = len(set(members_curr).intersection(set(members_next)))
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
    plt.title("Semantic Evolution of Communities", fontsize=18, pad=40)
    plt.savefig(pdf_out, bbox_inches='tight', dpi=300)
    plt.close()
    print(f"Sankey plot saved to: {pdf_out}")