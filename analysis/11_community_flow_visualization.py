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
                    selected = row.get("selected", row.get("selected_name", "")).strip()
                    choices = [row.get("generated_name", ""), *(row[f"candidate_{i}"] for i in range(1, 4))]
                    if selected in {"1", "2", "3", "4"}:
                        return choices[int(selected) - 1] or row["candidate_1"]
                    return selected or row.get("generated_name", "") or row["candidate_1"]
    except FileNotFoundError:
        pass
    return f"Rank {rank_idx}"


def distinct_color(index):
    hue = (index * 0.618033988749895) % 1
    return colorsys.hsv_to_rgb(hue, 0.65, 0.85)


def has_flow(interval_idx, members, communities):
    return flow_volume(interval_idx, members, communities) > 0


def flow_volume(interval_idx, members, communities):
    neighbours = []
    if interval_idx > 0:
        neighbours.extend(communities[interval_idx - 1])
    if interval_idx + 1 < len(communities):
        neighbours.extend(communities[interval_idx + 1])
    return sum(flow_size(members, other_members) for _, other_members in neighbours)


def flow_size(source, target):
    overlap = len(source & target)
    return overlap if overlap >= max(3, round(min(len(source), len(target)) * 0.02)) else 0

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
    patch = patches.PathPatch(path, facecolor=color, edgecolor='none', alpha=0.55, zorder=1)
    ax.add_patch(patch)

    if min(y1_start - y1_end, y2_start - y2_end) < 0.006:
        middle = (x1 + x2) / 2
        center1 = (y1_start + y1_end) / 2
        center2 = (y2_start + y2_end) / 2
        centerline = Path(
            [(x1, center1), (middle, center1), (middle, center2), (x2, center2)],
            [Path.MOVETO, Path.CURVE4, Path.CURVE4, Path.CURVE4],
        )
        ax.add_patch(patches.PathPatch(
            centerline, facecolor="none", edgecolor=color, linewidth=0.9, alpha=0.9, zorder=2
        ))

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
            (rank, members)
            for rank, members in sorted(filtered_comms, key=lambda item: len(item[1]), reverse=True)
        ]
        flow_communities.append(communities)

    flow_communities = [
        [community for community in communities if has_flow(i, community[1], flow_communities)]
        for i, communities in enumerate(flow_communities)
    ]
    # ponytail: cap static columns at 12; raise this only if the output becomes interactive.
    flow_communities = [
        sorted(communities, key=lambda community: flow_volume(i, community[1], flow_communities), reverse=True)[:12]
        for i, communities in enumerate(flow_communities)
    ]
    flow_communities = [
        [community for community in communities if has_flow(i, community[1], flow_communities)]
        for i, communities in enumerate(flow_communities)
    ]

    # Place each column near the communities feeding it to reduce crossings.
    for i in range(1, len(flow_communities)):
        previous = flow_communities[i - 1]
        def barycenter(community):
            weights = [flow_size(community[1], members) for _, members in previous]
            return sum(index * weight for index, weight in enumerate(weights)) / sum(weights) if sum(weights) else len(previous)
        flow_communities[i].sort(key=barycenter)

    x_positions = [index * 1.4 for index in range(len(community_time_intervals))]
    fig, ax = plt.subplots(figsize=(36, 18))
    ax.set_xlim(-0.4, x_positions[-1] + 1.5)
    ax.set_ylim(-0.1, 1.1)
    ax.axis('off')

    v_gap = 0.018
    author_scales = [
        (0.96 - (len(communities) - 1) * v_gap) / sum(len(members) for _, members in communities)
        for communities in flow_communities
    ]
    pos_map = {} 

    # 1. Draw Nodes & use RANK as the label lookup key
    for t_idx, (start, end) in enumerate(community_time_intervals):
        comms = flow_communities[t_idx]
        column_height = sum(len(members) * author_scales[t_idx] for _, members in comms) + (len(comms) - 1) * v_gap
        current_y = (1.0 + column_height) / 2
        for display_idx, (rank, members) in enumerate(comms):
            height = len(members) * author_scales[t_idx]
            y_top = current_y
            y_bottom = current_y - height
            
            node_color = distinct_color(sum(len(c) for c in flow_communities[:t_idx]) + display_idx)
            x = x_positions[t_idx]
            rect = patches.Rectangle((x - 0.04, y_bottom), 0.08, height,
                                     facecolor=node_color, edgecolor="white", linewidth=0.5, zorder=3)
            ax.add_patch(rect)
            
            label_str = get_label_for_community(start, end, rank)
            
            ax.text(x + 0.06, (y_top + y_bottom)/2, label_str,
                    ha='left', va='center', fontsize=10, color='black',
                    linespacing=1.1, zorder=4,
                    bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.8, "pad": 0.5})
            
            pos_map[(t_idx, display_idx)] = (y_top, y_bottom, node_color)
            current_y = y_bottom - v_gap
        
        ax.text(x_positions[t_idx], -0.06, f"{start}-{end}", ha='center', fontweight='bold', fontsize=15)

    # 2. Draw Ribbons
    for t_idx in range(len(community_time_intervals) - 1):
        comms_curr = flow_communities[t_idx]
        comms_next = flow_communities[t_idx + 1]
        
        curr_offset = {i: pos_map[(t_idx, i)][0] for i in range(len(comms_curr))}
        next_offset = {j: pos_map[(t_idx+1, j)][0] for j in range(len(comms_next))}

        for i, (_, members_curr) in enumerate(comms_curr):
            y_top_c, y_bot_c, flow_color = pos_map[(t_idx, i)]
            
            for j, (_, members_next) in enumerate(comms_next):
                overlap_count = flow_size(members_curr, members_next)
                if overlap_count > 0:
                    y_top_n, y_bot_n, _ = pos_map[(t_idx+1, j)]
                    
                    source_height = overlap_count * author_scales[t_idx]
                    target_height = overlap_count * author_scales[t_idx + 1]
                    
                    draw_sankey_flow(ax, x_positions[t_idx] + 0.04, x_positions[t_idx + 1] - 0.04,
                                     curr_offset[i], curr_offset[i] - source_height,
                                     next_offset[j], next_offset[j] - target_height,
                                     color=flow_color)
                    
                    curr_offset[i] -= source_height
                    next_offset[j] -= target_height

    pdf_out = os.path.join(statistics_out_basedir, "community_flow_visualization.pdf")
    plt.title("Semantic Evolution of Communities", fontsize=22, pad=40)
    plt.savefig(pdf_out, bbox_inches='tight', dpi=300)
    plt.close()
    print(f"Sankey plot saved to: {pdf_out}")
