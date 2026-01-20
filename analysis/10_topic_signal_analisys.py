import pandas as pd
import matplotlib.pyplot as plt
import tomllib
import sys, os
import json
import numpy as np
import copy
from pathlib import Path


toml_config_path = sys.argv[1] if len(sys.argv) > 1 else "default.toml"
print(f"Parsing {toml_config_path} configuration file")

with open(toml_config_path, "rb") as f:
    cfg = tomllib.load(f)

try:
    analyzed_country = cfg["analized_country_full"]
    base_dir = Path(cfg["statistics_out_basedir"])

    communities_folder = (
        Path(cfg["workflow_data"])
        / cfg["country"]
        / cfg["community_extraction"]["outputs"]["communities_folder"]
    )

except KeyError as e:
    raise RuntimeError(f"Missing config key: {e}")

communities_folder = Path(communities_folder)

topics = set()

# ---------------------------
# First pass: collect topics
# ---------------------------
for file in communities_folder.iterdir():
    if file.suffix != ".json":
        continue

    print(f"Loading topics from file {file}")
    with open(file, "r") as f:
        data = json.load(f)

    for community_id in data.values():
        topics.update(community_id.keys())

print(f"Loaded {len(topics)} unique topics")
topics = sorted(topics)

topic_map = {topic: idx for idx, topic in enumerate(topics)}

# ---------------------------
# Second pass: build signals
# ---------------------------
for file in communities_folder.iterdir():
    if file.suffix != ".json":
        continue

    print(f"Loading signals from file {file}")
    signals = []
    signals_old = []

    with open(file, "r") as f:
        data = json.load(f)

    for community in data.values():
        community_signal = [0] * len(topics)
        for key, value in community.items():
            community_signal[topic_map[key]] = value
        signals.append(community_signal)

    if not signals:
        continue

    # --------------------------------
    # Compute common (min) signal
    # --------------------------------
    min_signal_array = []

    for i in range(len(topics)):
        min_signal = min(signal[i] for signal in signals)
        min_signal_array.append(min_signal)
        
    threshold_array = [
        np.percentile([signal[i] for signal in signals], 99)
        for i in range(len(topics))
    ]
    
    signals_old = copy.deepcopy(signals)
    
    # subtract baseline
    if len(signals) > 1:
        for i in range(len(signals)):
            signals[i] = [
                max(0, signals[i][j] - min_signal_array[j] - threshold_array[j])
                for j in range(len(topics))
            ]

    # ---------------------------
    # Plot
    # ---------------------------

    print(f"Plotting {len(signals)} signals for community: {file.name}")
    fig, ax = plt.subplots(1, 1, figsize=(20, 10))

    #ax[0].set_title(f"Original: {file.stem}")
    #ax[1].set_title(f"Cleaned: {file.stem}")
    ax.set_title(file.stem)
    
    signal_best_candidate = []

    # Plot on ax[0] and collect labels
    for i, signal in enumerate(signals_old):
        best_label = topics[signal.index(max(signal))]
        signal_best_candidate.append(f"{i}: {best_label}")
        #ax[0].plot(range(len(signal)), signal, label=best_label)
        ax.plot(signal, label=f"{i}: {best_label}")
        
    # Plot on ax[1] (optional: same labels if you want)
    #for signal in signals:
    #    ax[1].plot(signal)

    #ax[0].grid()
    #ax[1].grid()
    ax.grid()

    fig.legend(bbox_to_anchor=(0.5, 1.1), loc="lower center",ncol=min(6, len(signal_best_candidate)), fontsize=12)
    fig.tight_layout()
    
    output_filename = base_dir / (file.stem.replace("topic_distribution", "community_labelling") + ".pdf")
    fig.savefig(output_filename, bbox_inches="tight")  # ensure everything fits
    plt.close(fig)

