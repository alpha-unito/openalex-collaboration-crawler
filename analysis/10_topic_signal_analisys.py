import pandas as pd
import matplotlib.pyplot as plt
import tomllib
import sys, os
import json
import numpy as np
from pathlib import Path

from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

def extract_common_thread(topics):
    
    if len(topics) == 0:
        return ""
    
    # 1. Load a pre-trained model that understands CS/Technical language
    # 'all-MiniLM-L6-v2' is fast and very accurate for short phrases
    model = SentenceTransformer('all-MiniLM-L6-v2')

    # 2. Encode the topics into semantic vectors (embeddings)
    embeddings = model.encode(topics)

    # 3. Calculate the "Centroid" (The mathematical average of all topics)
    centroid = np.mean(embeddings, axis=0).reshape(1, -1)

    # 4. Find which original topics are closest to the "Mean" of the group
    similarities = cosine_similarity(embeddings, centroid)
    
    # 5. Rank topics by how well they represent the group
    ranked_indices = np.argsort(similarities.flatten())[::-1]
    
    if len(ranked_indices) > 1:
        return "(" + topics[ranked_indices[0]] + ": " + topics[ranked_indices[1]] + ")"
    
    return "(" + topics[ranked_indices[0]] + ")"



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

    ax.set_title(file.stem)
    
    signal_best_candidate = []

    for i, signal in enumerate(signals):
        
        signal_level_treshold = max(signal)
        general_topic = topics[signal.index(max(signal))]
        
        signal_level_treshold -= signal_level_treshold / 100 * 5 # lower by 5% the treshold to allow more information
        
        keywords = [topics[index] for index, count in enumerate(signal) if count >= signal_level_treshold ]
        
        keywords.remove(general_topic)
        
        keyword = extract_common_thread(keywords)
        
        label = f"{i}: {general_topic}{keyword}"
        
        signal_best_candidate.append(label)
        
        ax.plot(signal, label=label)
        
    ax.grid()

    fig.legend(bbox_to_anchor=(0.5, 1.1), loc="lower center",ncol=min(5, len(signal_best_candidate)), fontsize=12)
    fig.tight_layout()
    
    output_filename = base_dir / (file.stem.replace("topic_distribution", "community_labelling") + ".pdf")
    fig.savefig(output_filename, bbox_inches="tight")  # ensure everything fits
    plt.close(fig)

