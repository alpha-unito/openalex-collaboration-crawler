import sys
import json
import tomllib
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity


# ============================================================
# Global Model (loaded once for speed)
# ============================================================
MODEL = SentenceTransformer("all-MiniLM-L6-v2")


# ============================================================
# Utility Functions
# ============================================================
def extract_common_thread(topics: list[str]) -> str:
    if not topics:
        return ""

    embeddings = MODEL.encode(topics)
    centroid = embeddings.mean(axis=0, keepdims=True)
    similarities = cosine_similarity(embeddings, centroid).flatten()

    ranked = np.argsort(similarities)[::-1]

    if len(ranked) > 1:
        return f" ({topics[ranked[0]]}: {topics[ranked[1]]})"
    return f" ({topics[ranked[0]]})"


def load_config(path: str) -> dict:
    with open(path, "rb") as f:
        return tomllib.load(f)


def collect_topics(folder: Path) -> list[str]:
    topics = set()

    for file in sorted(folder.glob("*.json")):
        print(f"Loading topics from {file.name}")
        with open(file) as f:
            data = json.load(f)

        for community in data.values():
            topics.update(community.keys())

    topics = sorted(topics)
    print(f"Loaded {len(topics)} unique topics")
    return topics


def build_signal_matrix(data: dict, topic_map: dict) -> np.ndarray:
    signals = []

    for community in data.values():
        signal = np.zeros(len(topic_map))
        for key, value in community.items():
            signal[topic_map[key]] = value
        signals.append(signal)

    if not signals:
        return np.empty((0, len(topic_map)))

    return np.vstack(signals)


# ============================================================
# Plot Styling
# ============================================================
def setup_plot_style():
    plt.style.use("seaborn-v0_8-whitegrid")

    plt.rcParams.update({
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.edgecolor": "#333333",
        "axes.linewidth": 1.2,
        "grid.linestyle": "--",
        "grid.alpha": 0.6,
    })


# ============================================================
# Main
# ============================================================
def main():
    setup_plot_style()

    config_path = sys.argv[1] if len(sys.argv) > 1 else "default.toml"
    print(f"Parsing configuration: {config_path}")

    cfg = load_config(config_path)

    try:
        base_dir = Path(cfg["statistics_out_basedir"])
        communities_folder = (
            Path(cfg["workflow_data"])
            / cfg["country"]
            / cfg["community_extraction"]["outputs"]["communities_folder"]
        )
    except KeyError as e:
        raise RuntimeError(f"Missing config key: {e}")

    base_dir.mkdir(parents=True, exist_ok=True)

    topics = collect_topics(communities_folder)
    topic_map = {topic: idx for idx, topic in enumerate(topics)}

    # ========================================================
    # Process each JSON file
    # ========================================================
    for file in sorted(communities_folder.glob("*.json")):
        print(f"Processing {file.name}")

        with open(file) as f:
            data = json.load(f)

        signals = build_signal_matrix(data, topic_map)

        if signals.size == 0:
            continue

        # ----------------------------------------------------
        # Baseline subtraction
        # ----------------------------------------------------
        min_signal = signals.min(axis=0)
        threshold = np.percentile(signals, 99, axis=0)

        if signals.shape[0] > 1:
            signals = np.maximum(0, signals - min_signal - threshold)

        # ----------------------------------------------------
        # Plot
        # ----------------------------------------------------
        fig, ax = plt.subplots(figsize=(16, 9))
        ax.set_facecolor("#f2f2f2")

        ax.set_title(
            file.stem.replace("_", " "),
            fontsize=24,
            fontweight="bold",
            pad=15,
        )

        ax.set_xlabel("Community ID", fontsize=18)
        ax.set_ylabel("Frequency", fontsize=18)

        cmap = plt.cm.viridis
        colors = cmap(np.linspace(0, 1, len(signals)))

        labels = []

        for i, (signal, color) in enumerate(zip(signals, colors)):

            if not np.any(signal):
                continue

            max_idx = np.argmax(signal)
            general_topic = topics[max_idx]

            threshold_level = signal[max_idx] * 0.95
            keyword_indices = np.where(signal >= threshold_level)[0]

            keywords = [
                topics[idx]
                for idx in keyword_indices
                if topics[idx] != general_topic
            ]

            semantic_hint = extract_common_thread(keywords)
            label = f"Community {i}: {general_topic}{semantic_hint}"
            labels.append(label)

            ax.plot(
                signal,
                label=label,
                linewidth=2.5,
                alpha=0.95,
                color=color,
            )

        ax.grid(True)
        ax.tick_params(axis="both", labelsize=14)

        # Larger legend + less empty space
        fig.legend(
            loc="center left",
            bbox_to_anchor=(0.98, 0.5),
            fontsize=16,            # <-- increased legend font
            frameon=False,
        )

        # Tighter layout with minimal empty space
        plt.subplots_adjust(
            left=0.08,
            right=0.78,   # tighter than before
            top=0.90,
            bottom=0.10,
        )

        output_filename = base_dir / (
            file.stem.replace("topic_distribution", "community_labelling")
            + ".pdf"
        )

        fig.savefig(output_filename, bbox_inches="tight")
        plt.close(fig)

        print(f"Saved → {output_filename}")


if __name__ == "__main__":
    main()