import sys
import csv
import json
import tomllib
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer


# ============================================================
# Utility Functions
# ============================================================
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


def generate_names(prompts, fallbacks, model, tokenizer):
    inputs = tokenizer(prompts, padding=True, return_tensors="pt").to(model.device)
    generated = model.generate(**inputs, max_new_tokens=12, do_sample=False)
    return [
        tokenizer.decode(result[inputs.input_ids.shape[1]:], skip_special_tokens=True).strip(' \"\'.') or fallback
        for result, fallback in zip(generated, fallbacks)
    ]


def write_name_candidates(data: dict, output_file: Path, model, tokenizer, used_names):
    selected_names = {}
    if output_file.exists():
        with open(output_file, newline="") as f:
            selected_names = {
                row["community_id"]: row.get("selected", row.get("selected_name", ""))
                for row in csv.DictReader(f)
            }

    candidate_lists = [sorted(topic_counts, key=topic_counts.get, reverse=True)[:3] for topic_counts in data.values()]
    prompts = [
        tokenizer.apply_chat_template([
            {"role": "system", "content": "Return only a concise research community name of at most six words."},
            {"role": "user", "content": f"Combine these topics: {', '.join(candidates)}"},
        ], tokenize=False, add_generation_prompt=True)
        for candidates in candidate_lists
    ]
    generated_names = generate_names(prompts, [candidates[0] for candidates in candidate_lists], model, tokenizer)

    period = output_file.stem.removeprefix("community_names_").replace("_", "-")
    for index, (name, candidates) in enumerate(zip(generated_names, candidate_lists)):
        if name.casefold() in used_names:
            prompt = tokenizer.apply_chat_template([
                {"role": "system", "content": "Return only a concise, specific research community name of at most six words."},
                {"role": "user", "content": f"The name '{name}' is already used. Create a distinct, more specific name for: {', '.join(candidates)}"},
            ], tokenize=False, add_generation_prompt=True)
            name = generate_names([prompt], [candidates[0]], model, tokenizer)[0]
            if name.casefold() in used_names:
                name = f"{name} ({period})"
            suffix = 2
            base_name = name
            while name.casefold() in used_names:
                name = f"{base_name} {suffix}"
                suffix += 1
            generated_names[index] = name
        used_names.add(generated_names[index].casefold())

    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["community_id", "generated_name", "candidate_1", "candidate_2", "candidate_3", "selected"])
        names_by_id = {}
        for (community_id, _), candidates, generated_name in zip(data.items(), candidate_lists, generated_names):
            names_by_id[community_id] = generated_name
            writer.writerow([
                community_id,
                generated_name,
                *(candidates + [""] * (3 - len(candidates))),
                selected_names.get(community_id, ""),
            ])
    return names_by_id


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
    model_name = "Qwen/Qwen2.5-1.5B-Instruct"
    tokenizer = AutoTokenizer.from_pretrained(model_name, local_files_only=True, padding_side="left")
    tokenizer.pad_token = tokenizer.eos_token
    model = AutoModelForCausalLM.from_pretrained(
        model_name,
        local_files_only=True,
        dtype=torch.float16 if torch.cuda.is_available() else torch.float32,
    ).to("cuda" if torch.cuda.is_available() else "cpu")
    used_names = set()

    # ========================================================
    # Process each JSON file
    # ========================================================
    for file in sorted(communities_folder.glob("*.json")):
        print(f"Processing {file.name}")

        with open(file) as f:
            data = json.load(f)

        names_filename = base_dir / (
            file.stem.replace("topic_distribution", "community_names") + ".csv"
        )
        generated_names = write_name_candidates(data, names_filename, model, tokenizer, used_names)
        print(f"Saved candidate names to {names_filename}")

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
        ax.set_xticks([])

        cmap = plt.cm.viridis
        colors = cmap(np.linspace(0, 1, len(signals)))

        labels = []

        for i, (signal, color) in enumerate(zip(signals, colors)):

            if not np.any(signal):
                continue

            label = f"Community {i}: {generated_names[str(i)]}"
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
