#!/usr/bin/env python3
import sys
import os
import glob
import re
import tomllib
import warnings

import pandas as pd
import matplotlib

matplotlib.use("Agg")  # no display needed
import matplotlib.pyplot as plt
import seaborn as sns

# Ridge plots overlap rows on purpose (negative hspace); silence the cosmetic warning.
warnings.filterwarnings("ignore", message="Tight layout not applied")


def timeframe_label(path: str, basename: str) -> str:
    """Turn 'collaborations_1990_2000_dataset.csv' into '1990-2000' (and 'all_dataset' -> 'all')."""
    name = os.path.basename(path)
    prefix = "collaborations_"
    if name.startswith(prefix):
        name = name[len(prefix):]
    if basename and name.endswith(basename):
        name = name[: -len(basename)]
    name = name.rstrip("_")
    m = re.fullmatch(r"(\d+)_(\d+)", name)
    if m:
        return "{}-{}".format(m.group(1), m.group(2))
    return name if name else os.path.basename(path)


def sort_key(label: str):
    """Chronological order by first year found; labels without a year (e.g. 'all') go last."""
    m = re.search(r"\d+", label)
    return (0, int(m.group())) if m else (1, label)


def main():
    toml_config_path = sys.argv[1] if len(sys.argv) > 1 else "default.toml"
    print("Parsing {} configuration file".format(toml_config_path))
    with open(toml_config_path, "rb") as f:
        configuration = tomllib.load(f)

    try:
        section = configuration["external_collaboration_kde"]
        collaboration_directory = (
            configuration["workflow_data"] + "/"
            + configuration["country"] + "/"
            + section["inputs"]["collaboration_directory"]
        )
        file_glob = section["inputs"].get("file_glob", "collaborations_*.csv")
        dataset_basename = section["inputs"].get("dataset_basename", "dataset.csv")
        output_plot_file = (
            configuration["statistics_out_basedir"] + "/"
            + section["outputs"]["output_plot_file"]
        )
        os.makedirs(configuration["statistics_out_basedir"], exist_ok=True)
    except Exception as e:
        print("Error: key {} not found".format(e))
        exit(-1)

    files = sorted(glob.glob(os.path.join(collaboration_directory, file_glob)))
    if not files:
        print("No files matching {} in {}".format(file_glob, collaboration_directory))
        exit(-1)

    required = {"total_collaborations", "external_collaborations"}
    frames = []
    seen_labels = []
    for path in files:
        try:
            df = pd.read_csv(path)
        except pd.errors.EmptyDataError:
            print("Skipping empty file: {}".format(path))
            continue
        except Exception as e:
            print("Skipping {} (read error: {})".format(path, e))
            continue

        if df.empty or not required.issubset(df.columns):
            print("Skipping empty / malformed file: {}".format(path))
            continue

        df = df[df["total_collaborations"] > 0].copy()
        if df.empty:
            print("Skipping file with no usable rows: {}".format(path))
            continue

        df["ratio"] = df["external_collaborations"] / df["total_collaborations"]
        label = timeframe_label(path, dataset_basename)
        df["timeframe"] = label
        frames.append(df[["timeframe", "ratio"]])
        seen_labels.append(label)
        print("Loaded {:>6d} authors from {}  (timeframe '{}')".format(len(df), path, label))

    if not frames:
        print("No non-empty collaboration files to plot.")
        exit(-1)

    data = pd.concat(frames, ignore_index=True)

    # Keep only timeframes that have enough spread for a KDE (>= 2 distinct values).
    ordered_labels = sorted(dict.fromkeys(seen_labels), key=sort_key)
    valid_labels = []
    for label in ordered_labels:
        ratios = data.loc[data["timeframe"] == label, "ratio"]
        if len(ratios) >= 2 and ratios.nunique() >= 2:
            valid_labels.append(label)
        else:
            print("Skipping timeframe '{}' (not enough variation for a KDE)".format(label))

    if not valid_labels:
        print("No timeframe has enough data to plot.")
        exit(-1)

    data = data[data["timeframe"].isin(valid_labels)]
    ordered_labels = valid_labels

    # ---- Ridge plot ---------------------------------------------------------
    sns.set_theme(style="white", rc={"axes.facecolor": (0, 0, 0, 0)})
    palette = sns.cubehelix_palette(len(ordered_labels), rot=-0.25, light=0.7)

    g = sns.FacetGrid(
        data,
        row="timeframe",
        hue="timeframe",
        row_order=ordered_labels,
        hue_order=ordered_labels,
        aspect=15,
        height=0.6,
        palette=palette,
    )

    # filled density + white contour on top
    g.map(sns.kdeplot, "ratio", bw_adjust=0.5, clip=(0, 1), clip_on=False,
          fill=True, alpha=1, linewidth=1.5)
    g.map(sns.kdeplot, "ratio", bw_adjust=0.5, clip=(0, 1), clip_on=False,
          color="w", lw=2)
    g.refline(y=0, linewidth=2, linestyle="-", color=None, clip_on=False)

    def add_label(x, color, label):
        ax = plt.gca()
        ax.text(-0.01, 0.2, label, fontweight="bold", color=color,
                ha="right", va="center", transform=ax.transAxes)

    g.map(add_label, "ratio")

    g.figure.subplots_adjust(hspace=-0.25, left=0.12)
    g.set_titles("")
    g.set(yticks=[], ylabel="", xlim=(0, 1))
    g.set_xlabels("External collaborations / total collaborations")
    g.despine(bottom=True, left=True)
    g.figure.suptitle("Fraction of external collaborations per author, by timeframe",
                      y=1.02, fontsize=12)

    g.savefig(output_plot_file, dpi=200, bbox_inches="tight")
    print("Saved ridge plot to {}".format(output_plot_file))


if __name__ == "__main__":
    main()