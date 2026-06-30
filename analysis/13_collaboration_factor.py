#!/usr/bin/env python3
"""
Ridge plot of the per-author "external collaboration fraction" for each timeframe.

For every `collaborations_*.csv` file produced by the adjacency-list generator
(columns: author_id,total_collaborations,external_collaborations) this script
computes, per author, the fraction

        external_collaborations / total_collaborations

and draws one KDE per timeframe, stacked as a seaborn ridge (joy) plot.
Empty files (or files without usable rows) are skipped.

Configuration is read from a TOML file. Example:

    workflow_data          = "/data/workflow"
    country                = "IT"
    statistics_out_basedir = "/data/stats"

    [external_collaboration_kde.inputs]
    collaboration_directory = "graphs"             # under workflow_data/country
    file_glob               = "collaborations_*.csv"   # optional
    dataset_basename        = "dataset.csv"            # optional, used to clean labels

    [external_collaboration_kde.outputs]
    output_plot_file        = "external_collab_ridge.png"

    [external_collaboration_kde.aggregate]
    # Optional: merge several timeframes into one ridge. Keys are the new
    # (aggregated) label; values are the labels to pool. A "START-END" token
    # expands to every individual year in that inclusive range, so per-year
    # data can be binned without listing each year:
    "1960-1962" = "1960-1962"               # pools years 1960, 1961, 1962
    "1960s"     = "1960-1969"               # a whole decade
    "misc"      = ["1971", "1973", "1975"]  # an explicit list still works

Usage:
    python plot_external_collab_ridge.py config.toml
"""

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
    # Drop the dataset suffix if present, otherwise fall back to dropping a bare
    # ".csv" extension so files that don't carry the dataset basename still get a
    # clean label (e.g. 'collaborations_1990_2000.csv' -> '1990-2000').
    if basename and name.endswith(basename):
        name = name[: -len(basename)]
    elif name.endswith(".csv"):
        name = name[:-4]
    name = name.strip("_")
    m = re.fullmatch(r"(\d+)_(\d+)", name)
    if m:
        return "{}-{}".format(m.group(1), m.group(2))
    return name if name else os.path.basename(path)


def sort_key(label: str):
    """Chronological order by first year found; labels without a year (e.g. 'all') go last."""
    m = re.search(r"\d+", label)
    return (0, int(m.group())) if m else (1, label)


def expand_members(spec):
    """Expand an aggregate spec into a flat list of literal timeframe labels.

    A token like '1960-1962' expands to the individual years '1960', '1961',
    '1962' (inclusive, in either direction). Any token that is not a plain
    integer range is taken literally, so explicit labels still work. The spec
    may be a single string or a list of tokens.
    """
    if isinstance(spec, str):
        spec = [spec]
    members = []
    for token in spec:
        token = str(token).strip()
        m = re.fullmatch(r"(\d+)-(\d+)", token)
        if m:
            start, end = int(m.group(1)), int(m.group(2))
            step = 1 if end >= start else -1
            members.extend(str(year) for year in range(start, end + step, step))
        else:
            members.append(token)
    return members


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
        aggregate = section.get("aggregate", {})
        output_plot_file = (
                configuration["statistics_out_basedir"] + "/"
                + section["outputs"]["output_plot_file"]
        )
        os.makedirs(configuration["statistics_out_basedir"], exist_ok=True)
    except Exception as e:
        print("Error: key {} not found".format(e))
        sys.exit(1)

    files = sorted(glob.glob(os.path.join(collaboration_directory, file_glob)))
    if not files:
        print("No files matching {} in {}".format(file_glob, collaboration_directory))
        sys.exit(1)

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

        label = timeframe_label(path, dataset_basename)

        # per-author external fraction (already a ratio, no further normalization)
        df["value"] = df["external_collaborations"] / df["total_collaborations"]
        df["timeframe"] = label
        frames.append(df[["timeframe", "value"]])
        seen_labels.append(label)
        print("Loaded {:>6d} authors from {}  (year '{}')".format(len(df), path, label))

    if not frames:
        print("No non-empty collaboration files to plot.")
        sys.exit(1)

    data = pd.concat(frames, ignore_index=True)

    if aggregate:
        remap = {}
        present = set(data["timeframe"].unique())
        for new_label, spec in aggregate.items():
            for member in expand_members(spec):
                if member not in present:
                    print("Aggregation: label '{}' not found, ignoring".format(member))
                    continue
                remap[member] = new_label
        if remap:
            data["timeframe"] = data["timeframe"].replace(remap)
            seen_labels = [remap.get(label, label) for label in seen_labels]
            merged = sorted(dict.fromkeys(remap.values()))
            print("Aggregated timeframes into: {}".format(", ".join(merged)))

    # Keep only timeframes that have enough spread for a KDE (>= 2 distinct values).
    ordered_labels = sorted(dict.fromkeys(seen_labels), key=sort_key)
    valid_labels = []
    for label in ordered_labels:
        values = data.loc[data["timeframe"] == label, "value"]
        if len(values) >= 2 and values.nunique() >= 2:
            valid_labels.append(label)
        else:
            print("Skipping timeframe '{}' (not enough variation for a KDE)".format(label))

    if not valid_labels:
        print("No timeframe has enough data to plot.")
        sys.exit(1)

    data = data[data["timeframe"].isin(valid_labels)]
    ordered_labels = valid_labels

    # ---- Ridge plot ---------------------------------------------------------
    sns.set_theme(style="white", rc={"axes.facecolor": (0, 0, 0, 0)})
    palette = sns.color_palette("hls", n_colors=len(ordered_labels))

    g = sns.FacetGrid(
        data,
        row="timeframe",
        hue="timeframe",
        row_order=ordered_labels,
        hue_order=ordered_labels,
        aspect=15,
        height=0.5,
        palette=palette,
    )

    g.map(sns.kdeplot, "value", bw_adjust=0.5, clip=(0, 1), clip_on=False, fill=True, alpha=0.6, linewidth=0)
    g.map(sns.kdeplot, "value", bw_adjust=0.5, clip=(0, 1), clip_on=False, fill=False, alpha=1, linewidth=1.5)
    g.refline(y=0, linewidth=0.4, linestyle="-", color="gray", clip_on=False)

    def add_label(x, color, label):
        ax = plt.gca()
        ax.text(-0.01, 0.2, label, color="black",
                ha="right", va="center", transform=ax.transAxes)

    g.map(add_label, "value")

    g.figure.subplots_adjust(hspace=-0, left=0.12)
    g.set_titles("")
    g.set(yticks=[], ylabel="", xlim=(0, 1))
    g.set_xlabels("Fraction of external collaborations over total collaborations")
    g.despine(bottom=True, left=True)

    plt.gca().axhline(0, color="gray", lw=0.0)

    g.savefig(output_plot_file, bbox_inches="tight")
    print("Saved ridge plot to {}".format(output_plot_file))


if __name__ == "__main__":
    main()