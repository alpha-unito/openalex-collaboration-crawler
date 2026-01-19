import pandas as pd
import matplotlib.pyplot as plt
import tomllib
import re, sys
from pathlib import Path


toml_config_path = sys.argv[1] if len(sys.argv) > 1 else "default.toml"
print(f"Parsing {toml_config_path} configuration file")

with open(toml_config_path, "rb") as f:
    cfg = tomllib.load(f)

try:
    analyzed_country = cfg["analized_country_full"]
    base_dir = Path(cfg["statistics_out_basedir"])

    stats_file_full = base_dir / cfg["structural_statistics"]["outputs"]["output_stats_file"]
    stats_file_cc = base_dir / cfg["structural_statistics"]["outputs"]["output_stats_file_largest_cc"]
    
    bacbone_stats_file_full = base_dir / cfg["bacbone_structural_statistics"]["outputs"]["output_stats_file"]
    bacbone_stats_file_cc = base_dir / cfg["bacbone_structural_statistics"]["outputs"]["output_stats_file_largest_cc"]

    output_plot = base_dir / cfg["plot_generation"]["outputs"]["structural_step_plot_filename"]
    bacbone_output_plot = base_dir / cfg["plot_generation"]["outputs"]["backbone_structural_step_plot_filename"]

except KeyError as e:
    raise RuntimeError(f"Missing config key: {e}")


def dataset_sort_key(name: str): 
    if re.search(r'(\d{4})_(\d{4})$', name): 
        return name 
    if re.search(r'_(\d{4})$', name): 
        return f"0{name}" 
    return f"{name}NOW"


def prepare_df(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)

    df["label"] = (
        df["graph_name"]
        .str.replace("weighted_", "", regex=False)
        .str.replace("_dataset", "", regex=False)
    )

    df["label"] = df["label"].apply(dataset_sort_key)
    df = df.sort_values("label").reset_index(drop=True)

    return df


df_full = prepare_df(stats_file_full)
df_cc = prepare_df(stats_file_cc)

# Ensure alignment
if not df_full["label"].equals(df_cc["label"]):
    raise RuntimeError("Label mismatch between full network and largest CC")

labels = df_full["label"]


fig, axes = plt.subplots(3, 3, figsize=(18, 15))
axes = axes.flatten()

style_full = dict(marker="o", linestyle="-", label="Full network")
style_cc = dict(marker="s", linestyle="--", label="Largest CC")


# ---- 1: Nodes vs Edges (scatter) ----
axes[0].scatter(df_full["number_of_nodes"], df_full["number_of_edges"], label="Full")
axes[0].scatter(df_cc["number_of_nodes"], df_cc["number_of_edges"], label="Largest CC")

for i in range(len(df_full)):
    axes[0].annotate(
        labels.iloc[i],
        (df_full["number_of_nodes"].iloc[i], df_full["number_of_edges"].iloc[i]),
        xytext=(5, 5),
        textcoords="offset points",
        fontsize=8
    )

axes[0].set_xscale("log")
axes[0].set_yscale("log")
axes[0].set_title("Nodes vs Edges")
axes[0].set_xlabel("Nodes")
axes[0].set_ylabel("Edges")
axes[0].grid(True)
axes[0].legend()


# ---- Helper for line plots ----
def line(ax, col, ylabel=None, logy=False):
    ax.plot(labels, df_full[col], **style_full)
    ax.plot(labels, df_cc[col], **style_cc)
    ax.set_title(col.replace("_", " ").title())
    if ylabel:
        ax.set_ylabel(ylabel)
    if logy:
        ax.set_yscale("log")
    ax.tick_params(axis="x", rotation=45)
    ax.grid(True)


# ---- 2–8: Line plots ----
line(axes[1], "mean_degree", "Degree")
line(axes[2], "median_degree")
line(axes[3], "w_mean_degree", "Weighted degree")
line(axes[4], "density", logy=True)
line(axes[5], "transitivity")
line(axes[6], "max_degree", "Degree")
line(axes[7], "w_max_degree", "Weighted degree")


# ---- 9: Number of nodes ----
axes[8].plot(labels, df_full["number_of_nodes"], **style_full)
axes[8].plot(labels, df_cc["number_of_nodes"], **style_cc)
axes[8].set_yscale("log")
axes[8].set_title("Number of Nodes")
axes[8].set_ylabel("Nodes")
axes[8].tick_params(axis="x", rotation=45)
axes[8].grid(True)



fig.suptitle(f"Structural Statistics — {analyzed_country}", fontsize=16)
fig.tight_layout(rect=[0, 0, 1, 0.95])

fig.savefig(output_plot)
plt.close()

print(f"Saved plot to {output_plot}")


############## BACBONE STRUCTURAL STATISTICS ##############


df_full = prepare_df(bacbone_stats_file_full)
df_cc = prepare_df(bacbone_stats_file_cc)

# Ensure alignment
if not df_full["label"].equals(df_cc["label"]):
    raise RuntimeError("Label mismatch between full network and largest CC")

labels = df_full["label"]


fig, axes = plt.subplots(3, 3, figsize=(18, 15))
axes = axes.flatten()

style_full = dict(marker="o", linestyle="-", label="Full network")
style_cc = dict(marker="s", linestyle="--", label="Largest CC")


# ---- 1: Nodes vs Edges (scatter) ----
axes[0].scatter(df_full["number_of_nodes"], df_full["number_of_edges"], label="Full")
axes[0].scatter(df_cc["number_of_nodes"], df_cc["number_of_edges"], label="Largest CC")

for i in range(len(df_full)):
    axes[0].annotate(
        labels.iloc[i],
        (df_full["number_of_nodes"].iloc[i], df_full["number_of_edges"].iloc[i]),
        xytext=(5, 5),
        textcoords="offset points",
        fontsize=8
    )

axes[0].set_xscale("log")
axes[0].set_yscale("log")
axes[0].set_title("Nodes vs Edges")
axes[0].set_xlabel("Nodes")
axes[0].set_ylabel("Edges")
axes[0].grid(True)
axes[0].legend()


# ---- Helper for line plots ----
def line(ax, col, ylabel=None, logy=False):
    ax.plot(labels, df_full[col], **style_full)
    ax.plot(labels, df_cc[col], **style_cc)
    ax.set_title(col.replace("_", " ").title())
    if ylabel:
        ax.set_ylabel(ylabel)
    if logy:
        ax.set_yscale("log")
    ax.tick_params(axis="x", rotation=45)
    ax.grid(True)


# ---- 2–8: Line plots ----
line(axes[1], "mean_degree", "Degree")
line(axes[2], "median_degree")
line(axes[3], "w_mean_degree", "Weighted degree")
line(axes[4], "density", logy=True)
line(axes[5], "transitivity")
line(axes[6], "max_degree", "Degree")
line(axes[7], "w_max_degree", "Weighted degree")


# ---- 9: Number of nodes ----
axes[8].plot(labels, df_full["number_of_nodes"], **style_full)
axes[8].plot(labels, df_cc["number_of_nodes"], **style_cc)
axes[8].set_yscale("log")
axes[8].set_title("Number of Nodes")
axes[8].set_ylabel("Nodes")
axes[8].tick_params(axis="x", rotation=45)
axes[8].grid(True)



fig.suptitle(f"Backbone Structural Statistics — {analyzed_country}", fontsize=16)
fig.tight_layout(rect=[0, 0, 1, 0.95])

fig.savefig(bacbone_output_plot)
plt.close()

print(f"Saved plot to {bacbone_output_plot}")
