import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import NullFormatter
import tomllib
import re, sys
from pathlib import Path


plt.rcParams.update({"font.size": 14})


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
    combined_output_plot = base_dir / cfg["plot_generation"]["outputs"]["combined_structural_step_plot_filename"]
    path_clustering_stats_file = base_dir / cfg["path_clustering_statistics"]["outputs"]["stats_file"]
    
    validation_stats_filename_random = cfg["statistics_out_basedir"] + "/" + cfg["graph_property_validation"]["outputs"]["stats_out_random"]
    validation_stats_filename = cfg["statistics_out_basedir"] + "/" + cfg["graph_property_validation"]["outputs"]["stats_out"]
    validation_out_filename = base_dir / cfg["plot_generation"]["outputs"]["random_validation_output_filename"]

except KeyError as e:
    raise RuntimeError(f"Missing config key: {e}")


interval_order = {
    f"{start}_{end}": index
    for index, (start, end) in enumerate(cfg["time_intervals"])
}


def dataset_sort_key(name: str): 
    interval = re.search(r"\d{4}_\d{4}", name)
    return interval_order.get(interval.group() if interval else "", len(interval_order)), name


def prepare_df(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = df[df["graph_name"].str.extract(r"(\d{4}_\d{4})", expand=False).isin(interval_order)]

    df["label"] = (
        df["graph_name"]
        .str.replace("weighted_", "", regex=False)
        .str.replace("backbone_", "", regex=False)
        .str.replace("_dataset", "", regex=False)
        .str.replace(".csv", "", regex=False)
    )

    df["sort_key"] = df["label"].apply(dataset_sort_key)
    df = df.sort_values("sort_key").drop(columns="sort_key").reset_index(drop=True)
    df["label"] = df["label"].str.replace("_", "-\n", regex=False)

    return df


# ---- Helper for line plots ----
def line(ax, df1, df2, style1, style2,  col,  ylabel=None, logy=False, variance_col = None):
    ax.plot(labels, df1[col], **style1)
    
    
    if variance_col is not None and type(variance_col) is str:
        ax.errorbar(labels, df2[col], yerr=df2[variance_col], capsize=3, **style2)
    else:
        ax.plot(labels, df2[col], **style2)

    ax.set_title(col.replace("_", " ").title())
    if ylabel:
        ax.set_ylabel(ylabel)
    if logy:
        ax.set_yscale("log")
    ax.tick_params(axis="x", rotation=90)
    ax.grid(True)


def nodes_vs_edges(ax, df1, df2, style1, style2):
    colors = plt.get_cmap("tab10").colors[:len(df1)]
    ax.scatter(
        df1["number_of_nodes"], df1["number_of_edges"], c=colors,
        marker=style1["marker"], label=style1["label"]
    )
    ax.scatter(
        df2["number_of_nodes"], df2["number_of_edges"], c=colors,
        marker=style2["marker"], s=style2["markersize"] ** 2,
        linewidths=style2["markeredgewidth"], label=style2["label"]
    )
    interval_handles = [
        ax.scatter([], [], color=color, label=label.replace("\n", ""))
        for color, label in zip(colors, df1["label"])
    ]
    ax.legend(
        handles=interval_handles, title="Time interval",
        fontsize=13, title_fontsize=13
    )

    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_title("Nodes vs Edges")
    ax.set_xlabel("Nodes")
    ax.set_ylabel("Edges")
    ax.tick_params(axis="x", rotation=90)
    ax.grid(True)


df_full = prepare_df(stats_file_full)
df_cc = prepare_df(stats_file_cc)

# Ensure alignment
if not df_full["label"].equals(df_cc["label"]):
    raise RuntimeError("Label mismatch between full network and largest CC")

labels = df_full["label"]


fig, axes = plt.subplots(3, 3, figsize=(18, 15))
axes = axes.flatten()

style_full = dict(marker="o", linestyle="-", label="Full network")
style_cc = dict(marker="x", markersize=9, markeredgewidth=1, linestyle="--", label="Largest CC")


# ---- 1: Nodes vs Edges (scatter) ----
nodes_vs_edges(axes[0], df_full, df_cc, style_full, style_cc)


# ---- 2–8: Line plots ----
line(axes[1], df_full, df_cc, style_full, style_cc, "mean_degree", "Degree")
line(axes[2], df_full, df_cc, style_full, style_cc, "median_degree")
line(axes[3], df_full, df_cc, style_full, style_cc, "w_mean_degree", "Weighted degree")
line(axes[4], df_full, df_cc, style_full, style_cc, "density", logy=True)
line(axes[5], df_full, df_cc, style_full, style_cc, "transitivity")
line(axes[6], df_full, df_cc, style_full, style_cc, "max_degree", "Degree")
line(axes[7], df_full, df_cc, style_full, style_cc, "w_max_degree", "Weighted degree")


# ---- 9: Number of nodes ----
axes[8].plot(labels, df_full["number_of_nodes"], **style_full)
axes[8].plot(labels, df_cc["number_of_nodes"], **style_cc)
axes[8].set_yscale("log")
axes[8].set_title("Number of Nodes")
axes[8].set_ylabel("Nodes")
axes[8].tick_params(axis="x", rotation=90)
axes[8].grid(True)



fig.legend(*axes[1].get_legend_handles_labels(), loc="upper center", bbox_to_anchor=(0.5, 0.96), ncol=2, fontsize=16)
fig.tight_layout(rect=[0, 0, 1, 0.91])

fig.savefig(output_plot)
plt.close()

print(f"Saved plot to {output_plot}")


############## BACBONE STRUCTURAL STATISTICS ##############


df_full = prepare_df(bacbone_stats_file_full)
df_cc = prepare_df(bacbone_stats_file_cc)

labels = df_full["label"]


fig, axes = plt.subplots(3, 3, figsize=(18, 15))
axes = axes.flatten()

style_full = dict(marker="o", linestyle="-", label="Full network")
style_cc = dict(marker="x", markersize=9, markeredgewidth=1, linestyle="--", label="Largest CC")


# ---- 1: Nodes vs Edges (scatter) ----
nodes_vs_edges(axes[0], df_full, df_cc, style_full, style_cc)


# ---- 2–8: Line plots ----
line(axes[1], df_full, df_cc, style_full, style_cc, "mean_degree", "Degree")
line(axes[2], df_full, df_cc, style_full, style_cc, "median_degree")
line(axes[3], df_full, df_cc, style_full, style_cc, "w_mean_degree", "Weighted degree")
line(axes[4], df_full, df_cc, style_full, style_cc, "density", logy=True)
line(axes[5], df_full, df_cc, style_full, style_cc, "transitivity")
line(axes[6], df_full, df_cc, style_full, style_cc, "max_degree", "Degree")
line(axes[7], df_full, df_cc, style_full, style_cc, "w_max_degree", "Weighted degree")


# ---- 9: Number of nodes ----
axes[8].plot(labels, df_full["number_of_nodes"], **style_full)
axes[8].plot(labels, df_cc["number_of_nodes"], **style_cc)
axes[8].set_yscale("log")
axes[8].set_title("Number of Nodes")
axes[8].set_ylabel("Nodes")
axes[8].tick_params(axis="x", rotation=90)
axes[8].grid(True)



fig.legend(*axes[1].get_legend_handles_labels(), loc="upper center", bbox_to_anchor=(0.5, 0.96), ncol=2, fontsize=16)
fig.tight_layout(rect=[0, 0, 1, 0.91])

fig.savefig(bacbone_output_plot)
plt.close()

print(f"Saved plot to {bacbone_output_plot}")

############## COMBINED: FULL NETWORK + BACKBONE ##############
# Same panels as above, with all four series (full network and backbone, each as
# whole graph and largest CC) in each panel. The four colors (blue, plum, orange,
# green) were chosen as a set and validated as colorblind-safe across all pairs
# (worst CVD dE 9.9, worst normal-vision dE 21.2, OKLab x100); solid lines for the
# whole graph, dashed for the largest CC; one marker per series.

combined_series = [
    (prepare_df(stats_file_full), dict(color="#2a78d6", marker="o", linestyle="-", label="Full network")),
    (prepare_df(stats_file_cc), dict(color="#7B2D8E", marker="s", linestyle="--", label="Full network (largest CC)")),
    (prepare_df(bacbone_stats_file_full), dict(color="#EE7733", marker="^", linestyle="-", label="Backbone")),
    (prepare_df(bacbone_stats_file_cc), dict(color="#117733", marker="D", linestyle="--", label="Backbone (largest CC)")),
]

labels = combined_series[0][0]["label"]
for df, style in combined_series:
    if not df["label"].equals(labels):
        raise RuntimeError(f"Time interval mismatch for series '{style['label']}'")

# Average clustering coefficient and degree assortativity come from 13_path_clustering_assortativity.py
# (one row per graph, whole-graph values plus "lcc_"-prefixed largest-CC values)
CLUSTERING_COLUMNS = ["avg_clustering", "degree_assortativity"]
if path_clustering_stats_file.exists():
    path_stats = prepare_df(path_clustering_stats_file)
    for (df, style), (network, prefix) in zip(
        combined_series, [("full", ""), ("full", "lcc_"), ("backbone", ""), ("backbone", "lcc_")]
    ):
        rows = path_stats[path_stats["network"] == network].set_index("label")
        for col in CLUSTERING_COLUMNS:
            df[col] = df["label"].map(rows[prefix + col])
        missing = df.loc[df[CLUSTERING_COLUMNS[0]].isna(), "label"].str.replace("\n", "", regex=False).tolist()
        if missing:
            print(f"Warning: no clustering/assortativity for '{style['label']}' in {missing}")
else:
    print(f"Warning: {path_clustering_stats_file} not found (run 13_path_clustering_assortativity.py); "
          f"clustering and assortativity panels will be empty")
    for df, _ in combined_series:
        for col in CLUSTERING_COLUMNS:
            df[col] = float("nan")


def combined_line(ax, col, title, ylabel=None, logy=False):
    for df, style in combined_series:
        ax.plot(labels, df[col], markersize=7, **style)
    ax.set_title(title)
    if ylabel:
        ax.set_ylabel(ylabel)
    if logy:
        ax.set_yscale("log")
        ax.yaxis.set_minor_formatter(NullFormatter())  # decade labels only
    ax.tick_params(axis="x", rotation=90)
    ax.grid(True)


fig, axes = plt.subplots(4, 3, figsize=(18, 20))
axes = axes.flatten()

# Rows: size, degree, weighted degree, clustering (log scale where full network and backbone
# differ by orders of magnitude)
combined_line(axes[0], "number_of_nodes", "Number of Nodes", "Nodes", logy=True)
combined_line(axes[1], "number_of_edges", "Number of Edges", "Edges", logy=True)
combined_line(axes[2], "density", "Density", logy=True)
combined_line(axes[3], "mean_degree", "Mean Degree", "Degree", logy=True)
combined_line(axes[4], "median_degree", "Median Degree", logy=True)
combined_line(axes[5], "max_degree", "Max Degree", logy=True)
combined_line(axes[6], "w_mean_degree", "W Mean Degree", "Weighted degree", logy=True)
combined_line(axes[7], "w_median_degree", "W Median Degree", logy=True)
combined_line(axes[8], "w_max_degree", "W Max Degree", logy=True)
combined_line(axes[9], "transitivity", "Transitivity")
combined_line(axes[10], "avg_clustering", "Avg Clustering Coefficient")
combined_line(axes[11], "degree_assortativity", "Degree Assortativity")
axes[11].axhline(0, color="#5f5e5a", linewidth=1, zorder=0)  # neutral (no degree correlation)

fig.legend(*axes[0].get_legend_handles_labels(), loc="upper center", bbox_to_anchor=(0.5, 0.995), ncol=4, fontsize=16)
fig.tight_layout(rect=[0, 0, 1, 0.965])

fig.savefig(combined_output_plot)
plt.close()

print(f"Saved plot to {combined_output_plot}")

########################################
#     Random Graph null hypotesis      #
########################################

style_validation = dict(marker="o", linestyle="-", label="Real network")
style_validation_random = dict(marker="x", markersize=9, markeredgewidth=1, linestyle="--", label="Random generated network")

df_validation = prepare_df(validation_stats_filename)
df_validation_random = prepare_df(validation_stats_filename_random)
labels = df_validation["label"]


fig, axes = plt.subplots(3, 3, figsize=(18, 15))
axes = axes.flatten()

# ---- 1: Nodes vs Edges ----
nodes_vs_edges(
    axes[0], df_validation, df_validation_random,
    style_validation, style_validation_random
)

# ---- 2–9: Metrics ----
line(axes[1], df_validation, df_validation_random, style_validation, style_validation_random, "mean_degree", "Mean Degree", "Degree", variance_col="mean_degree_var")
line(axes[2], df_validation, df_validation_random, style_validation, style_validation_random, "median_degree", "Median Degree",variance_col="median_degree_var")
line(axes[3], df_validation, df_validation_random, style_validation, style_validation_random, "density", "Density", logy=True, variance_col="density_var")
line(axes[4], df_validation, df_validation_random, style_validation, style_validation_random, "clustering_coefficent", "Clustering Coefficient", variance_col="clustering_coefficent_var")
line(axes[5], df_validation, df_validation_random, style_validation, style_validation_random, "transitivity", "Transitivity", variance_col="transitivity_var")
line(axes[6], df_validation, df_validation_random, style_validation, style_validation_random, "max_degree", "Max Degree", "Degree", variance_col="max_degree_var")
line(axes[7], df_validation, df_validation_random, style_validation, style_validation_random, "degree_assortativity", "Degree Assortativity", variance_col="degree_assortativity_var")
line(axes[8], df_validation, df_validation_random, style_validation, style_validation_random, "n_connected_components", "Connected Components", logy=True, variance_col="n_connected_components_var")

# =============================
# Final layout
# =============================
fig.legend(*axes[1].get_legend_handles_labels(), loc="upper center", bbox_to_anchor=(0.5, 0.96), ncol=2, fontsize=16)
fig.tight_layout(rect=[0, 0, 1, 0.91])

fig.savefig(validation_out_filename)
plt.close()

print(f"Saved plot to {validation_out_filename}")
