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
    
    validation_stats_filename_random = cfg["statistics_out_basedir"] + "/" + cfg["graph_property_validation"]["outputs"]["stats_out_random"]
    validation_stats_filename = cfg["statistics_out_basedir"] + "/" + cfg["graph_property_validation"]["outputs"]["stats_out"]
    validation_out_filename = base_dir / cfg["plot_generation"]["outputs"]["random_validation_output_filename"]

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


# ---- Helper for line plots ----
def line(ax, df1, df2, style1, style2,  col,  ylabel=None, logy=False, variance_col = None):
    ax.plot(labels, df1[col], **style1)
    
    
    if variance_col is not None and type(variance_col) is str:
        ax.errorbar(labels, df2[col], yerr=df2[variance_col], fmt="o-", capsize=3, **style2)
    else:
        ax.plot(labels, df2[col], **style2)

    ax.set_title(col.replace("_", " ").title())
    if ylabel:
        ax.set_ylabel(ylabel)
    if logy:
        ax.set_yscale("log")
    ax.tick_params(axis="x", rotation=45)
    ax.grid(True)
    ax.legend()


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

df_full["label"] = df_full["label"].str.split("_", n=1).str[1]
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
axes[8].tick_params(axis="x", rotation=45)
axes[8].grid(True)



fig.suptitle(f"Backbone Structural Statistics — {analyzed_country}", fontsize=16)
fig.tight_layout(rect=[0, 0, 1, 0.95])

fig.savefig(bacbone_output_plot)
plt.close()

print(f"Saved plot to {bacbone_output_plot}")

########################################
#     Random Graph null hypotesis      #
########################################

style_validation = dict(marker="o", linestyle="-", label="Real network")
style_validation_random = dict(marker="s", linestyle="--", label="Random generated network")

df_validation = prepare_df(validation_stats_filename)
df_validation_random = prepare_df(validation_stats_filename_random)


fig, axes = plt.subplots(3, 3, figsize=(18, 15))
axes = axes.flatten()

# ---- 1: Nodes vs Edges ----
axes[0].scatter(df_validation["number_of_nodes"], df_validation["number_of_edges"], label="Real network")
axes[0].scatter(df_validation_random["number_of_nodes"], df_validation_random["number_of_edges"], label="Random generated network")
for i in range(len(df_validation)):
    axes[0].annotate(
        labels.iloc[i],
        (df_validation["number_of_nodes"].iloc[i], df_validation["number_of_edges"].iloc[i]),
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
fig.suptitle(f"Random network null hypotesis - {analyzed_country}", fontsize=16)
fig.tight_layout(rect=[0, 0, 1, 0.95])

fig.savefig(validation_out_filename)
plt.close()

print(f"Saved plot to {validation_out_filename}")
