import sys, tomllib, os

toml_config_path = sys.argv[1] if len(sys.argv) > 1 else "default.toml"

print("Parsing {} configuration file".format(toml_config_path))
with open(toml_config_path, 'rb') as f:
    configuration = tomllib.load(f)

try:
    graph_directory                 = configuration["workflow_data"] + "/" + configuration["country"] + "/" + configuration["backbones"]["outputs"]["backbone_directory"]
    output_stats_file               = configuration["statistics_out_basedir"] + "/" + configuration["bacbone_structural_statistics"]["outputs"]["output_stats_file"]
    output_stats_file_largest_cc    = configuration["statistics_out_basedir"] + "/" + configuration["bacbone_structural_statistics"]["outputs"]["output_stats_file_largest_cc"]
    os.makedirs(configuration["statistics_out_basedir"], exist_ok=True)
except Exception as e:
    print("Error: key {} not found".format(e))
    exit(-1)

print(f"\n{'=' * 60}")
print(f"{" BACKBONE STRUCTURAL STATISTICS CONFIGURATION ".center(60, ' ')}")
print(f"{'=' * 60}")

# --- Inputs ---
print(f"\n[DATA SOURCE]")
print(f"  Graph Directory:          {graph_directory}")

# --- Outputs ---
print(f"\n[ANALYSIS OUTPUTS]")
print(f"  General Stats File:       {output_stats_file}")
print(f"  Largest CC Stats File:    {output_stats_file_largest_cc}")

print(f"\n{'='*60}\n")

from compute_structural_statistics import run
run(graph_directory, output_stats_file, output_stats_file_largest_cc, True)