# -----------------------------
# Configuration parameters
# -----------------------------

# Input directory containing the graph CSV files
graph_input_directory = "/beegfs/home/msantima/OpenAlexCollaborations/IT/nets_weighted"

# Output file for structural statistics
output_stats_file = "./structural_stats.csv"

# Output file for structural statistics of the largest connected component
output_stats_file_largest_cc = "./largestCC_structural_stats.csv"

# -----------------------------
# END Configuration parameters
# -----------------------------

from compute_structural_statistics import run
run(graph_input_directory, output_stats_file, output_stats_file_largest_cc, False)