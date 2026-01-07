import pickle
import os
import re
import numpy as np
import csv


# -----------------------------
# Configuration parameters
# -----------------------------

# Directory containing the community pickle files generated during the stability analysis
community_pickle_directory = "/beegfs/home/msantima/OpenAlexCollaborations/IT/stability/"

size_distribution_statics_output_file = "community_quantile_size_distribution.csv"

# Community quantiles to compute
quantiles = [25, 50, 60, 70, 80, 90, 95, 99]

# -----------------------------
# END Configuration parameters
# -----------------------------

def load_communities(community_directory: str) -> dict:
        
    loaded_communities = {}
    pattern = re.compile(r'(?<=_)(\d{4})(?:_(\d{4}))?(?=_)')
    print(f"Loading communities from directory: {community_directory}")
    for file in os.listdir(community_directory):
        if file.endswith(".pkl"):
            with open(os.path.join(community_directory, file), "rb") as f:
                match = pattern.search(file)
                if match:
                    start_year, _ = match.groups()
                    loaded_communities[start_year] = pickle.load(f)[0]
                    print(f"\t {start_year}: {len(loaded_communities[start_year])} communities.")
    print("All communities loaded.")
    return loaded_communities

def community_size_distribution(communities: dict, quantiles: list, output_file: str) -> dict:
    print("")
    sorted_keys = sorted(communities.keys())

    year_col_width = 8
    value_col_width = 10

    print("Community size distributions:")

    with open(output_file, "w") as f:
        # header: include an empty year cell
        writer = csv.writer(f)
        writer.writerow(["year"] + quantiles)

        print(
            f"{'':<{year_col_width}}"
            + "".join(f"{q:>{value_col_width}.0f} " for q in quantiles)
        )
        
        for year in sorted_keys:
            sizes = [len(c) for c in communities[year]]
            qs = np.percentile(sizes, quantiles)
            writer.writerow([year] + qs.tolist())
            print(
                f"{year:<{year_col_width}}"
                + "".join(f"{q:>{value_col_width}.2f} " for q in qs)
            )

def get_communities_over_99_percentile(communities: dict) -> dict:
    filtered_communities = {}
    sink_communities = {}
    sorted_keys = sorted(communities.keys())
    print("\nCommunities over 99th percentile:")
    for year in sorted_keys:
        sizes = [len(c) for c in communities[year]]
        threshold = np.percentile(sizes, 99)
        filtered_communities[year] = [c for c in communities[year] if len(c) >= threshold]
        sink_communities[year] = [c for c in communities[year] if len(c) < threshold]
        print(f"\t{year}: {len(filtered_communities[year])}")


def find_overlap(comm1, comm2, normalized=False):
    if normalized:
        return len(set(comm1).intersection(set(comm2))) / len(set(comm1)) #len(set(comm1).union(set(comm2)))

    return len(set(comm1).intersection(set(comm2)))

if __name__ == "__main__":
    communities = load_communities(community_pickle_directory)
    community_size_distribution(communities, quantiles, size_distribution_statics_output_file)
    get_communities_over_99_percentile(communities)