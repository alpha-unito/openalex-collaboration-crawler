"""
Average path length, diameter, clustering coefficient and degree assortativity.

All metrics are computed on the unweighted, undirected topology (self-loops and
duplicate edges are dropped):

- average local clustering coefficient and transitivity on the whole graph and
  on its largest connected component (LCC);
- degree assortativity on the whole graph and on the LCC;
- average shortest path length and diameter on the LCC only, since both are
  undefined (infinite) on a disconnected graph.

Backbones are always processed first; the full networks are processed afterwards
only if enabled (toml `include_full_network`, or --full / --no-full on the command
line). Within each group, input files are processed from the smallest to the largest.

Results are appended to the output CSV one graph at a time, and graphs already in
the CSV are skipped, so an interrupted run can be resumed.

Usage:
    python 13_path_clustering_assortativity.py [config.toml] [--full | --no-full]
"""
import argparse, os, sys, time, tomllib

import numpy as np
import pandas as pd
import rustworkx as rwx
import scipy.sparse as sp
from scipy.sparse.csgraph import connected_components, shortest_path

parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[1], formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("config", nargs="?", default="default.toml", help="TOML configuration file (default: default.toml)")
full_group = parser.add_mutually_exclusive_group()
full_group.add_argument("--full", dest="full", action="store_true", default=None, help="also process the full networks (overrides the toml)")
full_group.add_argument("--no-full", dest="full", action="store_false", help="process only the backbones (overrides the toml)")
args = parser.parse_args()

print("Parsing {} configuration file".format(args.config))
with open(args.config, 'rb') as f:
    configuration = tomllib.load(f)

try:
    base_path                   = configuration["workflow_data"] + "/" + configuration["country"] + "/"
    full_graph_directory        = base_path + configuration["backbones"]["inputs"]["graph_directory"]
    backbone_graph_directory    = base_path + configuration["backbones"]["outputs"]["backbone_directory"]
    step_config                 = configuration["path_clustering_statistics"]
    output_stats_file           = configuration["statistics_out_basedir"] + "/" + step_config["outputs"]["stats_file"]
    include_full_network        = step_config["include_full_network"]
    apl_sample_sources          = step_config["apl_sample_sources"]
    random_seed                 = step_config["random_seed"]
    os.makedirs(configuration["statistics_out_basedir"], exist_ok=True)
except Exception as e:
    print("Error: key {} not found".format(e))
    exit(-1)

if args.full is not None:
    include_full_network = args.full

print(f"\n{'=' * 60}")
print(f"{' PATH, CLUSTERING AND ASSORTATIVITY CONFIGURATION '.center(60, ' ')}")
print(f"{'=' * 60}")
print(f"\n[DATA SOURCE]")
print(f"  Backbone Directory:       {backbone_graph_directory}")
print(f"  Full Network Directory:   {full_graph_directory}")
print(f"  Include Full Networks:    {include_full_network}")
print(f"\n[PARAMETERS]")
print(f"  APL Sampled Sources:      {apl_sample_sources if apl_sample_sources > 0 else 'exact (all nodes)'}")
print(f"  Random Seed:              {random_seed}")
print(f"\n[ANALYSIS OUTPUTS]")
print(f"  Stats File:               {output_stats_file}")
print(f"\n{'=' * 60}\n")


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def load_adjacency(path):
    """
    Read an edge list (first two columns are the endpoints; the backbone files
    have a header, the full networks do not) into a symmetric, binary CSR matrix
    without self-loops.
    """
    with open(path) as f:
        first = f.readline().strip().split(",")
    has_header = first[0] == "source"

    edges = pd.read_csv(path, header=0 if has_header else None, usecols=[0, 1], dtype=str)
    src, dst = edges.iloc[:, 0].to_numpy(), edges.iloc[:, 1].to_numpy()

    codes, _ = pd.factorize(np.concatenate([src, dst]))
    u, v = codes[:len(src)], codes[len(src):]
    n = int(codes.max()) + 1

    keep = u != v
    u, v = u[keep], v[keep]
    adj = sp.coo_matrix((np.ones(2 * len(u), dtype=np.int64), (np.concatenate([u, v]), np.concatenate([v, u]))), shape=(n, n)).tocsr()
    adj.sum_duplicates()
    adj.data[:] = 1
    return adj


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def triangles_per_node(adj):
    """
    Number of triangles each node belongs to.

    Edges are oriented from lower to higher (degree, index) rank, which keeps the
    sparse products small even with high-degree hubs. With L the oriented
    adjacency matrix, a triangle a < b < c is counted
      - for its lowest node a by  rowsum((L @ L) * L),
      - for b and c by the row and column sums of (L.T @ L) * L.
    """
    n = adj.shape[0]
    deg = np.diff(adj.indptr)
    rank = np.empty(n, dtype=np.int64)
    rank[np.lexsort((np.arange(n), deg))] = np.arange(n)

    coo = adj.tocoo()
    keep = rank[coo.row] < rank[coo.col]
    lower = sp.csr_matrix((np.ones(keep.sum(), dtype=np.int64), (coo.row[keep], coo.col[keep])), shape=(n, n))

    q = (lower @ lower).multiply(lower)
    p = (lower.T.tocsr() @ lower).multiply(lower)

    tri = np.asarray(q.sum(axis=1)).ravel()
    tri += np.asarray(p.sum(axis=1)).ravel()
    tri += np.asarray(p.sum(axis=0)).ravel()
    return tri


def clustering(adj, tri):
    """Average local clustering (nodes with degree < 2 count as 0, as in networkx) and transitivity."""
    deg = np.diff(adj.indptr).astype(np.float64)
    pairs = deg * (deg - 1) / 2
    local = np.divide(tri, pairs, out=np.zeros_like(pairs), where=pairs > 0)
    transitivity = tri.sum() / pairs.sum() if pairs.sum() > 0 else 0.0
    return local.mean(), transitivity


def degree_assortativity(adj):
    """Pearson correlation between the degrees at the two ends of each edge."""
    deg = np.diff(adj.indptr).astype(np.float64)
    coo = adj.tocoo()
    x, y = deg[coo.row], deg[coo.col]
    if x.std() == 0:
        return float("nan")
    return float(np.corrcoef(x, y)[0, 1])


def bfs_distances(adj, sources):
    return shortest_path(adj, method="D", directed=False, unweighted=True, indices=sources)


def average_path_length(adj, sample_sources, rng):
    """
    Average shortest path length of a connected graph. Exact (multithreaded BFS
    from every node, via rustworkx) when sample_sources is 0 or not smaller than
    the number of nodes, otherwise estimated from BFS runs from that many uniformly
    sampled source nodes.
    """
    n = adj.shape[0]
    if n < 2:
        return 0.0, n

    if sample_sources <= 0 or sample_sources >= n:
        graph = rwx.PyGraph(multigraph=False)
        graph.add_nodes_from(range(n))
        coo = sp.triu(adj, k=1).tocoo()
        graph.add_edges_from_no_data(list(zip(coo.row.tolist(), coo.col.tolist())))
        return rwx.unweighted_average_shortest_path_length(graph), n

    sources = rng.choice(n, size=sample_sources, replace=False)
    total = 0.0
    for batch in np.array_split(sources, max(1, len(sources) // 64)):
        total += bfs_distances(adj, batch).sum()
    return total / (len(sources) * (n - 1)), len(sources)


def diameter(adj):
    """
    Exact diameter of a connected graph with the BoundingDiameters algorithm
    (Takes & Kosters, 2011): BFS runs from carefully chosen nodes tighten lower and
    upper bounds on every node's eccentricity until the diameter is pinned down.
    Usually needs only a handful of BFS runs on real-world networks.
    """
    n = adj.shape[0]
    if n < 2:
        return 0, 0

    deg = np.diff(adj.indptr)
    ecc_lower = np.zeros(n, dtype=np.int64)
    ecc_upper = np.full(n, np.iinfo(np.int64).max, dtype=np.int64)
    candidates = np.ones(n, dtype=bool)
    lower_bound = 0
    pick_high = True
    current = int(np.argmax(deg))
    bfs_runs = 0

    while candidates.any():
        dist = bfs_distances(adj, [current])[0].astype(np.int64)
        bfs_runs += 1
        ecc = int(dist.max())
        lower_bound = max(lower_bound, ecc)

        ecc_lower = np.maximum(ecc_lower, np.maximum(dist, ecc - dist))
        ecc_upper = np.minimum(ecc_upper, ecc + dist)
        upper_bound = int(ecc_upper[candidates].max())

        candidates &= ~((ecc_upper <= lower_bound) & (2 * ecc_lower >= upper_bound))
        candidates &= ecc_lower != ecc_upper
        candidates[current] = False

        if not candidates.any():
            break

        idx = np.flatnonzero(candidates)
        if pick_high:
            # Highest upper bound, ties broken by highest degree.
            current = int(idx[np.lexsort((-deg[idx], -ecc_upper[idx]))[0]])
        else:
            # Lowest lower bound, ties broken by highest degree.
            current = int(idx[np.lexsort((-deg[idx], ecc_lower[idx]))[0]])
        pick_high = not pick_high

    return lower_bound, bfs_runs


def compute_stats(adj, network, graph_name, rng):
    stats = {"network": network, "graph_name": graph_name}

    stats["number_of_nodes"] = adj.shape[0]
    stats["number_of_edges"] = adj.nnz // 2

    t = time.time()
    tri = triangles_per_node(adj)
    stats["avg_clustering"], stats["transitivity"] = clustering(adj, tri)
    stats["degree_assortativity"] = degree_assortativity(adj)
    print(f"  clustering and assortativity: {time.time() - t:.1f}s")

    _, labels = connected_components(adj, directed=False)
    lcc_nodes = np.flatnonzero(labels == np.argmax(np.bincount(labels)))
    lcc = adj[lcc_nodes][:, lcc_nodes].tocsr()
    lcc_tri = tri[lcc_nodes]  # triangles never cross components

    stats["lcc_number_of_nodes"] = lcc.shape[0]
    stats["lcc_number_of_edges"] = lcc.nnz // 2
    stats["lcc_avg_clustering"], stats["lcc_transitivity"] = clustering(lcc, lcc_tri)
    stats["lcc_degree_assortativity"] = degree_assortativity(lcc)

    t = time.time()
    stats["lcc_avg_path_length"], stats["lcc_apl_sources"] = average_path_length(lcc, apl_sample_sources, rng)
    stats["lcc_apl_exact"] = stats["lcc_apl_sources"] == lcc.shape[0]
    print(f"  average path length: {time.time() - t:.1f}s")

    t = time.time()
    stats["lcc_diameter"], bfs_runs = diameter(lcc)
    print(f"  diameter: {time.time() - t:.1f}s ({bfs_runs} BFS runs)")

    return stats


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def graphs_by_size(directory):
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(".csv")]
    return sorted(files, key=os.path.getsize)


jobs = [("backbone", backbone_graph_directory)]
if include_full_network:
    jobs.append(("full", full_graph_directory))

done = set()
if os.path.exists(output_stats_file):
    previous = pd.read_csv(output_stats_file)
    done = set(zip(previous["network"], previous["graph_name"]))

rng = np.random.default_rng(random_seed)

for network, directory in jobs:
    for path in graphs_by_size(directory):
        graph_name = os.path.splitext(os.path.basename(path))[0]

        if (network, graph_name) in done:
            print(f"[{network}] {graph_name}: already in {output_stats_file} — skipping")
            continue

        print(f"[{network}] {graph_name} ({os.path.getsize(path) / 1e6:.1f} MB)")
        t = time.time()
        adj = load_adjacency(path)
        print(f"  loaded {adj.shape[0]} nodes, {adj.nnz // 2} edges in {time.time() - t:.1f}s")

        stats = compute_stats(adj, network, graph_name, rng)
        print("  " + ", ".join(f"{k}={v:.4g}" if isinstance(v, float) else f"{k}={v}" for k, v in stats.items() if k.startswith(("avg", "trans", "degree", "lcc_avg", "lcc_diam"))))

        pd.DataFrame([stats]).to_csv(output_stats_file, mode="a", header=not os.path.exists(output_stats_file), index=False)
        done.add((network, graph_name))
        sys.stdout.flush()
