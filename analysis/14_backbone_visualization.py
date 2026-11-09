"""
Backbone visualization (NetworkX + matplotlib)
==============================================

Draws, for every time interval in the configuration, the disparity-filter backbone produced by
03_backbone.py, colored by the Louvain communities produced by 06_community_stability.py.
The method is made of five stages.

1. Inputs
---------
  * Backbone edge list:   <workflow_data>/<country>/<backbones.outputs.backbone_directory>/*<start>_<end>*.csv
                          (source, target, weight, p_value).
  * Communities:          <workflow_data>/<country>/<community_stability.outputs.communities_output_folder>,
                          first partition of the stability runs, i.e. the reference partition that
                          07_community_flow.py, 10_community_labelling.py and 11_community_flow_visualization.py
                          also use.
  * Community names:      <statistics_out_basedir>/community_names_<start>_<end>.csv from
                          10_community_labelling.py. Names are indexed by the position of the community in
                          the list of communities above the `community_flow.flow_percentile` size percentile,
                          and the `selected` column is honoured exactly as in 11_community_flow_visualization.py.

2. Node selection (what is plotted)
-----------------------------------
  * The top-N communities by number of authors (N = `backbone_visualization.top_communities`, default 10)
    are always plotted.
  * Any other community is plotted only if at least one backbone edge connects one of its authors to an
    author of a top-N community. Such a community is plotted in full (all of its authors), so the bridges
    between the top communities remain visible.
  * Every other author is dropped. The plotted graph is the subgraph of the backbone induced by the
    selected authors.

3. Hub collapsing (star simplification)
---------------------------------------
  Backbones contain many star-shaped communities (one author linked to almost everyone else in the
  group, e.g. large experimental collaborations). Drawn node by node they produce dense "sunbursts"
  that hide the structure, so they are collapsed:
  * For every plotted community C with at least `collapse_min_community_size` authors, the hub h is the
    author with the most neighbours inside C.
  * If h is connected to MORE than `collapse_threshold` (default 0.9 = 90%) of the other authors of C,
    h and all of its neighbours inside C are merged into a single node. Authors of C that are not
    adjacent to h (at most 10%) stay as individual nodes. At most one collapse per community, and only
    within the hub's own community, so a collapsed node never mixes two communities.
  * The collapsed node carries:
      - members:  number of authors merged into it;
      - degree:   sum of the backbone degrees of all merged authors (it has "as many connections" as
                  its authors together);
      - edges:    a SINGLE edge towards each distinct neighbour (parallel edges coming from different
                  members are merged; their weights are summed in the `weight` attribute and their
                  count is kept in `multiplicity`, but only one line is drawn).
  * A CSV with one row per collapsed node (community, hub, members, coverage, summed degree, distinct
    neighbours) is written next to the plots: backbone_collapsed_nodes_<start>_<end>.csv.

4. Layout
---------
  A single global force-directed layout of thousands of nodes collapses into an unreadable hairball,
  so the layout is built in layers. Every node has a "mass" equal to the number of authors it
  represents (1 for a plain author, `members` for a collapsed node); areas are proportional to mass so
  that a collapsed node takes the room its authors would have taken.
  * Components: the plotted graph is split into connected components, each component is laid out on its
    own, and the components are shelf-packed in rows, largest first.
  * Small components (<= KK_MAX_NODES nodes): Kamada-Kawai (stress minimisation), which draws trees,
    stars and small cliques cleanly.
  * Large components: community-aware (hierarchical) layout.
      a. Every community becomes a super-node; the super-graph is weighted by the number of edges between
         communities and laid out with Fruchterman-Reingold, so strongly linked communities sit close.
      b. Each community is laid out internally (step above) inside a disc whose area is proportional to
         its total mass.
      c. The discs are pushed apart until they no longer overlap, keeping the relative arrangement.
  * Space around collapsed nodes: a collapsed node is drawn as a disc of radius ~ sqrt(members); nodes
    that would fall inside that disc are pushed radially out to its rim.
  * Outliers flung far from their community by a huge hub are pulled back onto the community disc rim
    (a slight distortion of their true position).
  Positions are cached in <workflow_data>/<country>/<layout_directory> (one pickle per interval, top-N
  and collapse threshold); set `recompute_layout = true` to force a new layout.

5. Rendering
------------
  * Top-N communities use a fixed 10-slot categorical palette (8 validated reference slots + 2 added and
    re-validated for color-vision deficiency), assigned by size rank. Linked communities are gray.
  * Edges inside a top-N community take the community color; all other edges (between communities, or
    inside gray ones) are light gray and drawn underneath.
  * Plain authors: marker area grows with log(degree). Collapsed nodes: filled disc with a white outline
    whose area is the area of a plain author marker times the number of merged authors (capped by the
    room the layout reserved for it, and at least 3x the largest plain author), drawn under the plain
    authors so the remaining members stay visible.
  * Marker and line sizes scale with drawing density (points per layout unit), so small and large
    backbones are equally readable.
  * The legend lists every top community by rank, name and number of authors; no labels are drawn on the
    plot. The subtitle reports authors, nodes, edges, components, and the number of linked communities
    and collapsed hubs.
  * Output: <statistics_out_basedir>/<plot_prefix>_<start>_<end>.pdf (marks rasterised at 300 dpi,
    text stays vector).

Usage:  python 14_backbone_visualization.py [config.toml]
"""

import csv
import glob
import os
import pickle
import sys
import tomllib

import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd
from matplotlib.collections import LineCollection
from matplotlib.lines import Line2D

# ============================================================
# Configuration
# ============================================================
toml_config_path = sys.argv[1] if len(sys.argv) > 1 else "default.toml"
print(f"Parsing {toml_config_path} configuration file")
with open(toml_config_path, "rb") as f:
    configuration = tomllib.load(f)

try:
    country_dir = os.path.join(configuration["workflow_data"], configuration["country"])
    backbone_directory = os.path.join(country_dir, configuration["backbones"]["outputs"]["backbone_directory"])
    community_pickle_directory = os.path.join(country_dir, configuration["community_stability"]["outputs"]["communities_output_folder"])
    statistics_out_basedir = configuration["statistics_out_basedir"]
    flow_percentile = configuration["community_flow"]["flow_percentile"]
    time_intervals = [tuple(interval) for interval in configuration["time_intervals"]]
except KeyError as e:
    print(f"Error: key {e} not found")
    exit(-1)

viz_cfg = configuration.get("backbone_visualization", {})
TOP_N = viz_cfg.get("top_communities", 10)
COLLAPSE_THRESHOLD = viz_cfg.get("collapse_threshold", 0.9)
COLLAPSE_MIN_SIZE = viz_cfg.get("collapse_min_community_size", 3)
SEED = viz_cfg.get("seed", 42)
FORCE_LAYOUT = viz_cfg.get("recompute_layout", False)
layout_directory = os.path.join(country_dir, viz_cfg.get("outputs", {}).get("layout_directory", "backbone_layouts"))
plot_prefix = viz_cfg.get("outputs", {}).get("plot_prefix", "backbone_visualization")

os.makedirs(statistics_out_basedir, exist_ok=True)
os.makedirs(layout_directory, exist_ok=True)

print(f"\n{'=' * 60}")
print(f"{' BACKBONE VISUALIZATION CONFIGURATION '.center(60, ' ')}")
print(f"{'=' * 60}")
print(f"\n[INPUTS]")
print(f"  Backbone Directory:     {backbone_directory}")
print(f"  Communities Pickle:     {community_pickle_directory}")
print(f"\n[SETTINGS]")
print(f"  Top communities:        {TOP_N}")
print(f"  Collapse threshold:     > {COLLAPSE_THRESHOLD:.0%} of a community (min size {COLLAPSE_MIN_SIZE})")
print(f"  Naming percentile:      {flow_percentile}")
print(f"  Seed:                   {SEED}")
print(f"\n[OUTPUTS]")
print(f"  Layout cache:           {layout_directory}")
print(f"  Plots:                  {statistics_out_basedir}/{plot_prefix}_<start>_<end>.pdf")
print(f"  Collapsed nodes:        {statistics_out_basedir}/backbone_collapsed_nodes_<start>_<end>.csv")
print(f"\n{'=' * 60}\n")

# ============================================================
# Style
# ============================================================
# Categorical palette: slots 1-8 are the validated reference palette, slots 9-10 were
# added and re-validated (CVD adjacent dE >= 9.1, normal-vision >= 17.8). Some slots are
# below 3:1 contrast on white, so the legend lists every colored community by rank and name.
TOP_COLORS = [
    "#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4",
    "#008300", "#6250d6", "#e34948", "#a8338f", "#0f9fb8",
]
OTHER_NODE_COLOR = "#a9a9a6"
OTHER_EDGE_COLOR = "#c3c2b7"
TEXT_PRIMARY = "#1f1f1e"
TEXT_SECONDARY = "#5f5e5a"
SURFACE = "#ffffff"

KK_MAX_NODES = 200        # components up to this size use Kamada-Kawai
COMPONENT_PADDING = 1.2   # gap between packed components (layout units ~ node spacing)
COMMUNITY_PADDING = 1.5   # gap between community discs inside a large component
NODE_ROOM = 0.6           # free space kept around a collapsed node's disc


# ============================================================
# Loading
# ============================================================
def load_backbone(path: str) -> nx.Graph:
    data = pd.read_csv(path, dtype={0: str, 1: str})
    data = data.iloc[:, :3]
    data.columns = ["source", "target", "weight"]
    graph = nx.from_pandas_edgelist(data, "source", "target", edge_attr="weight")
    print(f"\tLoaded {path}: {graph.number_of_nodes()} nodes, {graph.number_of_edges()} edges")
    return graph


def load_communities(start, end) -> list:
    matches = glob.glob(os.path.join(community_pickle_directory, f"*_{start}_{end}_*.pkl"))
    if not matches:
        raise FileNotFoundError(f"No community pickle for {start}-{end} in {community_pickle_directory}")
    with open(matches[0], "rb") as f:
        data = pickle.load(f)
    # Stability pickles hold one partition per run: the first one is the reference partition
    communities = data[0] if isinstance(data, list) and data and not isinstance(data[0], (set, frozenset)) else data
    return [set(c) for c in communities]


def load_community_names(start, end) -> dict:
    """Map community_id -> name, same selection logic as 11_community_flow_visualization.py"""
    names = {}
    path = os.path.join(statistics_out_basedir, f"community_names_{start}_{end}.csv")
    if not os.path.exists(path):
        print(f"\tWarning: {path} not found, communities will be unnamed")
        return names
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            selected = row.get("selected", row.get("selected_name", "")).strip()
            choices = [row.get("generated_name", ""), *(row.get(f"candidate_{i}", "") for i in range(1, 4))]
            if selected in {"1", "2", "3", "4"}:
                name = choices[int(selected) - 1] or row["candidate_1"]
            else:
                name = selected or row.get("generated_name", "") or row["candidate_1"]
            names[int(row["community_id"])] = name
    return names


def select_top_communities(communities: list, names: dict, top_n: int) -> list:
    """
    Return [(rank, community_index, members, label)] for the top_n communities by size.
    The community ids used for naming are the positions in the list of communities above
    the flow percentile (same indexing as 07_community_flow.py / 10_community_labelling.py).
    """
    sizes = [len(c) for c in communities]
    threshold = np.percentile(sizes, flow_percentile)
    named_ids = {}
    for community_id, idx in enumerate(i for i, c in enumerate(communities) if len(c) >= threshold):
        named_ids[idx] = community_id

    ranked = sorted(range(len(communities)), key=lambda i: len(communities[i]), reverse=True)[:top_n]
    top = []
    for rank, idx in enumerate(ranked):
        community_id = named_ids.get(idx)
        label = names.get(community_id, f"Community {community_id}" if community_id is not None else "Unnamed community")
        top.append((rank, idx, communities[idx], label))
    return top


# ============================================================
# Node selection and hub collapsing
# ============================================================
def select_plotted_subgraph(graph: nx.Graph, communities: list, node_to_community: dict, top: list):
    """
    Keep the top communities plus every other community with at least one edge to a top community.
    Returns the induced subgraph and the number of connected (gray) communities kept.
    """
    top_nodes = set().union(*(members for _, _, members, _ in top)) & set(graph.nodes)
    connected = {
        node_to_community[v]
        for u in top_nodes for v in graph[u]
        if v not in top_nodes and v in node_to_community
    }
    kept_nodes = top_nodes.union(*(communities[idx] for idx in connected)) & set(graph.nodes)
    filtered = graph.subgraph(kept_nodes).copy()
    print(f"\tKeeping top {len(top)} communities ({len(top_nodes)} authors) + {len(connected)} connected "
          f"communities: {filtered.number_of_nodes()}/{graph.number_of_nodes()} authors, "
          f"{filtered.number_of_edges()}/{graph.number_of_edges()} edges")
    return filtered, len(connected)


def collapse_hubs(graph: nx.Graph, full_graph: nx.Graph, node_to_community: dict):
    """
    Merge every community hub connected to > COLLAPSE_THRESHOLD of its community, together with
    those neighbours, into one node. Returns (collapsed graph, list of collapse records).

    Node attributes of the returned graph: community, members (#authors), degree (sum of the
    full-backbone degrees of its authors), collapsed (bool). Parallel edges are merged into one,
    with summed `weight` and `multiplicity`.
    """
    by_community = {}
    for node in graph:
        by_community.setdefault(node_to_community.get(node, ("solo", node)), set()).add(node)

    representative, records = {}, []
    for community, nodes in by_community.items():
        if len(nodes) < COLLAPSE_MIN_SIZE:
            continue
        inner = {node: set(graph[node]) & nodes for node in nodes}
        hub = max(nodes, key=lambda node: (len(inner[node]), full_graph.degree(node)))
        others = len(nodes) - 1
        if len(inner[hub]) <= COLLAPSE_THRESHOLD * others:
            continue
        merged = {hub} | inner[hub]
        super_id = f"collapsed:{community}"
        for node in merged:
            representative[node] = super_id
        records.append({
            "id": super_id, "community": community, "hub": hub, "members": len(merged),
            "community_size": len(nodes), "coverage": len(inner[hub]) / others,
            "summed_degree": sum(full_graph.degree(node) for node in merged),
        })

    collapsed = nx.Graph()
    for node in graph:
        rep = representative.get(node, node)
        if rep not in collapsed:
            collapsed.add_node(rep, community=node_to_community.get(node, ("solo", node)),
                               members=0, degree=0, collapsed=rep != node)
        collapsed.nodes[rep]["members"] += 1
        collapsed.nodes[rep]["degree"] += full_graph.degree(node)

    for u, v, data in graph.edges(data=True):
        ru, rv = representative.get(u, u), representative.get(v, v)
        if ru == rv:
            continue
        if collapsed.has_edge(ru, rv):
            collapsed[ru][rv]["weight"] += data.get("weight", 1)
            collapsed[ru][rv]["multiplicity"] += 1
        else:
            collapsed.add_edge(ru, rv, weight=data.get("weight", 1), multiplicity=1)

    for record in records:
        record["distinct_neighbours"] = collapsed.degree(record["id"])
    print(f"\tCollapsed {len(records)} hubs: {graph.number_of_nodes()} -> {collapsed.number_of_nodes()} nodes, "
          f"{graph.number_of_edges()} -> {collapsed.number_of_edges()} edges")
    return collapsed, records


def dump_collapse_records(records: list, top: list, output_path: str):
    rank_of = {idx: rank for rank, idx, _, _ in top}
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["community_rank", "community_index", "hub", "collapsed_authors", "community_size",
                         "hub_coverage", "summed_degree", "distinct_neighbours"])
        for r in sorted(records, key=lambda r: -r["members"]):
            rank = rank_of.get(r["community"])
            writer.writerow([rank + 1 if rank is not None else "other", r["community"], r["hub"], r["members"],
                             r["community_size"], f"{r['coverage']:.4f}", r["summed_degree"],
                             r["distinct_neighbours"]])
    print(f"\tCollapsed nodes written to {output_path}")


# ============================================================
# Layout
# ============================================================
def _target_radius(mass: float) -> float:
    # constant density: area ~ number of authors represented
    return 0.55 * np.sqrt(mass)


def _draw_radius(mass: float) -> float:
    """Radius (layout units) of the disc drawn for a collapsed node; 0 for a plain author."""
    return 0.3 * np.sqrt(mass) if mass > 1 else 0.0


def _normalize(pos: dict, target_radius: float) -> dict:
    """Center a layout and scale it so that its 95th-percentile radius equals target_radius."""
    nodes = list(pos)
    xy = np.array([pos[n] for n in nodes], dtype=float)
    xy -= xy.mean(axis=0)
    radius = np.percentile(np.linalg.norm(xy, axis=1), 95) if len(nodes) > 1 else 0.0
    if radius > 0:
        xy *= target_radius / radius
        # pull far outliers (e.g. leaves flung away from a huge hub) back onto the disc rim
        norms = np.linalg.norm(xy, axis=1)
        limit = 1.1 * target_radius
        far = norms > limit
        xy[far] *= (limit / norms[far])[:, None]
    return dict(zip(nodes, xy))


def _make_room(pos: dict, mass: dict) -> dict:
    """Push nodes lying inside a collapsed node's disc radially out to its rim."""
    big = sorted((n for n in pos if mass[n] > 1), key=lambda n: -mass[n])
    if not big:
        return pos
    nodes = list(pos)
    xy = np.array([pos[n] for n in nodes], dtype=float)
    radius = np.array([_draw_radius(mass[n]) for n in nodes])
    index = {n: i for i, n in enumerate(nodes)}
    rng = np.random.default_rng(SEED)
    for c in big:
        i = index[c]
        diff = xy - xy[i]
        dist = np.linalg.norm(diff, axis=1)
        need = radius[i] + radius + NODE_ROOM
        inside = (dist < need)
        inside[i] = False
        if not inside.any():
            continue
        zero = inside & (dist < 1e-9)
        diff[zero] = rng.normal(size=(zero.sum(), 2))
        dist[zero] = np.linalg.norm(diff[zero], axis=1)
        xy[inside] = xy[i] + diff[inside] / dist[inside, None] * need[inside, None]
    return dict(zip(nodes, xy))


def _small_layout(graph: nx.Graph, mass: dict) -> dict:
    n = graph.number_of_nodes()
    if n == 1:
        return {next(iter(graph)): np.zeros(2)}
    if n == 2:
        a, b = graph.nodes
        gap = 0.5 + (_draw_radius(mass[a]) + _draw_radius(mass[b])) / 2
        return {a: np.array([-gap, 0.0]), b: np.array([gap, 0.0])}
    if nx.is_connected(graph) and n <= KK_MAX_NODES:
        pos = nx.kamada_kawai_layout(graph, weight=None)
    else:
        pos = nx.spring_layout(graph, seed=SEED, iterations=100)
    # spread the nodes at plain-author density, then open room around the collapsed discs
    pos = _normalize(pos, _target_radius(n))
    return _make_room(pos, mass)


def _extent_radius(pos: dict, mass: dict) -> float:
    return max(np.linalg.norm(xy) + _draw_radius(mass[n]) for n, xy in pos.items()) + 0.5


def _remove_overlaps(centers: np.ndarray, radii: np.ndarray, iterations: int = 500) -> np.ndarray:
    """Push circles apart until they do not overlap (keeps the relative arrangement)."""
    centers = centers.copy()
    for _ in range(iterations):
        diff = centers[:, None, :] - centers[None, :, :]
        dist = np.linalg.norm(diff, axis=2)
        np.fill_diagonal(dist, np.inf)
        overlap = radii[:, None] + radii[None, :] + COMMUNITY_PADDING - dist
        overlap[overlap < 0] = 0
        if not overlap.any():
            break
        dist[np.isinf(dist)] = 1.0
        dist[dist == 0] = 1e-6
        push = (diff / dist[..., None]) * (overlap[..., None] / 2)
        centers += push.sum(axis=1) * 0.5
    return centers


def _community_layout(graph: nx.Graph, mass: dict) -> dict:
    """Hierarchical layout: super-graph of communities, then each community inside its own disc."""
    groups = {}
    for node in graph:
        groups.setdefault(graph.nodes[node]["community"], []).append(node)
    group_keys = list(groups)
    group_index = {g: i for i, g in enumerate(group_keys)}

    super_graph = nx.Graph()
    super_graph.add_nodes_from(range(len(group_keys)))
    for u, v, data in graph.edges(data=True):
        gu, gv = group_index[graph.nodes[u]["community"]], group_index[graph.nodes[v]["community"]]
        if gu != gv:
            w = super_graph.get_edge_data(gu, gv, {"weight": 0})["weight"]
            super_graph.add_edge(gu, gv, weight=w + data.get("multiplicity", 1))

    # internal layouts first, so that every disc radius is the real extent of its community
    local_layouts = []
    for g in group_keys:
        local = _small_layout(graph.subgraph(groups[g]), mass)
        center = np.mean(list(local.values()), axis=0)
        local_layouts.append({n: xy - center for n, xy in local.items()})
    radii = np.array([_extent_radius(local, mass) for local in local_layouts])

    super_pos = nx.spring_layout(super_graph, weight="weight", seed=SEED, iterations=300)
    centers = np.array([super_pos[i] for i in range(len(group_keys))])
    # scale so that the super layout roughly has room for the discs, then resolve overlaps
    centers -= centers.mean(axis=0)
    spread = np.percentile(np.linalg.norm(centers, axis=1), 95) or 1.0
    centers *= np.sqrt((radii ** 2).sum()) / spread
    centers = _remove_overlaps(centers, radii)

    pos = {}
    for i, local in enumerate(local_layouts):
        for node, xy in local.items():
            pos[node] = xy + centers[i]
    return pos


def _pack_components(layouts: list, mass: dict) -> dict:
    """Shelf-pack component layouts (already sorted by size, largest first)."""
    boxes = []
    for pos in layouts:
        xy = np.array(list(pos.values()))
        r = np.array([_draw_radius(mass[n]) for n in pos])[:, None]
        lo, hi = (xy - r).min(axis=0), (xy + r).max(axis=0)
        boxes.append((lo, hi - lo))
    total_area = sum((w + COMPONENT_PADDING) * (h + COMPONENT_PADDING) for _, (w, h) in boxes)
    row_width = max(boxes[0][1][0], np.sqrt(total_area) * 1.15)

    final = {}
    x = y = row_height = 0.0
    for pos, (lo, (w, h)) in zip(layouts, boxes):
        if x > 0 and x + w > row_width:
            x, y = 0.0, y - row_height - COMPONENT_PADDING
            row_height = 0.0
        offset = np.array([x, y - h]) - lo
        for node, xy in pos.items():
            final[node] = xy + offset
        x += w + COMPONENT_PADDING
        row_height = max(row_height, h)
    return final


def compute_layout(graph: nx.Graph) -> dict:
    mass = nx.get_node_attributes(graph, "members")
    components = sorted(nx.connected_components(graph), key=lambda c: sum(mass[n] for n in c), reverse=True)
    print(f"\t{len(components)} connected components (largest: {len(components[0])} nodes)")

    layouts = []
    for component in components:
        sub = graph.subgraph(component)
        if len(component) > KK_MAX_NODES:
            print(f"\t\tCommunity-aware layout for component with {len(component)} nodes")
            layouts.append(_community_layout(sub, mass))
        else:
            layouts.append(_small_layout(sub, mass))
    return _pack_components(layouts, mass)


def get_layout(graph: nx.Graph, start, end) -> dict:
    cache = os.path.join(layout_directory, f"layout_{start}_{end}_top{TOP_N}_c{COLLAPSE_THRESHOLD:g}.pkl")
    if os.path.exists(cache) and not FORCE_LAYOUT:
        with open(cache, "rb") as f:
            pos = pickle.load(f)
        if set(pos) == set(graph.nodes):
            print(f"\tLoaded cached layout {cache}")
            return pos
    pos = compute_layout(graph)
    with open(cache, "wb") as f:
        pickle.dump(pos, f)
    print(f"\tLayout cached to {cache}")
    return pos


# ============================================================
# Drawing
# ============================================================
def draw_backbone(graph: nx.Graph, pos: dict, top: list, start, end, output_path: str, subtitle_note: str):
    rank_of = {idx: rank for rank, idx, _, _ in top}

    nodes = list(graph.nodes)
    xy = np.array([pos[n] for n in nodes])
    ranks = np.array([rank_of.get(graph.nodes[n]["community"], -1) for n in nodes])
    members = np.array([graph.nodes[n]["members"] for n in nodes])
    degree = np.array([graph.nodes[n]["degree"] for n in nodes])
    is_collapsed = np.array([graph.nodes[n]["collapsed"] for n in nodes], dtype=bool)

    radii = np.array([_draw_radius(m) if c else 0.0 for m, c in zip(members, is_collapsed)])
    lo, hi = (xy - radii[:, None]).min(axis=0), (xy + radii[:, None]).max(axis=0)
    extent = hi - lo
    width = 16
    height = float(np.clip(width * extent[1] / max(extent[0], 1e-9), 6, 40))

    # Scale marks with the drawing density: points per layout unit (1 unit ~ node spacing)
    points_per_unit = min(width * 72 / max(extent[0], 1e-9), height * 72 / max(extent[1], 1e-9))
    node_diameter = np.clip(0.35 * points_per_unit, 0.8, 6.0)
    sizes = (node_diameter * (0.7 + 0.25 * np.log1p(degree))) ** 2
    plain_max = sizes[~is_collapsed].max() if (~is_collapsed).any() else node_diameter ** 2
    # collapsed nodes: area = (plain author marker area) x (number of merged authors), never larger than
    # the room reserved for them by the layout, never smaller than 3x the largest plain author
    marker_area = node_diameter ** 2 * members[is_collapsed]
    layout_area = (2 * radii[is_collapsed] * points_per_unit) ** 2
    sizes[is_collapsed] = np.maximum(np.minimum(marker_area, layout_area), plain_max * 3.0)
    edge_width = float(np.clip(0.06 * points_per_unit, 0.15, 0.9))

    fig, ax = plt.subplots(figsize=(width, height + 1.8), facecolor=SURFACE)
    ax.set_facecolor(SURFACE)
    ax.set_aspect("equal")
    ax.axis("off")

    # Edges: gray ("Other" or between communities) below, intra-community colored edges above
    gray_segments, colored_segments = [], {rank: [] for rank, _, _, _ in top}
    for u, v in graph.edges():
        ru = rank_of.get(graph.nodes[u]["community"], -1)
        rv = rank_of.get(graph.nodes[v]["community"], -1)
        segment = (pos[u], pos[v])
        if ru == rv and ru >= 0:
            colored_segments[ru].append(segment)
        else:
            gray_segments.append(segment)

    ax.add_collection(LineCollection(gray_segments, colors=OTHER_EDGE_COLOR, linewidths=edge_width,
                                     alpha=0.55, zorder=1, rasterized=True))
    for rank, segments in colored_segments.items():
        if segments:
            ax.add_collection(LineCollection(segments, colors=TOP_COLORS[rank], linewidths=edge_width * 1.2,
                                             alpha=0.6, zorder=2, rasterized=True))

    def color_of(rank):
        return TOP_COLORS[rank] if rank >= 0 else OTHER_NODE_COLOR

    # collapsed nodes (discs) below the plain authors, largest first so small discs stay visible
    for i in sorted(np.flatnonzero(is_collapsed), key=lambda i: -sizes[i]):
        ax.scatter(xy[i, 0], xy[i, 1], s=sizes[i], c=color_of(ranks[i]), alpha=0.85,
                   edgecolors=SURFACE, linewidths=1.0, zorder=3, rasterized=True)

    other = (ranks < 0) & ~is_collapsed
    ax.scatter(xy[other, 0], xy[other, 1], s=sizes[other], c=OTHER_NODE_COLOR,
               linewidths=0, zorder=4, rasterized=True)
    for rank, _, _, _ in top:
        mask = (ranks == rank) & ~is_collapsed
        ax.scatter(xy[mask, 0], xy[mask, 1], s=sizes[mask], c=TOP_COLORS[rank],
                   linewidths=0, zorder=5, rasterized=True)

    ax.set_xlim(lo[0] - 1, hi[0] + 1)
    ax.set_ylim(lo[1] - 1, hi[1] + 1)

    handles = [
        Line2D([], [], marker="o", linestyle="", markersize=7, markerfacecolor=TOP_COLORS[rank],
               markeredgewidth=0, label=f"{rank + 1}. {label} ({len(community_members):,} authors)")
        for rank, _, community_members, label in top
    ]
    other_authors = int(members[ranks < 0].sum())
    handles.append(Line2D([], [], marker="o", linestyle="", markersize=7, markerfacecolor=OTHER_NODE_COLOR,
                          markeredgewidth=0, label=f"Connected communities ({other_authors:,} authors)"))
    if is_collapsed.any():
        handles.append(Line2D([], [], marker="o", linestyle="", markersize=13, markerfacecolor=OTHER_NODE_COLOR,
                              markeredgecolor=TEXT_SECONDARY, markeredgewidth=0.8, alpha=0.85,
                              label=f"Collapsed hub + its >{COLLAPSE_THRESHOLD:.0%} neighbours (area ∝ authors)"))
    fig.legend(handles=handles, loc="lower center", ncol=2, frameon=False, fontsize=11,
               labelcolor=TEXT_PRIMARY, bbox_to_anchor=(0.5, 0.0))

    n_components = nx.number_connected_components(graph)
    fig.suptitle(f"Backbone {start}–{end}", fontsize=20, fontweight="bold", color=TEXT_PRIMARY, y=0.995)
    ax.set_title(f"{int(members.sum()):,} authors in {graph.number_of_nodes():,} nodes · "
                 f"{graph.number_of_edges():,} edges · "
                 f"{n_components:,} component{'s' if n_components != 1 else ''} — {subtitle_note}",
                 fontsize=12, color=TEXT_SECONDARY, pad=8)

    fig.tight_layout(rect=(0, 1.6 / (height + 1.8), 1, 0.98))
    fig.savefig(output_path, dpi=300, facecolor=SURFACE)
    plt.close(fig)
    print(f"\tSaved → {output_path}")


# ============================================================
# Main
# ============================================================
if __name__ == "__main__":
    for start, end in time_intervals:
        print(f"\nProcessing interval {start}-{end}")
        matches = glob.glob(os.path.join(backbone_directory, f"*{start}_{end}*.csv"))
        if not matches:
            print(f"\tNo backbone found for {start}-{end}, skipping")
            continue

        full_graph = load_backbone(matches[0])
        communities = load_communities(start, end)
        node_to_community = {node: idx for idx, members in enumerate(communities) for node in members}
        names = load_community_names(start, end)
        top = select_top_communities(communities, names, TOP_N)

        plotted, n_connected = select_plotted_subgraph(full_graph, communities, node_to_community, top)
        graph, records = collapse_hubs(plotted, full_graph, node_to_community)
        dump_collapse_records(records, top, os.path.join(statistics_out_basedir,
                                                         f"backbone_collapsed_nodes_{start}_{end}.csv"))

        note = (f"top {len(top)} communities + {n_connected} linked to them, "
                f"{len(records)} hub{'s' if len(records) != 1 else ''} collapsed")
        pos = get_layout(graph, start, end)
        output_path = os.path.join(statistics_out_basedir, f"{plot_prefix}_{start}_{end}.pdf")
        draw_backbone(graph, pos, top, start, end, output_path, note)
