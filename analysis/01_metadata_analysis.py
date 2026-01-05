# -----------------------------
# Configuration and Input Paths
# -----------------------------

metadata_path = "/beegfs/home/msantima/OpenAlexCollaborations/IT/metadata_dataset.csv"
# Path to the metadata CSV file produced during the graph-generation stage.
# This file contains all publication- or work-level metadata used in the analysis.

analized_country = "Italy"
# Country to be analyzed. Only records associated with this country will be considered.

start_year = 1960  # inclusive
end_year = 2025  # exclusive
# Time window for the analysis. Only works published within [start_year, end_year) are used.

#intervals_years = None
intervals_years = [(1960, 1989), (1990, 1999), (2000, 2009), (2010, 2011), (2012, 2014), (2015, 2023),(2024, 2024)]
# List of year intervals for aggregating data. Each interval is a pair tuple of (start, end).
# If None, yearly intervals are used. Only affects CCDF computations.

max_topics = 8
# Maximum number of topics to consider when aggregating or visualizing topic distributions.

# ---------------------------------------
# Output Filenames for Datasets and Plots
# ---------------------------------------

works_per_year_plot_filename = "works_per_year.pdf"
# Output filename for the plot showing the number of works per year.

works_per_year_dataset = "works_per_year.csv"
# Output filename for the CSV dataset containing yearly work counts.

application_domain_plot_filename = "application_domains_over_time.pdf"
# Output filename for the plot tracking application-domain trends over time.

cs_topics_over_time_plot_filename = "cs_topics_over_time.pdf"
# Output filename for the plot tracking Computer Science topic trends over time.

ccdf_input_path = "/beegfs/home/msantima/openalex-collaboration-crawler/build"
# Datasets from which to compute CCDFs will be read.
ccdf_path = "/beegfs/home/msantima/OpenAlexCollaborations/IT/ccdf"
# Directory where CCDF (Complementary Cumulative Distribution Function) plots or data
# will be stored. This feature is still a work in progress.
ccdf_graph_output_filename = './ccdfs_year_by_year.pdf'

# ----------------------------------------------------
# External Mappings (Required for Topic Normalization)
# ----------------------------------------------------
# These mappings must be provided manually if analyzing fields outside Computer Science.

import json
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import os
import pandas as pd
import seaborn as sns
from collections import Counter

from mappings import topics_mapping, application_domains_mapping, application_domains_to_delete
from topic_to_category import topic_to_category

# Dictionary mapping fine-grained CS topics to broader topic categories.
# topics_mapping: Normalizes topic names and groups synonyms/variants.
# application_domains_mapping: Mapping to unify and filter application-domain labels.


colors = [
    "#1f77b4",  # blue
    "#ff7f0e",  # orange
    "#2ca02c",  # green
    "#d62728",  # red
    "#9467bd",  # purple
    "#8c564b",  # brown
    "#e377c2",  # pink
    "#7f7f7f",  # gray
    "#bcbd22",  # olive
    "#17becf",  # cyan

    "#393b79",  # dark blue
    "#637939",  # dark olive
    "#8c6d31",  # mustard
    "#843c39",  # dark red
    "#7b4173",  # plum
    "#3182bd",  # steel blue
    "#31a354",  # medium green
    "#756bb1",  # lavender
    "#636363",  # dark gray
    "#e6550d",  # burnt orange

    "#969696",  # light gray
    "#9c9ede",  # light purple
    "#cedb9c",  # pale green
    "#e7ba52",  # yellow
    "#e7969c",  # light red
    "#6baed6",  # light blue
    "#74c476",  # light green
    "#fd8d3c",  # light orange
    "#c49c94",  # tan
    "#bdbdbd",  # silver
]

data = {}
with open(metadata_path, 'r') as f:
    next(f)  # skip header

    for line in f:
        parts = line.strip().split(',')
        work_id = parts[0]
        year = parts[1]
        number_of_authors = parts[2]

        if year not in data:
            data[year] = []

        data[year].append({"id": parts[0], "author_count": parts[2], "topics": parts[3].split(';')})

data_sorted = dict(sorted(data.items()))

x = [
    int(year)
    for year in data_sorted.keys()
    if year.isdigit() and start_year <= int(year) < end_year
]
y = [len(data_sorted[str(year)]) for year in x]

fig, ax = plt.subplots(figsize=(13, 4))

ax.plot(
    x,
    y,
    marker='o',
    linewidth=1.5,
    markersize=5,
    label='Works per Year',
)

for i, (start, end) in enumerate(intervals_years):
    if i:
        ax.axvline(
            x=start,
            linestyle="--",
            linewidth=1,
            alpha=0.35,
            zorder=0,
            color="red",
            label="Interval Separator" if i == 1 else None,
        )

ax.set_xlabel("Year", fontsize=11)
ax.set_ylabel("Number of Works", fontsize=11)

ax.set_title(
    f"Number of Published Works per Year — {analized_country}",
    fontsize=15,
    fontweight="bold",
    pad=12,
)

ax.grid(axis="y", linestyle="--", alpha=0.4)
ax.set_axisbelow(True)

ax.set_xticks(x)
ax.tick_params(axis="x", rotation=60, labelsize=9)
ax.tick_params(axis="y", labelsize=9)

ax.margins(x=0.02)
ax.legend(
    fontsize=9,
    ncol=3,
    loc="upper left",
)

plt.tight_layout()
plt.savefig(works_per_year_plot_filename,bbox_inches="tight")

df = pd.DataFrame({"Year": x, "Papers": y})
df.to_csv(works_per_year_plot_filename.replace(".png", ".csv"), index=False)

if intervals_years:

    fig, ax = plt.subplots(figsize=(13, 4))

    ax.plot(
        x,
        y,
        marker='o',
        linewidth=1.5,
        markersize=5,
        label='Works per Year',
    )

    total_works_per_interval_y = []
    total_works_per_interval_x = []

    plt.yscale('log')

    for i, (start, end) in enumerate(intervals_years):
        if i:
            ax.axvline(
                x=start,
                linestyle="--",
                linewidth=1,
                alpha=0.35,
                zorder=0,
                color="red",
                label="Interval Separator" if i == 1 else None,
            )

    for start, end in intervals_years:
        total_works_per_interval_x.append(start + (((end + 1) - start) / 2))
        year_works = [len(data_sorted[str(year)]) for year in range(start, end + 1) if str(year) in data_sorted]

        total_works_per_interval_y.append(sum(year_works))

    ax.plot(
        total_works_per_interval_x,
        total_works_per_interval_y,
        marker='x',
        linestyle='--',
        color='orange',
        linewidth=1.5,
        markersize=6,
        label='Works per interval',
    )

    ax.set_xlabel("Year", fontsize=11)
    ax.set_ylabel("Number of Works", fontsize=11)

    ax.set_title(
        f"Number of Published Works per Year and Interval — {analized_country}",
        fontsize=15,
        fontweight="bold",
        pad=12,
    )

    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.set_axisbelow(True)

    ax.set_xticks(x)
    ax.tick_params(axis="x", rotation=60, labelsize=9)
    ax.tick_params(axis="y", labelsize=9)

    ax.margins(x=0.02)
    ax.legend(
        fontsize=9,
        ncol=3,
        loc="upper left",
    )

    plt.tight_layout()
    plt.savefig( f"intervals_{works_per_year_plot_filename}", bbox_inches="tight", )


def get_topics_by_year(data, year):
    if not year in data:
        return None
    
    topics = [
        t
        for work in data[year]
        for t in work['topics']
    ]
    topic_counts = Counter(topics)

    # remove 'Computer science'
    for domain in application_domains_to_delete:
        if domain in topic_counts:
            del topic_counts[domain]

    return topic_counts


def normalize_topic_counts(topic_counts):
    total = sum(topic_counts.values())
    normalized_counts = {topic: round((count / total) * 100, 2) for topic, count in topic_counts.items()}
    return normalized_counts


def get_application_domains(topics_by_year):
    # sort topics by frequency
    sorted_topics = sorted(topics_by_year.items(), key=lambda item: item[1], reverse=True)

    return (sorted_topics[:20])


def filter(topics, criteria):
    to_return = topics.copy()
    for topic in criteria.keys():
        if topic in to_return:
            del to_return[topic]
    return to_return


def marco_filter(topics, criteria):
    to_return = {}

    for topic in topics.keys():
        if topic not in criteria:
            # print(f"Topic '{topic}' not found in criteria mapping. Skipping.")
            continue

        if criteria[topic] == "Others":
            continue
        to_return[topic] = topics[topic]

    return to_return


def uniform_application_domain(topics, application_domains):
    to_return = {}

    for topic, freq in topics.items():
        new_key = application_domains[topic] if topic in application_domains else topic
        to_return[new_key] = to_return.get(new_key, 0) + freq

    return to_return


categories_over_time = {}

years = list(range(start_year, end_year))

for idx, year in enumerate(years):

    # here we get all topics by year
    topics = get_topics_by_year(data_sorted, str(year))
    if topics is None:
        continue

    # then we filter out CS topics 
    # to focus on the application domains
    filtered_topics = filter(topics, topics_mapping)
    # we need to uniform the application domains
    # otherwise we will have specific subtopics (e.g., Medicine, Internal medicine, etc.)
    uniformed_topics = uniform_application_domain(filtered_topics, application_domains_mapping)
    # then we normalize the counts to get percentages
    normalized_topics = normalize_topic_counts(uniformed_topics)
    # we sort the topics by percentage
    # and we get only the most frequent ones
    sorted_topics = dict(sorted(normalized_topics.items(), key=lambda item: item[1], reverse=True))

    for topic, percentage in list(sorted_topics.items())[:max_topics]:
        if topic not in categories_over_time:
            categories_over_time[topic] = np.zeros(len(years), dtype=float)

        categories_over_time[topic][idx] = percentage

years = list(range(start_year, end_year))

fig = plt.figure(figsize=(11, 6))
bottom = np.zeros(len(years))

width = 0.75

hatches = ['/', '\\', '|', '-', '+', 'x', 'o', '\\|', '.', '*']

for i, (category, percentage) in enumerate(categories_over_time.items()):
    plt.bar(years, percentage, width, bottom=bottom, label=category, color=colors[i % len(colors)], edgecolor='white',
            linewidth=2)  # , hatch=hatches[i % len(hatches)]
    bottom += np.array(percentage)

plt.xlim(start_year - 1, end_year)

# ylabel
plt.ylabel('Percentage of Works (%)', fontweight='bold', fontsize=13)
plt.title(f"Application Domains Over Time: {analized_country}", fontweight='bold', fontsize=15)
plt.legend(loc='upper right', bbox_to_anchor=(1.3, 1))
plt.savefig(application_domain_plot_filename, bbox_inches='tight')

cs_topics_over_time = {}

topics = get_topics_by_year(data_sorted, str(1990))
filtered_topics = filter(topics, application_domains_mapping)
marco_filtered_topics = marco_filter(filtered_topics, topic_to_category)

years = list(range(start_year, end_year))

for idx, year in enumerate(years):
    # here we get all topics by year
    topics = get_topics_by_year(data_sorted, str(year))

    if topics is None:
        continue

    # then we filter out app domains topics 
    # to focus on CS-related subfields
    filtered_topics = filter(topics, application_domains_mapping)
    marco_filtered_topics = marco_filter(filtered_topics, topic_to_category)
    # we need to uniform the subtopics
    uniformed_topics = uniform_application_domain(marco_filtered_topics, topic_to_category)  # cs_topics
    # then we normalize the counts to get percentages
    normalized_topics = normalize_topic_counts(uniformed_topics)
    # we sort the topics by percentage
    # and we get only the most frequent ones
    sorted_topics = dict(sorted(normalized_topics.items(), key=lambda item: item[1], reverse=True))

    for topic, percentage in list(sorted_topics.items())[:max_topics]:
        if topic not in cs_topics_over_time:
            cs_topics_over_time[topic] = np.zeros(len(years), dtype=float)

        cs_topics_over_time[topic][idx] = percentage

# transform cs_topic_over_time to df
cs_df = pd.DataFrame(cs_topics_over_time, index=years)

# sum the values over the rows
cs_df['Total'] = cs_df.sum(axis=1)

# create a new column 'Other' that is 100 - Total
cs_df['Other'] = 100 - cs_df['Total']

cs_topics_over_time['Other'] = cs_df['Other'].values

years = list(range(start_year, end_year))

fig = plt.figure(figsize=(10, 6))
bottom = np.zeros(len(years))

width = 0.75

hatches = ['/', '\\', '|', '-', '+', 'x', 'o', '\\|', '.', '*']

for i, (topic, percentage) in enumerate(cs_topics_over_time.items()):
    if topic == "Other":
        plt.bar(years, percentage, width, bottom=bottom, label=topic, color='lightgrey', edgecolor='white',
                linewidth=2)  # , hatch=hatches[i % len(hatches)]
    else:
        plt.bar(years, percentage, width, bottom=bottom, label=topic, color=colors[i], edgecolor='white',
                linewidth=2)  # , hatch=hatches[i % len(hatches)]
    bottom += np.array(percentage)

plt.xlim(start_year - 1, end_year)

# ylabel
plt.title(f"Disciplines Over Time: {analized_country}", fontweight='bold', fontsize=15)
plt.ylabel('Percentage of Works (%)', fontweight='bold', fontsize=13)
plt.legend(loc='upper right', bbox_to_anchor=(1.35, 1.), ncol=1)
plt.savefig(cs_topics_over_time_plot_filename, bbox_inches='tight')


def eval_ccdf(graph):
    degree_sequence = sorted(
        (d for _, d in graph.degree()),
        reverse=True
    )
    degreeCount = Counter(degree_sequence)
    deg, cnt = zip(*degreeCount.items())
    cs = np.cumsum(cnt)
    return np.array(deg), np.array(cs)


def load_interval_network(input_path, start, end):
    dfs = []
    if input_path.endswith(".csv"):
        df = pd.read_csv(input_path, names=['year', 'work_id', 'author_id1', 'author_id2'])
        dfs.append(df)
    else:
        for year in range(start, end + 1):
            csv_path = os.path.join(input_path, f"{year}.csv")
            if not os.path.exists(csv_path):
                continue
            df = pd.read_csv(csv_path, names=['year', 'work_id', 'author_id1', 'author_id2'])
            dfs.append(df)
    if not dfs:
        return None
    return pd.concat(dfs, ignore_index=True)


units = intervals_years if intervals_years else [(year, year) for year in range(start_year, end_year)]

print(f"Processing {len(units)} CCDF units")

os.makedirs(ccdf_path, exist_ok=True)

for start, end in units:
    label = f"{start}_{end}"
    output_path = os.path.join(ccdf_path, f"{label}.csv")

    print(f"Processing CCDF for {label}...")

    if os.path.exists(output_path):
        print(f"  CCDF already exists — skipping")
        continue
    input_file_name = f"{ccdf_input_path}/{label}_dataset.csv"
    print(f"Loading network from {input_file_name}...")
    net_df = load_interval_network(input_file_name, start, end)
    if net_df is None:
        print(f"  No data found — skipping")
        continue

    G = nx.from_pandas_edgelist(net_df, source='author_id1', target='author_id2')
    deg, cs = eval_ccdf(G)

    np.savetxt(output_path, np.column_stack((deg, cs)), delimiter=",", header="deg,cs", comments="", fmt="%d")

n = len(units)
cols = 5
rows = int(np.ceil(n / cols))

fig, axs = plt.subplots(
    rows,
    cols,
    figsize=(15, 3 * rows),
    constrained_layout=False
)
axs = axs.flatten()

for i, (start, end) in enumerate(units):
    label = f"{start}_{end}" #if start != end else str(start)
    input_path = os.path.join(ccdf_path, f"{label}.csv")

    if not os.path.exists(input_path):
        print(f"CCDF data for {label} not found — skipping")
        continue

    deg, cs = np.loadtxt(
        input_path,
        delimiter=",",
        skiprows=1,
        unpack=True
    )

    axs[i].plot(
        deg,
        cs,
        marker='o',
        linestyle='None',
        markersize=2,
        alpha=0.85,
    )

    axs[i].set_xscale('log')
    axs[i].set_yscale('log')
    axs[i].set_xlim(left=1)
    axs[i].set_ylim(bottom=1)

    title = f"{start}–{end}" if start != end else str(start)
    axs[i].set_title(title, fontsize=11, fontweight='bold')

    axs[i].grid(True, alpha=0.4)

    if i % cols == 0:
        axs[i].set_ylabel("CCDF", fontweight='bold')
    if i >= (rows - 1) * cols:
        axs[i].set_xlabel("Degree", fontweight='bold')

# Remove unused axes
for j in range(i + 1, len(axs)):
    fig.delaxes(axs[j])

fig.suptitle(
    f"CCDFs by Time Interval — {analized_country}",
    fontsize=15,
    fontweight='bold'
)

plt.tight_layout(rect=[0, 0, 1, 0.95])
plt.savefig(
    ccdf_graph_output_filename,
    bbox_inches='tight'
)
plt.show()
