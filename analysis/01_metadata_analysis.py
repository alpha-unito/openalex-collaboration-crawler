import sys, tomllib, os
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd
import seaborn as sns
from collections import Counter
from matplotlib.patches import Patch

plt.rcParams.update({'font.size': 15})


def get_tab20_palette(color_count):
    tab20 = sns.color_palette('tab20')
    if color_count <= len(tab20):
        return tab20[:color_count]
    return sns.blend_palette(tab20, n_colors=color_count)


def add_legend(ax):
    handles, labels = ax.get_legend_handles_labels()
    legend = ax.legend(
        handles,
        labels,
        loc='upper left',
        bbox_to_anchor=(1, 1),
        fontsize=15
    )

    fig = ax.figure
    fig.canvas.draw()
    legend_height = legend.get_window_extent(fig.canvas.get_renderer()).height / fig.dpi
    axes_height = ax.get_position().height * fig.get_figheight()
    if legend_height > axes_height:
        fig.set_figheight(fig.get_figheight() * legend_height / axes_height)


toml_config_path = sys.argv[1] if len(sys.argv) > 1 else "default.toml"

print("Parsing {} configuration file".format(toml_config_path))
with open(toml_config_path, 'rb') as f:
    configuration = tomllib.load(f)

try:
    metadata_path                       = configuration["workflow_data"] + "/" + configuration["country"] + "/" + configuration["metadata_analisys"]["inputs"]["metadata_path"]
    ccdf_input_path                     = configuration["workflow_data"] + "/" + configuration["country"] + "/" + configuration["metadata_analisys"]["inputs"]["graph_directory"]
    ccdf_path                           = configuration["workflow_data"] + "/" + configuration["country"] + "/" + configuration["metadata_analisys"]["outputs"]["ccdf_path"]
    
    works_per_year_plot_filename        = configuration["statistics_out_basedir"] + "/" + "intervals_" + configuration["metadata_analisys"]["outputs"]["works_per_year_plot_filename"]
    works_per_year_dataset              = configuration["statistics_out_basedir"] + "/" + configuration["metadata_analisys"]["outputs"]["works_per_year_dataset"]
    application_domain_plot_filename    = configuration["statistics_out_basedir"] + "/" + configuration["metadata_analisys"]["outputs"]["application_domain_plot_filename"]
    cs_topics_over_time_plot_filename   = configuration["statistics_out_basedir"] + "/" + configuration["metadata_analisys"]["outputs"]["cs_topics_over_time_plot_filename"]
    ccdf_graph_output_filename          = configuration["statistics_out_basedir"] + "/" + configuration["metadata_analisys"]["outputs"]["ccdf_graph_output_filename"]
    
    analized_country                    = configuration["analized_country_full"]
    start_year                          = configuration["metadata_analisys"]["config"]["start_year"]
    end_year                            = configuration["metadata_analisys"]["config"]["end_year"]
    max_topics                          = configuration["metadata_analisys"]["config"]["max_topics"]
    
    os.makedirs(configuration["statistics_out_basedir"], exist_ok=True)
except KeyError as e:
    print("Error: key {} not present in configuration file".format(e))
    exit(-1)

intervals_years = []
try:
    for interval in configuration["time_intervals"]:
        intervals_years.append((interval[0], interval[1]))
except KeyError :
    intervals_years = None

print(f"\n{'=' * 60}")
print(f"{ " METADATA ANALISYS CONFIGURATION ".center(60, ' ')}")
print(f"{'=' * 60}")

print(f"\n[INPUTS]")
print(f"  Metadata Path:        {metadata_path}")
print(f"  CCDF Input Path:      {ccdf_input_path}")

print(f"\n[SETTINGS]")
print(f"  Analyzed Country:     {analized_country}")
print(f"  Time Window:          {start_year} to {end_year}")
print(f"  Max Topics:           {max_topics}")

print(f"\n[OUTPUT FILES]")
print(f"  Works/Year Plot:      {works_per_year_plot_filename}")
print(f"  Works/Year Dataset:   {works_per_year_dataset}")
print(f"  App Domain Plot:      {application_domain_plot_filename}")
print(f"  CS Topics Plot:       {cs_topics_over_time_plot_filename}")
print(f"  CCDF Path:            {ccdf_path}")
print(f"  CCDF Graph Filename:  {ccdf_graph_output_filename}")

print(f"{'=' * 60}\n")

# ----------------------------------------------------
# External Mappings (Required for Topic Normalization)
# ----------------------------------------------------
# These mappings must be provided manually if analyzing fields outside Computer Science.
# Dictionary mapping fine-grained CS topics to broader topic categories.
# topics_mapping: Normalizes topic names and groups synonyms/variants.
# application_domains_mapping: Mapping to unify and filter application-domain labels.
# application_domains_to_delete: list of application domainst that should be removed from the analisys
from mappings import topics_mapping, application_domains_mapping, application_domains_to_delete
from topic_to_category import topic_to_category

def normalize_ascii(text):
    if isinstance(text, bytes):
        text = text.decode("utf-8", "ignore")
    return text.encode("ascii", "ignore").decode("ascii")

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

if intervals_years:

    fig, (ax_top, ax_bottom) = plt.subplots(
        2,
        1,
        figsize=(20, 10),
        sharex=True,
        gridspec_kw={'hspace': 0}
    )

    ax_top.plot(
        x,
        y,
        marker='o',
        linewidth=1.5,
        markersize=5,
        label='Works per Year',
    )

    for i, (start, end) in enumerate(intervals_years):
        if i:
            ax_top.axvline(
                x=start,
                linestyle="--",
                linewidth=1,
                alpha=0.35,
                color="red",
            )

    ax_top.set_ylabel("Number of Works", fontsize=17)
    #ax_top.set_title(
    #    f"Number of Published Works per Year — {analized_country}",
    #    fontsize=15,
    #    fontweight="bold",
    #    pad=10,
    #)

    ax_top.grid(axis="y", linestyle="--", alpha=0.4)
    ax_top.set_axisbelow(True)

    # hide x labels on top plot
    ax_top.tick_params(axis="x", labelbottom=False)

    ax_top.margins(x=0.02)
    add_legend(ax_top)

    ax_bottom.plot(
        x,
        y,
        marker='o',
        linewidth=1.5,
        markersize=5,
        label='Works per Year',
    )

    total_works_per_interval_y = []
    total_works_per_interval_x = []

    for start, end in intervals_years:
        midpoint = start + (((end + 1) - start) / 2)
        total_works_per_interval_x.append(midpoint)

        year_works = [
            len(data_sorted[str(year)])
            for year in range(start, end + 1)
            if str(year) in data_sorted
        ]

        total_works_per_interval_y.append(sum(year_works))

    ax_bottom.plot(
        total_works_per_interval_x,
        total_works_per_interval_y,
        marker='x',
        linestyle='--',
        color='orange',
        linewidth=1.5,
        markersize=6,
        label='Works per interval',
    )

    for i, (start, end) in enumerate(intervals_years):
        if i:
            ax_bottom.axvline(
                x=start,
                linestyle="--",
                linewidth=1,
                alpha=0.35,
                color="red",
            )

    ax_bottom.set_yscale('log')  
    ax_bottom.set_xlabel("Year", fontsize=15)
    ax_bottom.set_ylabel("Number of Works (log)", fontsize=17)
    ax_bottom.grid(axis="y", linestyle="--", alpha=0.4)
    ax_bottom.set_axisbelow(True)
    ax_bottom.set_xticks(x)
    ax_bottom.tick_params(axis="x", rotation=60, labelsize=15)
    ax_bottom.tick_params(axis="y", labelsize=15)
    ax_bottom.margins(x=0.02)
    add_legend(ax_bottom)

   
    fig.subplots_adjust(hspace=0)

    ax_bottom.spines['top'].set_visible(False)

    ax_top.spines['bottom'].set_visible(True)
    ax_top.spines['bottom'].set_linewidth(1.5)
    ax_top.spines['bottom'].set_color('black')

    plt.savefig(works_per_year_plot_filename, bbox_inches="tight")

else:
    # fallback: single linear plot if no intervals provided
    fig, ax = plt.subplots(figsize=(13, 4))

    ax.plot(
        x,
        y,
        marker='o',
        linewidth=1.5,
        markersize=5,
        label='Works per Year',
    )

    ax.set_xlabel("Year", fontsize=15)
    ax.set_ylabel("Number of Works", fontsize=17)
    #ax.set_title(
    #    f"Number of Published Works per Year — {analized_country}",
    #    fontsize=15,
    #    fontweight="bold",
    #    pad=12,
    #)

    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.set_axisbelow(True)
    ax.set_xticks(x)
    ax.tick_params(axis="x", rotation=60, labelsize=15)
    ax.tick_params(axis="y", labelsize=15)

    ax.margins(x=0.02)
    add_legend(ax)

    plt.savefig(works_per_year_plot_filename, bbox_inches="tight")

df = pd.DataFrame({"Year": x, "Papers": y})
new_name = works_per_year_plot_filename.split(".")[0] + ".csv"
df.to_csv(new_name, index=False)


print("saved plots works per year")


def get_topics_by_year(data, year):
    if not year in data:
        return None
    
    topics = [
        normalize_ascii(
            t[t.find("(") + 1 : t.find(")")].capitalize()
            if "(" in t and ")" in t
            else t
        )
        for work in data[year]
        for t in work["topics"]
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
        topic = normalize_ascii(topic)
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
application_palette = get_tab20_palette(len(categories_over_time))

width = 0.75

hatches = ['/', '\\', '|', '-', '+', 'x', 'o', '\\|', '.', '*']

for i, (category, percentage) in enumerate(categories_over_time.items()):
    plt.bar(years, percentage, width, bottom=bottom, label=category, color=application_palette[i], edgecolor='white',
            linewidth=2)  # , hatch=hatches[i % len(hatches)]
    bottom += np.array(percentage)

plt.xlim(start_year - 1, end_year)

# ylabel
plt.ylabel('Percentage of Works (%)', fontsize=17)#, fontweight='bold')
#plt.title(f"Application Domains Over Time: {analized_country}", fontweight='bold', fontsize=15)
add_legend(plt.gca())
plt.savefig(application_domain_plot_filename, bbox_inches='tight')

print("Saved plot for application domains over time")

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

other_color = sns.color_palette(['lightgray'])[0]
cs_base_palette = sns.color_palette('tab20')
closest_gray = min(
    cs_base_palette,
    key=lambda color: sum((channel - gray) ** 2 for channel, gray in zip(color, other_color))
)
cs_base_palette.remove(closest_gray)
non_other_count = len(cs_topics_over_time) - ('Other' in cs_topics_over_time)
cs_palette = (
    cs_base_palette[:non_other_count]
    if non_other_count <= len(cs_base_palette)
    else sns.blend_palette(cs_base_palette, n_colors=non_other_count)
)

color_index = 0
topic_colors = {}
for topic in cs_topics_over_time:
    topic_colors[topic] = other_color if topic == 'Other' else cs_palette[color_index]
    color_index += topic != 'Other'

cs_plot_df = pd.DataFrame(cs_topics_over_time, index=years)
plot_intervals = intervals_years or [(start_year, end_year - 1)]
interval_frames = [
    cs_plot_df.loc[(cs_plot_df.index >= start) & (cs_plot_df.index <= end)]
    for start, end in plot_intervals
]
interval_widths = [max(1, len(interval_df.index)) for interval_df in interval_frames]

fig, axes = plt.subplots(
    1,
    len(plot_intervals),
    figsize=(max(12, 0.32 * sum(interval_widths)), 7),
    sharey=True,
    squeeze=False,
    gridspec_kw={'width_ratios': interval_widths, 'wspace': 0.05}
)

for ax, interval_df in zip(axes.flat, interval_frames):
    if interval_df.empty:
        ax.text(0.5, 0.5, 'No data', ha='center', va='center', transform=ax.transAxes)
    else:
        interval_df.plot(
            kind='bar',
            stacked=True,
            ax=ax,
            width=0.75,
            color=[topic_colors[topic] for topic in interval_df.columns],
            edgecolor='white',
            linewidth=2,
            legend=False
        )
        tick_step = max(1, (len(interval_df.index) + 3) // 4)
        tick_positions = range(0, len(interval_df.index), tick_step)
        ax.set_xticks(tick_positions)
        ax.set_xticklabels(interval_df.index[tick_positions], rotation=90)

    ax.set_xlabel('')
    ax.tick_params(axis='both', labelsize=13)

axes[0, 0].set_ylabel('Percentage of Works (%)', fontsize=17)
fig.supxlabel('Year', fontsize=15, y=-0.025)

sorted_topics = sorted(cs_topics_over_time, key=str.casefold)
fig.legend(
    handles=[Patch(facecolor=topic_colors[topic], label=topic) for topic in sorted_topics],
    loc='upper center',
    bbox_to_anchor=(0, -0.06, 1, 0),
    fontsize=13,
    ncol=min(3, len(sorted_topics)),
    mode='expand'
)
fig.tight_layout()
fig.savefig(cs_topics_over_time_plot_filename, bbox_inches='tight')
plt.close(fig)

print("Saved plot for computer science topics over time")

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
cols = 3
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
    axs[i].set_title(title, fontsize=15)#, fontweight='bold')

    axs[i].grid(True, alpha=0.4)

    if i % cols == 0:
        axs[i].set_ylabel("CCDF", fontsize=17)#, fontweight='bold')
    if i >= (rows - 1) * cols:
        axs[i].set_xlabel("Degree", fontsize=15)#, fontweight='bold')

# Remove unused axes
for j in range(i + 1, len(axs)):
    fig.delaxes(axs[j])

fig.suptitle(
    f"CCDFs by Time Interval — {analized_country}",
    fontsize=15,
    #fontweight='bold'
)

plt.tight_layout(rect=[0, 0, 1, 0.95])
plt.savefig(
    ccdf_graph_output_filename,
    bbox_inches='tight'
)
plt.show()
