import json
import os
import sys
import tomllib

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import seaborn as sns

subfield_output = 'onthology_subfields_distribution.pdf'
topic_output = 'onthology_topic_distribution.pdf'
subfield_input = '/home/marco/Desktop/papers_it_subfields_distribution.json'
topic_input = '/home/marco/Desktop/papers_it_topics_distribution.json'
limit = 10


def plot_stackbar(input_file_path: str, output_file_path: str, time_intervals: list, N: int) -> None:
    if not os.path.exists(input_file_path):
        print(f"Error: File not found at {input_file_path}")
        return

    # 1. Load the data
    with open(input_file_path, 'r') as f:
        data = json.load(f)

    # 2. Flatten JSON data
    records = []
    for category_name, years_data in data.items():
        for year, count in years_data.items():
            records.append({
                'Category': category_name,
                'Year': int(year),
                'Count': int(count)
            })

    df = pd.DataFrame(records)

    # 3. Keep years covered by at least one configured interval.
    interval_mask = pd.Series(False, index=df.index)
    for start_year, end_year in time_intervals:
        interval_mask |= df['Year'].between(start_year, end_year)
    df = df[interval_mask]

    if df.empty:
        print("Error: No data found in the configured time intervals")
        return

    # 4. Calculate Yearly Totals BEFORE filtering
    # This ensures our percentage is relative to the WHOLE dataset
    yearly_totals = df.groupby('Year')['Count'].transform('sum')
    df['Percentage'] = (df['Count'] / yearly_totals) * 100

    # 5. Filter for Top N based on rank
    df['rank'] = df.groupby('Year')['Count'].rank(method='first', ascending=False)
    plot_df = df[df['rank'] <= N].copy()

    # 6. Pivot the data using the pre-calculated Percentage
    percentage_df = plot_df.pivot_table(
        index='Year',
        columns='Category',
        values='Percentage',  # Use the percentage we calculated in step 4
        aggfunc='sum'
    ).fillna(0)

    # 7. Generate one subplot per configured interval.
    tab20 = sns.color_palette('tab20')
    category_count = len(percentage_df.columns)
    palette = (
        tab20
        if category_count <= len(tab20)
        else sns.blend_palette(tab20, n_colors=category_count)
    )
    category_colors = dict(zip(percentage_df.columns, palette))

    interval_frames = [
        percentage_df.loc[
            (percentage_df.index >= start_year) & (percentage_df.index <= end_year)
        ]
        for start_year, end_year in time_intervals
    ]
    interval_widths = [max(1, len(interval_df.index)) for interval_df in interval_frames]

    fig, axes = plt.subplots(
        1,
        len(time_intervals),
        figsize=(max(12, 0.32 * sum(interval_widths)), 7),
        sharey=True,
        squeeze=False,
        gridspec_kw={'width_ratios': interval_widths, 'wspace': 0.05}
    )

    for ax, (start_year, end_year), interval_df in zip(
        axes.flat, time_intervals, interval_frames
    ):
        if interval_df.empty:
            ax.text(0.5, 0.5, 'No data', ha='center', va='center', transform=ax.transAxes)
        else:
            interval_df.plot(
                kind='bar',
                stacked=True,
                ax=ax,
                width=0.85,
                color=[category_colors[category] for category in interval_df.columns],
                legend=False
            )
            tick_step = max(1, (len(interval_df.index) + 3) // 4)
            tick_positions = range(0, len(interval_df.index), tick_step)
            ax.set_xticks(tick_positions)
            ax.set_xticklabels(interval_df.index[tick_positions], rotation=90)

        ax.set_xlabel('')
        ax.tick_params(axis='both', labelsize=13)

    axes[0, 0].set_ylabel('Relative Percentage (%)', fontsize=17)
    fig.supxlabel('Year', fontsize=15, y=-0.025)

    sorted_categories = sorted(percentage_df.columns, key=str.casefold)
    legend_handles = [
        Patch(facecolor=category_colors[category], label=category)
        for category in sorted_categories
    ]
    fig.legend(
        handles=legend_handles,
        loc='upper center',
        bbox_to_anchor=(0, -0.06, 1, 0),
        fontsize=13,
        ncol=min(3, len(sorted_categories)),
        mode='expand'
    )
    fig.tight_layout()
    fig.savefig(output_file_path, bbox_inches='tight')
    plt.close()
    print(f"Generated: {output_file_path} for {len(time_intervals)} time intervals")


toml_config_path = sys.argv[1] if len(sys.argv) > 1 else "default.toml"
print(f"Parsing {toml_config_path} configuration file")
with open(toml_config_path, 'rb') as f:
    configuration = tomllib.load(f)

time_intervals = configuration.get("time_intervals", [])
if not time_intervals:
    raise ValueError("The configuration must define at least one time interval")

plot_stackbar(subfield_input, subfield_output, time_intervals, limit)
plot_stackbar(topic_input, topic_output, time_intervals, limit)
