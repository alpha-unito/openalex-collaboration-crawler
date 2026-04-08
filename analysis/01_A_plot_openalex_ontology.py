import json
import pandas as pd
import matplotlib.pyplot as plt
import os

subfield_output = 'subfields_distribution.pdf'
topic_output = 'topic_distribution.pdf'
subfield_input = '/home/marco/Desktop/papers_it_subfields_distribution.json'
topic_input = '/home/marco/Desktop/papers_it_topics_distribution.json'
START = 1970
END = 2024
limit = 10


def plot_stackbar(input_file_path: str, output_file_path: str, start_year: int, end_year: int, N: int) -> None:
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

    # 3. Explicit Year Filtering
    df = df[(df['Year'] >= start_year) & (df['Year'] <= end_year)]

    if df.empty:
        print(f"Error: No data found between {start_year} and {end_year}")
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

    # 7. Generate the plot
    label = "Topics" if "topic" in input_file_path.lower() else "Subfields"

    plt.figure(figsize=(18, 10))

    # Plotting using the same Category labels
    ax = percentage_df.plot(
        kind='bar',
        stacked=True,
        ax=plt.gca(),
        width=0.85,
        colormap='tab20'
    )

    plt.title(f'Top {N} {label} ({start_year}-{end_year})', fontsize=16)
    plt.xlabel('Year', fontsize=12)
    plt.ylabel('Relative Percentage (%)', fontsize=12)

    # Legend Management: Only show categories that appear in this specific time range
    handles, labels = ax.get_legend_handles_labels()

    # Sort legend by average appearance in the plot
    avg_pct = percentage_df.mean().sort_values(ascending=False)
    sorted_labels = avg_pct.index.tolist()
    handle_dict = dict(zip(labels, handles))
    sorted_handles = [handle_dict[l] for l in sorted_labels if l in handle_dict]

    # Adjust layout to fit legend
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.75, box.height])

    plt.legend(
        sorted_handles,
        sorted_labels,
        title=label,
        bbox_to_anchor=(1.02, 1),
        loc='upper left',
        fontsize=9,
        ncol=2 if len(sorted_labels) > 30 else 1
    )

    plt.savefig(output_file_path, bbox_inches='tight')
    plt.close()
    print(f"Generated: {output_file_path} for period {start_year} to {end_year}")


plot_stackbar(subfield_input, subfield_output, START, END, limit)
plot_stackbar(topic_input, topic_output, START, END, limit)
