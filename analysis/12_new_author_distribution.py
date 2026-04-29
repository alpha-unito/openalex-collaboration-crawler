#!/usr/bin/env python3
"""
For each CSV file (id1,id2,count) in the input directory:
  - Track which file each ID appears in first
  - Calculate Total New IDs and Average New IDs per year
  - Plot both on the SAME scale (single Y-axis)
"""

import os
import sys
import glob
import re
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker


def parse_years(filename: str) -> tuple[int, int, int]:
    """Returns (start_year, end_year, duration)."""
    stem = os.path.splitext(filename)[0]
    years = [int(y) for y in re.findall(r"\d{4}", stem)]
    
    if re.search(r"__\d{4}", stem): # Before 2000
        return years[0], years[0], 1
    if re.search(r"\d{4}__", stem): # 2020 and after
        return years[0], years[0], 1
    if len(years) >= 2:             # 2010_2014
        start, end = sorted(years[:2])
        return start, end, (end - start + 1)
    
    return (years[0], years[0], 1) if years else (0, 0, 1)


def label_from_filename(filename: str) -> str:
    stem = os.path.splitext(filename)[0]
    if "__" in stem:
        year = re.search(r"(\d{4})", stem).group(1)
        return f"<{year}" if stem.startswith("__") else f"{year}+"
    years = re.findall(r"\d{4}", stem)
    return f"{years[0]}\u2013{years[1]}" if len(years) >= 2 else (years[0] if years else filename)


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <input_directory> [<output_pdf_path>]")
        sys.exit(1)

    input_dir = sys.argv[1]
    pdf_path = sys.argv[2] if len(sys.argv) > 2 else "new_authors_per_interval.pdf"

    files = sorted(
        glob.glob(os.path.join(input_dir, "*.csv")),
        key=lambda p: parse_years(os.path.basename(p))[0]
    )
    
    if not files:
        print(f"No .csv files found in '{input_dir}'")
        sys.exit(1)

    first_seen = {}
    results = []

    for filepath in files:
        filename = os.path.basename(filepath)
        label = label_from_filename(filename)
        _, _, duration = parse_years(filename)
        new_ids_count = 0

        with open(filepath, "r") as f:
            for line in f:
                parts = line.strip().split(",")
                if len(parts) < 2: continue
                for id_ in (parts[0].strip(), parts[1].strip()):
                    if id_ not in first_seen:
                        first_seen[id_] = filename
                        new_ids_count += 1
        
        results.append({
            "label": label, 
            "total": new_ids_count, 
            "avg": new_ids_count / duration
        })

    # Data for plotting
    labels = [r["label"] for r in results]
    totals = [r["total"] for r in results]
    averages = [r["avg"] for r in results]

    fig, ax = plt.subplots(figsize=(11, 6))

    # 1. Bar Chart (Total)
    bars = ax.bar(labels, totals, color="#aec7e8", edgecolor="#4C72B0", label="Total New Authors")

    # 2. Line Chart (Average) - Same Axis
    line = ax.plot(labels, averages, color="#d62728", marker="o", markersize=8, 
                   linewidth=3, label="Avg New Authors / Year", zorder=3)

    # Styling
    ax.set_title("New Authors: Interval Totals & Annual Average", fontsize=14, fontweight="bold")
    ax.set_ylabel("Count (Same Scale)", fontsize=11)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.tick_params(axis="x", rotation=35)
    
    # Legend
    ax.legend(frameon=True, loc="upper left")

    # Value Labels for the Line (since it might be much lower than the bars)
    for i, avg in enumerate(averages):
        ax.text(i, avg + (max(totals) * 0.02), f"{avg:.1f}", 
                ha="center", color="#d62728", fontweight="bold", fontsize=9)

    plt.tight_layout()
    fig.savefig(pdf_path)
    print(f"Chart saved with uniform scale to: {pdf_path}")


if __name__ == "__main__":
    main()