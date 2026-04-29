import os
import sys
import glob
import re


def extract_start_year(filename: str) -> int:
    """Return the first 4-digit year found in the filename, or 0 if none."""
    match = re.search(r"\d{4}", filename)
    return int(match.group()) if match else 0


def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <input_directory>")
        sys.exit(1)

    input_dir = sys.argv[1]

    if not os.path.isdir(input_dir):
        print(f"Error: '{input_dir}' is not a directory")
        sys.exit(1)

    pattern = os.path.join(input_dir, "*.csv")
    files = sorted(glob.glob(pattern), key=lambda p: extract_start_year(os.path.basename(p)))

    if not files:
        print(f"No .csv files found in '{input_dir}'")
        sys.exit(1)

    first_seen: dict[str, str] = {}

    results = [] 

    total_files = len(files)
    print(f"Found {total_files} file(s) to process.\n")

    for file_index, filepath in enumerate(files, start=1):
        filename = os.path.basename(filepath)
        new_ids = []
        
        with open(filepath, "r") as f:
            total_lines = sum(1 for line in f if line.strip())

        print(f"[{file_index}/{total_files}] {filename}")

        with open(filepath, "r") as f:
            processed_lines = 0
            for lineno, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue

                parts = line.split(",")
                if len(parts) < 2:
                    print(f"\n  Warning: line {lineno} — skipping malformed line: {line!r}")
                    continue

                id1, id2 = parts[0].strip(), parts[1].strip()

                for id_ in (id1, id2):
                    if id_ not in first_seen:
                        first_seen[id_] = filename
                        new_ids.append(id_)

                processed_lines += 1

                # Redraw progress bar every 1000 lines (or always if file is small)
                if processed_lines % 1000 == 0 or processed_lines == total_lines:
                    bar_width = 40
                    filled = int(bar_width * processed_lines / total_lines) if total_lines else bar_width
                    bar = "█" * filled + "░" * (bar_width - filled)
                    pct = 100 * processed_lines // total_lines if total_lines else 100
                    print(f"\r  [{bar}] {pct:3d}%  {processed_lines:,}/{total_lines:,} lines", end="", flush=True)

        print(f"\r  [{'█' * 40}] 100%  {total_lines:,}/{total_lines:,} lines  ✓ {len(new_ids):,} new authors")
        print()

        results.append((filename, new_ids))

    print("=" * 51)
    print(f"{'File':<40}  {'New Authors':>8}")
    print("-" * 51)
    for filename, new_ids in results:
        print(f"{filename:<40}  {len(new_ids):>8,}")

    print("=" * 51)
    print(f"Total unique authors across all files: {len(first_seen):,}")


if __name__ == "__main__":
    main()