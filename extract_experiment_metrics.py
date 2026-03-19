#!/usr/bin/env python3
"""
Extract configurable JSON values from experiment subdirectories and plot them.

Scans timestamped subdirectories in experiment directories, extracts a value
(e.g., ts_size) from pre-run-state.json or post-run-state.json, uses file
creation time as the timestamp, and outputs side-by-side comparison plus a
gnuplot chart.

Usage:
    python3 extract_experiment_metrics.py DIR1 DIR2 --field ts_size
    python3 extract_experiment_metrics.py DIR --field ts_size   # single directory
    python3 extract_experiment_metrics.py DIR1 DIR2 -f tablespace.ts_size -o chart.png --plot-file chart.plot
    python3 extract_experiment_metrics.py DIR --show-metrics   # list available metrics
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

_SIZE_UNITS = {
    "bytes": 1, "kb": 1024, "mb": 1024**2,
    "gb": 1024**3, "tb": 1024**4, "pb": 1024**5,
}


def _normalize_size_str(s: str) -> str:
    """Insert space before unit if missing (e.g. '518G' -> '518 GB', '2.0T' -> '2.0 TB')."""
    s = s.strip()
    # Check multi-char then single-char suffixes (longest first)
    for suffix, unit in [("GB", "GB"), ("TB", "TB"), ("MB", "MB"), ("kB", "kB"), ("PB", "PB"),
                        ("G", "GB"), ("T", "TB"), ("M", "MB"), ("k", "kB")]:
        if s.upper().endswith(suffix.upper()) and len(s) > len(suffix):
            rest = s[:-len(suffix)].strip()
            if rest.replace(".", "").replace("-", "").isdigit():
                return rest + " " + unit
    return s


def parse_size(s: str) -> Optional[float]:
    """Parse a human-readable size string like '180 GB' or '518G' back to bytes."""
    if not s or s == "?" or not isinstance(s, str):
        return None
    s = _normalize_size_str(s)
    parts = s.split()
    if len(parts) != 2:
        return None
    try:
        num = float(parts[0])
        unit = parts[1].lower()
        if unit in _SIZE_UNITS:
            return num * _SIZE_UNITS[unit]
    except (ValueError, IndexError):
        pass
    return None


def size_to_gb(s: str) -> Optional[float]:
    """Convert human-readable size to GB for plotting."""
    b = parse_size(s)
    return b / (1024**3) if b is not None else None


def flatten_metrics(obj, prefix: str = "") -> List[Tuple[str, str]]:
    """Flatten JSON to (path, value) pairs for scalar leaf nodes."""
    items: List[Tuple[str, str]] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            path = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                items.extend(flatten_metrics(v, path))
            elif isinstance(v, list):
                if v and isinstance(v[0], dict):
                    # Show keys available in array elements
                    for key in v[0].keys():
                        items.append((f"{path}[].{key}", "(per element)"))
                else:
                    items.append((path, f"[{len(v)} items]"))
            else:
                items.append((path, str(v)))
    return items


def get_json_value(data: dict, path: str):
    """Extract value from nested dict using dot notation (e.g., tablespace.ts_size)."""
    parts = path.split(".")
    obj = data
    for p in parts:
        if isinstance(obj, dict) and p in obj:
            obj = obj[p]
        else:
            return None
    return obj


def get_file_timestamp(path: str) -> Optional[float]:
    """Get file creation (birth) time, fallback to mtime. Returns epoch seconds."""
    try:
        result = subprocess.run(
            ["stat", "--printf", "%W\n%Y", path],
            capture_output=True,
            text=True,
            check=True,
        )
        lines = result.stdout.strip().split("\n")
        birth = int(lines[0]) if lines else 0
        mtime = int(lines[1]) if len(lines) > 1 else 0
        return float(birth) if birth > 0 else float(mtime)
    except (subprocess.CalledProcessError, FileNotFoundError, ValueError):
        return None


def collect_data(
    exp_dir: str,
    field: str,
    use_pre_run: bool,
) -> List[Tuple[float, str, Optional[float]]]:
    """
    Collect (timestamp_epoch, value_str, value_numeric) for each subdir.
    Returns list sorted by timestamp.
    """
    state_file = "pre-run-state.json" if use_pre_run else "post-run-state.json"
    exp_path = Path(exp_dir)
    if not exp_path.is_dir():
        return []

    # Resolve field path shortcuts to full JSON paths
    _FIELD_SHORTCUTS = {
        "ts_size": "tablespace.ts_size",
        "used": "tablespace.disk.used",
        "disk.used": "tablespace.disk.used",
        "disk.size": "tablespace.disk.size",
        "use_pct": "tablespace.disk.use_pct",
        "disk.use_pct": "tablespace.disk.use_pct",
    }
    field_path = _FIELD_SHORTCUTS.get(field, field)

    results = []
    for subdir in sorted(exp_path.iterdir()):
        if not subdir.is_dir():
            continue
        json_path = subdir / state_file
        if not json_path.exists():
            print(f"Warning: missing {json_path}", file=sys.stderr)
            continue

        ts = get_file_timestamp(str(json_path))
        if ts is None:
            continue

        try:
            with open(json_path) as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"Warning: failed to read {json_path}: {e}", file=sys.stderr)
            continue

        val = get_json_value(data, field_path)
        if val is None:
            print(f"Warning: field '{field}' not found in {json_path}", file=sys.stderr)
            continue

        val_str = str(val)
        val_num = size_to_gb(val_str) if parse_size(val_str) is not None else None
        results.append((ts, val_str, val_num))

    results.sort(key=lambda x: x[0])
    return results


def show_metrics_table(exp_dir: str, use_pre_run: bool) -> None:
    """Print available metrics from a sample JSON as a formatted table."""
    state_file = "pre-run-state.json" if use_pre_run else "post-run-state.json"
    exp_path = Path(exp_dir)
    if not exp_path.is_dir():
        print(f"Error: {exp_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    json_path = None
    for subdir in sorted(exp_path.iterdir()):
        if subdir.is_dir():
            candidate = subdir / state_file
            if candidate.exists():
                json_path = candidate
                break

    if not json_path:
        print(f"Error: no {state_file} found in subdirs of {exp_dir}", file=sys.stderr)
        sys.exit(1)

    try:
        with open(json_path) as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"Error reading {json_path}: {e}", file=sys.stderr)
        sys.exit(1)

    metrics = flatten_metrics(data)
    if not metrics:
        print("No metrics found.", file=sys.stderr)
        return

    path_width = max(len(p) for p, _ in metrics)
    val_width = min(max(len(str(v)) for _, v in metrics), 60)
    path_width = max(path_width, 20)
    val_width = max(val_width, 12)

    header_path = "Path".ljust(path_width)
    header_val = "Value"
    print(f"{header_path}  {header_val}")
    print("-" * (path_width + 2 + max(val_width, len(header_val))))
    for path, val in metrics:
        val_str = str(val)
        if len(val_str) > 55:
            val_str = val_str[:52] + "..."
        print(f"{path.ljust(path_width)}  {val_str}")


def format_timestamp(epoch: float) -> str:
    return datetime.fromtimestamp(epoch).strftime("%Y-%m-%d %H:%M:%S")


def print_table(
    data1: List[Tuple[float, str, Optional[float]]],
    data2: Optional[List[Tuple[float, str, Optional[float]]]],
    label1: str,
    label2: Optional[str],
    field: str,
) -> None:
    """Print table: single dir (timestamp | value) or dual (label1 | field | label2)."""
    if data2 is None:
        # Single directory: timestamp | value
        col_widths = [24, 14]
        header = f"{'timestamp'.ljust(col_widths[0])}  {field.center(col_widths[1])}"
        sep = "-" * len(header)
        print(header)
        print(sep)
        for ts, val, _ in data1:
            ts_str = format_timestamp(ts) if ts is not None else ""
            print(f"{ts_str.ljust(col_widths[0])}  {val.ljust(col_widths[1])}")
    else:
        # Two directories: label1 | field | label2
        max_rows = max(len(data1), len(data2))
        rows = []
        for i in range(max_rows):
            ts1, val1, _ = data1[i] if i < len(data1) else (None, "", None)
            ts2, val2, _ = data2[i] if i < len(data2) else (None, "", None)
            ts1_str = format_timestamp(ts1) if ts1 is not None else ""
            ts2_str = format_timestamp(ts2) if ts2 is not None else ""
            rows.append([ts1_str, val1, val2, ts2_str])

        col_widths = [max(len(label1), 24), 12, 12, max(len(label2), 24)]
        center_width = col_widths[1] + 2 + col_widths[2]
        field_centered = field.center(center_width)
        header = f"{label1.ljust(col_widths[0])}  {field_centered}  {label2.ljust(col_widths[3])}"
        sep = "-" * len(header)
        print(header)
        print(sep)
        for row in rows:
            print("  ".join(str(c).ljust(w) for c, w in zip(row, col_widths)))


def write_gnuplot_script(
    label1: str,
    label2: Optional[str],
    field: str,
    output_png: str,
    data_file1: str,
    data_file2: Optional[str],
) -> str:
    """Generate gnuplot script content. Returns the script as string."""
    title = field.replace("'", "\\'")
    lines = [
        "set terminal png size 1200,600 noenhanced",
        "set output '" + output_png.replace("'", "\\'") + "'",
        f"set title '{title}'",
        "set xlabel 'Sub-experiment'",
        "set ylabel 'Value (GB)'",
        "set xtics 1",
        "set grid",
        "set key outside right top",
        "",
    ]
    if data_file2 is None:
        lines.append(f"plot '{data_file1}' using 1:2 with linespoints title '{label1}'")
    else:
        lines.append("plot \\")
        lines.append(f"  '{data_file1}' using 1:2 with linespoints title '{label1}', \\")
        lines.append(f"  '{data_file2}' using 1:2 with linespoints title '{label2}'")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract JSON values from experiment subdirs and plot comparison",
    )
    parser.add_argument(
        "dir1",
        nargs="?",
        help="First experiment directory (contains timestamped subdirs)",
    )
    parser.add_argument(
        "dir2",
        nargs="?",
        help="Second experiment directory (optional; if omitted, only dir1 is printed and plotted)",
    )
    parser.add_argument(
        "--field", "-f",
        metavar="FIELD",
        help="JSON path to extract (e.g., ts_size, tablespace.ts_size)",
    )
    parser.add_argument(
        "--show-metrics",
        action="store_true",
        help="Print available metrics from JSON as a table (use with dir1 only)",
    )
    parser.add_argument(
        "--output", "-o",
        default=None,
        metavar="FILE",
        help="Output PNG path (default: <field>.png)",
    )
    parser.add_argument(
        "--plot-file",
        default=None,
        metavar="FILE",
        help="Path to save the gnuplot script (default: <field>.plot)",
    )
    parser.add_argument(
        "--pre-run",
        action="store_true",
        help="Use pre-run-state.json instead of post-run-state.json",
    )
    args = parser.parse_args()

    if args.show_metrics:
        if not args.dir1:
            parser.error("--show-metrics requires a directory (dir1)")
        show_metrics_table(args.dir1, args.pre_run)
        return

    if not args.dir1:
        parser.error("dir1 is required (omit --show-metrics)")
    if not args.field:
        parser.error("--field is required")

    output_file = args.output or f"{args.field}.png"
    plot_file = args.plot_file or f"{args.field}.plot"

    data1 = collect_data(args.dir1, args.field, args.pre_run)
    data2 = collect_data(args.dir2, args.field, args.pre_run) if args.dir2 else None

    label1 = os.path.basename(os.path.normpath(args.dir1))
    label2 = os.path.basename(os.path.normpath(args.dir2)) if args.dir2 else None

    if not data1 and (data2 is None or not data2):
        print("No data found.", file=sys.stderr)
        sys.exit(1)

    # Print table
    print_table(data1, data2, label1, label2, args.field)

    # Filter to rows with numeric values for gnuplot (use index 1,2,3... for overlay)
    plot_data1 = [val_num for _, _, val_num in data1 if val_num is not None]
    plot_data2 = [val_num for _, _, val_num in data2 if val_num is not None] if data2 else None

    if not plot_data1 and (plot_data2 is None or not plot_data2):
        print("\nNo numeric values to plot (field may not be a size string).", file=sys.stderr)
    else:
        # Write data files alongside the plot file so the saved .plot file can be re-run
        plot_dir = os.path.dirname(os.path.abspath(plot_file))
        plot_base = os.path.splitext(os.path.basename(plot_file))[0]
        data_path1 = os.path.join(plot_dir, f"{plot_base}_dir1.dat")
        data_path2 = os.path.join(plot_dir, f"{plot_base}_dir2.dat") if plot_data2 else None

        with open(data_path1, "w") as f1:
            for i, val in enumerate(plot_data1, start=1):
                f1.write(f"{i} {val}\n")
        if plot_data2:
            with open(data_path2, "w") as f2:
                for i, val in enumerate(plot_data2, start=1):
                    f2.write(f"{i} {val}\n")

        script = write_gnuplot_script(
            label1, label2, args.field,
            os.path.abspath(output_file),
            os.path.abspath(data_path1),
            os.path.abspath(data_path2) if data_path2 else None,
        )

        with open(plot_file, "w") as f:
            f.write(script)

        try:
            subprocess.run(["gnuplot", plot_file], check=True)
            print(f"\nChart saved to {output_file}")
        except subprocess.CalledProcessError as e:
            print(f"\nWarning: gnuplot failed: {e}", file=sys.stderr)
        except FileNotFoundError:
            print("\nWarning: gnuplot not found", file=sys.stderr)

        print(f"Gnuplot script saved to {plot_file}")


if __name__ == "__main__":
    main()
