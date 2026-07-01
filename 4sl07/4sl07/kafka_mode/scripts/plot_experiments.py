#!/usr/bin/env python3
"""
Draw the three experiment graphs from collected per-worker-count metrics.

Expected layout (produced by collect_experiment.sh):

    experiments/
        05_workers/phase_breakdown.csv
        10_workers/phase_breakdown.csv
        15_workers/phase_breakdown.csv
        25_workers/phase_breakdown.csv
        35_workers/phase_breakdown.csv
        50_workers/phase_breakdown.csv

Produces (in experiments/graphs/ by default):
    01_total_execution_time.png     - total job time per worker count
    02_map_phase_breakdown.png      - avg MAP sub-step time (stacked)
    03_reduce_phase_breakdown.png   - avg REDUCE sub-step time (stacked)

Usage:
    python3 scripts/plot_experiments.py
    python3 scripts/plot_experiments.py --experiments-dir experiments --out-dir experiments/graphs
"""
import argparse
import csv
import json
import re
import sys
from pathlib import Path

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    print("matplotlib is required. Install it with: pip install matplotlib", file=sys.stderr)
    raise

CONFIG_RE = re.compile(r"^(\d+)_workers$")

MAP_STEPS = [
    ("map_download_avg_s", "Download", "#440154"),
    ("map_read_avg_s", "Reading", "#31688e"),
    ("map_process_avg_s", "Processing (Mapping)", "#35b779"),
    ("map_temp_save_avg_s", "Temp Saving", "#fde725"),
]

REDUCE_STEPS = [
    ("reduce_transfer_avg_s", "File Transfer", "#000000"),
    ("reduce_process_avg_s", "Processing (Reduce)", "#ffcf9e"),
]


def to_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def load_config(config_dir: Path) -> dict | None:
    """Load one worker-count config from phase_breakdown.csv, falling back to metrics.json."""
    breakdown = config_dir / "phase_breakdown.csv"
    if breakdown.exists():
        with breakdown.open(encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))
        if rows:
            return rows[0]

    metrics_json = config_dir / "metrics.json"
    if metrics_json.exists():
        data = json.loads(metrics_json.read_text(encoding="utf-8"))
        pb = data.get("phase_breakdown")
        if pb:
            return pb

    return None


def collect_experiments(experiments_dir: Path) -> list[dict]:
    configs = []
    for child in sorted(experiments_dir.iterdir()):
        if not child.is_dir():
            continue
        match = CONFIG_RE.match(child.name)
        if not match:
            continue
        row = load_config(child)
        if row is None:
            print(f"warning: no metrics found in {child}, skipping", file=sys.stderr)
            continue
        worker_count = int(match.group(1))
        row["_workers"] = worker_count
        row["_label"] = f"{worker_count:02d}_workers"
        configs.append(row)
    configs.sort(key=lambda r: r["_workers"])
    return configs


def plot_total_time(configs: list[dict], out_dir: Path) -> None:
    labels = [c["_label"] for c in configs]
    totals = [to_float(c.get("total_duration_seconds")) for c in configs]
    colors = plt.cm.Blues([0.4 + 0.5 * i / max(len(configs) - 1, 1) for i in range(len(configs))])

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(labels, totals, color=colors)
    ax.set_title("Total Job Execution Time Comparison by Configuration")
    ax.set_xlabel("Configuration (Files)")
    ax.set_ylabel("Total Time (seconds)")
    ax.grid(axis="y", linestyle="-", alpha=0.3)
    ax.set_axisbelow(True)
    fig.tight_layout()
    out = out_dir / "01_total_execution_time.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"Wrote {out}")


def plot_stacked(configs: list[dict], steps, title: str, out_name: str, out_dir: Path) -> None:
    labels = [c["_label"] for c in configs]
    bottoms = [0.0] * len(configs)

    fig, ax = plt.subplots(figsize=(10, 5))
    for field, legend, color in steps:
        values = [to_float(c.get(field)) for c in configs]
        ax.bar(labels, values, bottom=bottoms, label=legend, color=color)
        bottoms = [b + v for b, v in zip(bottoms, values)]

    ax.set_title(title)
    ax.set_xlabel("Configuration")
    ax.set_ylabel("Average Time (seconds)")
    ax.legend(title="Sub-Parameters")
    ax.grid(axis="y", linestyle="-", alpha=0.3)
    ax.set_axisbelow(True)
    fig.tight_layout()
    out = out_dir / out_name
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"Wrote {out}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Plot kafka_mode scaling experiments")
    parser.add_argument("--experiments-dir", default="experiments")
    parser.add_argument("--out-dir", default=None, help="Defaults to <experiments-dir>/graphs")
    args = parser.parse_args()

    experiments_dir = Path(args.experiments_dir)
    if not experiments_dir.is_dir():
        print(f"error: experiments dir not found: {experiments_dir}", file=sys.stderr)
        return 1

    out_dir = Path(args.out_dir) if args.out_dir else experiments_dir / "graphs"
    out_dir.mkdir(parents=True, exist_ok=True)

    configs = collect_experiments(experiments_dir)
    if not configs:
        print(
            f"error: no '<N>_workers' folders with metrics found in {experiments_dir}",
            file=sys.stderr,
        )
        return 1

    print(f"Loaded {len(configs)} configs: {', '.join(c['_label'] for c in configs)}")
    plot_total_time(configs, out_dir)
    plot_stacked(
        configs,
        MAP_STEPS,
        "Average Time Spent per Sub-Parameter (MAP Phase)",
        "02_map_phase_breakdown.png",
        out_dir,
    )
    plot_stacked(
        configs,
        REDUCE_STEPS,
        "Average Time Spent per Sub-Parameter (REDUCE Phase)",
        "03_reduce_phase_breakdown.png",
        out_dir,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
