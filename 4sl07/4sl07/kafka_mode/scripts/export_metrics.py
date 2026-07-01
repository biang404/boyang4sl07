#!/usr/bin/env python3
import argparse
import csv
import json
import sys
from pathlib import Path


MAP_FIELDS = [
    "job_id",
    "map_task_id",
    "worker_id",
    "input_bytes",
    "download_duration_ms",
    "input_read_duration_ms",
    "map_process_duration_ms",
    "temp_save_duration_ms",
    "map_task_duration_ms",
    "map_start_time",
    "download_start_time",
    "download_end_time",
    "input_read_start_time",
    "input_read_end_time",
    "map_process_start_time",
    "map_process_end_time",
    "temp_save_start_time",
    "temp_save_end_time",
    "map_done_time",
]

REDUCE_FIELDS = [
    "job_id",
    "reduce_task_id",
    "worker_id",
    "output_bytes",
    "reduce_task_duration_ms",
    "file_transfer_duration_ms",
    "reduce_process_duration_ms",
    "sort_duration_ms",
    "output_write_duration_ms",
    "reduce_start_time",
    "shuffle_all_inputs_done_time",
    "reduce_merge_start_time",
    "sort_start_time",
    "sort_end_time",
    "output_write_start_time",
    "output_write_end_time",
    "reduce_done_time",
]

# Averaged per-sub-step breakdown (seconds) used by scripts/plot_experiments.py.
PHASE_BREAKDOWN_FIELDS = [
    "workers",
    "total_duration_seconds",
    "map_download_avg_s",
    "map_read_avg_s",
    "map_process_avg_s",
    "map_temp_save_avg_s",
    "reduce_transfer_avg_s",
    "reduce_process_avg_s",
]

SUMMARY_FIELDS = [
    "job_id",
    "workers",
    "job_start_time",
    "job_done_time",
    "total_duration_ms",
    "total_duration_seconds",
    "input_files_processed_ms",
    "input_files_processed_seconds",
    "map_task_count",
    "map_task_duration_sum_ms",
    "map_task_duration_max_ms",
    "reduce_task_count",
    "reduce_task_duration_sum_ms",
    "reduce_task_duration_max_ms",
]


def load_metrics(path: Path) -> list[dict]:
    metrics = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8-sig", errors="replace").splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        if line.startswith("[metric] "):
            line = line[len("[metric] ") :]
        try:
            metrics.append(json.loads(line))
        except json.JSONDecodeError as exc:
            print(f"warning: skipped invalid JSON line {line_number}: {exc}", file=sys.stderr)
    return metrics


def write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def latest_by_event(metrics: list[dict], event: str) -> dict:
    rows = [m for m in metrics if m.get("event") == event]
    return rows[-1] if rows else {}


def build_summary(metrics: list[dict], map_tasks: list[dict], reduce_tasks: list[dict]) -> dict:
    job_start = latest_by_event(metrics, "job_start")
    job_done = latest_by_event(metrics, "job_done")
    input_done = latest_by_event(metrics, "all_input_files_processed")
    job_id = (
        job_done.get("job_id")
        or job_start.get("job_id")
        or input_done.get("job_id")
        or (map_tasks[0].get("job_id") if map_tasks else "")
        or (reduce_tasks[0].get("job_id") if reduce_tasks else "")
    )
    total_ms = job_done.get("job_duration_ms", "")
    return {
        "job_id": job_id,
        "workers": job_start.get("workers", job_done.get("workers", "")),
        "job_start_time": job_done.get("job_start_time", job_start.get("job_start_time", "")),
        "job_done_time": job_done.get("job_done_time", ""),
        "total_duration_ms": total_ms,
        "total_duration_seconds": round(total_ms / 1000, 3) if isinstance(total_ms, int) else "",
        "input_files_processed_ms": input_done.get("elapsed_ms", ""),
        "input_files_processed_seconds": input_done.get("elapsed_seconds", ""),
        "map_task_count": len(map_tasks),
        "map_task_duration_sum_ms": sum_int(map_tasks, "map_task_duration_ms"),
        "map_task_duration_max_ms": max_int(map_tasks, "map_task_duration_ms"),
        "reduce_task_count": len(reduce_tasks),
        "reduce_task_duration_sum_ms": sum_int(reduce_tasks, "reduce_task_duration_ms"),
        "reduce_task_duration_max_ms": max_int(reduce_tasks, "reduce_task_duration_ms"),
    }


def sum_int(rows: list[dict], field: str) -> int:
    return sum(v for row in rows if isinstance((v := row.get(field)), int))


def max_int(rows: list[dict], field: str) -> int:
    values = [v for row in rows if isinstance((v := row.get(field)), int)]
    return max(values) if values else 0


def avg_seconds(rows: list[dict], field: str) -> float:
    """Average a millisecond field across rows, returned in seconds (3 dp)."""
    values = [v for row in rows if isinstance((v := row.get(field)), (int, float))]
    if not values:
        return 0.0
    return round(sum(values) / len(values) / 1000, 3)


def build_phase_breakdown(summary: dict, map_tasks: list[dict], reduce_tasks: list[dict]) -> dict:
    return {
        "workers": summary.get("workers", ""),
        "total_duration_seconds": summary.get("total_duration_seconds", ""),
        "map_download_avg_s": avg_seconds(map_tasks, "download_duration_ms"),
        "map_read_avg_s": avg_seconds(map_tasks, "input_read_duration_ms"),
        "map_process_avg_s": avg_seconds(map_tasks, "map_process_duration_ms"),
        "map_temp_save_avg_s": avg_seconds(map_tasks, "temp_save_duration_ms"),
        "reduce_transfer_avg_s": avg_seconds(reduce_tasks, "file_transfer_duration_ms"),
        "reduce_process_avg_s": avg_seconds(reduce_tasks, "reduce_process_duration_ms"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Export kafka_mode timing metrics to CSV and JSON")
    parser.add_argument("--input", required=True, help="JSON-lines metrics file, usually from metrics.sh")
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()

    input_path = Path(args.input)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    metrics = load_metrics(input_path)
    map_tasks = sorted(
        [m for m in metrics if m.get("event") == "map_task"],
        key=lambda m: m.get("map_task_id", -1),
    )
    reduce_tasks = sorted(
        [m for m in metrics if m.get("event") == "reduce_task"],
        key=lambda m: m.get("reduce_task_id", -1),
    )
    summary = build_summary(metrics, map_tasks, reduce_tasks)
    phase_breakdown = build_phase_breakdown(summary, map_tasks, reduce_tasks)

    write_csv(out_dir / "map_tasks.csv", map_tasks, MAP_FIELDS)
    write_csv(out_dir / "reduce_tasks.csv", reduce_tasks, REDUCE_FIELDS)
    write_csv(out_dir / "summary.csv", [summary], SUMMARY_FIELDS)
    write_csv(out_dir / "phase_breakdown.csv", [phase_breakdown], PHASE_BREAKDOWN_FIELDS)
    (out_dir / "metrics.json").write_text(
        json.dumps(
            {
                "summary": summary,
                "phase_breakdown": phase_breakdown,
                "map_tasks": map_tasks,
                "reduce_tasks": reduce_tasks,
                "all_metrics": metrics,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    print(f"Wrote {out_dir / 'summary.csv'}")
    print(f"Wrote {out_dir / 'phase_breakdown.csv'}")
    print(f"Wrote {out_dir / 'map_tasks.csv'}")
    print(f"Wrote {out_dir / 'reduce_tasks.csv'}")
    print(f"Wrote {out_dir / 'metrics.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
