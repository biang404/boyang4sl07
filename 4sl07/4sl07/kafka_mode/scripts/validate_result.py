#!/usr/bin/env python3
import argparse
import json
from collections import Counter
from pathlib import Path


DELIMITERS = {
    " ",
    "\n",
    "\r",
    ".",
    ",",
    "?",
    ":",
    "!",
    "(",
    ")",
    ";",
    "-",
    "_",
    '"',
    "{",
    "}",
    "[",
    "]",
    "+",
    "=",
    "/",
    "\\",
}

ASCII_LOWER_TABLE = str.maketrans(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
    "abcdefghijklmnopqrstuvwxyz",
)


def load_json_file(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def split_like_rust_mapper(text: str):
    start = None
    for index, char in enumerate(text):
        if char in DELIMITERS:
            if start is not None:
                yield text[start:index]
                start = None
        elif start is None:
            start = index
    if start is not None:
        yield text[start:]


def count_input_chunks(input_file: Path, map_task_count: int, chunk_size_bytes: int) -> Counter:
    input_len = input_file.stat().st_size
    computed_maps = map_task_count if map_task_count > 0 else max(input_len // chunk_size_bytes, 1)
    counts: Counter = Counter()

    with input_file.open("rb") as f:
        for map_id in range(computed_maps):
            f.seek(map_id * chunk_size_bytes)
            data = f.read(chunk_size_bytes)
            text = data.decode("utf-8", errors="replace").translate(ASCII_LOWER_TABLE)
            counts.update(token for token in split_like_rust_mapper(text) if token)

    return counts


def load_result_counts(result_dir: Path, job_id: str | None, reduce_count: int | None) -> Counter:
    if job_id:
        paths = sorted(result_dir.glob(f"reduce_*_{job_id}.json"))
    else:
        paths = sorted(result_dir.glob("reduce_*.json"))

    if reduce_count is not None and len(paths) != reduce_count:
        raise SystemExit(
            f"Expected {reduce_count} reduce result file(s), found {len(paths)} in {result_dir}"
        )
    if not paths:
        raise SystemExit(f"No reduce result files found in {result_dir}")

    counts: Counter = Counter()
    for path in paths:
        with path.open(encoding="utf-8") as f:
            entries = json.load(f)
        for token, count in entries:
            counts[token] += count
    return counts


def print_examples(title: str, items, sample_limit: int) -> None:
    print(title)
    for index, item in enumerate(items):
        if index >= sample_limit:
            break
        print(f"  {item}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate kafka_mode reduce results by recomputing the expected counts from input chunks."
    )
    parser.add_argument("--input-file", type=Path, help="Input WET file used by the coordinator")
    parser.add_argument("--result-dir", type=Path, help="Directory containing reduce_*_<job_id>.json files")
    parser.add_argument("--job-id", default=None)
    parser.add_argument("--map-task-count", type=int, default=None)
    parser.add_argument("--chunk-size-bytes", type=int, default=None)
    parser.add_argument("--reduce-count", type=int, default=None)
    parser.add_argument("--state-file", type=Path, default=Path("scripts/deployed_state.json"))
    parser.add_argument("--deploy-command", type=Path, default=Path("scripts/deploy_command.json"))
    parser.add_argument("--sample-limit", type=int, default=20)
    args = parser.parse_args()

    state = load_json_file(args.state_file)
    deploy_command = load_json_file(args.deploy_command)

    input_file = args.input_file or (Path(state["input_file"]) if "input_file" in state else None)
    result_dir = args.result_dir or (Path(state["result_dir"]) if "result_dir" in state else None)
    job_id = args.job_id or state.get("job_id") or deploy_command.get("job_id")
    map_task_count = args.map_task_count
    if map_task_count is None:
        map_task_count = deploy_command.get("map_task_count")
    chunk_size_bytes = args.chunk_size_bytes
    if chunk_size_bytes is None:
        chunk_size_bytes = deploy_command.get("chunk_size_bytes")
    reduce_count = args.reduce_count
    if reduce_count is None:
        reduce_count = deploy_command.get("reduce_count")

    if input_file is None:
        raise SystemExit("Missing --input-file and no input_file found in deployed_state.json")
    if result_dir is None:
        raise SystemExit("Missing --result-dir and no result_dir found in deployed_state.json")
    if map_task_count is None:
        raise SystemExit("Missing --map-task-count and no map_task_count found in deploy_command.json")
    if chunk_size_bytes is None:
        raise SystemExit("Missing --chunk-size-bytes and no chunk_size_bytes found in deploy_command.json")

    print(f"Input: {input_file}")
    print(f"Results: {result_dir}")
    print(f"job_id={job_id} map_task_count={map_task_count} chunk_size_bytes={chunk_size_bytes} reduce_count={reduce_count}")

    expected = count_input_chunks(input_file, map_task_count, chunk_size_bytes)
    actual = load_result_counts(result_dir, job_id, reduce_count)

    print(f"Expected unique tokens: {len(expected)}")
    print(f"Actual unique tokens:   {len(actual)}")
    print(f"Expected total count:   {sum(expected.values())}")
    print(f"Actual total count:     {sum(actual.values())}")

    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    mismatched = sorted(
        (token, expected[token], actual[token])
        for token in set(expected) & set(actual)
        if expected[token] != actual[token]
    )

    if not missing and not extra and not mismatched:
        print("Validation passed: result files match recomputed input counts.")
        return 0

    print("Validation failed.")
    if missing:
        print_examples("Missing tokens:", missing, args.sample_limit)
    if extra:
        print_examples("Extra tokens:", extra, args.sample_limit)
    if mismatched:
        print_examples("Mismatched counts: token, expected, actual", mismatched, args.sample_limit)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())