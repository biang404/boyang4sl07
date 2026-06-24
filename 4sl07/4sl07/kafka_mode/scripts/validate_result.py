#!/usr/bin/env python3
import argparse
import gzip
import json
import shutil
from collections import Counter
from pathlib import Path
from urllib.request import Request, urlopen


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


def count_file(input_file: Path) -> Counter:
    with input_file.open("rb") as f:
        data = f.read()
    text = data.decode("utf-8", errors="replace").translate(ASCII_LOWER_TABLE)
    return Counter(token for token in split_like_rust_mapper(text) if token)


def manifest_inputs(manifest_path: Path) -> list[tuple[Path, str]]:
    inputs = []
    with manifest_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            fields = line.split("\t", 1)
            path = Path(fields[0])
            url = fields[1] if len(fields) > 1 else ""
            inputs.append((path, url))
    if not inputs:
        raise SystemExit(f"Input manifest is empty: {manifest_path}")
    return inputs


def download_input_file(path: Path, url: str) -> None:
    if not url:
        raise SystemExit(f"Input file is missing and manifest has no URL: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading missing validation input: {url} -> {path}")
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=60) as response:
        if url.endswith(".gz"):
            with gzip.GzipFile(fileobj=response) as gz, path.open("wb") as out:
                shutil.copyfileobj(gz, out)
        else:
            with path.open("wb") as out:
                shutil.copyfileobj(response, out)


def count_manifest_inputs(manifest_path: Path, download_missing_inputs: bool) -> Counter:
    counts: Counter = Counter()
    inputs = manifest_inputs(manifest_path)
    for index, (input_file, input_url) in enumerate(inputs, start=1):
        if not input_file.exists():
            if download_missing_inputs:
                download_input_file(input_file, input_url)
            else:
                raise SystemExit(f"Input file is missing: {input_file}")
        print(f"Counting manifest input {index}/{len(inputs)}: {input_file}")
        counts.update(count_file(input_file))
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
    parser.add_argument("--input-manifest", type=Path, help="Manifest containing WET input files used by the coordinator")
    parser.add_argument("--result-dir", type=Path, help="Directory containing reduce_*_<job_id>.json files")
    parser.add_argument("--job-id", default=None)
    parser.add_argument("--reduce-count", type=int, default=None)
    parser.add_argument("--state-file", type=Path, default=Path("scripts/deployed_state.json"))
    parser.add_argument("--deploy-command", type=Path, default=Path("scripts/deploy_command.json"))
    parser.add_argument("--sample-limit", type=int, default=20)
    parser.add_argument("--no-download-missing-inputs", action="store_false", dest="download_missing_inputs")
    args = parser.parse_args()

    state = load_json_file(args.state_file)
    deploy_command = load_json_file(args.deploy_command)

    input_manifest = args.input_manifest or (Path(state["input_manifest"]) if state.get("input_manifest") else None)
    input_file = args.input_file or (Path(state["input_file"]) if state.get("input_file") else None)
    result_dir = args.result_dir or (Path(state["result_dir"]) if "result_dir" in state else None)
    job_id = args.job_id or state.get("job_id") or deploy_command.get("job_id")
    reduce_count = args.reduce_count
    if reduce_count is None:
        reduce_count = deploy_command.get("reduce_count")

    if input_file is None and input_manifest is None:
        raise SystemExit("Missing --input-file/--input-manifest and no input path found in deployed_state.json")
    if result_dir is None:
        raise SystemExit("Missing --result-dir and no result_dir found in deployed_state.json")
    if input_manifest is not None:
        print(f"Input manifest: {input_manifest}")
    else:
        print(f"Input: {input_file}")
    print(f"Results: {result_dir}")
    print(f"job_id={job_id} reduce_count={reduce_count}")

    expected = count_manifest_inputs(input_manifest, args.download_missing_inputs) if input_manifest is not None else count_file(input_file)
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