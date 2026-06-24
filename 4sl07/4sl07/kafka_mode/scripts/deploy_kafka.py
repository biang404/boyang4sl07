#!/usr/bin/env python3
import argparse
import gzip
import json
import tempfile
import sys
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.request import Request, urlopen

ROOT_DIR = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from machines import MachineState

WET_PATHS_URL = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-14/wet.paths.gz"
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "deploy_command.json"
DEPLOYED_HOSTS_PATH = Path(__file__).resolve().parent / "deployed_hosts.txt"
DEPLOYED_STATE_PATH = Path(__file__).resolve().parent / "deployed_state.json"


def as_tp_fqdn(host: str) -> str:
    h = host.strip()
    if h.endswith(".enst.fr"):
        return h
    return f"{h}.enst.fr"


def shell_quote(s: str) -> str:
    return "'" + s.replace("'", "'\"'\"'") + "'"


def load_config() -> dict:
    if DEFAULT_CONFIG_PATH.exists():
        try:
            return json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_config(config: dict) -> None:
    DEFAULT_CONFIG_PATH.write_text(json.dumps(config, indent=2), encoding="utf-8")


def load_deployed_hosts() -> list[str]:
    if DEPLOYED_HOSTS_PATH.exists():
        try:
            return [line.strip() for line in DEPLOYED_HOSTS_PATH.read_text(encoding="utf-8").splitlines() if line.strip()]
        except Exception:
            return []
    return []


def save_deployed_hosts(hosts: list[str]) -> None:
    DEPLOYED_HOSTS_PATH.write_text("\n".join(hosts), encoding="utf-8")


def save_deployed_state(state: dict) -> None:
    DEPLOYED_STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def cleanup_remote_tmp(user: str, hosts: list[str], tmp_dir: str) -> None:
    if not hosts:
        return
    print(f"Cleaning temporary files from {len(hosts)} previous host(s)...")
    with ThreadPoolExecutor(max_workers=8) as pool:
        jobs = []
        for host in hosts:
            cmd = (
                "tmux list-sessions 2>/dev/null | cut -d: -f1 | grep '^kafka-mode-' | "
                "while read -r session; do tmux kill-session -t \"$session\"; done; "
                f"rm -rf {tmp_dir} 2>/dev/null || true"
            )
            jobs.append(pool.submit(ssh, user, host, cmd))
        for job in as_completed(jobs):
            try:
                job.result()
            except subprocess.CalledProcessError:
                pass


def session_name(role: str, job_id: str) -> str:
    return f"kafka-mode-{role}-{job_id}"


SSH_OPTS = [
    "-o", "BatchMode=yes",
    "-o", "StrictHostKeyChecking=no",
    "-o", "ConnectTimeout=10",
]
# scp uses the same flags but spelled with -o prefix each time
SCP_OPTS = SSH_OPTS  # scp accepts -o just like ssh


def run(cmd: list[str], timeout: int | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=True, text=True, capture_output=True, timeout=timeout)


def ssh(user: str, host: str, remote_cmd: str) -> None:
    run(["ssh"] + SSH_OPTS + [f"{user}@{host}", remote_cmd])


def scp(local_path: str, user: str, host: str, remote_path: str) -> None:
    run(["scp"] + SCP_OPTS + [local_path, f"{user}@{host}:{remote_path}"])


def scp_dir(local_dir: str, user: str, host: str, remote_parent: str) -> None:
    run(["scp"] + SCP_OPTS + ["-r", local_dir, f"{user}@{host}:{remote_parent}"])


def host_ready(user: str, host: str, remote_dir: str, local_binary: str, quiet: bool = False) -> bool:
    try:
        ssh(user, host, f"mkdir -p {remote_dir} && echo ok")
        scp(local_binary, user, host, f"{remote_dir}/kafka_mode")
        ssh(user, host, f"chmod +x {remote_dir}/kafka_mode")
        return True
    except subprocess.CalledProcessError as exc:
        if quiet:
            return False
        err = (exc.stderr or "").strip()
        if err:
            print(f"[{host}] rejected during setup: {err}")
        else:
            print(f"[{host}] rejected during setup (exit {exc.returncode})")
        return False


def choose_ready_hosts(
    user: str,
    required: int,
    coordinator_host: str | None,
    candidates: list[str],
    remote_dir: str,
    local_binary: str,
) -> tuple[str, list[str]]:
    remaining = list(dict.fromkeys(candidates))
    if coordinator_host and coordinator_host in remaining:
        remaining.remove(coordinator_host)

    if coordinator_host:
        if not host_ready(user, coordinator_host, remote_dir, local_binary):
            raise SystemExit(f"Coordinator host {coordinator_host} is not reachable for ssh/scp")
        ready_hosts = [coordinator_host]
    else:
        ready_hosts = []

    rejected_count = 0
    for host in remaining:
        if len(ready_hosts) >= required:
            break
        if host_ready(user, host, remote_dir, local_binary, quiet=True):
            ready_hosts.append(host)
        else:
            rejected_count += 1

    if rejected_count:
        print(f"Skipped {rejected_count} unreachable or unauthorized TP machine(s) during setup.")

    if len(ready_hosts) < required:
        raise SystemExit(
            f"Could not provision enough reachable hosts via ssh/scp. Need {required}, got {len(ready_hosts)}"
        )

    coordinator = ready_hosts[0]
    workers = ready_hosts[1:required]
    return coordinator, workers


def list_commoncrawl_links() -> list[str]:
    req = Request(WET_PATHS_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=20) as resp:
        compressed = resp.read()
    content = gzip.decompress(compressed).decode("utf-8", errors="replace")
    return [line.strip() for line in content.splitlines() if line.strip()]


def prepare_remote_input(
    user: str,
    hosts: list[str],
    tmp_dir: str,
    link: str,
    output_name: str,
) -> None:
    remote_url = f"https://data.commoncrawl.org/{link}"
    with ThreadPoolExecutor(max_workers=8) as pool:
        jobs = []
        for host in hosts:
            cmd = (
                f"mkdir -p {tmp_dir}/data; "
                f"if [ ! -f {tmp_dir}/data/{output_name} ]; then "
                f"echo '[{host}] Downloading {output_name}...'; "
                f"curl -L --retry 5 --retry-delay 3 -C - {remote_url} -o {tmp_dir}/data/{output_name}.gz 2>&1 | tee {tmp_dir}/curl.log && "
                f"gunzip -f {tmp_dir}/data/{output_name}.gz || (cat {tmp_dir}/curl.log && exit 1); "
                f"else echo '[{host}] {output_name} already exists'; "
                f"fi"
            )
            jobs.append(pool.submit(ssh, user, host, cmd))
        for job in as_completed(jobs):
            try:
                job.result()
            except subprocess.CalledProcessError as e:
                print(f"Warning: download failed on a host, stderr: {e.stderr}")
                raise


def write_remote_manifest(
    user: str,
    hosts: list[str],
    tmp_dir: str,
    job_id: str,
    selected_links: list[str],
) -> str:
    manifest_path = f"{tmp_dir}/data/input_manifest.txt"
    lines = []
    for index, link in enumerate(selected_links):
        output_name = f"CC-MAIN-{job_id}-{index:04d}.warc.wet"
        remote_path = f"{tmp_dir}/data/{output_name}"
        remote_url = f"https://data.commoncrawl.org/{link}"
        lines.append(f"{remote_path}\t{remote_url}")
    content = "\n".join(lines) + "\n"

    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as tmp:
        tmp.write(content)
        local_manifest = Path(tmp.name)

    try:
        with ThreadPoolExecutor(max_workers=8) as pool:
            jobs = []
            for host in hosts:
                ssh(user, host, f"mkdir -p {tmp_dir}/data")
                jobs.append(pool.submit(scp, str(local_manifest), user, host, manifest_path))
            for job in as_completed(jobs):
                job.result()
    finally:
        local_manifest.unlink(missing_ok=True)

    return manifest_path


def parse_bootstrap_server(bootstrap_servers: str) -> tuple[str, int]:
    first = bootstrap_servers.split(",")[0].strip()
    if ":" not in first:
        raise SystemExit(f"Invalid --bootstrap-servers value: {bootstrap_servers}")
    host, port_str = first.rsplit(":", 1)
    try:
        return host, int(port_str)
    except ValueError as exc:
        raise SystemExit(f"Invalid broker port in --bootstrap-servers: {bootstrap_servers}") from exc


def verify_broker_reachable_from_host(user: str, probe_host: str, bootstrap_servers: str) -> None:
    broker_host, broker_port = parse_bootstrap_server(bootstrap_servers)
    remote_cmd = (
        "python3 -c \"import socket; "
        f"socket.create_connection(('{broker_host}', {broker_port}), 5).close(); "
        "print('ok')\""
    )
    try:
        ssh(user, probe_host, remote_cmd)
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        msg = stderr if stderr else f"exit {exc.returncode}"
        raise SystemExit(
            "Kafka broker is unreachable from coordinator candidate "
            f"{probe_host} using {bootstrap_servers}. Details: {msg}. "
            "Start a reachable broker first or pass --bootstrap-servers <host:port>."
        )


def host_has_local_broker(user: str, host: str, port: int) -> bool:
    probe_cmd = (
        "python3 -c \"import socket; "
        f"socket.create_connection(('127.0.0.1', {port}), 2).close(); "
        "print('ok')\""
    )
    try:
        ssh(user, host, probe_cmd)
        return True
    except subprocess.CalledProcessError:
        return False


def wait_for_local_broker(user: str, host: str, port: int, timeout_seconds: int = 60) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if host_has_local_broker(user, host, port):
            return True
        time.sleep(2)
    return host_has_local_broker(user, host, port)


def discover_broker_host(user: str, candidate_hosts: list[str], port: int) -> str | None:
    for host in candidate_hosts:
        if host_has_local_broker(user, host, port):
            return host
    return None


def resolve_bootstrap_if_localhost(
    bootstrap_servers: str,
    broker_host: str,
) -> str:
    first = bootstrap_servers.split(",")[0].strip()
    if first.startswith("127.0.0.1:") or first.startswith("localhost:"):
        port = first.rsplit(":", 1)[1]
        return f"{as_tp_fqdn(broker_host)}:{port}"
    return bootstrap_servers


def ensure_tmux_session_running(user: str, host: str, session: str, log_path: str) -> None:
    check_cmd = (
        f"tmux has-session -t {session} 2>/dev/null && echo running || "
        f"(echo missing; test -f {log_path} && tail -n 40 {log_path}; exit 1)"
    )
    try:
        ssh(user, host, check_cmd)
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        msg = stderr if stderr else f"exit {exc.returncode}"
        raise SystemExit(
            f"Coordinator session {session} exited immediately on {host}. "
            f"See {log_path}. Details: {msg}"
        )


def discover_kafka_home(user: str, host: str, kafka_home_hint: str | None) -> str | None:
    hint = kafka_home_hint or ""
    remote_cmd = (
        "set -e; "
        f"HINT={shell_quote(hint)}; "
        "for d in \"$HINT\" \"$HOME/4sl07-src/4sl07/kafka_2.13-4.3.0\" \"$HOME/4sl07-src/4sl07/kafka_2.13-4.3.0/kafka_2.13-4.3.0\" "
        "\"$HOME/4sl07-src/kafka_2.13-4.3.0\" \"$HOME/4sl07-src/kafka_2.13-4.3.0/kafka_2.13-4.3.0\" "
        "\"$HOME/kafka_2.13-4.3.0\" \"$HOME/kafka_2.13-4.3.0/kafka_2.13-4.3.0\"; do "
        "if [ -n \"$d\" ] && [ -x \"$d/bin/kafka-server-start.sh\" ] && [ -x \"$d/bin/kafka-storage.sh\" ]; then echo \"$d\"; exit 0; fi; "
        "done; "
        "found=$(find \"$HOME\" -maxdepth 8 -type f -name kafka-server-start.sh 2>/dev/null | head -n 1 || true); "
        "if [ -n \"$found\" ]; then dirname \"$(dirname \"$found\")\"; exit 0; fi; "
        "exit 1"
    )
    try:
        completed = run(["ssh"] + SSH_OPTS + [f"{user}@{host}", remote_cmd])
        home = completed.stdout.strip()
        return home if home else None
    except subprocess.CalledProcessError:
        return None


def resolve_local_kafka_dir(kafka_local_dir: str | None) -> Path | None:
    candidates: list[Path] = []
    if kafka_local_dir:
        candidates.append(Path(kafka_local_dir).expanduser())

    candidates.extend(
        [
            ROOT_DIR / "kafka_2.13-4.3.0",
            ROOT_DIR.parent / "kafka_2.13-4.3.0",
            ROOT_DIR.parent / "kafka_2.13-4.3.0" / "kafka_2.13-4.3.0",
            ROOT_DIR / "kafka_2.13-4.3.0" / "kafka_2.13-4.3.0",
        ]
    )

    for c in candidates:
        if (c / "bin" / "kafka-server-start.sh").exists() and (c / "bin" / "kafka-storage.sh").exists():
            return c.resolve()
    return None


def stage_local_kafka_to_host(user: str, host: str, tmp_dir: str, local_kafka_dir: Path) -> str:
    remote_parent = tmp_dir
    remote_kafka_dir = f"{tmp_dir}/{local_kafka_dir.name}"
    ssh(user, host, f"mkdir -p {remote_parent}")
    scp_dir(str(local_kafka_dir), user, host, remote_parent)
    return remote_kafka_dir


def start_broker_on_host(
    user: str,
    host: str,
    job_id: str,
    tmp_dir: str,
    port: int,
    kafka_home_hint: str | None,
    local_kafka_dir: Path | None,
) -> tuple[bool, str | None]:
    if port != 9092:
        return False, "auto-start broker currently supports port 9092 only"

    kafka_home = discover_kafka_home(user, host, kafka_home_hint)
    if not kafka_home and local_kafka_dir is not None:
        try:
            print(f"[{host}] staging local Kafka distribution ({local_kafka_dir})...")
            kafka_home = stage_local_kafka_to_host(user, host, tmp_dir, local_kafka_dir)
        except subprocess.CalledProcessError as exc:
            err = (exc.stderr or "").strip()
            return False, f"failed to stage local Kafka distribution: {err if err else f'exit {exc.returncode}'}"

    if not kafka_home:
        return False, "kafka installation not found on host and no local kafka distribution available"

    broker_session = session_name("broker", job_id)
    advertised_host = as_tp_fqdn(host)
    remote_cmd = (
        f"set -e; KAFKA_HOME='{kafka_home}'; "
        f"STATE_DIR='{tmp_dir}/broker'; mkdir -p \"$STATE_DIR\"; "
        "CFG=\"$STATE_DIR/server.auto.properties\"; "
        "LOG_DIR=\"$STATE_DIR/kraft-logs\"; META_DIR=\"$STATE_DIR/kraft-meta\"; "
        "rm -rf \"$LOG_DIR\" \"$META_DIR\"; mkdir -p \"$LOG_DIR\" \"$META_DIR\"; "
        "cat > \"$CFG\" <<EOF\n"
        "process.roles=broker,controller\n"
        "node.id=1\n"
        "controller.quorum.voters=1@127.0.0.1:9093\n"
        "listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093\n"
        f"advertised.listeners=PLAINTEXT://{advertised_host}:9092\n"
        "inter.broker.listener.name=PLAINTEXT\n"
        "controller.listener.names=CONTROLLER\n"
        "listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT\n"
        "num.partitions=8\n"
        "message.max.bytes=67108864\n"
        "replica.fetch.max.bytes=67108864\n"
        "socket.request.max.bytes=104857600\n"
        "offsets.topic.replication.factor=1\n"
        "transaction.state.log.replication.factor=1\n"
        "transaction.state.log.min.isr=1\n"
        "log.dirs=$LOG_DIR\n"
        "metadata.log.dir=$META_DIR\n"
        "EOF\n"
        "chmod +x \"$KAFKA_HOME\"/bin/*.sh 2>/dev/null || true; "
        "if [ ! -f \"$STATE_DIR/cluster.id\" ]; then \"$KAFKA_HOME/bin/kafka-storage.sh\" random-uuid > \"$STATE_DIR/cluster.id\"; fi; "
        "CLUSTER_ID=$(cat \"$STATE_DIR/cluster.id\"); "
        "bash \"$KAFKA_HOME/bin/kafka-storage.sh\" format --ignore-formatted -t \"$CLUSTER_ID\" -c \"$CFG\" > \"$STATE_DIR/format.log\" 2>&1; "
        f"tmux kill-session -t {broker_session} 2>/dev/null || true; "
        f"tmux new -d -s {broker_session} \"bash $KAFKA_HOME/bin/kafka-server-start.sh $CFG 2>&1 | tee {tmp_dir}/broker.log\""
    )

    try:
        ssh(user, host, remote_cmd)
        print(f"[{host}] waiting for broker port {port} to become reachable...")
        if wait_for_local_broker(user, host, port):
            return True, kafka_home
        diagnostic_cmd = (
            f"tmux has-session -t {broker_session} 2>/dev/null && echo tmux:running || echo tmux:missing; "
            f"if [ -f {tmp_dir}/broker.log ]; then echo 'broker.log:'; tail -n 25 {tmp_dir}/broker.log; else echo no_broker_log; fi; "
            f"if [ -f {tmp_dir}/broker/format.log ]; then echo 'format.log:'; tail -n 20 {tmp_dir}/broker/format.log; else echo no_format_log; fi"
        )
        try:
            diag = run(["ssh"] + SSH_OPTS + [f"{user}@{host}", diagnostic_cmd]).stdout.strip()
        except subprocess.CalledProcessError as exc:
            diag = (exc.stderr or "").strip() or f"exit {exc.returncode}"
        return False, f"broker process started but port 9092 is not reachable; diagnostics: {diag}"
    except subprocess.CalledProcessError as exc:
        err = (exc.stderr or "").strip()
        return False, err if err else f"exit {exc.returncode}"


def main() -> int:
    saved = load_config()
    parser = argparse.ArgumentParser(description="Deploy kafka_mode coordinator and workers on free TP machines")
    parser.add_argument("-u", "--user", required=True)
    parser.add_argument(
        "-b",
        "--bootstrap-servers",
        default=saved.get("bootstrap_servers", "127.0.0.1:9092"),
    )
    parser.add_argument("-j", "--job-id", default=saved.get("job_id", "run01"))
    parser.add_argument("-w", "--workers", type=int, default=saved.get("workers", 4))
    parser.add_argument("--coordinator-host", default=None, help="Optional coordinator host override")
    parser.add_argument("-t", "--map-task-count", type=int, default=saved.get("map_task_count", 64))
    parser.add_argument(
        "--chunk-size-bytes",
        type=int,
        default=saved.get("chunk_size_bytes", 4 * 1024 * 1024),
    )
    parser.add_argument("--reduce-count", type=int, default=saved.get("reduce_count", 8))
    parser.add_argument(
        "--wet-link-index",
        type=int,
        default=saved.get("wet_link_index", 0),
        help="0 selects most recent path from wet.paths, 1 second most recent, etc.",
    )
    parser.add_argument(
        "--wet-file-count",
        type=int,
        default=saved.get("wet_file_count", 1),
        help="Number of CommonCrawl WET files to process. 1 keeps the legacy single-file chunk mode.",
    )
    parser.add_argument("--tmp-dir", default=saved.get("tmp_dir", "/tmp/kafka_mode"))
    parser.add_argument("--local-binary", default=saved.get("local_binary", "./target/release/kafka_mode"))
    parser.add_argument(
        "--kafka-home-hint",
        default=saved.get("kafka_home_hint", "~/4sl07-src/4sl07/kafka_2.13-4.3.0"),
        help="Preferred Kafka installation directory on remote machines",
    )
    parser.add_argument(
        "--kafka-local-dir",
        default=saved.get("kafka_local_dir", ""),
        help="Local Kafka distribution directory to stage on remote host when missing",
    )
    parser.add_argument(
        "--no-auto-start-broker",
        action="store_true",
        help="Disable automatic broker startup when none is discovered",
    )
    parser.add_argument("--save", action="store_true", default=True)
    parser.add_argument("--no-save", action="store_false", dest="save")
    args = parser.parse_args()

    binary_path = Path(args.local_binary)
    if not binary_path.exists():
        raise SystemExit(
            f"Local binary not found: {args.local_binary}. Run 'cargo build --release' in kafka_mode first."
        )

    if args.save:
        save_config(
            {
                "bootstrap_servers": args.bootstrap_servers,
                "job_id": args.job_id,
                "workers": args.workers,
                "map_task_count": args.map_task_count,
                "chunk_size_bytes": args.chunk_size_bytes,
                "reduce_count": args.reduce_count,
                "wet_link_index": args.wet_link_index,
                "wet_file_count": args.wet_file_count,
                "tmp_dir": args.tmp_dir,
                "local_binary": args.local_binary,
                "kafka_home_hint": args.kafka_home_hint,
                "kafka_local_dir": args.kafka_local_dir,
            }
        )

    previous_hosts = load_deployed_hosts()
    if previous_hosts:
        cleanup_remote_tmp(args.user, previous_hosts, args.tmp_dir)

    state = MachineState()
    state.update()
    free_hosts = state.available()

    required = args.workers + 1
    if len(free_hosts) < required:
        raise SystemExit(f"Not enough free TP machines. Need {required}, got {len(free_hosts)}")

    if args.coordinator_host and args.coordinator_host not in free_hosts:
        raise SystemExit(f"Coordinator host {args.coordinator_host} is not currently free")

    coordinator_host, selected_workers = choose_ready_hosts(
        user=args.user,
        required=required,
        coordinator_host=args.coordinator_host,
        candidates=free_hosts,
        remote_dir=args.tmp_dir,
        local_binary=args.local_binary,
    )
    print(f"Coordinator: {coordinator_host}")
    print(f"Workers: {', '.join(selected_workers)}")

    all_hosts = [coordinator_host] + selected_workers
    broker_host = None
    broker_session = session_name("broker", args.job_id)
    broker_auto_started = False
    broker_kafka_home = None
    local_kafka_dir = resolve_local_kafka_dir(args.kafka_local_dir)
    if local_kafka_dir is not None:
        print(f"Local Kafka distribution available for staging: {local_kafka_dir}")
    else:
        print("No local Kafka distribution found for staging; will rely on remote installations only.")

    # Auto-resolve broker address when left as localhost.
    # We first try to discover a selected host already running Kafka on that port.
    # If not found, fail early with a clear message (do not continue with a fake fallback).
    bootstrap_servers = args.bootstrap_servers
    if bootstrap_servers.startswith("127.0.0.1") or bootstrap_servers.startswith("localhost"):
        port = bootstrap_servers.split(":")[-1]
        print(f"Auto-discovering broker on selected hosts (port {port})...")
        broker_host = discover_broker_host(args.user, all_hosts, int(port))
        if broker_host:
            bootstrap_servers = resolve_bootstrap_if_localhost(bootstrap_servers, broker_host)
            print(f"Broker discovered on: {bootstrap_servers}")
        else:
            if args.no_auto_start_broker:
                host_list = ", ".join(all_hosts)
                raise SystemExit(
                    "No running Kafka broker discovered on selected hosts "
                    f"[{host_list}] at port {port}. "
                    "Start a broker first, or pass --bootstrap-servers <broker-host:port>."
                )

            print("No running broker found. Attempting to auto-start broker on selected hosts...")
            start_errors: list[str] = []
            for candidate in all_hosts:
                ok, details = start_broker_on_host(
                    args.user,
                    candidate,
                    args.job_id,
                    args.tmp_dir,
                    int(port),
                    args.kafka_home_hint,
                    local_kafka_dir,
                )
                if ok:
                    broker_host = candidate
                    broker_auto_started = True
                    broker_kafka_home = details
                    bootstrap_servers = resolve_bootstrap_if_localhost(bootstrap_servers, broker_host)
                    print(f"Broker auto-started on: {bootstrap_servers}")
                    break
                start_errors.append(f"[{candidate}] {details}")

            if not broker_host:
                error_text = " | ".join(start_errors)
                raise SystemExit(
                    "Failed to auto-start Kafka broker on selected hosts. "
                    f"Details: {error_text}. "
                    "Start a broker manually or pass --bootstrap-servers <broker-host:port>."
                )

    print(f"Checking broker reachability from coordinator {coordinator_host}...")
    verify_broker_reachable_from_host(args.user, coordinator_host, bootstrap_servers)
    print("Broker connectivity check passed.")

    print("Fetching CommonCrawl wet.paths index...")
    links = list_commoncrawl_links()
    if not links:
        raise SystemExit("Could not fetch CommonCrawl wet.paths list")
    if args.wet_link_index < 0 or args.wet_link_index >= len(links):
        raise SystemExit(f"wet-link-index out of range: {args.wet_link_index}")
    if args.wet_file_count <= 0:
        raise SystemExit(f"wet-file-count must be greater than 0: {args.wet_file_count}")
    start = len(links) - 1 - args.wet_link_index
    end = start - args.wet_file_count
    if end < -1:
        raise SystemExit(
            f"wet-file-count out of range: requested {args.wet_file_count} from index {args.wet_link_index}, "
            f"but only {start + 1} link(s) are available"
        )
    selected_links = links[end + 1:start + 1]
    selected_links.reverse()
    remote_binary = f"{args.tmp_dir}/kafka_mode"
    input_file_arg = ""

    print(f"Preparing manifest for {args.wet_file_count} WET file(s)...")
    print("  Workers will download assigned WET files lazily during map tasks.")
    manifest_path = write_remote_manifest(args.user, all_hosts, args.tmp_dir, args.job_id, selected_links)
    print(f"Manifest written to: {manifest_path}")

    coordinator_session = session_name("coordinator", args.job_id)
    coordinator_input_args = f"--input-manifest {manifest_path} "
    coord_cmd = (
        f"mkdir -p {args.tmp_dir}; "
        f"tmux kill-session -t {coordinator_session} 2>/dev/null || true; "
        f"tmux new -d -s {coordinator_session} \""
        f"{remote_binary} coordinator "
        f"--bootstrap-servers {bootstrap_servers} "
        f"--job-id {args.job_id} "
        f"--workers {args.workers} "
        f"--map-task-count {args.map_task_count} "
        f"--chunk-size-bytes {args.chunk_size_bytes} "
        f"--reduce-count {args.reduce_count} "
        f"--version DefaultWithLanguageSplit "
        f"{coordinator_input_args}"
        f"--result-dir {args.tmp_dir}/result "
        f"--work-dir {args.tmp_dir} "
        f"2>&1 | tee {args.tmp_dir}/coordinator.log\""
    )
    print(f"Starting coordinator on {coordinator_host}...")
    ssh(args.user, coordinator_host, coord_cmd)
    time.sleep(2)
    ensure_tmux_session_running(
        args.user,
        coordinator_host,
        coordinator_session,
        f"{args.tmp_dir}/coordinator.log",
    )
    print("Coordinator session is running.")

    print(f"Starting {len(selected_workers)} worker(s)...")
    with ThreadPoolExecutor(max_workers=8) as pool:
        jobs = []
        for host in selected_workers:
            worker_session = session_name("worker", args.job_id)
            worker_cmd = (
                f"mkdir -p {args.tmp_dir}; "
                f"tmux kill-session -t {worker_session} 2>/dev/null || true; "
                f"tmux new -d -s {worker_session} \""
                f"{remote_binary} worker "
                f"--bootstrap-servers {bootstrap_servers} "
                f"--job-id {args.job_id} "
                f"--workers {args.workers} "
                f"--map-task-count {args.map_task_count} "
                f"--chunk-size-bytes {args.chunk_size_bytes} "
                f"--reduce-count {args.reduce_count} "
                f"--version DefaultWithLanguageSplit "
                f"--work-dir {args.tmp_dir} "
                f"2>&1 | tee {args.tmp_dir}/worker.log\""
            )
            jobs.append(pool.submit(ssh, args.user, host, worker_cmd))
        for job in as_completed(jobs):
            job.result()

    save_deployed_hosts(all_hosts)
    save_deployed_state(
        {
            "user": args.user,
            "job_id": args.job_id,
            "bootstrap_servers": bootstrap_servers,
            "tmp_dir": args.tmp_dir,
            "coordinator_host": coordinator_host,
            "worker_hosts": selected_workers,
            "all_hosts": all_hosts,
            "broker_host": broker_host,
            "broker_session": broker_session,
            "broker_auto_started": broker_auto_started,
            "broker_kafka_home": broker_kafka_home,
            "coordinator_session": coordinator_session,
            "worker_session": session_name("worker", args.job_id),
            "input_file": input_file_arg,
            "input_manifest": manifest_path,
            "result_dir": f"{args.tmp_dir}/result",
        }
    )
    print(f"Deployment launched.")
    print(f"Work directory on all hosts: {args.tmp_dir}")
    print(f"Input manifest: {manifest_path}")
    print(f"Logs: {args.tmp_dir}/*.log")
    print(f"Results: {args.tmp_dir}/result/")
    if broker_host:
        mode = "auto-started" if broker_auto_started else "pre-existing"
        print(f"Broker host ({mode}): {broker_host}")
        print(f"Broker session: {broker_session}")
    print(f"Deployed hosts saved to {DEPLOYED_HOSTS_PATH}")
    print(f"Deployed state saved to {DEPLOYED_STATE_PATH}")
    print(f"To cleanup later: rm -rf {args.tmp_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
