"""
Benchmark runner: runs trace experiments with multiple dispatch policies
and produces a comparison summary.

Usage:
    python run_trace_benchmark.py [--experiment-seconds 60] [--max-funcs 150]

Workflow:
  1) Runs trace_adapter.py to generate configs for each policy
  2) For each policy: starts cluster → runs trace_experiment → collects results
  3) Prints comparison table
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

BASE_DIR = Path(__file__).resolve().parent
DATASET_DIR = BASE_DIR.parent.parent.parent / "dataset"
CONDA_PYTHON = (
    r"C:\ProgramData\miniconda3\Scripts\conda.exe run "
    r"-p C:\Users\15770\.conda\envs\cardstorch --no-banner python"
)

POLICIES = ["farfly", "round_robin", "random", "least_violation"]


def run_cmd(cmd: str, cwd: str | Path | None = None, timeout: int = 300) -> str:
    """Run a shell command and return stdout."""
    print(f"  >> {cmd}")
    result = subprocess.run(
        cmd, shell=True, cwd=str(cwd) if cwd else None,
        capture_output=True, text=True, timeout=timeout,
    )
    if result.returncode != 0:
        print(f"  STDERR: {result.stderr[:500]}")
        raise RuntimeError(f"Command failed (rc={result.returncode}): {cmd}")
    return result.stdout


def generate_configs(
    policy: str,
    experiment_seconds: float,
    max_funcs: int,
    minute_start: int,
    minute_end: int,
) -> Path:
    """Run trace_adapter.py to create config directory for a given policy."""
    out_dir = BASE_DIR / "trace_configs" / policy
    cmd = (
        f"{CONDA_PYTHON} {DATASET_DIR / 'trace_adapter.py'} "
        f"--output-dir {out_dir} "
        f"--policy {policy} "
        f"--experiment-seconds {experiment_seconds} "
        f"--max-funcs {max_funcs} "
        f"--minute-start {minute_start} "
        f"--minute-end {minute_end}"
    )
    run_cmd(cmd, cwd=DATASET_DIR)
    return out_dir


def start_cluster(config_dir: Path) -> subprocess.Popen:
    """Start FarFly cluster in background using the trace configs."""
    # Generate a small cluster runner script
    runner_script = config_dir / "_run_cluster.py"
    runner_script.write_text(
        f"""
import sys, threading, time
from pathlib import Path
BASE = Path(r"{config_dir}")
sys.path.insert(0, str(Path(r"{BASE_DIR}")))
from farfly_scheduler import start_scheduler
from farfly_server import start_server

server_configs = sorted(BASE.glob("config_server*.json"))
for cfg in server_configs:
    t = threading.Thread(target=start_server, args=(str(cfg),), daemon=True)
    t.start()
time.sleep(2.0)
start_scheduler(str(BASE / "trace_scheduler_config.json"))
""",
        encoding="utf-8",
    )

    # Use conda to run the cluster
    cmd = f"{CONDA_PYTHON} {runner_script}"
    proc = subprocess.Popen(
        cmd, shell=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    # Wait for cluster to start
    time.sleep(4.0)
    if proc.poll() is not None:
        stderr = proc.stderr.read().decode("utf-8", errors="replace") if proc.stderr else ""
        raise RuntimeError(f"Cluster exited prematurely: {stderr[:500]}")
    return proc


def run_experiment(config_dir: Path, output_path: Path, max_workers: int = 64) -> Dict[str, Any]:
    """Run trace_experiment.py against a running cluster."""
    cmd = (
        f"{CONDA_PYTHON} {BASE_DIR / 'trace_experiment.py'} "
        f"--config-dir {config_dir} "
        f"--max-workers {max_workers} "
        f"--output {output_path}"
    )
    run_cmd(cmd, cwd=BASE_DIR, timeout=600)
    return json.loads(output_path.read_text(encoding="utf-8"))


def stop_cluster(proc: subprocess.Popen) -> None:
    """Kill the cluster process."""
    try:
        proc.terminate()
        proc.wait(timeout=10)
    except Exception:
        proc.kill()


def print_comparison(all_results: Dict[str, Dict[str, Any]]) -> None:
    """Print a formatted comparison table."""
    header = f"{'Policy':<20} {'Submitted':>10} {'Success':>10} {'Violations':>10} {'Viol%':>8} {'AvgLatency':>12} {'P50':>10} {'P99':>10} {'Throughput':>12}"
    print("\n" + "=" * len(header))
    print("BENCHMARK COMPARISON")
    print("=" * len(header))
    print(header)
    print("-" * len(header))

    for policy, result in all_results.items():
        print(
            f"{policy:<20} "
            f"{result['submitted']:>10} "
            f"{result['success_count']:>10} "
            f"{result['violation_count']:>10} "
            f"{result['slo_violation_rate']*100:>7.1f}% "
            f"{result['avg_duration_ms']:>10.1f}ms "
            f"{result['p50_duration_ms']:>8.1f}ms "
            f"{result['p99_duration_ms']:>8.1f}ms "
            f"{result['requests_per_second']:>10.1f}/s"
        )
    print("=" * len(header))

    # Per-service breakdown
    print("\nPER-SERVICE SLO VIOLATION RATES:")
    all_services = set()
    for result in all_results.values():
        all_services.update(result.get("service_summary", {}).keys())

    svc_header = f"{'Service':<30} " + " ".join(f"{p:>16}" for p in all_results.keys())
    print(svc_header)
    print("-" * len(svc_header))
    for svc in sorted(all_services):
        row = f"{svc:<30} "
        for policy, result in all_results.items():
            svc_data = result.get("service_summary", {}).get(svc, {})
            vr = svc_data.get("slo_violation_rate", 0) * 100
            n = svc_data.get("submitted", 0)
            row += f"{vr:>6.1f}% ({n:>4}req) "
        print(row)


def main() -> None:
    parser = argparse.ArgumentParser(description="FarFly trace benchmark runner")
    parser.add_argument("--experiment-seconds", type=float, default=60.0)
    parser.add_argument("--max-funcs", type=int, default=150)
    parser.add_argument("--minute-start", type=int, default=600)
    parser.add_argument("--minute-end", type=int, default=660)
    parser.add_argument("--max-workers", type=int, default=64)
    parser.add_argument("--policies", nargs="+", default=POLICIES, choices=POLICIES)
    args = parser.parse_args()

    results_dir = BASE_DIR / "trace_results"
    results_dir.mkdir(parents=True, exist_ok=True)

    all_results: Dict[str, Dict[str, Any]] = {}

    for policy in args.policies:
        print(f"\n{'='*60}")
        print(f"  POLICY: {policy}")
        print(f"{'='*60}")

        # 1. Generate configs
        print(f"\n[1/3] Generating configs for {policy} …")
        config_dir = generate_configs(
            policy,
            args.experiment_seconds,
            args.max_funcs,
            args.minute_start,
            args.minute_end,
        )

        # 2. Start cluster
        print(f"\n[2/3] Starting cluster …")
        proc = start_cluster(config_dir)

        try:
            # 3. Run experiment
            print(f"\n[3/3] Running experiment …")
            output_path = results_dir / f"result_{policy}.json"
            result = run_experiment(config_dir, output_path, args.max_workers)
            all_results[policy] = result
            print(f"  → {result['submitted']} req, "
                  f"{result['violation_count']} violations "
                  f"({result['slo_violation_rate']*100:.1f}%), "
                  f"avg {result['avg_duration_ms']:.1f}ms")
        finally:
            print(f"  Stopping cluster …")
            stop_cluster(proc)
            time.sleep(2.0)  # cool-down between runs

    # Save combined results
    combined_path = results_dir / "benchmark_comparison.json"
    combined_path.write_text(
        json.dumps(all_results, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"\nAll results saved to {results_dir}")

    print_comparison(all_results)


if __name__ == "__main__":
    main()
