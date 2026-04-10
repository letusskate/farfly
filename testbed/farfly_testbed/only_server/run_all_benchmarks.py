"""
All-in-one: generate configs for all policies, run each experiment sequentially.
Each cluster is started as a separate subprocess so it can be reliably killed.
"""
from __future__ import annotations
import json, subprocess, sys, time, os, signal
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATASET_DIR = BASE_DIR.parent.parent.parent / "dataset"

PYTHON = r"C:\Users\15770\.conda\envs\cardstorch\python.exe"
POLICIES = ["farfly", "round_robin", "random", "least_violation"]
# Different base port per policy to avoid port conflicts between runs
POLICY_BASE_PORTS = {"farfly": 8000, "round_robin": 8200, "random": 8400, "least_violation": 8600}

ADAPTER_ARGS = [
    "--experiment-seconds", "30",
    "--max-funcs", "100",
    "--minute-start", "600",
    "--minute-end", "610",
    "--max-events", "2500",
]


def run_adapter(policy: str) -> Path:
    out_dir = BASE_DIR / "trace_configs" / policy
    base_port = POLICY_BASE_PORTS[policy]
    cmd = [
        PYTHON,
        str(DATASET_DIR / "trace_adapter.py"),
        "--output-dir", str(out_dir),
        "--policy", policy,
        "--base-port", str(base_port),
    ] + ADAPTER_ARGS
    print(f"  Generating configs for {policy} ...")
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        print(f"  ADAPTER ERROR: {r.stderr[:300]}")
        raise RuntimeError(f"trace_adapter failed for {policy}")
    for line in r.stdout.strip().split("\n")[-3:]:
        print(f"    {line}")
    return out_dir


def start_cluster(config_dir: Path) -> subprocess.Popen:
    """Start cluster as a separate subprocess using run_trace_cluster.py."""
    cmd = [PYTHON, str(BASE_DIR / "run_trace_cluster.py"), "--config-dir", str(config_dir)]
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
    )
    # Wait for cluster to be ready
    time.sleep(5.0)
    if proc.poll() is not None:
        stderr = proc.stderr.read().decode("utf-8", errors="replace") if proc.stderr else ""
        raise RuntimeError(f"Cluster exited prematurely: {stderr[:300]}")
    return proc


def stop_cluster(proc: subprocess.Popen) -> None:
    """Terminate the cluster subprocess and all its children."""
    try:
        # On Windows, kill the process tree
        subprocess.run(
            f'taskkill /F /T /PID {proc.pid}',
            shell=True, capture_output=True, timeout=10,
        )
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass
    proc.wait(timeout=10)
    time.sleep(2.0)


def run_experiment(config_dir: Path, output_path: Path) -> dict:
    cmd = [
        PYTHON,
        str(BASE_DIR / "trace_experiment.py"),
        "--config-dir", str(config_dir),
        "--output", str(output_path),
        "--max-workers", "64",
    ]
    print(f"  Running experiment ...")
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if r.returncode != 0:
        print(f"  EXPERIMENT ERROR: {r.stderr[:500]}")
        raise RuntimeError("experiment failed")
    # print last 10 lines of stdout
    lines = r.stdout.strip().split("\n")
    for line in lines[-12:]:
        print(f"    {line}")
    return json.loads(output_path.read_text(encoding="utf-8"))


def main():
    results_dir = BASE_DIR / "trace_results"
    results_dir.mkdir(parents=True, exist_ok=True)
    all_results = {}

    for policy in POLICIES:
        print(f"\n{'='*60}")
        print(f"  POLICY: {policy}")
        print(f"{'='*60}")

        # 1. Generate configs
        config_dir = run_adapter(policy)

        # 2. Start cluster as subprocess
        print(f"  Starting cluster ...")
        cluster_proc = start_cluster(config_dir)

        # 3. Run experiment (as subprocess)
        output_path = results_dir / f"result_{policy}.json"
        try:
            result = run_experiment(config_dir, output_path)
            all_results[policy] = result
        except Exception as e:
            print(f"  FAILED: {e}")
            all_results[policy] = {"error": str(e)}

        # 4. Stop cluster
        print(f"  Stopping cluster ...")
        stop_cluster(cluster_proc)
        print(f"  Cluster stopped.")

    # Save combined
    combined = results_dir / "benchmark_comparison.json"
    combined.write_text(json.dumps(all_results, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    # Print comparison table
    print(f"\n{'='*80}")
    print("BENCHMARK COMPARISON")
    print(f"{'='*80}")
    hdr = f"{'Policy':<18} {'Submit':>7} {'OK':>6} {'Viol':>6} {'Viol%':>7} {'AvgMs':>8} {'P50':>8} {'P99':>8} {'RPS':>7}"
    print(hdr)
    print("-" * len(hdr))
    for policy in POLICIES:
        r = all_results.get(policy, {})
        if "error" in r:
            print(f"{policy:<18} ERROR: {r['error']}")
            continue
        print(
            f"{policy:<18} "
            f"{r.get('submitted',0):>7} "
            f"{r.get('success_count',0):>6} "
            f"{r.get('violation_count',0):>6} "
            f"{r.get('slo_violation_rate',0)*100:>6.1f}% "
            f"{r.get('avg_duration_ms',0):>7.1f} "
            f"{r.get('p50_duration_ms',0):>7.1f} "
            f"{r.get('p99_duration_ms',0):>7.1f} "
            f"{r.get('requests_per_second',0):>6.1f}"
        )
    print(f"{'='*80}")

    # Per-service
    all_svcs = set()
    for r in all_results.values():
        if isinstance(r, dict) and "service_summary" in r:
            all_svcs.update(r["service_summary"].keys())
    if all_svcs:
        print("\nPER-SERVICE SLO VIOLATION RATES:")
        for svc in sorted(all_svcs):
            parts = [f"  {svc:<32}"]
            for policy in POLICIES:
                r = all_results.get(policy, {})
                sd = r.get("service_summary", {}).get(svc, {})
                vr = sd.get("slo_violation_rate", 0) * 100
                n = sd.get("submitted", 0)
                parts.append(f"{policy}:{vr:.1f}%({n})")
            print("  ".join(parts))


if __name__ == "__main__":
    main()
