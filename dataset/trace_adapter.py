"""
Azure Functions Trace → FarFly adapter.

Reads the Microsoft Azure Functions trace dataset (day 01) and generates
FarFly-compatible configuration files + an event timeline for trace replay.

Usage:
    python trace_adapter.py --output-dir ../testbed/farfly_testbed/only_server/trace_configs
"""
from __future__ import annotations

import argparse
import json
import math
import random
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

DATASET_DIR = Path(__file__).resolve().parent


# ---------------------------------------------------------------------------
# Step 1 : Load & merge Azure trace CSVs
# ---------------------------------------------------------------------------

def load_trace(day: str = "d01") -> pd.DataFrame:
    inv = pd.read_csv(DATASET_DIR / f"invocations_per_function_md.anon.{day}.csv")
    dur = pd.read_csv(DATASET_DIR / f"function_durations_percentiles.anon.{day}.csv")

    minute_cols = [str(c) for c in range(1, 1441)]
    present = [c for c in minute_cols if c in inv.columns]
    inv["total_invocations"] = inv[present].sum(axis=1)
    inv["active_minutes"] = (inv[present] > 0).sum(axis=1)

    merged = inv.merge(
        dur[["HashOwner", "HashApp", "HashFunction",
             "Average", "Count", "Minimum", "Maximum",
             "percentile_Average_50", "percentile_Average_99"]],
        on=["HashOwner", "HashApp", "HashFunction"],
        how="inner",
    )
    # keep minute columns for event generation
    merged["_minute_cols"] = [present] * len(merged)
    return merged


# ---------------------------------------------------------------------------
# Step 2 : Classify functions → frequency / latency
# ---------------------------------------------------------------------------

def classify_functions(df: pd.DataFrame) -> pd.DataFrame:
    """Add 'task_type' column: 'frequency' or 'latency'."""
    is_freq = (df["Trigger"] == "timer") | (df["active_minutes"] > 500)
    df = df.copy()
    df["task_type"] = np.where(is_freq, "frequency", "latency")
    return df


# ---------------------------------------------------------------------------
# Step 3 : Map functions to service tiers
# ---------------------------------------------------------------------------

TIER_DEFS = [
    # (tier_name, lower_ms, upper_ms)
    ("fast", 0, 100),
    ("medium", 100, 500),
    ("slow", 500, 2000),
    ("heavy", 2000, float("inf")),
]

SERVICE_PROFILES: Dict[str, Dict[str, Any]] = {
    # latency tiers
    "trace_fast_latency": {
        "task_type": "latency", "model_family": "classification",
        "batch_size": 8, "mt_degree": 2, "mp_degree": 1,
        "base_service_time_ms": 30, "compute_cost": 0.08, "vram_cost_gb": 1.0,
        "timeout_ms": 500, "queue_target_ms": 40,
        "additive_credit_step": 1, "multiplicative_beta": 0.18,
        "max_credit_pool": 32, "priority_weight": 1.0,
    },
    "trace_medium_latency": {
        "task_type": "latency", "model_family": "detection",
        "batch_size": 4, "mt_degree": 1, "mp_degree": 1,
        "base_service_time_ms": 80, "compute_cost": 0.15, "vram_cost_gb": 2.0,
        "timeout_ms": 800, "queue_target_ms": 80,
        "additive_credit_step": 1, "multiplicative_beta": 0.20,
        "max_credit_pool": 24, "priority_weight": 1.0,
    },
    "trace_slow_latency": {
        "task_type": "latency", "model_family": "segmentation",
        "batch_size": 2, "mt_degree": 1, "mp_degree": 1,
        "base_service_time_ms": 200, "compute_cost": 0.25, "vram_cost_gb": 3.5,
        "timeout_ms": 1500, "queue_target_ms": 150,
        "additive_credit_step": 1, "multiplicative_beta": 0.25,
        "max_credit_pool": 16, "priority_weight": 1.1,
    },
    "trace_heavy_latency": {
        "task_type": "latency", "model_family": "llm",
        "batch_size": 2, "mt_degree": 1, "mp_degree": 2,
        "allow_cross_server_mp": True,
        "base_service_time_ms": 500, "compute_cost": 0.40, "vram_cost_gb": 8.0,
        "timeout_ms": 3000, "queue_target_ms": 300,
        "additive_credit_step": 1, "multiplicative_beta": 0.30,
        "max_credit_pool": 12, "priority_weight": 1.2,
    },
    # frequency tiers
    "trace_fast_frequency": {
        "task_type": "frequency", "model_family": "classification",
        "batch_size": 8, "mt_degree": 2, "mp_degree": 1,
        "max_inter_frame_count": 2,
        "fps_gpu": 30, "fps_slo": 20,
        "base_service_time_ms": 30, "compute_cost": 0.08, "vram_cost_gb": 1.0,
        "timeout_ms": 400, "queue_target_ms": 30,
        "additive_credit_step": 2, "multiplicative_beta": 0.20,
        "max_credit_pool": 48, "priority_weight": 1.2,
    },
    "trace_medium_frequency": {
        "task_type": "frequency", "model_family": "detection",
        "batch_size": 4, "mt_degree": 1, "mp_degree": 1,
        "max_inter_frame_count": 2,
        "fps_gpu": 18, "fps_slo": 12,
        "base_service_time_ms": 80, "compute_cost": 0.18, "vram_cost_gb": 2.5,
        "timeout_ms": 600, "queue_target_ms": 60,
        "additive_credit_step": 2, "multiplicative_beta": 0.22,
        "max_credit_pool": 48, "priority_weight": 1.3,
    },
    "trace_slow_frequency": {
        "task_type": "frequency", "model_family": "segmentation",
        "batch_size": 2, "mt_degree": 1, "mp_degree": 1,
        "max_inter_frame_count": 3,
        "fps_gpu": 8, "fps_slo": 5,
        "base_service_time_ms": 200, "compute_cost": 0.25, "vram_cost_gb": 3.5,
        "timeout_ms": 800, "queue_target_ms": 100,
        "additive_credit_step": 2, "multiplicative_beta": 0.25,
        "max_credit_pool": 32, "priority_weight": 1.4,
    },
}


def assign_tier(avg_ms: float) -> str:
    for tier_name, lo, hi in TIER_DEFS:
        if lo <= avg_ms < hi:
            return tier_name
    return "heavy"


def map_to_services(df: pd.DataFrame) -> pd.DataFrame:
    """Assign each function a service name based on tier + task_type."""
    df = df.copy()
    avg = df["Average"].fillna(100.0).clip(lower=0)
    tier = avg.apply(assign_tier)
    df["service"] = tier.str.cat(df["task_type"], sep="_").apply(
        lambda s: f"trace_{s}"
    )
    # drop combos that don't exist (e.g. heavy_frequency is very rare)
    valid = set(SERVICE_PROFILES.keys())
    df.loc[~df["service"].isin(valid), "service"] = df.loc[
        ~df["service"].isin(valid), "task_type"
    ].apply(lambda tt: f"trace_slow_{tt}")
    return df


# ---------------------------------------------------------------------------
# Step 4 : Sample functions & generate event timeline
# ---------------------------------------------------------------------------

def sample_functions(
    df: pd.DataFrame,
    max_funcs: int = 150,
    seed: int = 42,
) -> pd.DataFrame:
    """Select top-activity functions per service tier, up to max_funcs total."""
    rng = random.Random(seed)
    sampled_parts = []
    service_groups = df.groupby("service")
    # proportional allocation
    tier_sizes = service_groups.size()
    total = tier_sizes.sum()
    for svc_name, group in service_groups:
        alloc = max(3, int(max_funcs * len(group) / total))
        top = group.nlargest(alloc, "total_invocations")
        sampled_parts.append(top)
    result = pd.concat(sampled_parts, ignore_index=True)
    if len(result) > max_funcs:
        result = result.nlargest(max_funcs, "total_invocations")
    return result.reset_index(drop=True)


def generate_events(
    sampled: pd.DataFrame,
    minute_start: int = 600,
    minute_end: int = 660,
    experiment_seconds: float = 60.0,
    max_events: int = 5000,
    seed: int = 42,
) -> List[Dict[str, Any]]:
    """
    Generate a timestamped event list from per-minute invocation counts.

    Real minutes [minute_start, minute_end) → experiment time [0, experiment_seconds).
    If total events would exceed max_events, subsample uniformly.
    """
    rng = random.Random(seed)
    real_minutes = minute_end - minute_start
    time_scale = experiment_seconds / (real_minutes * 60.0)  # real seconds → exp seconds

    # First pass: count total raw events to compute subsample ratio
    total_raw = 0
    minute_col_names = [str(c) for c in range(1, 1441)]
    for _, row in sampled.iterrows():
        for minute_idx in range(minute_start, minute_end):
            col = str(minute_idx)
            if col in sampled.columns:
                total_raw += max(0, int(row.get(col, 0)))
    subsample_rate = min(1.0, max_events / max(1, total_raw))

    events: List[Dict[str, Any]] = []

    for _, row in sampled.iterrows():
        service = row["service"]
        task_type = row["task_type"]
        func_hash = f"{row['HashApp']}_{row['HashFunction']}"
        profile = SERVICE_PROFILES[service]

        flow_id = f"{service}-{func_hash}" if task_type == "frequency" else None
        frame_counter = 0

        for minute_idx in range(minute_start, minute_end):
            col = str(minute_idx)
            if col not in sampled.columns:
                continue
            count = int(row.get(col, 0))
            if count <= 0:
                continue

            # subsample if needed
            effective_count = count
            if subsample_rate < 1.0:
                effective_count = int(count * subsample_rate)
                if effective_count == 0 and rng.random() < (count * subsample_rate):
                    effective_count = 1
                if effective_count <= 0:
                    continue

            # distribute invocations uniformly in this minute
            real_second_start = (minute_idx - minute_start) * 60.0
            for _ in range(effective_count):
                offset_in_minute = rng.random() * 60.0
                real_offset = real_second_start + offset_in_minute
                exp_offset = real_offset * time_scale

                event: Dict[str, Any] = {
                    "scheduled_offset": round(exp_offset, 6),
                    "service": service,
                    "task_type": task_type,
                    "timeout_ms": profile["timeout_ms"],
                    "payload": {
                        "source": "azure-trace",
                        "func": func_hash,
                        "service": service,
                    },
                }
                if task_type == "frequency":
                    event["flow_id"] = flow_id
                    event["frame_id"] = frame_counter
                    frame_counter += 1
                events.append(event)

    events.sort(key=lambda e: (e["scheduled_offset"], e["service"], e.get("flow_id", "")))
    return events


# ---------------------------------------------------------------------------
# Step 5 : Generate FarFly config files
# ---------------------------------------------------------------------------

def build_service_catalog(services_used: set) -> Dict[str, Any]:
    catalog = []
    for svc_name in sorted(services_used):
        profile = dict(SERVICE_PROFILES[svc_name])
        profile["name"] = svc_name
        profile.setdefault("workspace_model", "")
        profile.setdefault("allow_cross_server_mp", False)
        profile.setdefault("max_inter_frame_count", 1)
        profile.setdefault("fps_gpu", 0)
        profile.setdefault("fps_slo", 0)
        catalog.append(profile)
    return {"services": catalog}


def build_workload_forecast(
    events: List[Dict[str, Any]],
    experiment_seconds: float,
    services_used: set,
) -> Dict[str, Any]:
    from collections import Counter
    service_counts: Counter = Counter()
    flow_ids_per_service: Dict[str, set] = {}
    for ev in events:
        svc = ev["service"]
        service_counts[svc] += 1
        if ev["task_type"] == "frequency" and ev.get("flow_id"):
            flow_ids_per_service.setdefault(svc, set()).add(ev["flow_id"])

    forecasts = []
    for svc_name in sorted(services_used):
        profile = SERVICE_PROFILES[svc_name]
        total = service_counts.get(svc_name, 0)
        if profile["task_type"] == "frequency":
            flow_count = len(flow_ids_per_service.get(svc_name, set())) or 1
            fps = total / experiment_seconds / flow_count if experiment_seconds > 0 else 1.0
            forecasts.append({
                "service": svc_name,
                "task_type": "frequency",
                "flow_count": flow_count,
                "fps_slo": round(max(1.0, fps), 2),
                "weight": profile.get("priority_weight", 1.0),
            })
        else:
            rate = total / experiment_seconds if experiment_seconds > 0 else 1.0
            forecasts.append({
                "service": svc_name,
                "task_type": "latency",
                "arrival_rate": round(max(0.1, rate), 2),
                "weight": profile.get("priority_weight", 1.0),
            })
    return {"forecasts": forecasts}


def build_placement(services_used: set, server_names: List[str]) -> Dict[str, Any]:
    """Spread services across servers round-robin, heavy_latency gets multi-server MP."""
    placements = []
    sorted_svcs = sorted(services_used)
    for i, svc_name in enumerate(sorted_svcs):
        profile = SERVICE_PROFILES[svc_name]
        if profile.get("mp_degree", 1) > 1 and profile.get("allow_cross_server_mp", False):
            # model-parallel: span first mp_degree servers
            mp = profile["mp_degree"]
            ids = [server_names[j % len(server_names)] for j in range(mp)]
            placements.append({
                "service": svc_name,
                "server_ids": ids,
                "stage": "static-group",
            })
        else:
            # place on 2 servers for redundancy
            s1 = server_names[i % len(server_names)]
            s2 = server_names[(i + 1) % len(server_names)]
            placements.append({"service": svc_name, "server_ids": [s1], "stage": "static"})
            if s1 != s2:
                placements.append({"service": svc_name, "server_ids": [s2], "stage": "static"})
    return {
        "description": "Auto-generated placement from Azure trace adapter",
        "window_seconds": 60,
        "placements": placements,
    }


def build_scheduler_config(
    services_used: set,
    server_names: List[str],
    dispatch_policy: str = "farfly",
    base_port: int = 5000,
) -> Dict[str, Any]:
    servers = []
    for i, name in enumerate(server_names):
        servers.append({
            "name": name,
            "ip": "127.0.0.1",
            "port": base_port + i,
            "resource": {
                "compute_capacity": 1.0,
                "vram_capacity_gb": 24.0,
                "queue_capacity": 128,
                "gpu_count": 1,
                "initial_credit_pool": 64,
            },
        })
    priority = [s for s in sorted(services_used) if "frequency" in s]
    return {
        "self": {"ip": "127.0.0.1", "port": base_port + 100, "exposed_ip": "127.0.0.1"},
        "dispatch_timeout_seconds": 30,
        "dispatch_workers": 16,
        "violation_alpha": 0.15,
        "credit_poll_interval": 0.5,
        "dispatch_policy": dispatch_policy,
        "placement": {
            "mode": "static",
            "plan_file": "trace_placement.json",
            "allow_runtime_recompute": False,
        },
        "planner": {
            "window_seconds": 60,
            "priority_services": priority[:3],
        },
        "service_catalog_file": "trace_services.json",
        "workload_forecast_file": "trace_workload.json",
        "servers": servers,
    }


def build_server_configs(server_names: List[str], base_port: int = 5000) -> List[Dict[str, Any]]:
    servers_section = []
    for i, name in enumerate(server_names):
        servers_section.append({
            "name": name,
            "ip": "127.0.0.1",
            "port": base_port + i,
            "resource": {
                "compute_capacity": 1.0,
                "vram_capacity_gb": 24.0,
                "queue_capacity": 128,
                "gpu_count": 1,
                "initial_credit_pool": 64,
                "max_credit_pool": 128,
            },
        })
    configs = []
    for i, name in enumerate(server_names):
        configs.append({
            "name": name,
            "self": {"ip": "127.0.0.1", "port": base_port + i, "exposed_ip": "127.0.0.1"},
            "scheduler": {"ip": "127.0.0.1", "port": base_port + 100},
            "servers": servers_section,
            "service_catalog_file": "trace_services.json",
            "max_offload_count": 3,
            "timeout_seconds": 30,
            "max_concurrent_tasks": 4,
            "resource": dict(servers_section[i]["resource"]),
        })
    return configs


def build_sender_config(server_names: List[str], base_port: int = 5000) -> Dict[str, Any]:
    return {
        "scheduler": {"ip": "127.0.0.1", "port": base_port + 100, "submit_path": "/submit"},
        "servers": [
            {"name": name, "ip": "127.0.0.1", "port": base_port + i}
            for i, name in enumerate(server_names)
        ],
        "max_offload_count": 3,
        "timeout_seconds": 30,
        "max_concurrent_tasks": 4,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Azure trace → FarFly config adapter")
    parser.add_argument(
        "--output-dir",
        default=str(DATASET_DIR.parent / "testbed" / "farfly_testbed" / "only_server" / "trace_configs"),
    )
    parser.add_argument("--day", default="d01")
    parser.add_argument("--minute-start", type=int, default=600, help="Start minute (0-1439)")
    parser.add_argument("--minute-end", type=int, default=660, help="End minute (exclusive)")
    parser.add_argument("--experiment-seconds", type=float, default=60.0)
    parser.add_argument("--max-funcs", type=int, default=150)
    parser.add_argument("--servers", type=int, default=3)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-events", type=int, default=3000, help="Max events (subsample if exceeded)")
    parser.add_argument("--policy", default="farfly", choices=["farfly", "round_robin", "random", "least_violation"])
    parser.add_argument("--base-port", type=int, default=5000, help="Base port for servers (scheduler = base+100)")
    args = parser.parse_args()

    out = Path(args.output_dir).resolve()
    out.mkdir(parents=True, exist_ok=True)
    server_names = [f"server{i}" for i in range(args.servers)]

    print("[1/6] Loading trace …")
    df = load_trace(args.day)
    print(f"       {len(df)} functions loaded")

    print("[2/6] Classifying functions …")
    df = classify_functions(df)
    freq_n = (df["task_type"] == "frequency").sum()
    lat_n = (df["task_type"] == "latency").sum()
    print(f"       frequency={freq_n}, latency={lat_n}")

    print("[3/6] Mapping to service tiers …")
    df = map_to_services(df)
    print(f"       services: {df['service'].value_counts().to_dict()}")

    print("[4/6] Sampling functions …")
    sampled = sample_functions(df, max_funcs=args.max_funcs, seed=args.seed)
    services_used = set(sampled["service"].unique())
    print(f"       {len(sampled)} functions sampled, {len(services_used)} service tiers")

    print("[5/6] Generating events …")
    events = generate_events(
        sampled,
        minute_start=args.minute_start,
        minute_end=args.minute_end,
        experiment_seconds=args.experiment_seconds,
        max_events=args.max_events,
        seed=args.seed,
    )
    print(f"       {len(events)} events generated over {args.experiment_seconds}s")

    print("[6/6] Writing configs …")
    # service catalog
    svc_catalog = build_service_catalog(services_used)
    (out / "trace_services.json").write_text(
        json.dumps(svc_catalog, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    # workload forecast
    wl = build_workload_forecast(events, args.experiment_seconds, services_used)
    (out / "trace_workload.json").write_text(
        json.dumps(wl, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    # placement
    pl = build_placement(services_used, server_names)
    (out / "trace_placement.json").write_text(
        json.dumps(pl, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    # scheduler config
    sched = build_scheduler_config(services_used, server_names, args.policy, args.base_port)
    (out / "trace_scheduler_config.json").write_text(
        json.dumps(sched, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    # server configs
    srv_cfgs = build_server_configs(server_names, args.base_port)
    for cfg in srv_cfgs:
        (out / f"config_{cfg['name']}.json").write_text(
            json.dumps(cfg, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
    # sender config
    sender = build_sender_config(server_names, args.base_port)
    (out / "config_sender.json").write_text(
        json.dumps(sender, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    # event timeline
    (out / "trace_events.json").write_text(
        json.dumps(events, ensure_ascii=False) + "\n", encoding="utf-8"
    )

    print(f"\nDone. Configs written to {out}")
    svc_summary = sampled.groupby("service").agg(
        funcs=("HashFunction", "count"),
        total_inv=("total_invocations", "sum"),
    )
    print(svc_summary.to_string())
    print(f"\nTotal events: {len(events)}")


if __name__ == "__main__":
    main()
