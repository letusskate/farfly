from __future__ import annotations

import argparse
import collections
from concurrent.futures import ThreadPoolExecutor, as_completed
import copy
import json
import os
from pathlib import Path
import random
import subprocess
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple
import uuid

from farfly_client import SchedulerClient, ServerClient, load_json_file


BASE_DIR = Path(__file__).resolve().parent
RESULTS_DIR = BASE_DIR / "paper_results"
CONFIGS_DIR = BASE_DIR / "paper_configs"

BASE_SERVICES_PATH = BASE_DIR / "farfly_services.json"
BASE_WORKLOAD_PATH = BASE_DIR / "farfly_workload.json"
BASE_PLACEMENT_PATH = BASE_DIR / "farfly_static_placement.json"

SERVER_NAMES = ["server0", "server1", "server2"]
SOURCE_SERVER_WEIGHTS = {"server0": 0.56, "server1": 0.29, "server2": 0.15}
POLICY_BASE_PORTS = {"local": 9000, "alpaserve": 9200, "farfly": 9400}
SUPPORTED_POLICIES = ("local", "alpaserve", "farfly")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the local/alpaserve/farfly paper-style testbed benchmark")
    parser.add_argument("--duration-seconds", type=float, default=6.0, help="Realtime benchmark duration")
    parser.add_argument("--latency-scale", type=float, default=1.0, help="Scale factor for latency arrivals")
    parser.add_argument("--frequency-scale", type=float, default=0.57, help="Scale factor for frequency flows")
    parser.add_argument("--max-workers", type=int, default=32, help="Concurrent client RPC workers")
    parser.add_argument("--repeats", type=int, default=2, help="How many times to repeat each policy and aggregate")
    parser.add_argument("--seed", type=int, default=7, help="Random seed")
    parser.add_argument(
        "--policies",
        default=",".join(SUPPORTED_POLICIES),
        help="Comma-separated subset of local,alpaserve,farfly",
    )
    parser.add_argument(
        "--output",
        default=str(RESULTS_DIR / "paper_benchmark_summary.json"),
        help="Where to write the combined JSON summary",
    )
    return parser.parse_args()


def choose_home_server(rng: random.Random, candidates: Optional[List[str]] = None) -> str:
    if candidates:
        if len(candidates) == 1:
            return candidates[0]
        if len(candidates) == 2:
            return rng.choices(candidates, weights=[0.72, 0.28], k=1)[0]
        weights = [max(1, len(candidates) - index) for index in range(len(candidates))]
        return rng.choices(candidates, weights=weights, k=1)[0]
    return rng.choices(
        population=list(SOURCE_SERVER_WEIGHTS.keys()),
        weights=list(SOURCE_SERVER_WEIGHTS.values()),
        k=1,
    )[0]


def load_base_specs() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    services_payload = load_json_file(str(BASE_SERVICES_PATH))
    workload_payload = load_json_file(str(BASE_WORKLOAD_PATH))
    placement_payload = load_json_file(str(BASE_PLACEMENT_PATH))
    return list(services_payload["services"]), list(workload_payload["forecasts"]), placement_payload


def build_policy_services(base_services: List[Dict[str, Any]], policy: str) -> List[Dict[str, Any]]:
    services = copy.deepcopy(base_services)
    if policy == "farfly":
        return services

    for service in services:
        if service["task_type"] != "frequency":
            continue
        fps_value = max(float(service.get("fps_slo") or service.get("fps_gpu") or 1.0), 1.0)
        per_frame_timeout_ms = max(100, int(3500.0 / fps_value))
        service["task_type"] = "latency"
        service["max_inter_frame_count"] = 1
        service["fps_gpu"] = 0
        service["fps_slo"] = 0
        service["timeout_ms"] = min(int(service["timeout_ms"]), per_frame_timeout_ms)
        service["queue_target_ms"] = max(30.0, float(service["timeout_ms"]) * 0.4)
        service["batch_size"] = 1
        service["additive_credit_step"] = 1
        service["multiplicative_beta"] = 0.2
    return services


def build_policy_workload(
    base_services: List[Dict[str, Any]],
    base_workload: List[Dict[str, Any]],
    policy: str,
    latency_scale: float,
    frequency_scale: float,
) -> List[Dict[str, Any]]:
    by_name = {item["name"]: item for item in base_services}
    forecasts: List[Dict[str, Any]] = []
    for forecast in base_workload:
        forecast_copy = copy.deepcopy(forecast)
        service = by_name[forecast_copy["service"]]
        if forecast_copy["task_type"] == "latency":
            forecast_copy["arrival_rate"] = round(float(forecast_copy["arrival_rate"]) * latency_scale, 4)
            forecasts.append(forecast_copy)
            continue

        if policy == "farfly":
            forecast_copy["fps_slo"] = round(float(forecast_copy.get("fps_slo") or service.get("fps_slo", 0.0)) * frequency_scale, 4)
            forecasts.append(forecast_copy)
            continue

        fps = float(forecast_copy.get("fps_slo") or service.get("fps_slo", 0.0)) * frequency_scale
        flow_count = max(1, int(forecast_copy.get("flow_count", 1)))
        forecasts.append(
            {
                "service": forecast_copy["service"],
                "task_type": "latency",
                "arrival_rate": round(flow_count * fps, 4),
                "weight": float(forecast_copy.get("weight", 1.0)),
            }
        )
    return forecasts


def build_policy_placement(base_services: List[Dict[str, Any]], base_placement: Dict[str, Any], policy: str) -> Dict[str, Any]:
    del base_services
    del policy
    return copy.deepcopy(base_placement)


def build_servers_section(base_port: int, initial_credit_pool: int) -> List[Dict[str, Any]]:
    servers = []
    for index, server_name in enumerate(SERVER_NAMES):
        servers.append(
            {
                "name": server_name,
                "ip": "127.0.0.1",
                "port": base_port + index,
                "resource": {
                    "compute_capacity": 1.0,
                    "vram_capacity_gb": 24.0,
                    "queue_capacity": 64,
                    "gpu_count": 1,
                    "initial_credit_pool": initial_credit_pool,
                    "max_credit_pool": max(48, initial_credit_pool * 4),
                },
            }
        )
    return servers


def build_scheduler_config(policy: str, base_port: int) -> Dict[str, Any]:
    is_farfly = policy == "farfly"
    dispatch_policy = "farfly" if is_farfly else "round_robin"
    initial_credit_pool = 64 if is_farfly else 8
    priority_services = ["deeplabv3_video_segmentation", "unet_video_segmentation", "llama3_chat_generation"]
    return {
        "self": {"ip": "127.0.0.1", "port": base_port + 100, "exposed_ip": "127.0.0.1"},
        "dispatch_timeout_seconds": 20,
        "dispatch_workers": 12,
        "violation_alpha": 0.15 if is_farfly else 0.25,
        "credit_poll_interval": 0.5 if is_farfly else 2.0,
        "dispatch_policy": dispatch_policy,
        "placement": {
            "mode": "static",
            "plan_file": "trace_placement.json",
            "allow_runtime_recompute": False,
        },
        "planner": {
            "window_seconds": 60,
            "priority_services": priority_services,
        },
        "service_catalog_file": "trace_services.json",
        "workload_forecast_file": "trace_workload.json",
        "servers": build_servers_section(base_port, initial_credit_pool),
    }


def build_server_configs(base_port: int, initial_credit_pool: int) -> List[Dict[str, Any]]:
    servers_section = build_servers_section(base_port, initial_credit_pool)
    configs = []
    for index, server_name in enumerate(SERVER_NAMES):
        configs.append(
            {
                "name": server_name,
                "self": {"ip": "127.0.0.1", "port": base_port + index, "exposed_ip": "127.0.0.1"},
                "scheduler": {"ip": "127.0.0.1", "port": base_port + 100},
                "servers": copy.deepcopy(servers_section),
                "service_catalog_file": "trace_services.json",
                "max_offload_count": 3,
                "timeout_seconds": 30,
                "max_concurrent_tasks": 3,
                "resource": copy.deepcopy(servers_section[index]["resource"]),
            }
        )
    return configs


def build_sender_config(base_port: int) -> Dict[str, Any]:
    return {
        "scheduler": {"ip": "127.0.0.1", "port": base_port + 100, "submit_path": "/submit"},
        "servers": [
            {"name": server_name, "ip": "127.0.0.1", "port": base_port + index}
            for index, server_name in enumerate(SERVER_NAMES)
        ],
        "max_offload_count": 3,
        "timeout_seconds": 30,
        "max_concurrent_tasks": 3,
    }


def generate_base_events(
    base_services: List[Dict[str, Any]],
    base_workload: List[Dict[str, Any]],
    base_placement: Dict[str, Any],
    duration_seconds: float,
    latency_scale: float,
    frequency_scale: float,
    seed: int,
) -> List[Dict[str, Any]]:
    rng = random.Random(seed)
    services_by_name = {item["name"]: item for item in base_services}
    home_candidates: Dict[str, List[str]] = {}
    for item in base_placement.get("placements", []):
        home_candidates.setdefault(str(item["service"]), []).append(str(item["server_ids"][0]))
    for service_name, server_names in list(home_candidates.items()):
        deduped: List[str] = []
        for server_name in server_names:
            if server_name not in deduped:
                deduped.append(server_name)
        home_candidates[service_name] = deduped
    events: List[Dict[str, Any]] = []

    for forecast in base_workload:
        service = services_by_name[forecast["service"]]
        timeout_ms = int(service["timeout_ms"])
        if forecast["task_type"] == "latency":
            rate = float(forecast["arrival_rate"]) * latency_scale
            if rate <= 0.0:
                continue
            offset = 0.0
            sequence = 0
            while offset < duration_seconds:
                offset += rng.expovariate(rate)
                if offset >= duration_seconds:
                    break
                home_server = choose_home_server(rng, home_candidates.get(forecast["service"]))
                events.append(
                    {
                        "scheduled_offset": round(offset, 6),
                        "service": forecast["service"],
                        "kind": "latency",
                        "flow_id": None,
                        "frame_id": None,
                        "timeout_ms": timeout_ms,
                        "home_server": home_server,
                        "payload": {
                            "source": "paper-latency",
                            "sequence": sequence,
                            "home_server": home_server,
                        },
                    }
                )
                sequence += 1
            continue

        fps = float(forecast.get("fps_slo") or service.get("fps_slo", 0.0)) * frequency_scale
        if fps <= 0.0:
            continue
        interval = 1.0 / fps
        flow_count = max(1, int(forecast.get("flow_count", 1)))
        for flow_index in range(flow_count):
            flow_id = f"{forecast['service']}-flow-{flow_index}"
            home_server = choose_home_server(rng, home_candidates.get(forecast["service"]))
            offset = rng.uniform(0.0, min(interval, 0.2))
            frame_id = 0
            while offset < duration_seconds:
                events.append(
                    {
                        "scheduled_offset": round(offset, 6),
                        "service": forecast["service"],
                        "kind": "frequency",
                        "flow_id": flow_id,
                        "frame_id": frame_id,
                        "timeout_ms": timeout_ms,
                        "home_server": home_server,
                        "payload": {
                            "source": flow_id,
                            "frame": frame_id,
                            "home_server": home_server,
                        },
                    }
                )
                offset += interval
                frame_id += 1

    events.sort(key=lambda item: (item["scheduled_offset"], item["service"], item["home_server"], item.get("flow_id") or ""))
    return events


def materialize_policy_events(
    base_events: List[Dict[str, Any]],
    policy_services: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    task_type_by_service = {item["name"]: item["task_type"] for item in policy_services}
    timeout_by_service = {item["name"]: int(item["timeout_ms"]) for item in policy_services}
    materialized = []
    for event in base_events:
        event_copy = copy.deepcopy(event)
        event_copy["task_type"] = task_type_by_service[event_copy["service"]]
        event_copy["timeout_ms"] = timeout_by_service[event_copy["service"]]
        materialized.append(event_copy)
    return materialized


def write_policy_configs(
    policy: str,
    config_name: str,
    base_port: int,
    services: List[Dict[str, Any]],
    workload: List[Dict[str, Any]],
    placement: Dict[str, Any],
    events: List[Dict[str, Any]],
) -> Path:
    config_dir = CONFIGS_DIR / config_name
    config_dir.mkdir(parents=True, exist_ok=True)
    initial_credit_pool = 64 if policy == "farfly" else 8

    (config_dir / "trace_services.json").write_text(
        json.dumps({"services": services}, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    (config_dir / "trace_workload.json").write_text(
        json.dumps({"forecasts": workload}, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    (config_dir / "trace_placement.json").write_text(
        json.dumps(placement, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    (config_dir / "paper_events.json").write_text(
        json.dumps(events, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    scheduler_config = build_scheduler_config(policy, base_port)
    (config_dir / "trace_scheduler_config.json").write_text(
        json.dumps(scheduler_config, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    for server_config in build_server_configs(base_port, initial_credit_pool):
        (config_dir / f"config_{server_config['name']}.json").write_text(
            json.dumps(server_config, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    (config_dir / "config_sender.json").write_text(
        json.dumps(build_sender_config(base_port), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return config_dir


def aggregate_policy_runs(runs: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not runs:
        raise ValueError("cannot aggregate empty policy runs")

    aggregated: Dict[str, Any] = {
        "submitted": 0,
        "elapsed_seconds": 0.0,
        "success_count": 0,
        "good_count": 0,
        "violation_count": 0,
        "avg_duration_ms": 0.0,
        "p50_duration_ms": 0.0,
        "p99_duration_ms": 0.0,
        "status_code_counts": collections.Counter(),
        "service_summary": {},
        "runs": runs,
    }

    for run in runs:
        aggregated["submitted"] += int(run.get("submitted", 0))
        aggregated["elapsed_seconds"] += float(run.get("elapsed_seconds", 0.0))
        aggregated["success_count"] += int(run.get("success_count", 0))
        aggregated["good_count"] += int(run.get("good_count", 0))
        aggregated["violation_count"] += int(run.get("violation_count", 0))
        aggregated["avg_duration_ms"] += float(run.get("avg_duration_ms", 0.0)) * max(1, int(run.get("success_count", 0)))
        aggregated["p50_duration_ms"] += float(run.get("p50_duration_ms", 0.0))
        aggregated["p99_duration_ms"] += float(run.get("p99_duration_ms", 0.0))
        aggregated["status_code_counts"].update(run.get("status_code_counts", {}))
        for service_name, service_summary in run.get("service_summary", {}).items():
            target = aggregated["service_summary"].setdefault(
                service_name,
                {
                    "submitted": 0,
                    "success": 0,
                    "violations": 0,
                    "avg_queue_delay_ms_total": 0.0,
                    "avg_processing_time_ms_total": 0.0,
                    "server_distribution": collections.Counter(),
                },
            )
            target["submitted"] += int(service_summary.get("submitted", 0))
            target["success"] += int(service_summary.get("success", 0))
            target["violations"] += int(service_summary.get("violations", 0))
            target["avg_queue_delay_ms_total"] += float(service_summary.get("avg_queue_delay_ms", 0.0)) * max(1, int(service_summary.get("success", 0)))
            target["avg_processing_time_ms_total"] += float(service_summary.get("avg_processing_time_ms", 0.0)) * max(1, int(service_summary.get("success", 0)))
            target["server_distribution"].update(service_summary.get("server_distribution", {}))

    elapsed = max(1e-6, aggregated["elapsed_seconds"])
    success_count = max(1, aggregated["success_count"])
    aggregated["requests_per_second"] = round(aggregated["success_count"] / elapsed, 3)
    aggregated["goodput_rps"] = round(aggregated["good_count"] / elapsed, 3)
    aggregated["slo_violation_rate"] = round(aggregated["violation_count"] / max(1, aggregated["submitted"]), 4)
    aggregated["avg_duration_ms"] = round(aggregated["avg_duration_ms"] / success_count, 2)
    aggregated["p50_duration_ms"] = round(aggregated["p50_duration_ms"] / len(runs), 2)
    aggregated["p99_duration_ms"] = round(aggregated["p99_duration_ms"] / len(runs), 2)
    aggregated["status_code_counts"] = dict(aggregated["status_code_counts"])

    service_summary_final: Dict[str, Any] = {}
    for service_name, service_summary in aggregated["service_summary"].items():
        success = max(1, int(service_summary["success"]))
        service_summary_final[service_name] = {
            "submitted": int(service_summary["submitted"]),
            "success": int(service_summary["success"]),
            "violations": int(service_summary["violations"]),
            "avg_queue_delay_ms": round(float(service_summary["avg_queue_delay_ms_total"]) / success, 2),
            "avg_processing_time_ms": round(float(service_summary["avg_processing_time_ms_total"]) / success, 2),
            "slo_violation_rate": round(int(service_summary["violations"]) / max(1, int(service_summary["submitted"])), 4),
            "server_distribution": dict(service_summary["server_distribution"]),
        }
    aggregated["service_summary"] = service_summary_final
    return aggregated


def start_cluster(config_dir: Path) -> subprocess.Popen:
    command = [sys.executable, str(BASE_DIR / "run_trace_cluster.py"), "--config-dir", str(config_dir)]
    proc = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
    )
    time.sleep(4.0)
    if proc.poll() is not None:
        stderr = ""
        if proc.stderr is not None:
            stderr = proc.stderr.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"cluster exited prematurely: {stderr[:500]}")
    return proc


def stop_cluster(proc: subprocess.Popen) -> None:
    if os.name == "nt":
        try:
            subprocess.run(
                f"taskkill /F /T /PID {proc.pid}",
                shell=True,
                capture_output=True,
                timeout=10,
            )
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
    else:
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
    try:
        proc.wait(timeout=10)
    except Exception:
        pass
    time.sleep(1.5)


def execute_scheduler_event(client: SchedulerClient, event: Dict[str, Any]) -> Dict[str, Any]:
    started_at = time.perf_counter()
    payload: Dict[str, Any] = {
        "request_id": str(uuid.uuid4()),
        "service": event["service"],
        "task_type": event["task_type"],
        "timestamp": time.time(),
        "timeout_ms": event["timeout_ms"],
        "payload": dict(event.get("payload", {})),
    }
    if event.get("flow_id") is not None:
        payload["flow_id"] = event["flow_id"]
    if event.get("frame_id") is not None:
        payload["frame_id"] = event["frame_id"]

    timeout_seconds = max(float(event["timeout_ms"]) / 1000.0 * 2.0, 30.0)
    try:
        status_code, body = client.submit(payload, timeout=timeout_seconds)
    except Exception as exc:
        return {
            "service": event["service"],
            "task_type": event["task_type"],
            "status_code": 599,
            "duration_ms": (time.perf_counter() - started_at) * 1000.0,
            "violated_slo": False,
            "scheduled_server": None,
            "error": str(exc),
        }

    metrics = body.get("metrics", {}) if isinstance(body, dict) else {}
    return {
        "service": event["service"],
        "task_type": event["task_type"],
        "status_code": status_code,
        "duration_ms": (time.perf_counter() - started_at) * 1000.0,
        "violated_slo": bool(metrics.get("violated_slo", False)),
        "scheduled_server": body.get("scheduled_server") if isinstance(body, dict) else None,
        "queue_delay_ms": float(metrics.get("queue_delay_ms", 0.0)),
        "processing_time_ms": float(metrics.get("processing_time_ms", 0.0)),
        "error": None,
    }


def execute_local_event(server_clients: Dict[str, ServerClient], event: Dict[str, Any]) -> Dict[str, Any]:
    started_at = time.perf_counter()
    home_server = str(event["home_server"])
    request_id = str(uuid.uuid4())
    created_at = time.time()
    deadline_at = created_at + float(event["timeout_ms"]) / 1000.0
    payload: Dict[str, Any] = {
        "service": event["service"],
        "task_type": event["task_type"],
        "timeout_seconds": max(float(event["timeout_ms"]) / 1000.0 * 2.0, 30.0),
        "requests": [
            {
                "request_id": request_id,
                "payload": dict(event.get("payload", {})),
                "created_at": created_at,
                "deadline_at": deadline_at,
                "flow_id": event.get("flow_id") or request_id,
                "frame_id": event.get("frame_id"),
            }
        ],
    }
    try:
        status_code, body = server_clients[home_server].execute_batch(payload, timeout=payload["timeout_seconds"])
    except Exception as exc:
        return {
            "service": event["service"],
            "task_type": event["task_type"],
            "status_code": 599,
            "duration_ms": (time.perf_counter() - started_at) * 1000.0,
            "violated_slo": False,
            "scheduled_server": home_server,
            "error": str(exc),
        }

    request_results = body.get("request_results", []) if isinstance(body, dict) else []
    first_result = request_results[0] if request_results else {}
    return {
        "service": event["service"],
        "task_type": event["task_type"],
        "status_code": status_code,
        "duration_ms": (time.perf_counter() - started_at) * 1000.0,
        "violated_slo": bool(first_result.get("violated_slo", False)),
        "scheduled_server": home_server,
        "queue_delay_ms": float(first_result.get("queue_delay_ms", body.get("queue_delay_ms", 0.0))),
        "processing_time_ms": float(first_result.get("processing_time_ms", body.get("processing_time_ms", 0.0))),
        "error": None,
    }


def run_timed_events(
    events: List[Dict[str, Any]],
    max_workers: int,
    submit_fn,
) -> Tuple[List[Dict[str, Any]], float]:
    results: List[Dict[str, Any]] = []
    wall_start = time.time()
    monotonic_start = time.perf_counter() + 1.0
    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
        futures = []
        for event in events:
            wake_at = monotonic_start + float(event["scheduled_offset"])
            remaining = wake_at - time.perf_counter()
            if remaining > 0:
                time.sleep(remaining)
            futures.append(executor.submit(submit_fn, event))
        for future in as_completed(futures):
            results.append(future.result())
    elapsed_seconds = time.time() - wall_start
    return results, elapsed_seconds


def summarize_results(results: Iterable[Dict[str, Any]], elapsed_seconds: float) -> Dict[str, Any]:
    result_list = list(results)
    service_summary: Dict[str, Dict[str, Any]] = {}
    status_code_counts = collections.Counter(item["status_code"] for item in result_list)
    durations = []
    success_without_violation = 0

    for item in result_list:
        service_name = item["service"]
        summary = service_summary.setdefault(
            service_name,
            {
                "submitted": 0,
                "success": 0,
                "violations": 0,
                "queue_delay_ms": 0.0,
                "processing_time_ms": 0.0,
                "server_distribution": collections.Counter(),
            },
        )
        summary["submitted"] += 1
        if item["status_code"] < 400 and not item.get("error"):
            summary["success"] += 1
            summary["violations"] += 1 if item.get("violated_slo", False) else 0
            summary["queue_delay_ms"] += float(item.get("queue_delay_ms", 0.0))
            summary["processing_time_ms"] += float(item.get("processing_time_ms", 0.0))
            if item.get("scheduled_server"):
                summary["server_distribution"][str(item["scheduled_server"])] += 1
            durations.append(float(item.get("duration_ms", 0.0)))
            if not item.get("violated_slo", False):
                success_without_violation += 1

    for summary in service_summary.values():
        success = max(1, summary["success"])
        summary["avg_queue_delay_ms"] = round(summary.pop("queue_delay_ms") / success, 2)
        summary["avg_processing_time_ms"] = round(summary.pop("processing_time_ms") / success, 2)
        summary["slo_violation_rate"] = round(summary["violations"] / max(1, summary["submitted"]), 4)
        summary["server_distribution"] = dict(summary["server_distribution"])

    success_count = sum(1 for item in result_list if item["status_code"] < 400 and not item.get("error"))
    violation_count = sum(1 for item in result_list if item.get("violated_slo", False))
    durations.sort()
    return {
        "submitted": len(result_list),
        "elapsed_seconds": round(elapsed_seconds, 3),
        "requests_per_second": round(success_count / elapsed_seconds, 3) if elapsed_seconds > 0 else 0.0,
        "goodput_rps": round(success_without_violation / elapsed_seconds, 3) if elapsed_seconds > 0 else 0.0,
        "success_count": success_count,
        "good_count": success_without_violation,
        "violation_count": violation_count,
        "slo_violation_rate": round(violation_count / max(1, len(result_list)), 4),
        "avg_duration_ms": round(sum(durations) / max(1, len(durations)), 2),
        "p50_duration_ms": round(durations[len(durations) // 2], 2) if durations else 0.0,
        "p99_duration_ms": round(durations[min(len(durations) - 1, int(len(durations) * 0.99))], 2) if durations else 0.0,
        "status_code_counts": dict(status_code_counts),
        "service_summary": service_summary,
    }


def run_policy(policy: str, config_dir: Path, events: List[Dict[str, Any]], max_workers: int) -> Dict[str, Any]:
    sender_config = load_json_file(str(config_dir / "config_sender.json"))
    cluster_proc = start_cluster(config_dir)
    try:
        if policy == "local":
            server_clients = {
                str(item["name"]): ServerClient.from_server_definition(item)
                for item in sender_config["servers"]
            }
            try:
                results, elapsed_seconds = run_timed_events(
                    events,
                    max_workers,
                    lambda event: execute_local_event(server_clients, event),
                )
            finally:
                for client in server_clients.values():
                    client.close()
            summary = summarize_results(results, elapsed_seconds)
            scheduler_client = SchedulerClient.from_config(sender_config)
            try:
                _, scheduler_metrics = scheduler_client.metrics()
                summary["scheduler_metrics"] = scheduler_metrics
            finally:
                scheduler_client.close()
        else:
            scheduler_client = SchedulerClient.from_config(sender_config)
            try:
                results, elapsed_seconds = run_timed_events(
                    events,
                    max_workers,
                    lambda event: execute_scheduler_event(scheduler_client, event),
                )
                summary = summarize_results(results, elapsed_seconds)
                _, scheduler_metrics = scheduler_client.metrics()
                summary["scheduler_metrics"] = scheduler_metrics
            finally:
                scheduler_client.close()

        server_metrics: Dict[str, Any] = {}
        for item in sender_config["servers"]:
            client = ServerClient.from_server_definition(item)
            try:
                _, metrics = client.metrics()
                server_metrics[str(item["name"])] = metrics
            finally:
                client.close()
        summary["server_metrics"] = server_metrics
        summary["experiment"] = {
            "policy": policy,
            "config_dir": str(config_dir),
            "planned_events": len(events),
            "max_workers": max_workers,
        }
        return summary
    finally:
        stop_cluster(cluster_proc)


def main() -> None:
    args = parse_args()
    policies = [item.strip() for item in args.policies.split(",") if item.strip()]
    unknown = sorted(set(policies) - set(SUPPORTED_POLICIES))
    if unknown:
        raise ValueError(f"unsupported policies: {unknown}")

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    CONFIGS_DIR.mkdir(parents=True, exist_ok=True)

    base_services, base_workload, base_placement = load_base_specs()
    base_events = generate_base_events(
        base_services=base_services,
        base_workload=base_workload,
        base_placement=base_placement,
        duration_seconds=max(0.1, args.duration_seconds),
        latency_scale=max(0.0, args.latency_scale),
        frequency_scale=max(0.0, args.frequency_scale),
        seed=args.seed,
    )

    all_results: Dict[str, Dict[str, Any]] = {}
    for policy in policies:
        policy_services = build_policy_services(base_services, policy)
        policy_workload = build_policy_workload(
            base_services=base_services,
            base_workload=base_workload,
            policy=policy,
            latency_scale=max(0.0, args.latency_scale),
            frequency_scale=max(0.0, args.frequency_scale),
        )
        policy_placement = build_policy_placement(base_services, base_placement, policy)
        policy_events = materialize_policy_events(base_events, policy_services)
        runs: List[Dict[str, Any]] = []

        print(f"\n{'=' * 60}")
        print(f"POLICY: {policy}")
        print(f"{'=' * 60}")
        print(
            f"events={len(policy_events)}  latency_scale={args.latency_scale}  "
            f"frequency_scale={args.frequency_scale}  repeats={args.repeats}"
        )
        for repeat_index in range(max(1, args.repeats)):
            repeat_base_port = POLICY_BASE_PORTS[policy] + repeat_index * 300
            config_dir = write_policy_configs(
                policy,
                f"{policy}_r{repeat_index + 1}",
                repeat_base_port,
                policy_services,
                policy_workload,
                policy_placement,
                policy_events,
            )
            repeat_result = run_policy(policy, config_dir, policy_events, args.max_workers)
            runs.append(repeat_result)
            print(
                f"  run {repeat_index + 1}/{args.repeats}: goodput={repeat_result['goodput_rps']:.2f} rps  "
                f"throughput={repeat_result['requests_per_second']:.2f} rps  "
                f"viol={repeat_result['slo_violation_rate'] * 100:.1f}%"
            )

        result = aggregate_policy_runs(runs)
        all_results[policy] = result
        print(
            f"goodput={result['goodput_rps']:.2f} rps  throughput={result['requests_per_second']:.2f} rps  "
            f"viol={result['slo_violation_rate'] * 100:.1f}%  avg={result['avg_duration_ms']:.1f} ms"
        )

    if "farfly" in all_results and "alpaserve" in all_results:
        alpaserve_goodput = max(1e-6, float(all_results["alpaserve"]["goodput_rps"]))
        all_results["comparison"] = {
            "farfly_over_alpaserve_goodput_ratio": round(float(all_results["farfly"]["goodput_rps"]) / alpaserve_goodput, 3),
            "latency_scale": args.latency_scale,
            "frequency_scale": args.frequency_scale,
        }

    output_path = Path(args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(all_results, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"\n{'=' * 84}")
    print("TESTBED GOODPUT COMPARISON")
    print(f"{'=' * 84}")
    header = f"{'Policy':<14} {'Submit':>7} {'Good':>7} {'Goodput':>10} {'Throughput':>11} {'Viol%':>7} {'AvgMs':>8} {'P99Ms':>8}"
    print(header)
    print("-" * len(header))
    for policy in policies:
        result = all_results[policy]
        print(
            f"{policy:<14} {result['submitted']:>7} {result['good_count']:>7} {result['goodput_rps']:>10.2f} "
            f"{result['requests_per_second']:>11.2f} {result['slo_violation_rate'] * 100:>6.1f}% "
            f"{result['avg_duration_ms']:>8.1f} {result['p99_duration_ms']:>8.1f}"
        )
    if "comparison" in all_results:
        print("-" * len(header))
        print(
            f"farfly / alpaserve goodput ratio = "
            f"{all_results['comparison']['farfly_over_alpaserve_goodput_ratio']:.3f}x"
        )
    print(f"results saved to {output_path}")


if __name__ == "__main__":
    main()