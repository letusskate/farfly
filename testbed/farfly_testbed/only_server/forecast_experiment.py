from __future__ import annotations

import argparse
import collections
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from pathlib import Path
import time
from typing import Any, Dict, Iterable, List
import uuid

from farfly_client import SchedulerClient, ServerClient, load_json_file
from farfly_core import load_json_file, load_service_catalog, load_workload_forecast


BASE_DIR = Path(__file__).resolve().parent


def build_event_timeline(
    scheduler_config_path: Path,
    duration_seconds: float,
    latency_scale: float,
    frequency_scale: float,
) -> List[Dict[str, Any]]:
    scheduler_config = load_json_file(scheduler_config_path)
    service_catalog = load_service_catalog(scheduler_config_path, scheduler_config)
    forecasts = load_workload_forecast(scheduler_config_path, scheduler_config)

    events: List[Dict[str, Any]] = []
    for forecast in forecasts:
        profile = service_catalog[forecast.service]
        if forecast.task_type == "latency":
            scaled_rate = max(0.0, forecast.arrival_rate * latency_scale)
            if scaled_rate <= 0.0:
                continue
            interval = 1.0 / scaled_rate
            offset = 0.0
            index = 0
            while offset < duration_seconds:
                events.append(
                    {
                        "scheduled_offset": offset,
                        "service": forecast.service,
                        "task_type": forecast.task_type,
                        "timeout_ms": profile.timeout_ms,
                        "payload": {
                            "source": "forecast-latency",
                            "sequence": index,
                            "service": forecast.service,
                        },
                    }
                )
                offset += interval
                index += 1
            continue

        scaled_fps = max(0.0, (forecast.fps_slo or profile.fps_slo or profile.fps_gpu or 0.0) * frequency_scale)
        if scaled_fps <= 0.0:
            continue
        interval = 1.0 / scaled_fps
        flow_count = max(1, forecast.flow_count)
        for flow_index in range(flow_count):
            offset = 0.0
            frame_id = 0
            flow_id = f"{forecast.service}-flow-{flow_index}"
            while offset < duration_seconds:
                events.append(
                    {
                        "scheduled_offset": offset,
                        "service": forecast.service,
                        "task_type": forecast.task_type,
                        "timeout_ms": profile.timeout_ms,
                        "flow_id": flow_id,
                        "frame_id": frame_id,
                        "payload": {
                            "source": flow_id,
                            "frame": frame_id,
                            "service": forecast.service,
                        },
                    }
                )
                offset += interval
                frame_id += 1

    events.sort(key=lambda item: (item["scheduled_offset"], item["service"], item.get("flow_id", "")))
    return events


def execute_event(client: SchedulerClient, monotonic_start: float, event: Dict[str, Any]) -> Dict[str, Any]:
    wake_at = monotonic_start + float(event["scheduled_offset"])
    remaining = wake_at - time.perf_counter()
    if remaining > 0:
        time.sleep(remaining)

    payload: Dict[str, Any] = {
        "request_id": str(uuid.uuid4()),
        "service": event["service"],
        "task_type": event["task_type"],
        "timestamp": time.time(),
        "timeout_ms": event["timeout_ms"],
        "payload": dict(event.get("payload", {})),
    }
    if "flow_id" in event:
        payload["flow_id"] = event["flow_id"]
    if "frame_id" in event:
        payload["frame_id"] = event["frame_id"]

    timeout_seconds = max(float(event["timeout_ms"]) / 1000.0 * 2.0, 30.0)
    started_at = time.perf_counter()
    try:
        status_code, body = client.submit(payload, timeout=timeout_seconds)
    except Exception as exc:
        return {
            "service": event["service"],
            "task_type": event["task_type"],
            "scheduled_offset": event["scheduled_offset"],
            "flow_id": event.get("flow_id"),
            "frame_id": event.get("frame_id"),
            "status_code": 599,
            "duration_ms": (time.perf_counter() - started_at) * 1000.0,
            "error": str(exc),
        }

    metrics = body.get("metrics", {}) if isinstance(body, dict) else {}
    return {
        "service": event["service"],
        "task_type": event["task_type"],
        "scheduled_offset": event["scheduled_offset"],
        "flow_id": event.get("flow_id"),
        "frame_id": event.get("frame_id"),
        "status_code": status_code,
        "duration_ms": (time.perf_counter() - started_at) * 1000.0,
        "scheduled_server": body.get("scheduled_server") if isinstance(body, dict) else None,
        "violated_slo": bool(metrics.get("violated_slo", False)),
        "queue_delay_ms": float(metrics.get("queue_delay_ms", 0.0)),
        "processing_time_ms": float(metrics.get("processing_time_ms", 0.0)),
        "available_credits": metrics.get("available_credits"),
        "body": body,
    }


def summarize_results(results: Iterable[Dict[str, Any]], elapsed_seconds: float) -> Dict[str, Any]:
    result_list = list(results)
    http_status_counts = collections.Counter(item["status_code"] for item in result_list)
    service_summary: Dict[str, Dict[str, Any]] = {}

    for item in result_list:
        service_name = item["service"]
        summary = service_summary.setdefault(
            service_name,
            {
                "submitted": 0,
                "success": 0,
                "violations": 0,
                "avg_queue_delay_ms": 0.0,
                "avg_processing_time_ms": 0.0,
                "server_distribution": collections.Counter(),
                "sample_errors": [],
            },
        )
        summary["submitted"] += 1
        if item["status_code"] < 400 and not item.get("error"):
            summary["success"] += 1
            summary["violations"] += 1 if item.get("violated_slo", False) else 0
            summary["avg_queue_delay_ms"] += float(item.get("queue_delay_ms", 0.0))
            summary["avg_processing_time_ms"] += float(item.get("processing_time_ms", 0.0))
            if item.get("scheduled_server"):
                summary["server_distribution"][item["scheduled_server"]] += 1
        elif len(summary["sample_errors"]) < 5:
            summary["sample_errors"].append(item.get("error") or item.get("body"))

    for service_name, summary in service_summary.items():
        success = max(1, summary["success"])
        summary["avg_queue_delay_ms"] = summary["avg_queue_delay_ms"] / success
        summary["avg_processing_time_ms"] = summary["avg_processing_time_ms"] / success
        summary["server_distribution"] = dict(summary["server_distribution"])

    success_count = sum(1 for item in result_list if item["status_code"] < 400 and not item.get("error"))
    violation_count = sum(1 for item in result_list if item.get("violated_slo", False))
    return {
        "submitted": len(result_list),
        "elapsed_seconds": elapsed_seconds,
        "requests_per_second": len(result_list) / elapsed_seconds if elapsed_seconds > 0 else None,
        "success_count": success_count,
        "violation_count": violation_count,
        "status_code_counts": dict(http_status_counts),
        "service_summary": service_summary,
        "sample_failures": [item for item in result_list if item["status_code"] >= 400 or item.get("error")][:10],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a forecast-driven FarFly experiment")
    parser.add_argument("--sender-config", default=str(BASE_DIR / "config_sender.json"), help="Path to sender config")
    parser.add_argument(
        "--scheduler-config",
        default=str(BASE_DIR / "farfly_scheduler_config.json"),
        help="Path to scheduler config",
    )
    parser.add_argument("--duration-seconds", type=float, default=3.0, help="Experiment duration in seconds")
    parser.add_argument("--latency-scale", type=float, default=1.0, help="Multiplier for latency arrival rates")
    parser.add_argument("--frequency-scale", type=float, default=0.25, help="Multiplier for frequency FPS rates")
    parser.add_argument("--max-workers", type=int, default=32, help="Client worker count")
    parser.add_argument("--output", help="Optional path to write the JSON summary")
    args = parser.parse_args()

    sender_config_path = Path(args.sender_config).resolve()
    scheduler_config_path = Path(args.scheduler_config).resolve()
    sender_config = load_json_file(sender_config_path)
    events = build_event_timeline(
        scheduler_config_path=scheduler_config_path,
        duration_seconds=max(0.1, args.duration_seconds),
        latency_scale=max(0.0, args.latency_scale),
        frequency_scale=max(0.0, args.frequency_scale),
    )

    scheduler_client = SchedulerClient.from_config(sender_config)
    monotonic_start = time.perf_counter() + 0.5
    wall_start = time.time()
    results: List[Dict[str, Any]] = []

    try:
        with ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as executor:
            futures = [executor.submit(execute_event, scheduler_client, monotonic_start, event) for event in events]
            for future in as_completed(futures):
                results.append(future.result())

        elapsed_seconds = time.time() - wall_start
        summary = summarize_results(results, elapsed_seconds)
        summary["experiment"] = {
            "duration_seconds": args.duration_seconds,
            "latency_scale": args.latency_scale,
            "frequency_scale": args.frequency_scale,
            "max_workers": args.max_workers,
            "planned_events": len(events),
            "sender_config": str(sender_config_path),
            "scheduler_config": str(scheduler_config_path),
        }

        _, scheduler_metrics = scheduler_client.metrics()
        summary["scheduler_metrics"] = scheduler_metrics

        server_metrics: Dict[str, Dict[str, Any]] = {}
        for server_info in sender_config.get("servers", []):
            server_client = ServerClient.from_server_definition(server_info)
            try:
                _, metrics = server_client.metrics()
                server_metrics[str(server_info["name"])] = metrics
            finally:
                server_client.close()
        summary["server_metrics"] = server_metrics
    finally:
        scheduler_client.close()

    output_text = json.dumps(summary, ensure_ascii=False, indent=2)
    print(output_text)

    if args.output:
        output_path = Path(args.output).resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(output_text + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()