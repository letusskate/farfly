import argparse
import copy
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import collections
import json
import random
import time
import uuid

from farfly_client import SchedulerClient, load_json_file


BASE_DIR = Path(__file__).resolve().parent
LATENCY_SERVICES = [
    ("resnet50_image_classification", 1200),
    ("yolov10_image_detection", 1500),
    ("llama3_chat_generation", 2500),
]
FREQUENCY_SERVICES = [
    ("deeplabv3_video_segmentation", 400),
    ("unet_video_segmentation", 350),
]


def load_sender_config(config_file: str) -> dict:
    return load_json_file(config_file)


def build_random_payload(index: int) -> dict:
    if random.random() < 0.45:
        service_name, timeout_ms = random.choice(LATENCY_SERVICES)
        return {
            "service": service_name,
            "task_type": "latency",
            "timeout_ms": timeout_ms,
            "payload": {"request_index": index, "kind": "latency"},
        }

    service_name, timeout_ms = random.choice(FREQUENCY_SERVICES)
    flow_id = f"camera-{index % 6}"
    return {
        "service": service_name,
        "task_type": "frequency",
        "timeout_ms": timeout_ms,
        "flow_id": flow_id,
        "frame_id": index,
        "payload": {"request_index": index, "kind": "frequency", "flow": flow_id},
    }


def materialize_payload(payload_template: dict) -> dict:
    payload = copy.deepcopy(payload_template)
    payload.setdefault("request_id", str(uuid.uuid4()))
    payload["timestamp"] = time.time()
    return payload


def submit_request(client: SchedulerClient, payload_template: dict) -> tuple[int, dict]:
    payload = materialize_payload(payload_template)
    timeout_seconds = max(float(payload.get("timeout_ms", 1000.0)) / 1000.0 * 2.0, 30.0)
    return client.submit(payload, timeout=timeout_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(description="Send a concurrent mixed workload to the FarFly scheduler")
    parser.add_argument("--config", default=str(BASE_DIR / "config_sender.json"), help="Path to sender config")
    parser.add_argument("--count", type=int, default=120, help="Number of requests to submit")
    parser.add_argument("--workers", type=int, default=12, help="Concurrent client workers")
    args = parser.parse_args()

    config = load_sender_config(args.config)
    client = SchedulerClient.from_config(config)
    started_at = time.time()
    results = []

    try:
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
            future_map = {
                executor.submit(submit_request, client, build_random_payload(index)): index
                for index in range(args.count)
            }
            for future in as_completed(future_map):
                index = future_map[future]
                try:
                    status_code, body = future.result()
                except Exception as exc:
                    results.append({"index": index, "status_code": 599, "body": {"status": "failed", "reason": str(exc)}})
                    continue
                results.append({"index": index, "status_code": status_code, "body": body})
    finally:
        client.close()

    elapsed = time.time() - started_at
    status_counter = collections.Counter(item["status_code"] for item in results)
    success_count = sum(1 for item in results if item["body"].get("status") == "success")
    violation_count = sum(
        1 for item in results if item["body"].get("metrics", {}).get("violated_slo", False)
    )

    summary = {
        "submitted": args.count,
        "elapsed_seconds": elapsed,
        "requests_per_second": args.count / elapsed if elapsed > 0 else None,
        "status_code_counts": dict(status_counter),
        "success_count": success_count,
        "violation_count": violation_count,
        "sample_failures": [item for item in results if item["status_code"] >= 400][:5],
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
