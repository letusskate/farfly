import argparse
import copy
from pathlib import Path
import json
import time
import uuid

from farfly_client import SchedulerClient, load_json_file


BASE_DIR = Path(__file__).resolve().parent


def load_sender_config(config_file: str) -> dict:
    return load_json_file(config_file)


def materialize_payload(payload_template: dict) -> dict:
    payload = copy.deepcopy(payload_template)
    payload.setdefault("request_id", str(uuid.uuid4()))
    payload["timestamp"] = time.time()
    return payload


def submit_request(client: SchedulerClient, payload_template: dict) -> dict:
    payload = materialize_payload(payload_template)
    timeout_seconds = max(float(payload.get("timeout_ms", 1000.0)) / 1000.0 * 2.0, 30.0)
    status_code, body = client.submit(payload, timeout=timeout_seconds)
    return {
        "status_code": status_code,
        "body": body,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Send a small mixed workload to the FarFly scheduler")
    parser.add_argument("--config", default=str(BASE_DIR / "config_sender.json"), help="Path to sender config")
    args = parser.parse_args()

    config = load_sender_config(args.config)
    client = SchedulerClient.from_config(config)

    requests_to_send = [
        {
            "label": "latency classification",
            "payload": {
                "service": "resnet50_image_classification",
                "task_type": "latency",
                "timeout_ms": 1200,
                "payload": {"source": "camera-a", "image": "frame_001.jpg"},
            },
        },
        {
            "label": "tight-deadline detection",
            "payload": {
                "service": "yolov10_image_detection",
                "task_type": "latency",
                "timeout_ms": 1500,
                "payload": {"source": "camera-b", "image": "frame_188.jpg"},
            },
        },
    ]

    frequency_flow = "video-stream-0"
    for frame_id in range(6):
        requests_to_send.append(
            {
                "label": f"frequency segmentation frame {frame_id}",
                "payload": {
                    "service": "deeplabv3_video_segmentation",
                    "task_type": "frequency",
                    "timeout_ms": 400,
                    "flow_id": frequency_flow,
                    "frame_id": frame_id,
                    "payload": {"source": frequency_flow, "frame": frame_id},
                },
            }
        )

    try:
        for item in requests_to_send:
            print(f"Submitting {item['label']}...")
            result = submit_request(client, item["payload"])
            print(json.dumps(result, ensure_ascii=False, indent=2))
    finally:
        client.close()


if __name__ == "__main__":
    main()