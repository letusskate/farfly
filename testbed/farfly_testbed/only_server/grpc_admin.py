from __future__ import annotations

import argparse
import json
from pathlib import Path

from farfly_client import SchedulerClient, ServerClient


BASE_DIR = Path(__file__).resolve().parent


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect FarFly gRPC services")
    subparsers = parser.add_subparsers(dest="target", required=True)

    scheduler_parser = subparsers.add_parser("scheduler", help="Query the scheduler gRPC service")
    scheduler_parser.add_argument("action", choices=["health", "metrics", "plan", "recompute", "reload"])
    scheduler_parser.add_argument(
        "--config",
        default=str(BASE_DIR / "local_thread_test" / "config_sender.json"),
        help="Path to a sender config file containing scheduler ip/port",
    )

    server_parser = subparsers.add_parser("server", help="Query a server gRPC service")
    server_parser.add_argument("action", choices=["health", "metrics"])
    server_parser.add_argument(
        "--config",
        required=True,
        help="Path to a specific server config file such as config_server0.json",
    )

    args = parser.parse_args()

    if args.target == "scheduler":
        client = SchedulerClient.from_config_file(args.config)
        try:
            if args.action == "health":
                status_code, body = client.health()
            elif args.action == "metrics":
                status_code, body = client.metrics()
            elif args.action == "plan":
                status_code, body = client.plan()
            elif args.action == "recompute":
                status_code, body = client.recompute_plan()
            else:
                status_code, body = client.reload_plan()
        finally:
            client.close()
    else:
        client = ServerClient.from_server_config_file(args.config)
        try:
            if args.action == "health":
                status_code, body = client.health()
            else:
                status_code, body = client.metrics()
        finally:
            client.close()

    print(json.dumps({"status_code": status_code, "body": body}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()