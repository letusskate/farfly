"""Launch a FarFly cluster from a trace_configs directory."""
from __future__ import annotations

import argparse
import sys
import threading
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from farfly_scheduler import start_scheduler
from farfly_server import start_server


def main() -> None:
    parser = argparse.ArgumentParser(description="Start FarFly cluster from trace configs")
    parser.add_argument("--config-dir", required=True, help="Directory containing trace configs")
    args = parser.parse_args()

    config_dir = Path(args.config_dir).resolve()

    server_configs = sorted(config_dir.glob("config_server*.json"))
    if not server_configs:
        print(f"No server configs found in {config_dir}")
        sys.exit(1)

    scheduler_config = config_dir / "trace_scheduler_config.json"
    if not scheduler_config.exists():
        print(f"No scheduler config found: {scheduler_config}")
        sys.exit(1)

    print(f"Starting {len(server_configs)} servers ...")
    for cfg in server_configs:
        t = threading.Thread(target=start_server, args=(str(cfg),), daemon=True)
        t.start()
        print(f"  {cfg.name} started")

    time.sleep(2.0)

    print(f"Starting scheduler: {scheduler_config.name}")
    start_scheduler(str(scheduler_config))


if __name__ == "__main__":
    main()
